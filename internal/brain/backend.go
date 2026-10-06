package brain

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Backend is an LLM endpoint the agent may actually run tasks on. Unlike a
// Catalog candidate, a backend exists because it was either pointed at by the
// operator (local Ollama, an explicit brain.backends entry) or promoted after
// the agent itself verified it — the agent probes a discovered Ollama endpoint
// and, when the endpoint answers, accepts it autonomously (no central
// confirmation exists in a decentralized mesh).
type Backend struct {
	ID        string   `json:"id"`
	BaseURL   string   `json:"base_url"`
	Kind      string   `json:"kind"` // local | endpoint
	APIKey    string   `json:"-"`    // never serialized
	Models    []string `json:"models,omitempty"`
	LatencyMs int64    `json:"latency_ms,omitempty"`
	Reachable bool     `json:"reachable"`
	// Verified marks an endpoint the agent itself probed successfully
	// (answered with a model list). True means this node's own probe said the
	// endpoint speaks Ollama; no external confirmation exists in the mesh.
	Verified bool `json:"verified"`
	// PromotedFrom names the catalog candidate this backend was promoted from,
	// when it was ("" for operator-declared backends).
	PromotedFrom string    `json:"promoted_from,omitempty"`
	CheckedAt    time.Time `json:"checked_at,omitempty"`
	Error        string    `json:"error,omitempty"`
}

// BackendConfig is one operator-declared LLM endpoint. The API key is named by
// the env var in APIKeyEnv, so the credential itself never sits in config.
type BackendConfig struct {
	ID        string `yaml:"id"`
	BaseURL   string `yaml:"base_url"`
	APIKeyEnv string `yaml:"api_key_env"`
}

// BackendPool is the set of LLM endpoints the agent may think with. It probes
// each backend, learns the models it serves, and reports the healthy subset.
// The pool is the only source of models the agent will actually use: catalog
// candidates never enter it.
type BackendPool struct {
	backends []*Backend
	log      *slog.Logger
	mu       sync.Mutex
	last     map[string]*Backend // by ID, for reporting
}

// NewBackendPool builds the pool from the local Ollama URL (if any) plus the
// operator's explicit endpoints. API keys are resolved from the environment by
// the caller and passed in alongside each config entry.
func NewBackendPool(localURL string, endpoints []BackendConfig, keys map[string]string, log *slog.Logger) *BackendPool {
	p := &BackendPool{log: log, last: map[string]*Backend{}}
	if localURL != "" {
		p.backends = append(p.backends, &Backend{
			ID:      "local",
			BaseURL: strings.TrimRight(localURL, "/"),
			Kind:    "local",
		})
	}
	for _, ep := range endpoints {
		if ep.BaseURL == "" {
			continue
		}
		b := &Backend{
			ID:      orDefault(ep.ID, "endpoint"),
			BaseURL: strings.TrimRight(ep.BaseURL, "/"),
			Kind:    "endpoint",
			APIKey:  keys[ep.APIKeyEnv],
		}
		p.backends = append(p.backends, b)
	}
	return p
}

// AddVerified appends a promoted endpoint to the pool and probes it
// immediately. The probe IS the autonomous verification (FOA §4.4.4 is not
// applied — see the user directive): when the endpoint answers with a model
// list, the backend is stored Verified=true and its models/latency are
// recorded; when it does not, the backend is still stored (Reachable=false) so
// the operator sees the failed promotion and later passes may re-probe it.
// An endpoint already in the pool (same base URL) is updated in place.
func (p *BackendPool) AddVerified(ctx context.Context, b Backend) *Backend {
	b.BaseURL = strings.TrimRight(b.BaseURL, "/")
	p.mu.Lock()
	for _, ex := range p.backends {
		if ex.BaseURL == b.BaseURL {
			// Known endpoint: refresh identity fields, keep the pool entry.
			ex.Kind = b.Kind
			if b.APIKey != "" {
				ex.APIKey = b.APIKey
			}
			if b.PromotedFrom != "" {
				ex.PromotedFrom = b.PromotedFrom
			}
			p.mu.Unlock()
			p.ProbeOne(ctx, ex)
			return ex
		}
	}
	added := &b
	p.backends = append(p.backends, added)
	p.mu.Unlock()
	p.ProbeOne(ctx, added)
	return added
}

// ProbeOne probes a single backend and records the result — the pool-wide
// Probe, scoped to one entry, for freshly promoted endpoints.
func (p *BackendPool) ProbeOne(ctx context.Context, b *Backend) {
	models, latency, err := p.probeBackend(ctx, b)
	p.mu.Lock()
	defer p.mu.Unlock()
	b.CheckedAt = time.Now()
	if err != nil {
		b.Reachable = false
		b.Verified = false
		b.Error = err.Error()
		b.Models = nil
		p.last[b.ID] = b
		if p.log != nil {
			p.log.Debug("brain_backend_unreachable", "backend", b.ID, "url", b.BaseURL, "err", err.Error())
		}
		return
	}
	b.Reachable = true
	b.Verified = true
	b.Error = ""
	b.Models = models
	b.LatencyMs = latency
	p.last[b.ID] = b
}

// Get returns one backend by id.
func (p *BackendPool) Get(id string) (Backend, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, b := range p.backends {
		if b.ID == id {
			return *b, true
		}
	}
	return Backend{}, false
}

// Count returns the pool size.
func (p *BackendPool) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.backends)
}

// Probe reaches every backend and records which answered and what they serve.
// A backend that fails is reported, not removed: it may come back, and the
// operator wants to see it in the status either way. Probing is the only
// traffic the pool ever generates, and it only ever touches operator-declared
// endpoints.
func (p *BackendPool) Probe(ctx context.Context) {
	var wg sync.WaitGroup
	for _, b := range p.backends {
		wg.Add(1)
		go func(b *Backend) {
			defer wg.Done()
			models, latency, err := p.probeBackend(ctx, b)
			p.mu.Lock()
			defer p.mu.Unlock()
			b.CheckedAt = time.Now()
			if err != nil {
				b.Reachable = false
				b.Verified = false
				b.Error = err.Error()
				b.Models = nil
				p.last[b.ID] = b
				if p.log != nil {
					p.log.Debug("brain_backend_unreachable", "backend", b.ID, "url", b.BaseURL, "err", err.Error())
				}
				return
			}
			b.Reachable = true
			b.Verified = true
			b.Error = ""
			b.Models = models
			b.LatencyMs = latency
			p.last[b.ID] = b
		}(b)
	}
	wg.Wait()
}

// probeBackend asks an endpoint for its model list. Both shapes are supported:
// an Ollama-native /api/tags document and an OpenAI-compatible /v1/models one.
func (p *BackendPool) probeBackend(ctx context.Context, b *Backend) ([]string, int64, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	start := time.Now()
	models, err := p.fetchModels(ctx, client, b)
	if err != nil {
		return nil, 0, err
	}
	return models, time.Since(start).Milliseconds(), nil
}

func (p *BackendPool) fetchModels(ctx context.Context, client *http.Client, b *Backend) ([]string, error) {
	// Try the Ollama-native tags endpoint first; fall back to the
	// OpenAI-compatible listing so a generic gateway works as a backend too.
	if models, err := p.fetchOllamaTags(ctx, client, b); err == nil && len(models) > 0 {
		return models, nil
	}
	return p.fetchOpenAIModels(ctx, client, b)
}

func (p *BackendPool) fetchOllamaTags(ctx context.Context, client *http.Client, b *Backend) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.BaseURL+"/api/tags", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(p.withAuth(req, b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var doc struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(doc.Models))
	for _, m := range doc.Models {
		if m.Name != "" {
			out = append(out, m.Name)
		}
	}
	return out, nil
}

func (p *BackendPool) fetchOpenAIModels(ctx context.Context, client *http.Client, b *Backend) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.BaseURL+"/v1/models", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(p.withAuth(req, b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var doc struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(doc.Data))
	for _, m := range doc.Data {
		if m.ID != "" {
			out = append(out, m.ID)
		}
	}
	return out, nil
}

func (p *BackendPool) withAuth(req *http.Request, b *Backend) *http.Request {
	if b.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+b.APIKey)
	}
	return req
}

// List returns a snapshot of every backend and its last probe result, for the
// admin API and for brain selection.
func (p *BackendPool) List() []Backend {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Backend, 0, len(p.last))
	for _, b := range p.last {
		out = append(out, *b)
	}
	return out
}

// Usable returns the backends that answered the last probe and the models they
// serve. This is the pool a brain model is chosen from.
func (p *BackendPool) Usable() []Backend {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []Backend
	for _, b := range p.last {
		if b.Reachable && len(b.Models) > 0 {
			out = append(out, *b)
		}
	}
	return out
}

// SelectModel picks the model the agent will think with, preferring an explicit
// operator choice, then the first model of the fastest reachable backend. It
// returns false when no backend is usable — the caller falls back to the stub
// executor rather than silently thinking with nothing.
func (p *BackendPool) SelectModel(preferred string) (backend *Backend, model string, ok bool) {
	usable := p.Usable()
	if len(usable) == 0 {
		return nil, "", false
	}
	if preferred != "" {
		for i := range usable {
			for _, m := range usable[i].Models {
				if m == preferred {
					return &usable[i], m, true
				}
			}
		}
	}
	// Fastest reachable backend wins; its largest model is a reasonable default
	// because the operator's backends are trusted to advertise what they have.
	best := 0
	for i := range usable {
		if usable[i].LatencyMs < usable[best].LatencyMs {
			best = i
		}
	}
	b := usable[best]
	if len(b.Models) == 0 {
		return nil, "", false
	}
	return &b, biggestModel(b.Models), true
}

// biggestModel prefers a fuller parameter size when a backend names several
// variants of one family (qwen2.5:0.5b vs qwen2.5:1.5b). Size is best-effort:
// models it cannot parse sort last, so an unparsable name never displaces a
// known-good one.
func biggestModel(models []string) string {
	best := models[0]
	bestScore := modelSizeScore(models[0])
	for _, m := range models[1:] {
		if s := modelSizeScore(m); s > bestScore {
			best, bestScore = m, s
		}
	}
	return best
}

// modelSizeScore extracts a coarse parameter size from a model tag so a node
// picks the strongest local brain available. Unknown sizes score 0, which still
// beats nothing: a usable unknown model is better than no brain at all.
func modelSizeScore(name string) int64 {
	low := strings.ToLower(name)
	scale := int64(1)
	switch {
	case strings.Contains(low, "70b"), strings.Contains(low, "8x7b"), strings.Contains(low, "405b"):
		scale = 70000
	case strings.Contains(low, "32b"), strings.Contains(low, "34b"), strings.Contains(low, "35b"):
		scale = 32000
	case strings.Contains(low, "13b"), strings.Contains(low, "14b"):
		scale = 13000
	case strings.Contains(low, "8b"), strings.Contains(low, "9b"):
		scale = 8000
	case strings.Contains(low, "7b"):
		scale = 7000
	case strings.Contains(low, "3b"):
		scale = 3000
	case strings.Contains(low, "1.5b"):
		scale = 1500
	case strings.Contains(low, "1b"):
		scale = 1000
	case strings.Contains(low, "0.5b"):
		scale = 500
	}
	// A quantization suffix lowers the effective size modestly.
	if strings.Contains(low, "q4") || strings.Contains(low, "q5") {
		scale = scale * 8 / 10
	}
	return scale
}
