package picoclaw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/developer3000S/zeptoclaw/internal/brain"
	"github.com/developer3000S/zeptoclaw/internal/config"
)

// OllamaAdapter executes task instructions on an Ollama-compatible backend
// chosen by the brain backend pool: a Free-Ollama-API gateway, a local Ollama
// or any OpenAI-compatible chat service the agent has verified itself. Unlike
// the stub it runs real inference; unlike the CLI/WS modes it needs no local
// PicoClaw installation.
//
// The adapter is a plain chat client — it has no tools, shell or network of
// its own, so the mesh constraints (allow_shell=false by default) hold
// trivially: the model receives the instruction text and returns text.
type OllamaAdapter struct {
	pool    *brain.BackendPool
	cfg     config.PicoClawConfig
	skills  []string
	timeout time.Duration
	log     *slog.Logger

	// proto remembers, per backend, which chat protocol answered last
	// ("ollama" or "openai"): the first task per backend pays one extra 404,
	// the rest go straight to the known dialect.
	mu    sync.Mutex
	proto map[string]string
}

// NewOllama builds the chat adapter over the brain backend pool. A nil pool
// makes the adapter permanently unavailable; the factory rejects that
// configuration instead of starting a node that cannot execute anything.
func NewOllama(pool *brain.BackendPool, cfg config.PicoClawConfig, skills []string, logger *slog.Logger) *OllamaAdapter {
	if logger == nil {
		logger = slog.Default()
	}
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	return &OllamaAdapter{
		pool:    pool,
		cfg:     cfg,
		skills:  append([]string(nil), skills...),
		timeout: timeout,
		log:     logger,
		proto:   map[string]string{},
	}
}

// Name implements Adapter.
func (a *OllamaAdapter) Name() string { return "ollama" }

// Model implements ModelReporter: the pinned model wins when the operator set
// it; otherwise the pool's own selection names what the node actually thinks
// with (the fallback happens in picoclaw.Describe → ModelOf).
func (a *OllamaAdapter) Model() string {
	if a.cfg.Model != "" {
		return a.cfg.Model
	}
	if a.pool != nil {
		_, model, _ := a.pool.SelectModel("")
		return model
	}
	return ""
}

// BaseURL implements BaseReporter: reports the currently selected backend's
// endpoint so the operator can see which host the node is actually calling.
// Empty when the pool has no usable backend yet.
func (a *OllamaAdapter) BaseURL() string {
	if a.pool != nil {
		if b, _, _ := a.pool.SelectModel(""); b != nil {
			return b.BaseURL
		}
	}
	return ""
}

// Healthy reports whether the brain pool currently holds at least one backend
// that answered the last probe with a model list.
func (a *OllamaAdapter) Healthy(_ context.Context) error {
	if a.pool == nil {
		return ErrUnavailable
	}
	if len(a.pool.Usable()) == 0 {
		return fmt.Errorf("%w: no usable brain backend yet", ErrUnavailable)
	}
	return nil
}

// Capabilities returns the configured skill list: chat execution serves any
// skill the node declares.
func (a *OllamaAdapter) Capabilities(context.Context) ([]string, error) {
	return append([]string(nil), a.skills...), nil
}

// Execute resolves the brain-selected backend, sends the instruction to its
// chat endpoint and returns the model's answer. The Ollama-native /api/chat is
// tried first, the OpenAI-compatible /v1/chat/completions second — the same
// order the pool uses to learn a backend's model list. Model choice follows
// the Request contract: the per-task model overrides the node's configured
// picoclaw.model, and when neither is set the pool picks by itself.
func (a *OllamaAdapter) Execute(ctx context.Context, req Request) (*Response, error) {
	if a.pool == nil {
		return nil, ErrUnavailable
	}
	preferred := req.Model
	if preferred == "" {
		preferred = a.cfg.Model
	}
	b, model, ok := a.pool.SelectModel(preferred)
	if !ok {
		return nil, fmt.Errorf("%w: no usable brain backend for model %q", ErrUnavailable, preferred)
	}
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}

	started := time.Now().UTC()
	text, answered, err := a.chat(ctx, b, model, req.Instruction)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%w: %v", ErrTimeout, err)
		}
		return nil, err
	}
	if text == "" {
		return nil, fmt.Errorf("picoclaw: backend %s returned an empty answer", b.ID)
	}

	var arts []Artifact
	if req.Workspace != "" {
		path := filepath.Join(req.Workspace, "result.txt")
		if err := os.MkdirAll(req.Workspace, 0o700); err == nil {
			if err := os.WriteFile(path, []byte(text), 0o600); err == nil {
				arts = append(arts, Artifact{Name: "result.txt", Path: path, Size: int64(len(text))})
			}
		}
	}
	return &Response{
		Text:       text,
		StartedAt:  started,
		FinishedAt: time.Now().UTC(),
		Artifacts:  arts,
		ExitCode:   0,
		Model:      answered,
	}, nil
}

// chat sends the instruction twice-willing: Ollama dialect first, OpenAI
// second. A backend that already answered in one dialect is asked in it again.
func (a *OllamaAdapter) chat(ctx context.Context, b *brain.Backend, model, instruction string) (string, string, error) {
	if proto := a.knownProto(b.ID); proto != "" {
		text, answered, err := a.callChat(ctx, b, model, instruction, proto)
		if err == nil {
			return text, answered, nil
		}
		a.log.Warn("brain_chat_failed", "backend", b.ID, "protocol", proto, "err", err.Error())
		// The remembered dialect stopped working; fall through and relearn.
	}
	text, answered, err := a.callChat(ctx, b, model, instruction, "ollama")
	if err == nil {
		a.rememberProto(b.ID, "ollama")
		return text, answered, nil
	}
	a.log.Warn("brain_chat_failed", "backend", b.ID, "protocol", "ollama", "err", err.Error())
	text, answered, err = a.callChat(ctx, b, model, instruction, "openai")
	if err == nil {
		a.rememberProto(b.ID, "openai")
		return text, answered, nil
	}
	return "", "", fmt.Errorf("brain backend %s (%s) rejected both chat protocols: %w", b.ID, b.BaseURL, err)
}

func (a *OllamaAdapter) callChat(ctx context.Context, b *brain.Backend, model, instruction, proto string) (string, string, error) {
	url, body, err := chatPayload(b.BaseURL, proto, model, instruction)
	if err != nil {
		return "", "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if b.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+b.APIKey)
	}
	client := &http.Client{Timeout: a.timeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode != http.StatusOK {
		// Only a 404 may flip the adapter to the other dialect; anything else
		// (401, 500, 503) is a real backend problem both dialects share.
		if resp.StatusCode == http.StatusNotFound {
			return "", "", fmt.Errorf("status 404")
		}
		return "", "", fmt.Errorf("status %d: %.200s", resp.StatusCode, string(raw))
	}
	switch proto {
	case "ollama":
		var doc struct {
			Model   string `json:"model"`
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			return "", "", err
		}
		return strings.TrimSpace(doc.Message.Content), orDefault(doc.Model, model), nil
	default:
		var doc struct {
			Model   string `json:"model"`
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			return "", "", err
		}
		if len(doc.Choices) == 0 {
			return "", "", errors.New("empty choices")
		}
		return strings.TrimSpace(doc.Choices[0].Message.Content), orDefault(doc.Model, model), nil
	}
}

// chatPayload builds the URL and the request body for one chat call.
func chatPayload(baseURL, proto, model, instruction string) (string, []byte, error) {
	switch proto {
	case "ollama":
		body, err := json.Marshal(map[string]any{
			"model": model,
			"messages": []map[string]string{
				{"role": "user", "content": instruction},
			},
			"stream": false,
		})
		return strings.TrimRight(baseURL, "/") + "/api/chat", body, err
	default:
		body, err := json.Marshal(map[string]any{
			"model": model,
			"messages": []map[string]string{
				{"role": "user", "content": instruction},
			},
		})
		return strings.TrimRight(baseURL, "/") + "/v1/chat/completions", body, err
	}
}

func (a *OllamaAdapter) knownProto(id string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.proto[id]
}

func (a *OllamaAdapter) rememberProto(id, proto string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.proto[id] = proto
}

// Close implements Adapter.
func (a *OllamaAdapter) Close() error { return nil }

var _ Adapter = (*OllamaAdapter)(nil)

// orDefault substitutes a fallback for an empty disclosed value: a backend
// that omits the model field in its answer did answer with what was asked.
func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
