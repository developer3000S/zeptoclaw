package brain

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// quietPool is a pool whose logs are discarded.
func quietPool() *BackendPool {
	return NewBackendPool("", nil, nil, slog.New(slog.DiscardHandler))
}

// ollamaServer stands in for a discovered Ollama endpoint: it answers /api/tags
// with a model list, which is exactly what the pool's probe asks for.
func ollamaServer(models ...string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tags" {
			http.NotFound(w, r)
			return
		}
		items := make([]map[string]string, 0, len(models))
		for _, m := range models {
			items = append(items, map[string]string{"name": m})
		}
		_ = jsonWrite(w, map[string]any{"models": items})
	}))
}

// AddVerified is the autonomous verification: the probe, and nothing else,
// decides whether a discovered endpoint may serve tasks. An endpoint that
// answers with a model list becomes verified.
func TestAddVerified_ProbeIsTheVerdict(t *testing.T) {
	srv := ollamaServer("qwen2.5:0.5b", "qwen2.5:7b")
	defer srv.Close()

	p := quietPool()
	b := p.AddVerified(context.Background(), Backend{
		ID: "cnd_abc", BaseURL: srv.URL, Kind: "endpoint", PromotedFrom: "cnd_abc",
	})
	if !b.Verified || !b.Reachable {
		t.Fatalf("probe of a answering endpoint: verified=%t reachable=%t err=%q",
			b.Verified, b.Reachable, b.Error)
	}
	if len(b.Models) != 2 {
		t.Fatalf("models = %v", b.Models)
	}
	if b.PromotedFrom != "cnd_abc" {
		t.Fatalf("promoted_from lost: %q", b.PromotedFrom)
	}
	if p.Count() != 1 {
		t.Fatalf("pool size = %d", p.Count())
	}
}

// A dead endpoint is recorded, not verified — the operator sees the failed
// promotion and a later pass may re-probe it.
func TestAddVerified_DeadEndpointIsRecordedNotVerified(t *testing.T) {
	srv := ollamaServer("qwen2.5:7b")
	srv.Close() // gone before the probe

	p := quietPool()
	b := p.AddVerified(context.Background(), Backend{ID: "dead", BaseURL: srv.URL, Kind: "endpoint"})
	if b.Verified || b.Reachable {
		t.Fatalf("dead endpoint verified=%t reachable=%t", b.Verified, b.Reachable)
	}
	if b.Error == "" {
		t.Fatalf("a dead endpoint must record why it failed")
	}
	if _, ok := p.Get("dead"); !ok {
		t.Fatalf("the failed promotion must be visible in the pool")
	}
}

// Re-promoting the same base URL refreshes the entry in place instead of
// adding a second one: the pool deduplicates by endpoint, so a candidate the
// search engines report repeatedly cannot flood it.
func TestAddVerified_DedupesByBaseURL(t *testing.T) {
	srv := ollamaServer("qwen2.5:7b")
	defer srv.Close()

	p := quietPool()
	p.AddVerified(context.Background(), Backend{ID: "first", BaseURL: srv.URL, Kind: "endpoint"})
	p.AddVerified(context.Background(), Backend{ID: "second", BaseURL: srv.URL + "/", Kind: "endpoint"})
	if p.Count() != 1 {
		t.Fatalf("duplicate base URL: pool size = %d", p.Count())
	}
}

// SelectModel prefers the operator's named model when a backend serves it, and
// otherwise falls back to the largest model of the fastest reachable backend.
func TestSelectModel_PrefersNamedThenLargest(t *testing.T) {
	fast := ollamaServer("qwen2.5:0.5b", "qwen2.5:7b")
	defer fast.Close()
	slow := ollamaServer("llama3.1:8b")
	defer slow.Close()

	p := quietPool()
	p.AddVerified(context.Background(), Backend{ID: "fast", BaseURL: fast.URL, Kind: "endpoint"})
	p.AddVerified(context.Background(), Backend{ID: "slow", BaseURL: slow.URL, Kind: "endpoint"})

	if b, m, ok := p.SelectModel("llama3.1:8b"); !ok || m != "llama3.1:8b" || b.ID != "slow" {
		t.Fatalf("named model not honoured: backend=%q model=%q ok=%t", b.ID, m, ok)
	}
	// No named match: the fastest backend's largest model wins (7b beats 0.5b).
	b, m, ok := p.SelectModel("nonexistent:1b")
	if !ok || m != "qwen2.5:7b" {
		t.Fatalf("fallback model = %q ok=%t", m, ok)
	}
	if b == nil || b.ID != "fast" {
		t.Fatalf("expected the fastest backend, got %v", b)
	}
}

// With no usable backend the pool says so, and the caller falls back instead of
// silently thinking with nothing.
func TestSelectModel_NothingUsable(t *testing.T) {
	p := quietPool()
	if _, _, ok := p.SelectModel(""); ok {
		t.Fatalf("empty pool must not select a model")
	}
	dead := ollamaServer("qwen2.5:7b")
	dead.Close()
	p.AddVerified(context.Background(), Backend{ID: "dead", BaseURL: dead.URL, Kind: "endpoint"})
	if _, _, ok := p.SelectModel(""); ok {
		t.Fatalf("a pool of unreachable backends must not select a model")
	}
}

// Usable reports only the backends that answered with models — the subset a
// brain model is chosen from, and the only endpoints traffic is routed to.
func TestUsable_OnlyAnsweredBackends(t *testing.T) {
	live := ollamaServer("qwen2.5:7b")
	defer live.Close()
	dead := ollamaServer("qwen2.5:7b")
	dead.Close()

	p := quietPool()
	p.AddVerified(context.Background(), Backend{ID: "live", BaseURL: live.URL, Kind: "endpoint"})
	p.AddVerified(context.Background(), Backend{ID: "dead", BaseURL: dead.URL, Kind: "endpoint"})

	usable := p.Usable()
	if len(usable) != 1 || usable[0].ID != "live" {
		t.Fatalf("usable = %+v", usable)
	}
	if usable[0].Verified != true {
		t.Fatalf("a usable backend must be verified")
	}
}

// An operator-declared local backend is probed with the same rules as a
// promoted one: verification does not depend on how the endpoint was added.
func TestLocalBackend_IsProbed(t *testing.T) {
	srv := ollamaServer("qwen2.5:7b")
	defer srv.Close()

	p := NewBackendPool(srv.URL, nil, nil, slog.New(slog.DiscardHandler))
	p.Probe(context.Background())
	backends := p.List()
	if len(backends) != 1 || backends[0].ID != "local" {
		t.Fatalf("local backend: %+v", backends)
	}
	if !backends[0].Verified {
		t.Fatalf("local backend must be verified by its own probe")
	}
}

// The OpenAI-compatible listing works as a fallback, so a generic gateway can
// serve as a backend even without an Ollama-native /api/tags.
func TestBackend_OpenAICompatibleFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			_ = jsonWrite(w, map[string]any{"data": []map[string]string{{"id": "gpt-oss:7b"}}})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	p := quietPool()
	b := p.AddVerified(context.Background(), Backend{ID: "gw", BaseURL: srv.URL, Kind: "endpoint"})
	if !b.Verified || len(b.Models) != 1 || b.Models[0] != "gpt-oss:7b" {
		t.Fatalf("openai-compatible backend: verified=%t models=%v", b.Verified, b.Models)
	}
}
