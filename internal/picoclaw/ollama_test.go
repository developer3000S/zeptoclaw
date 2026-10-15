package picoclaw

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/developer3000S/zeptoclaw/internal/brain"
	"github.com/developer3000S/zeptoclaw/internal/config"
)

// fakeGateway mimics an Ollama-compatible gateway: /api/tags for the pool
// probe, /api/chat in the Ollama dialect and /v1/chat/completions in the
// OpenAI dialect. Counters expose which endpoint the adapter actually used.
type fakeGateway struct {
	srv       *httptest.Server
	tags      int
	ollama    int
	openai    int
	ollama404 bool
	mu        sync.Mutex
	lastBody  map[string]any
}

func newFakeGateway(t *testing.T, models ...string) *fakeGateway {
	t.Helper()
	fg := &fakeGateway{lastBody: map[string]any{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/tags", func(w http.ResponseWriter, r *http.Request) {
		fg.mu.Lock()
		fg.tags++
		fg.mu.Unlock()
		items := make([]map[string]string, 0, len(models))
		for _, m := range models {
			items = append(items, map[string]string{"name": m})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"models": items})
	})
	mux.HandleFunc("POST /api/chat", func(w http.ResponseWriter, r *http.Request) {
		fg.mu.Lock()
		fg.ollama++
		if fg.ollama404 {
			fg.mu.Unlock()
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		fg.lastBody = body
		fg.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model":   body["model"],
			"message": map[string]string{"role": "assistant", "content": "оллама-ответ: " + str(body["messages"])},
			"done":    true,
		})
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		fg.mu.Lock()
		fg.openai++
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		fg.lastBody = body
		fg.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": body["model"],
			"choices": []map[string]any{
				{"message": map[string]string{"role": "assistant", "content": "openai-ответ: " + str(body["messages"])}},
			},
		})
	})
	fg.srv = httptest.NewServer(mux)
	t.Cleanup(fg.srv.Close)
	return fg
}

// str flattens the decoded request's "messages" array to the content of its
// first user message, for the canned replies and assertions.
func str(v any) string {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return ""
	}
	m, ok := v.([]any)[0].(map[string]any)
	if !ok {
		return ""
	}
	c, _ := m["content"].(string)
	return c
}

func (fg *fakeGateway) counts() (ollama, openai int) {
	fg.mu.Lock()
	defer fg.mu.Unlock()
	return fg.ollama, fg.openai
}

func (fg *fakeGateway) bodyModel() string {
	fg.mu.Lock()
	defer fg.mu.Unlock()
	m, _ := fg.lastBody["model"].(string)
	return m
}

// ollamaPool returns a probed pool with one usable backend: the fake gateway.
func ollamaPool(t *testing.T, fg *fakeGateway) *brain.BackendPool {
	t.Helper()
	pool := brain.NewBackendPool("", []brain.BackendConfig{
		{ID: "gateway", BaseURL: fg.srv.URL},
	}, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool.Probe(ctx)
	if len(pool.Usable()) == 0 {
		t.Fatalf("fake gateway did not become usable")
	}
	return pool
}

func ollamaCfg() config.PicoClawConfig {
	return config.PicoClawConfig{TimeoutSeconds: 30}
}

// The happy path: the instruction reaches the backend's /api/chat in the
// Ollama dialect, the model comes from the pool selection (the biggest of the
// listed ones), and the answer plus its artifact come back.
func TestOllamaAdapter_ExecuteOllamaDialect(t *testing.T) {
	fg := newFakeGateway(t, "qwen2.5:0.5b", "qwen2.5:7b")
	pool := ollamaPool(t, fg)
	a := NewOllama(pool, ollamaCfg(), []string{"general"}, nil)

	ws := t.TempDir()
	resp, err := a.Execute(context.Background(), Request{
		TaskID:      "t1",
		Instruction: "сложи два и два",
		Workspace:   ws,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.Text != "оллама-ответ: сложи два и два" {
		t.Fatalf("Text = %q", resp.Text)
	}
	if resp.Model != "qwen2.5:7b" {
		t.Fatalf("Model = %q, want the biggest listed model", resp.Model)
	}
	if bodyModel := fg.bodyModel(); bodyModel != "qwen2.5:7b" {
		t.Fatalf("chat request model = %q", bodyModel)
	}
	oll, openai := fg.counts()
	if oll != 1 || openai != 0 {
		t.Fatalf("dialect calls = ollama:%d openai:%d, want 1/0", oll, openai)
	}
	if len(resp.Artifacts) != 1 || resp.Artifacts[0].Name != "result.txt" {
		t.Fatalf("artifacts = %+v, want result.txt in the workspace", resp.Artifacts)
	}
}

// A backend that serves only the OpenAI dialect is still usable: the adapter
// falls back on the 404 and remembers the dialect for later calls.
func TestOllamaAdapter_FallsBackToOpenAIDialect(t *testing.T) {
	fg := newFakeGateway(t, "llama3.2:3b")
	fg.mu.Lock()
	fg.ollama404 = true
	fg.mu.Unlock()
	pool := ollamaPool(t, fg)
	a := NewOllama(pool, ollamaCfg(), nil, nil)

	for i := 0; i < 2; i++ {
		resp, err := a.Execute(context.Background(), Request{
			TaskID:      "t-fallback",
			Instruction: "привет",
			Workspace:   filepath.Join(t.TempDir(), "w"),
		})
		if err != nil {
			t.Fatalf("Execute #%d: %v", i, err)
		}
		if resp.Text != "openai-ответ: привет" {
			t.Fatalf("Text #%d = %q", i, resp.Text)
		}
	}
	oll, openai := fg.counts()
	if oll != 1 || openai != 2 {
		t.Fatalf("dialect calls = ollama:%d openai:%d, want one 404 probe then only openai", oll, openai)
	}
}

// An explicitly requested model is honoured only where a usable backend lists
// it; an unknown one falls back to the pool's own selection.
func TestOllamaAdapter_PrefersRequestedModel(t *testing.T) {
	fg := newFakeGateway(t, "qwen2.5:0.5b", "qwen2.5:7b")
	pool := ollamaPool(t, fg)
	a := NewOllama(pool, ollamaCfg(), nil, nil)

	resp, err := a.Execute(context.Background(), Request{
		TaskID:      "t-pref",
		Instruction: "x",
		Model:       "qwen2.5:0.5b",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.Model != "qwen2.5:0.5b" {
		t.Fatalf("Model = %q, want the explicitly requested one", resp.Model)
	}
}

// The operator's pinned picoclaw.model is the executable choice in ollama
// mode, not just the status-card label (ТЗ 10.3): without a per-task override
// the node must think with the pinned model even when the pool would have
// picked a bigger one.
func TestOllamaAdapter_HonoursPinnedModel(t *testing.T) {
	fg := newFakeGateway(t, "qwen2.5:0.5b", "qwen2.5:7b")
	pool := ollamaPool(t, fg)
	cfg := ollamaCfg()
	cfg.Model = "qwen2.5:0.5b"
	a := NewOllama(pool, cfg, nil, nil)

	resp, err := a.Execute(context.Background(), Request{
		TaskID:      "t-pin",
		Instruction: "x",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.Model != "qwen2.5:0.5b" {
		t.Fatalf("Model = %q, want the pinned qwen2.5:0.5b", resp.Model)
	}
	if bodyModel := fg.bodyModel(); bodyModel != "qwen2.5:0.5b" {
		t.Fatalf("chat request model = %q, want the pinned qwen2.5:0.5b", bodyModel)
	}
}

// The per-task model keeps precedence over the configured one.
func TestOllamaAdapter_TaskModelOverridesPinned(t *testing.T) {
	fg := newFakeGateway(t, "qwen2.5:0.5b", "qwen2.5:7b")
	pool := ollamaPool(t, fg)
	cfg := ollamaCfg()
	cfg.Model = "qwen2.5:0.5b"
	a := NewOllama(pool, cfg, nil, nil)

	_, err := a.Execute(context.Background(), Request{
		TaskID:      "t-override",
		Instruction: "x",
		Model:       "qwen2.5:7b",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if bodyModel := fg.bodyModel(); bodyModel != "qwen2.5:7b" {
		t.Fatalf("chat request model = %q, want the per-task qwen2.5:7b", bodyModel)
	}
}

// Nothing usable in the pool: the adapter refuses instead of echoing.
func TestOllamaAdapter_NoBackendIsUnavailable(t *testing.T) {
	pool := brain.NewBackendPool("", nil, nil, nil)
	a := NewOllama(pool, ollamaCfg(), nil, nil)

	if err := a.Healthy(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Healthy = %v, want ErrUnavailable", err)
	}
	_, err := a.Execute(context.Background(), Request{TaskID: "t-none", Instruction: "x"})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Execute err = %v, want ErrUnavailable", err)
	}
}

// The factory wires mode "ollama" to the pool and refuses a nil one.
func TestFactory_OllamaMode(t *testing.T) {
	fg := newFakeGateway(t, "qwen2.5:7b")
	pool := ollamaPool(t, fg)
	cfg := config.Default()
	cfg.PicoClaw.Mode = "ollama"

	a, err := New(cfg, nil, pool)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a.Name() != "ollama" {
		t.Fatalf("adapter name = %q", a.Name())
	}
	if _, err := New(cfg, nil, nil); err == nil {
		t.Fatalf("New with nil pool must fail in ollama mode")
	}
}
