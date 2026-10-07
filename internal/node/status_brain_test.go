//go:build integration

package node

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/developer3000S/zeptoclaw/internal/config"
)

// brainOllama stands in for the node's own Ollama: it answers /api/tags with a
// model list, which is what the pool's probe reads to mark a backend usable.
func brainOllama(t *testing.T, models ...string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tags" {
			http.NotFound(w, r)
			return
		}
		items := make([]map[string]string, 0, len(models))
		for _, m := range models {
			items = append(items, map[string]string{"name": m})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"models": items})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// brainNode starts one mesh node whose brain points at a fake local Ollama.
func brainNode(t *testing.T, ollama *httptest.Server) *testNode {
	t.Helper()
	m := newManualMesh(t)
	return m.node("brain-0", []string{"general"}, func(cfg *config.Config) {
		cfg.Brain.Enabled = true
		cfg.Brain.LocalURL = ollama.URL
		// Probe often so the test does not wait on the default interval; the
		// catalog stays off — no internet scan is wanted here.
		cfg.Brain.ProbeInterval = config.Duration(50 * time.Millisecond)
		cfg.Brain.Catalog.Enabled = false
	})
}

// Status reports the model and the BASE_URL of the backend the brain selected:
// this is what the agent card shows as "модель ИИ" and "BASE_URL".
func TestStatus_ReportsBrainSelection(t *testing.T) {
	ollama := brainOllama(t, "qwen2.5:0.5b", "qwen2.5:7b")
	tn := brainNode(t, ollama)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tn.StartBrain(ctx)

	// The pool probes asynchronously; wait until the local backend is usable.
	deadline := time.Now().Add(30 * time.Second)
	for {
		if len(tn.BrainPool.Usable()) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("brain pool never became usable: %+v", tn.BrainPool.List())
		}
		time.Sleep(25 * time.Millisecond)
	}

	st := tn.Status()
	if !st.Brain.Enabled {
		t.Fatalf("Brain.Enabled = false, want true")
	}
	if st.Brain.Backends < 1 || st.Brain.Usable < 1 {
		t.Fatalf("Brain counts = %d/%d, want at least 1 usable backend", st.Brain.Usable, st.Brain.Backends)
	}
	if st.Brain.Model != "qwen2.5:7b" {
		t.Fatalf("Brain.Model = %q, want %q (largest model of the local backend)", st.Brain.Model, "qwen2.5:7b")
	}
	if st.Brain.BaseURL == "" {
		t.Fatalf("Brain.BaseURL = empty, want the local Ollama URL")
	}
	if st.Brain.BackendID == "" {
		t.Fatalf("Brain.BackendID = empty, want the backend id")
	}
	// Without an operator-pinned picoclaw.model the adapter reports no model of
	// its own — the brain is what the node thinks with, so the adapter row
	// staying empty is exactly the honest answer.
	if st.Adapter.Model != "" {
		t.Fatalf("Adapter.Model = %q, want empty (picoclaw.model is not pinned)", st.Adapter.Model)
	}
}

// With the brain off, the status says so instead of reporting a phantom model.
func TestStatus_BrainDisabled(t *testing.T) {
	m := newManualMesh(t)
	tn := m.node("nobrain-0", []string{"general"}, func(cfg *config.Config) {
		cfg.Brain.Enabled = false
	})
	st := tn.Status()
	if st.Brain.Enabled {
		t.Fatalf("Brain.Enabled = true, want false")
	}
	if st.Brain.Model != "" || st.Brain.BaseURL != "" {
		t.Fatalf("Brain fields leaked when disabled: model=%q base_url=%q", st.Brain.Model, st.Brain.BaseURL)
	}
}
