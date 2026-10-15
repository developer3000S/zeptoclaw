package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/developer3000S/zeptoclaw/internal/brain"
	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/node"
	"github.com/developer3000S/zeptoclaw/internal/security"
)

// ollamaFake stands in for a discovered Ollama endpoint: it answers /api/tags
// with a model list, which is what promotion probes.
func ollamaFake(t *testing.T, models ...string) *httptest.Server {
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
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"models": items})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// searchFake stands in for every search engine at once and reports the fake
// Ollama endpoint as an open Ollama host.
func searchFake(t *testing.T, ollama *httptest.Server) *httptest.Server {
	t.Helper()
	u, _ := url.Parse(ollama.URL)
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	doc := map[string]any{
		"matches": []any{
			map[string]any{
				"ip_str": u.Hostname(), "port": port, "transport": "tcp",
				"hostnames": []any{"gpu-box.local"},
				"location":  map[string]any{"country_code": "DE"},
				"asn":       "AS64500",
				"data":      "Ollama is running",
			},
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// brainServer wires a Server whose only live subsystem is the brain: a catalog
// pointed at the fake search engine and an empty backend pool. Nothing else of
// the node exists, which is what keeps these tests HTTP-level.
func brainServer(t *testing.T, ollama *httptest.Server) (*Server, *brain.Catalog, *brain.BackendPool) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cat := brain.NewCatalog(brain.CatalogConfig{
		Enabled: true, Sources: []string{"shodan"}, Query: "port:11434",
		Timeout: 10e9, MaxPerSource: 25,
	}, brain.CatalogKeys{Shodan: "test-key"}, logger)
	cat.WithEndpoints(brain.CatalogEndpoints{Shodan: searchFake(t, ollama).URL + "/shodan"})

	pool := brain.NewBackendPool("", nil, nil, logger)
	audit, err := security.OpenAudit("", logger)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	n := &node.Node{
		Cfg: config.Default(), Audit: audit,
		BrainCatalog: cat, BrainPool: pool,
	}
	return New(n, n.Cfg, nil, logger), cat, pool
}

func apiGet(t *testing.T, s *Server, path string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: %d: %s", path, rec.Code, rec.Body)
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("GET %s decode: %v", path, err)
	}
	return out
}

func apiPost(t *testing.T, s *Server, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// apiScan runs one catalog scan and returns the answer, so a test never has to
// remember that the scan endpoint is a POST.
func apiScan(t *testing.T, s *Server) map[string]any {
	t.Helper()
	code, res := apiPost(t, s, "/api/v1/brain/scan", `{}`)
	if code != http.StatusOK {
		t.Fatalf("scan: %d: %v", code, res)
	}
	return res
}

// The whole point of the brain: a search engine reports an endpoint, the agent
// probes it itself and, when it answers, marks it verified and routes traffic
// to it. No central confirmation is consulted anywhere in the loop.
func TestBrainAPI_ScanPromoteFlow(t *testing.T) {
	ollama := ollamaFake(t, "qwen2.5:0.5b", "qwen2.5:7b")
	s, cat, pool := brainServer(t, ollama)

	// The scan touches only the fake search engine; the candidate is inventory.
	scan := apiScan(t, s)
	if scan["status"] != "scanned" {
		t.Fatalf("scan status = %v", scan["status"])
	}
	report, _ := scan["report"].(map[string]any)
	if report["created"] != float64(1) {
		t.Fatalf("scan report = %+v", report)
	}

	list := apiGet(t, s, "/api/v1/brain/candidates")
	cands, _ := list["candidates"].([]any)
	if len(cands) != 1 {
		t.Fatalf("candidates = %v", cands)
	}
	id := cands[0].(map[string]any)["candidate_id"].(string)

	// Promotion is the autonomous verification: the node's own probe decides.
	code, res := apiPost(t, s, "/api/v1/candidates/promote",
		`{"candidate_id":"`+id+`","reason":"test"}`)
	if code != http.StatusOK {
		t.Fatalf("promote code = %d: %v", code, res)
	}
	if res["status"] != "promoted" {
		t.Fatalf("promote status = %v", res["status"])
	}
	b, _ := res["backend"].(map[string]any)
	if b["verified"] != true {
		t.Fatalf("the promoted backend must be verified: %v", b)
	}
	models, _ := b["models"].([]any)
	if len(models) != 2 {
		t.Fatalf("models = %v", models)
	}

	// The verified endpoint is now in the pool the node thinks with.
	backends := apiGet(t, s, "/api/v1/brain/backends")
	if backends["verified"] != float64(1) {
		t.Fatalf("verified backends = %v", backends["verified"])
	}
	// An api key never leaves the pool: Backend.APIKey is json:"-" on purpose.
	raw, _ := json.Marshal(pool.List())
	if strings.Contains(string(raw), "test-key") {
		t.Fatalf("an api key leaked into the backend view")
	}
	if cat.LastScan() == nil {
		t.Fatalf("the scan report must be kept for the admin API")
	}
}

// Promotion validates its input: an absent id is a client error and an unknown
// one is not-found, and neither touches the pool.
func TestBrainAPI_PromoteValidation(t *testing.T) {
	ollama := ollamaFake(t, "qwen2.5:7b")
	s, _, pool := brainServer(t, ollama)

	if code, _ := apiPost(t, s, "/api/v1/candidates/promote", `{}`); code != http.StatusBadRequest {
		t.Fatalf("empty candidate_id: code = %d", code)
	}
	if code, _ := apiPost(t, s, "/api/v1/candidates/promote",
		`{"candidate_id":"cnd_nope"}`); code != http.StatusNotFound {
		t.Fatalf("unknown candidate: code = %d", code)
	}
	if pool.Count() != 0 {
		t.Fatalf("a rejected promotion must not touch the pool")
	}
}

// A dead endpoint is promoted but reported unverified: the honest answer, since
// nothing in the mesh can confirm an endpoint that does not answer.
func TestBrainAPI_PromoteDeadEndpointReportsUnverified(t *testing.T) {
	dead := ollamaFake(t, "qwen2.5:7b")
	dead.Close()
	s, _, _ := brainServer(t, dead)

	apiScan(t, s)
	list := apiGet(t, s, "/api/v1/brain/candidates")
	id := list["candidates"].([]any)[0].(map[string]any)["candidate_id"].(string)

	code, res := apiPost(t, s, "/api/v1/candidates/promote", `{"candidate_id":"`+id+`"}`)
	if code != http.StatusOK {
		t.Fatalf("promote code = %d: %v", code, res)
	}
	b, _ := res["backend"].(map[string]any)
	if b["verified"] != false {
		t.Fatalf("a dead endpoint must not report verified: %v", b)
	}
	backends := apiGet(t, s, "/api/v1/brain/backends")
	if backends["verified"] != float64(0) {
		t.Fatalf("verified backends = %v", backends["verified"])
	}
}

// Sharing keys and relaying a promotion both require a friend the operator
// configured: an unknown friend is a client error, not a silent no-op.
func TestBrainAPI_UnknownFriendIsRejected(t *testing.T) {
	ollama := ollamaFake(t, "qwen2.5:7b")
	s, _, _ := brainServer(t, ollama)

	if code, _ := apiPost(t, s, "/api/v1/keys/share", `{"friend":"stranger"}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("share with unknown friend: code = %d", code)
	}
	if code, _ := apiPost(t, s, "/api/v1/friends/promote",
		`{"friend":"stranger","candidate_id":"cnd_1"}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("friend-promote with unknown friend: code = %d", code)
	}
	if code, _ := apiPost(t, s, "/api/v1/friends/promote", `{"friend":""}`); code != http.StatusBadRequest {
		t.Fatalf("friend-promote without friend: code = %d", code)
	}
}

// With the brain off, the brain endpoints report that honestly instead of
// nil-dereferencing: the operator sees a disabled subsystem, not a crash.
func TestBrainAPI_DisabledBrainReportsDisabled(t *testing.T) {
	s := New(&node.Node{Cfg: config.Default()}, config.Default(), nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	if cands := apiGet(t, s, "/api/v1/brain/candidates"); cands["enabled"] != false {
		t.Fatalf("candidates with no brain: %v", cands)
	}
	if backends := apiGet(t, s, "/api/v1/brain/backends"); backends["enabled"] != false {
		t.Fatalf("backends with no brain: %v", backends)
	}
	if code, _ := apiPost(t, s, "/api/v1/candidates/promote", `{"candidate_id":"x"}`); code != http.StatusServiceUnavailable {
		t.Fatalf("promote with no brain: code = %d", code)
	}
	if code, _ := apiPost(t, s, "/api/v1/brain/scan", `{}`); code != http.StatusServiceUnavailable {
		t.Fatalf("scan with no brain: code = %d", code)
	}
}
