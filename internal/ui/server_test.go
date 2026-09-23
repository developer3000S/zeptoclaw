package ui

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// recordingNode is a stub agent node that records what the BFF proxy sent.
type recordingNode struct {
	t          *testing.T
	token      string
	gotMethod  string
	gotPath    string
	gotBody    string
	gotAuth    string
	statusCode int
	reply      string
}

func (n *recordingNode) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.gotMethod = r.Method
		n.gotPath = r.URL.Path
		n.gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		n.gotBody = string(body)
		code := n.statusCode
		if code == 0 {
			code = http.StatusOK
		}
		reply := n.reply
		if reply == "" {
			reply = `{"ok":true}`
		}
		if n.token != "" && n.gotAuth != "Bearer "+n.token {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write([]byte(reply))
	})
}

// newTestServer wires a Server against stub nodes. Each stub runs on its own
// httptest listener, exactly like the agent admin API would.
func newTestServer(t *testing.T, entries []NodeEntry, stubs map[string]*recordingNode, uiToken string) (*Server, *Aggregator) {
	t.Helper()
	cfg := &Config{
		Listen:       "127.0.0.1:0",
		NodesFile:    t.TempDir() + "/nodes.json",
		Token:        uiToken,
		PollInterval: time.Hour, // the aggregator is driven manually in tests
		Nodes:        entries,
	}
	agg := &Aggregator{log: testLogger(t), interval: time.Hour}
	srv := New(cfg, agg, testLogger(t))
	srv.nodes.entries = entries
	for name, stub := range stubs {
		ts := httptest.NewServer(stub.handler())
		t.Cleanup(ts.Close)
		// Point the entry at the live stub address.
		for i := range srv.nodes.entries {
			if srv.nodes.entries[i].Name == name {
				srv.nodes.entries[i].URL = ts.URL
			}
		}
	}
	agg.SetClients(buildClients(srv.nodes.entries, cfg))
	return srv, agg
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestProxyForwardsMethodPathBodyAndBearer(t *testing.T) {
	stub := &recordingNode{t: t, token: "node-secret"}
	srv, _ := newTestServer(t, []NodeEntry{{Name: "zepto-0", URL: "http://stub", Token: "node-secret"}}, map[string]*recordingNode{"zepto-0": stub}, "")

	body := `{"instruction":"hello","required_skills":["general"]}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/nodes/zepto-0/tasks", strings.NewReader(body))
	req.SetPathValue("name", "zepto-0")
	srv.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if stub.gotMethod != http.MethodPost || stub.gotPath != "/api/v1/tasks" {
		t.Errorf("forwarded %s %s, want POST /api/v1/tasks", stub.gotMethod, stub.gotPath)
	}
	if stub.gotBody != body {
		t.Errorf("forwarded body = %q, want %q", stub.gotBody, body)
	}
	if stub.gotAuth != "Bearer node-secret" {
		t.Errorf("forwarded auth = %q", stub.gotAuth)
	}
}

func TestProxySubstitutesPathValues(t *testing.T) {
	stub := &recordingNode{t: t}
	srv, _ := newTestServer(t, []NodeEntry{{Name: "zepto-0", URL: "http://stub"}}, map[string]*recordingNode{"zepto-0": stub}, "")

	for _, tc := range []struct {
		route, method, wantPath string
		pathValues              map[string]string
	}{
		{"/api/v1/nodes/zepto-0/tasks/abc-123", http.MethodGet, "/api/v1/tasks/abc-123", nil},
		{"/api/v1/nodes/zepto-0/tasks/abc-123/cancel", http.MethodPost, "/api/v1/tasks/abc-123/cancel", nil},
		{"/api/v1/nodes/zepto-0/skills/general", http.MethodDelete, "/api/v1/skills/general", nil},
		{"/api/v1/nodes/zepto-0/skills/sync", http.MethodPost, "/api/v1/skills/sync", nil},
		{"/api/v1/nodes/zepto-0/reload-config", http.MethodPost, "/api/v1/admin/reload-config", nil},
		{"/api/v1/nodes/zepto-0/leave", http.MethodPost, "/api/v1/admin/leave", nil},
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(tc.method, tc.route, nil)
		req.SetPathValue("name", "zepto-0")
		srv.handleProxy(rec, req)
		if stub.gotPath != tc.wantPath {
			t.Errorf("%s: forwarded path = %s, want %s", tc.route, stub.gotPath, tc.wantPath)
		}
	}
}

func TestProxyUnknownNodeIs404(t *testing.T) {
	srv, _ := newTestServer(t, []NodeEntry{{Name: "zepto-0", URL: "http://stub"}}, map[string]*recordingNode{"zepto-0": {t: t}}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/nodes/nope/tasks", nil)
	req.SetPathValue("name", "nope")
	srv.handleProxy(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestProxyPreservesNodeErrorStatus(t *testing.T) {
	stub := &recordingNode{t: t, statusCode: http.StatusUnprocessableEntity, reply: `{"error":"instruction is required"}`}
	srv, _ := newTestServer(t, []NodeEntry{{Name: "zepto-0", URL: "http://stub"}}, map[string]*recordingNode{"zepto-0": stub}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/nodes/zepto-0/tasks", strings.NewReader(`{}`))
	req.SetPathValue("name", "zepto-0")
	srv.handleProxy(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 passthrough", rec.Code)
	}
	if got := rec.Body.String(); !strings.Contains(got, "instruction is required") {
		t.Errorf("body = %q, want the node error text", got)
	}
}

func TestMeshEndpointServesSnapshot(t *testing.T) {
	a, _ := twoNodeMesh()
	polls := []NodePoll{pollFromStub(a, "zepto-0")}
	agg := &Aggregator{log: testLogger(t), interval: 5 * time.Second}
	agg.snapshot = buildSnapshot(polls, 5000, 1700000000)
	srv := &Server{log: testLogger(t), agg: agg, cfg: &Config{}, nodes: &nodeStore{entries: nil}}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/mesh", nil)
	srv.handleMesh(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"kind":"managed"`) {
		t.Errorf("mesh payload missing managed vertices: %s", rec.Body.String())
	}
}

func TestNodeCRUDPersistsAndHidesTokens(t *testing.T) {
	srv, _ := newTestServer(t, nil, nil, "")

	// Add a node carrying a token.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/nodes",
		strings.NewReader(`{"name":"zepto-0","url":"http://zepto-0:8081","token":"sec"}`))
	srv.handleAddNode(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("add status = %d: %s", rec.Code, rec.Body.String())
	}

	// Listing must not leak the token.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/nodes", nil)
	srv.handleListNodes(rec, req)
	if strings.Contains(rec.Body.String(), "sec") {
		t.Errorf("node list leaked the token: %s", rec.Body.String())
	}

	// The persisted file keeps it, so the backend can still authenticate.
	got, err := loadNodes(srv.nodes.file)
	if err != nil {
		t.Fatalf("loadNodes: %v", err)
	}
	if len(got) != 1 || got[0].Token != "sec" {
		t.Errorf("persisted = %+v, want the token kept on disk", got)
	}

	// Duplicate name is refused.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/nodes",
		strings.NewReader(`{"name":"zepto-0","url":"http://other:8081"}`))
	srv.handleAddNode(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("duplicate add status = %d, want 422", rec.Code)
	}

	// Delete removes it from memory and from disk.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/nodes/zepto-0", nil)
	req.SetPathValue("name", "zepto-0")
	srv.handleDeleteNode(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status = %d", rec.Code)
	}
	got, err = loadNodes(srv.nodes.file)
	if err != nil {
		t.Fatalf("loadNodes after delete: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("nodes file still has %d entries after delete", len(got))
	}
}

func TestAuthGateRejectsAndAdmits(t *testing.T) {
	srv, _ := newTestServer(t, []NodeEntry{{Name: "zepto-0", URL: "http://stub"}}, map[string]*recordingNode{"zepto-0": {t: t}}, "ui-secret")
	router := srv.routes()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/mesh", nil)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("without token status = %d, want 401", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/mesh", nil)
	req.Header.Set("Authorization", "Bearer ui-secret")
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("with token status = %d, want 200", rec.Code)
	}
}

func TestServeFullWiringServesStaticAndMesh(t *testing.T) {
	stub := &recordingNode{t: t}
	srv, agg := newTestServer(t, []NodeEntry{{Name: "zepto-0", URL: "http://stub"}}, map[string]*recordingNode{"zepto-0": stub}, "")
	// Prime the aggregator's cached snapshot through the real poller.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	agg.pollOnce(ctx)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close() // free the port: Serve binds it itself
	srv.cfg.Listen = addr
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(context.Background()) }()
	defer func() {
		if srv.httpSrv != nil {
			_ = srv.httpSrv.Shutdown(context.Background())
		}
	}()

	base := "http://" + addr
	if err := waitFor(base + "/healthz"); err != nil {
		t.Fatalf("healthz: %v", err)
	}
	res, err := http.Get(base + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if !strings.Contains(string(body), "ZeptoClaw") {
		t.Errorf("index served %q, want the ZeptoClaw shell", truncate(string(body), 80))
	}
	res, err = http.Get(base + "/api/v1/mesh")
	if err != nil {
		t.Fatalf("GET /api/v1/mesh: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("mesh status = %d", res.StatusCode)
	}
	select {
	case err := <-serveErr:
		t.Fatalf("Serve returned early: %v", err)
	default:
	}
}

func TestIndexFallsBackForHashRoutes(t *testing.T) {
	srv, _ := newTestServer(t, nil, nil, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	srv.handleIndex(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the SPA shell", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("content type = %q", ct)
	}
}

// ---------- helpers ----------

func waitFor(url string) error {
	for i := 0; i < 50; i++ {
		res, err := http.Get(url)
		if err == nil {
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return context.DeadlineExceeded
}
