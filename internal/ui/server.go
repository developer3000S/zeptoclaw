package ui

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

//go:embed all:static
var staticFS embed.FS

// Server is the dashboard backend: static UI + mesh snapshot + node action
// proxy. It is loopback-first like the agent admin API; the auth gate only
// activates when ZETOMESH_UI_TOKEN is set.
type Server struct {
	log     *slog.Logger
	agg     *Aggregator
	cfg     *Config
	token   string
	httpSrv *http.Server
	nodes   *nodeStore
}

type nodeStore struct {
	file    string
	mu      sync.Mutex
	entries []NodeEntry
}

// New wires the server. cfg is kept by pointer: node list edits mutate it.
func New(cfg *Config, agg *Aggregator, log *slog.Logger) *Server {
	return &Server{
		log:   log,
		agg:   agg,
		cfg:   cfg,
		token: cfg.Token,
		nodes: &nodeStore{file: cfg.NodesFile, entries: cfg.Nodes},
	}
}

// Serve runs until ctx ends.
func (s *Server) Serve(ctx context.Context) error {
	s.httpSrv = &http.Server{
		Addr:              s.cfg.Listen,
		Handler:           s.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      90 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- s.httpSrv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpSrv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// routes builds the full mux. Exported as a method so tests exercise the same
// middleware chain (notably the auth gate) as a live server.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.Handle("GET /static/", http.FileServerFS(staticFS))
	mux.HandleFunc("GET /", s.handleIndex)

	s.authed(mux, "GET /api/v1/mesh", s.handleMesh)
	s.authed(mux, "GET /api/v1/nodes", s.handleListNodes)
	s.authed(mux, "POST /api/v1/nodes", s.handleAddNode)
	s.authed(mux, "DELETE /api/v1/nodes/{name}", s.handleDeleteNode)
	s.authed(mux, "GET /api/v1/nodes/{name}/tasks", s.handleProxy)
	s.authed(mux, "POST /api/v1/nodes/{name}/tasks", s.handleProxy)
	s.authed(mux, "GET /api/v1/nodes/{name}/tasks/{id}", s.handleProxy)
	s.authed(mux, "POST /api/v1/nodes/{name}/tasks/{id}/cancel", s.handleProxy)
	s.authed(mux, "POST /api/v1/nodes/{name}/tasks/{id}/resubmit", s.handleProxy)
	s.authed(mux, "GET /api/v1/nodes/{name}/skills", s.handleProxy)
	s.authed(mux, "POST /api/v1/nodes/{name}/skills", s.handleProxy)
	s.authed(mux, "DELETE /api/v1/nodes/{name}/skills/{skill}", s.handleProxy)
	s.authed(mux, "POST /api/v1/nodes/{name}/skills/sync", s.handleProxy)
	s.authed(mux, "GET /api/v1/nodes/{name}/triggers", s.handleProxy)
	s.authed(mux, "POST /api/v1/nodes/{name}/triggers", s.handleProxy)
	s.authed(mux, "DELETE /api/v1/nodes/{name}/triggers/{id}", s.handleProxy)
	s.authed(mux, "GET /api/v1/nodes/{name}/config", s.handleProxy)
	s.authed(mux, "POST /api/v1/nodes/{name}/reload-config", s.handleProxy)
	s.authed(mux, "POST /api/v1/nodes/{name}/leave", s.handleProxy)
	return limitBody(mux)
}

// ---------- middleware ----------

func (s *Server) authed(mux *http.ServeMux, pattern string, h func(http.ResponseWriter, *http.Request)) {
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if !s.authorize(r) {
			writeErr(w, http.StatusUnauthorized, "missing or bad bearer token")
			return
		}
		h(w, r)
	})
}

func (s *Server) authorize(r *http.Request) bool {
	if s.token == "" {
		return true
	}
	const prefix = "Bearer "
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, prefix) {
		return false
	}
	got := strings.TrimSpace(auth[len(prefix):])
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) == 1
}

func limitBody(next http.Handler) http.Handler {
	return http.MaxBytesHandler(next, maxNodesBodyBytes)
}

// ---------- static ----------

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		// Unknown paths fall back to the SPA shell so hash-routing works,
		// but source maps are not invented.
		if strings.HasSuffix(r.URL.Path, ".map") {
			http.NotFound(w, r)
			return
		}
	}
	data, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "index missing")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

// ---------- mesh snapshot ----------

func (s *Server) handleMesh(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.agg.Snapshot())
}

// ---------- node list CRUD ----------

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	s.nodes.mu.Lock()
	defer s.nodes.mu.Unlock()
	out := make([]map[string]string, 0, len(s.nodes.entries))
	for _, e := range s.nodes.entries {
		out = append(out, map[string]string{"name": e.Name, "url": e.URL})
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(out), "nodes": out})
}

func (s *Server) handleAddNode(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name  string `json:"name"`
		URL   string `json:"url"`
		Token string `json:"token"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if strings.TrimSpace(in.Name) == "" || strings.TrimSpace(in.URL) == "" {
		writeErr(w, http.StatusBadRequest, "name and url are required")
		return
	}
	if err := s.nodes.add(NodeEntry{Name: in.Name, URL: in.URL, Token: in.Token}); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	s.reloadClients()
	writeJSON(w, http.StatusCreated, map[string]any{"name": in.Name, "url": in.URL})
}

func (s *Server) handleDeleteNode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.nodes.delete(name); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	s.reloadClients()
	writeJSON(w, http.StatusOK, map[string]any{"removed": true, "name": name})
}

// reloadClients rebuilds the aggregator's client set after a node list change
// so the next poll round covers the new membership.
func (s *Server) reloadClients() {
	s.nodes.mu.Lock()
	entries := append([]NodeEntry(nil), s.nodes.entries...)
	s.nodes.mu.Unlock()
	s.agg.SetClients(buildClients(entries, s.cfg))
}

// ---------- action proxy ----------

// handleProxy forwards the request to the named node, preserving the method,
// the rewritten path, the query string and the body.
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	node, ok := s.nodes.lookup(r.PathValue("name"))
	if !ok {
		writeErr(w, http.StatusNotFound, "no configured node with name "+r.PathValue("name"))
		return
	}
	target, err := nodePath(r.URL.Path)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxNodesBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	client := clientFor(node, s.cfg)
	req, err := http.NewRequestWithContext(r.Context(), r.Method, client.base+target, strings.NewReader(string(body)))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	if client.token != "" {
		req.Header.Set("Authorization", "Bearer "+client.token)
	}
	resp, err := client.http.Do(req)
	if err != nil {
		writeErr(w, http.StatusBadGateway, node.Name+": "+err.Error())
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(resp.StatusCode)
	// Node responses are bounded by the agent's own frame limit (8 MiB); a
	// limit here keeps a misbehaving node from streaming forever.
	_, _ = io.Copy(w, io.LimitReader(resp.Body, 8<<20))
}

// nodePrefix is the BFF namespace for per-node routes.
const nodePrefix = "/api/v1/nodes/"

// nodePath rewrites a BFF route to the agent's admin API path. The BFF mirrors
// the admin surface one level down under the node name:
//
//	/api/v1/nodes/zepto-0/tasks/abc/cancel → /api/v1/tasks/abc/cancel
//	/api/v1/nodes/zepto-0/reload-config    → /api/v1/admin/reload-config
//
// Deriving the target from the path itself keeps the proxy independent of the
// mux's populated Pattern field, so the handler stays testable in isolation.
func nodePath(p string) (string, error) {
	if !strings.HasPrefix(p, nodePrefix) {
		return "", fmt.Errorf("not a node route: %s", p)
	}
	rest := p[len(nodePrefix):]
	if i := strings.IndexByte(rest, '/'); i < 0 {
		return "", fmt.Errorf("node route is missing its action: %s", p)
	} else {
		rest = rest[i+1:]
	}
	switch {
	case rest == "reload-config" || rest == "leave" || strings.HasPrefix(rest, "reload-config/") || strings.HasPrefix(rest, "leave/"):
		return "/api/v1/admin/" + rest, nil
	default:
		return "/api/v1/" + rest, nil
	}
}

// ---------- helpers ----------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		slog.Default().Warn("ui_write", "err", err.Error())
	}
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}

// nodeStore guards the persistent node list. Tokens stay inside the backend;
// only name/url are ever served to the browser.
func (ns *nodeStore) lookup(name string) (NodeEntry, bool) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	for _, e := range ns.entries {
		if e.Name == name {
			return e, true
		}
	}
	return NodeEntry{}, false
}

func (ns *nodeStore) add(entry NodeEntry) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	for _, e := range ns.entries {
		if e.Name == entry.Name {
			return errors.New("a node with this name already exists")
		}
	}
	ns.entries = append(ns.entries, entry)
	return saveNodes(ns.file, ns.entries)
}

func (ns *nodeStore) delete(name string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	for i, e := range ns.entries {
		if e.Name == name {
			ns.entries = append(ns.entries[:i], ns.entries[i+1:]...)
			return saveNodes(ns.file, ns.entries)
		}
	}
	return errors.New("no stored node with name " + name)
}

// clientFor builds a client with the action timeout for proxy requests.
func clientFor(entry NodeEntry, cfg *Config) *NodeClient {
	if entry.Token == "" {
		entry.Token = cfg.APIToken
	}
	c := NewNodeClient(entry)
	c.http = &http.Client{Timeout: actionTimeout}
	return c
}

// buildClients materialises polling clients for a node list.
func buildClients(entries []NodeEntry, cfg *Config) []*NodeClient {
	out := make([]*NodeClient, 0, len(entries))
	for _, e := range entries {
		if e.Token == "" {
			e.Token = cfg.APIToken
		}
		out = append(out, NewNodeClient(e))
	}
	return out
}

// SetClients replaces the polled set (used after node list edits).
func (a *Aggregator) SetClients(clients []*NodeClient) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.clients = clients
}
