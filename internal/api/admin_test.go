package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/metrics"
	"github.com/developer3000S/zeptoclaw/internal/node"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/triggers"
)

// rbacServer builds a Server the way the sibling tests do: a minimal node
// whose only live collaborators are the audit journal (backed by a temp file
// so its events are assertable) and the trigger scheduler. Handlers under test
// touch nothing else of the node.
func rbacServer(t *testing.T, cfg *config.Config, logger *slog.Logger) (*Server, string) {
	t.Helper()
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	audit, err := security.OpenAudit(auditPath, logger)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	t.Cleanup(func() { _ = audit.Close() })
	sched, err := triggers.New(triggers.Options{
		Store:  newMetaStore(),
		Submit: func(context.Context, triggers.Job) (string, error) { return "task-test", nil },
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	n := &node.Node{Cfg: cfg, Audit: audit, Scheduler: sched}
	return New(n, cfg, nil, logger), auditPath
}

// call performs one request against the server, optionally carrying a bearer
// token, and returns the recorder.
func call(t *testing.T, s *Server, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// readAudit parses the JSONL journal the test node wrote.
func readAudit(t *testing.T, path string) []security.AuditEvent {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	var out []security.AuditEvent
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var ev security.AuditEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("audit line: %v", err)
		}
		out = append(out, ev)
	}
	return out
}

// assertAuditEvent finds one event and checks the identity annotations the
// RBAC middleware is supposed to record.
func assertAuditEvent(t *testing.T, events []security.AuditEvent, event, actor, role string) {
	t.Helper()
	for _, ev := range events {
		if ev.Event != event {
			continue
		}
		if ev.Actor != actor || ev.Role != role {
			t.Fatalf("audit %s: actor=%q role=%q, want %q/%q", event, ev.Actor, ev.Role, actor, role)
		}
		return
	}
	t.Fatalf("audit journal has no %s event among %d records", event, len(events))
}

// The whole point of RBAC: a viewer reads, an operator also mutates the
// task-adjacent surfaces, an admin also moves the node itself — and every
// mutation and denial lands in the audit journal with the actor's name.
func TestRBACRolesGateTheAPI(t *testing.T) {
	t.Setenv("ZEPTO_TEST_VIEWER", "viewer-secret")
	t.Setenv("ZEPTO_TEST_OPERATOR", "operator-secret")
	t.Setenv("ZEPTO_TEST_ADMIN", "admin-secret")
	cfg := config.Default()
	cfg.API.Enabled = true
	cfg.API.Tokens = []config.APIToken{
		{Name: "watcher", Role: "viewer", TokenEnv: "ZEPTO_TEST_VIEWER"},
		{Name: "runner", Role: "operator", TokenEnv: "ZEPTO_TEST_OPERATOR"},
		{Name: "root", Role: "admin", TokenEnv: "ZEPTO_TEST_ADMIN"},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, auditPath := rbacServer(t, cfg, logger)

	// The viewer reads the journal listing.
	if rec := call(t, s, http.MethodGet, "/api/v1/triggers", "", "viewer-secret"); rec.Code != http.StatusOK {
		t.Fatalf("viewer GET = %d: %s", rec.Code, rec.Body)
	}
	// …but cannot mutate: the role gate answers before the handler runs.
	if rec := call(t, s, http.MethodPost, "/api/v1/triggers",
		`{"id":"x","schedule":"* * * * *","job":{"instruction":"y"}}`, "viewer-secret"); rec.Code != http.StatusForbidden {
		t.Fatalf("viewer POST = %d: %s", rec.Code, rec.Body)
	}
	// An unknown bearer is a 401, before roles are even consulted.
	if rec := call(t, s, http.MethodGet, "/api/v1/triggers", "", "wrong-secret"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad token = %d", rec.Code)
	}
	// The operator mutates task-adjacent surfaces…
	if rec := call(t, s, http.MethodPost, "/api/v1/triggers",
		`{"id":"nightly","schedule":"30 2 * * *","job":{"instruction":"sweep"}}`, "operator-secret"); rec.Code != http.StatusAccepted {
		t.Fatalf("operator POST = %d: %s", rec.Code, rec.Body)
	}
	// …but not the node-moving admin surface.
	if rec := call(t, s, http.MethodPost, "/api/v1/admin/reload-config", "{}", "operator-secret"); rec.Code != http.StatusForbidden {
		t.Fatalf("operator admin reload = %d: %s", rec.Code, rec.Body)
	}
	// Admin passes the same route (the role gate must not be the obstacle).
	rec := call(t, s, http.MethodPost, "/api/v1/triggers",
		`{"id":"root-job","schedule":"* * * * *","job":{"instruction":"x"}}`, "admin-secret")
	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Fatalf("admin POST = %d: %s", rec.Code, rec.Body)
	}

	// The journal shows who did what: mutations carry the token's name and
	// role; denials name the refused actor — or nobody, for a bad token.
	events := readAudit(t, auditPath)
	assertAuditEvent(t, events, "api_mutation", "runner", "operator")
	assertAuditEvent(t, events, "api_role_denied", "watcher", "viewer")
	assertAuditEvent(t, events, "api_auth_denied", "", "")
}

// Without any configured token the surface stays loopback-first: every caller
// is treated as a full admin, exactly as before RBAC existed.
func TestRBACNoTokensStaysLoopbackFirst(t *testing.T) {
	cfg := config.Default()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, _ := rbacServer(t, cfg, logger)

	if rec := call(t, s, http.MethodGet, "/api/v1/triggers", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("anonymous GET = %d: %s", rec.Code, rec.Body)
	}
	if rec := call(t, s, http.MethodPost, "/api/v1/triggers", `not json`, ""); rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Fatalf("unconfigured auth must not deny: %d: %s", rec.Code, rec.Body)
	}
}

// The pre-RBAC single token keeps working and keeps its full-admin standing.
func TestLegacyAuthTokenEnvMapsToAdmin(t *testing.T) {
	t.Setenv("ZEPTO_TEST_LEGACY", "legacy-secret")
	cfg := config.Default()
	cfg.API.AuthTokenEnv = "ZEPTO_TEST_LEGACY"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, _ := rbacServer(t, cfg, logger)

	if rec := call(t, s, http.MethodGet, "/api/v1/triggers", "", "legacy-secret"); rec.Code != http.StatusOK {
		t.Fatalf("legacy GET = %d: %s", rec.Code, rec.Body)
	}
	if rec := call(t, s, http.MethodPost, "/api/v1/triggers", `{"job":{}}`, "legacy-secret"); rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Fatalf("legacy token must stay admin: %d: %s", rec.Code, rec.Body)
	}
	if rec := call(t, s, http.MethodGet, "/api/v1/triggers", "", "not-the-token"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token = %d", rec.Code)
	}
}

// roleAllows is the route table: the table pins every boundary an operator
// relies on — viewer reads, operator mutates but never the node lifecycle,
// admin does everything, an unknown role is denied by default.
func TestRoleAllows(t *testing.T) {
	cases := []struct {
		role    config.APIRole
		method  string
		pattern string
		want    bool
	}{
		{config.RoleViewer, "GET", "GET /api/v1/tasks", true},
		{config.RoleViewer, "POST", "/api/v1/tasks", false},
		{config.RoleViewer, "POST", "/api/v1/admin/scan", false},
		{config.RoleOperator, "POST", "/api/v1/tasks", true},
		{config.RoleOperator, "POST", "/api/v1/skills", true},
		{config.RoleOperator, "POST", "/api/v1/admin/scan", true},
		{config.RoleOperator, "POST", "/api/v1/admin/rotate-key", false},
		{config.RoleOperator, "POST", "/api/v1/admin/revoke", false},
		{config.RoleAdmin, "POST", "/api/v1/admin/revoke", true},
		{config.APIRole(99), "GET", "/api/v1/status", false},
	}
	for _, tc := range cases {
		if got := roleAllows(tc.role, tc.method, tc.pattern); got != tc.want {
			t.Fatalf("roleAllows(%v, %s, %s) = %v, want %v", tc.role, tc.method, tc.pattern, got, tc.want)
		}
	}
}

// The request counter is labelled by the route pattern, never the raw URL.
func TestHTTPRequestMetricLabelsRoutes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Default()
	audit, err := security.OpenAudit("", logger)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	defer audit.Close() //nolint:errcheck
	sched, err := triggers.New(triggers.Options{
		Store:  newMetaStore(),
		Submit: func(context.Context, triggers.Job) (string, error) { return "task-test", nil },
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	n := &node.Node{Cfg: cfg, Audit: audit, Scheduler: sched}
	mets := metrics.New("test")
	s := New(n, cfg, mets, logger)

	rec := call(t, s, http.MethodGet, "/api/v1/triggers", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/triggers = %d: %s", rec.Code, rec.Body)
	}
	if got := testutil.ToFloat64(mets.HTTPRequests.WithLabelValues("GET", "GET /api/v1/triggers", "200")); got != 1 {
		t.Fatalf("route-labelled counter = %v", got)
	}
}
