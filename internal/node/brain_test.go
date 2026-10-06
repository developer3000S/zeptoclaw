package node

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/developer3000S/zeptoclaw/internal/brain"
	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/security"
)

// testBrainNode wires a node whose only live subsystem is the brain: a catalog
// pointed at a fake search engine reporting the fake Ollama endpoint, and an
// empty backend pool. Nothing else of the node exists.
func testBrainNode(t *testing.T, ollama *httptest.Server) *Node {
	t.Helper()
	u, _ := url.Parse(ollama.URL)
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	search := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"matches": []any{map[string]any{
				"ip_str": u.Hostname(), "port": port, "transport": "tcp",
				"hostnames": []any{"gpu-box.local"},
				"location":  map[string]any{"country_code": "DE"},
				"asn":       "AS64500", "data": "Ollama is running",
			}},
		})
	}))
	t.Cleanup(search.Close)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cat := brain.NewCatalog(brain.CatalogConfig{
		Enabled: true, Sources: []string{"shodan"}, Query: "port:11434",
		Timeout: 10e9, MaxPerSource: 25,
	}, brain.CatalogKeys{Shodan: "test-key"}, logger)
	cat.WithEndpoints(brain.CatalogEndpoints{Shodan: search.URL + "/shodan"})
	pool := brain.NewBackendPool("", nil, nil, logger)
	audit, err := security.OpenAudit("", logger)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	cfg := config.Default()
	return &Node{
		Cfg: cfg, Audit: audit, log: logger, BrainCatalog: cat, BrainPool: pool,
		// buildBrain owns this map in a real node; the partial test node has to
		// set it up itself or autoPromotePass's bookkeeping has nowhere to write.
		brainTried: make(map[string]time.Time),
	}
}

// testOllama stands in for a discovered Ollama endpoint: it answers /api/tags
// with a model list, which is what promotion probes.
func testOllama(t *testing.T, models ...string) *httptest.Server {
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

// firstCandidate scans once and returns the first candidate id found.
func firstCandidate(t *testing.T, n *Node) string {
	t.Helper()
	report := n.BrainCatalog.Scan(context.Background())
	if report.Created == 0 {
		t.Fatalf("scan found nothing: %+v", report)
	}
	cands := n.BrainCandidates()
	if len(cands) == 0 {
		t.Fatalf("catalog is empty after scan")
	}
	return cands[0].ID
}

// Promotion is the autonomous verification: the node's own probe decides, and a
// verified endpoint enters the pool the node thinks with.
func TestBrain_PromoteAndSelect(t *testing.T) {
	ollama := testOllama(t, "qwen2.5:0.5b", "qwen2.5:7b")
	n := testBrainNode(t, ollama)
	id := firstCandidate(t, n)

	ctx := context.Background()
	b, err := n.PromoteCandidate(ctx, id, "", "test")
	if err != nil {
		t.Fatalf("PromoteCandidate: %v", err)
	}
	if !b.Verified || len(b.Models) != 2 {
		t.Fatalf("promoted backend: verified=%t models=%v", b.Verified, b.Models)
	}
	if b.PromotedFrom != id {
		t.Fatalf("promoted_from = %q", b.PromotedFrom)
	}

	// Model selection resolves to the verified backend, preferring a named model
	// and otherwise taking the largest model of the fastest one.
	if bid, model, ok := n.SelectModel(""); !ok || bid != id || model != "qwen2.5:7b" {
		t.Fatalf("SelectModel(\"\") = %q %q %t", bid, model, ok)
	}
	if bid, model, ok := n.SelectModel("qwen2.5:0.5b"); !ok || bid != id || model != "qwen2.5:0.5b" {
		t.Fatalf("SelectModel(preferred) = %q %q %t", bid, model, ok)
	}
	// An empty pool (brain off) selects nothing.
	off := &Node{Cfg: config.Default()}
	if _, _, ok := off.SelectModel(""); ok {
		t.Fatalf("SelectModel with no pool must fail")
	}
}

// Promoting an unknown candidate id is a clean error, not a pool mutation.
func TestBrain_PromoteUnknownCandidate(t *testing.T) {
	ollama := testOllama(t, "qwen2.5:7b")
	n := testBrainNode(t, ollama)
	ctx := context.Background()
	if _, err := n.PromoteCandidate(ctx, "cnd_nope", "", ""); err == nil {
		t.Fatalf("unknown candidate must error")
	}
	if n.BrainPool.Count() != 0 {
		t.Fatalf("unknown candidate promoted into the pool")
	}
	if _, err := n.PromoteCandidate(ctx, "cnd_nope", "", ""); !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error should name the problem: %v", err)
	}
}

// A disabled brain refuses promotion and reports why.
func TestBrain_DisabledRefusesPromotion(t *testing.T) {
	n := &Node{Cfg: config.Default(), log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if _, err := n.PromoteCandidate(context.Background(), "cnd_1", "", ""); err == nil {
		t.Fatalf("promotion with no brain must fail")
	}
	if _, err := n.BrainScanNow(context.Background()); err == nil {
		t.Fatalf("scan with no brain must fail")
	}
	if out := n.BrainCandidates(); out != nil {
		t.Fatalf("candidates with no brain: %v", out)
	}
}

// autoPromotePass respects its per-pass cap and does not retry a dead endpoint
// on every pass: a failing host is probed again only after the retry window.
func TestBrain_AutoPromoteHonoursCapAndRetry(t *testing.T) {
	ollama := testOllama(t, "qwen2.5:7b")
	n := testBrainNode(t, ollama)
	n.Cfg.Brain.PromoteMaxPerPass = 1
	firstCandidate(t, n) // one candidate in the catalog

	ctx := context.Background()
	attempted, verified := n.autoPromotePass(ctx)
	if attempted != 1 || verified != 1 {
		t.Fatalf("first pass: attempted=%d verified=%d", attempted, verified)
	}
	// Immediately again: the same candidate is within its retry window, so the
	// pass is a no-op even though the endpoint is healthy.
	attempted, verified = n.autoPromotePass(ctx)
	if attempted != 0 || verified != 0 {
		t.Fatalf("retry window: attempted=%d verified=%d", attempted, verified)
	}
}

// With auto-promote disabled, a scan stays a pure inventory pass: no endpoint is
// ever dialed.
func TestBrain_ScanWithoutAutoPromote(t *testing.T) {
	ollama := testOllama(t, "qwen2.5:7b")
	n := testBrainNode(t, ollama)
	n.Cfg.Brain.AutoPromote = false
	n.runBrainScanPass(context.Background())
	if n.BrainPool.Count() != 0 {
		t.Fatalf("auto-promote off, yet the pool grew: %d", n.BrainPool.Count())
	}
	if len(n.BrainCandidates()) != 1 {
		t.Fatalf("the scan must still fill the catalog")
	}
}

// AcceptKeys merges a friend's search credentials into the catalog's in-memory
// key set, taking only the canonical variable names and never persisting them.
func TestBrain_AcceptKeys(t *testing.T) {
	ollama := testOllama(t, "qwen2.5:7b")
	n := testBrainNode(t, ollama)

	accepted := n.AcceptKeys(map[string]string{
		"shodan_api_key": "friend-shodan", "CENSYS_API_KEY": "friend-censys",
		"NOT_A_BRAIN_KEY": "ignored",
	})
	if accepted != 2 {
		t.Fatalf("accepted = %d, want 2", accepted)
	}
	keys := n.BrainCatalog.Keys()
	if keys.Shodan != "friend-shodan" || keys.CensysToken != "friend-censys" {
		t.Fatalf("keys = %+v", keys)
	}
	// A second exchange replaces, not appends: the key set is one slot per engine.
	accepted = n.AcceptKeys(map[string]string{"SHODAN_API_KEY": "newer"})
	if accepted != 1 || n.BrainCatalog.Keys().Shodan != "newer" {
		t.Fatalf("re-exchange: accepted=%d keys=%+v", accepted, n.BrainCatalog.Keys())
	}
	if keys.CensysToken != "friend-censys" {
		t.Fatalf("re-exchange must keep the other keys: %+v", n.BrainCatalog.Keys())
	}
}

// A node configured to refuse inbound keys takes none, which is how an operator
// keeps a node's search identity to itself.
func TestBrain_AcceptKeysRefusedByConfig(t *testing.T) {
	ollama := testOllama(t, "qwen2.5:7b")
	n := testBrainNode(t, ollama)
	n.Cfg.Brain.Keys.Accept = false
	if accepted := n.AcceptKeys(map[string]string{"SHODAN_API_KEY": "x"}); accepted != 0 {
		t.Fatalf("refusing node accepted %d keys", accepted)
	}
	n.Cfg.Brain.Keys.Accept = true
	n.Cfg.Brain.Keys.Exchange = false
	if accepted := n.AcceptKeys(map[string]string{"SHODAN_API_KEY": "x"}); accepted != 0 {
		t.Fatalf("exchange-disabled node accepted %d keys", accepted)
	}
}

// ShareableKeys discloses only the configured variables, skips blanks, and omits
// variables that are unset.
func TestBrain_ShareableKeys(t *testing.T) {
	n := &Node{Cfg: config.Default()}
	t.Setenv("SHODAN_API_KEY", "s")
	t.Setenv("CENSYS_API_KEY", "c")
	n.Cfg.Brain.Keys.ShareEnvs = []string{"SHODAN_API_KEY", "  ", "UNSET_ENV"}
	got := n.ShareableKeys()
	if len(got) != 1 || got["SHODAN_API_KEY"] != "s" {
		t.Fatalf("shareable keys = %v", got)
	}
}

// Outbound operations go only to friends the operator configured, by name or by
// URL, and carry the friend's own bearer token.
func TestBrain_FriendOperations(t *testing.T) {
	ollama := testOllama(t, "qwen2.5:7b")
	n := testBrainNode(t, ollama)
	t.Setenv("ZETOMESH_TEST_FRIEND_TOKEN", "friend-secret")

	// The friend: an admin API that records what it was handed.
	var seen map[string]string
	friend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer friend-secret" {
			t.Errorf("friend saw auth %q", r.Header.Get("Authorization"))
		}
		payload, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(payload, &seen)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  "promoted",
			"backend": map[string]any{"verified": true, "base_url": "http://203.0.113.9:11434", "models": []any{"qwen2.5:7b"}},
		})
	}))
	t.Cleanup(friend.Close)

	n.Cfg.Brain.Friends = []config.BrainFriendConfig{{
		Name: "gpu-friend", AdminURL: friend.URL, TokenEnv: "ZETOMESH_TEST_FRIEND_TOKEN",
	}}
	ctx := context.Background()
	if err := n.PromoteOnFriend(ctx, "gpu-friend", "cnd_abcd", "relay"); err != nil {
		t.Fatalf("PromoteOnFriend: %v", err)
	}
	if seen["candidate_id"] != "cnd_abcd" {
		t.Fatalf("the friend received %v", seen)
	}
	// A friend named by its URL resolves the same way.
	if err := n.PromoteOnFriend(ctx, friend.URL, "cnd_abcd", ""); err != nil {
		t.Fatalf("promote by URL: %v", err)
	}
	// An unconfigured friend is refused outright.
	if err := n.PromoteOnFriend(ctx, "stranger", "cnd_abcd", ""); err == nil {
		t.Fatalf("an unknown friend must be refused")
	}
	if err := n.PromoteOnFriend(ctx, "stranger", "cnd_abcd", ""); !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("error should name the problem: %v", err)
	}
}

// Sharing keys hands the friend only the configured values and reports how many
// were accepted by the friend's own exchange endpoint.
func TestBrain_ShareKeysWithFriend(t *testing.T) {
	ollama := testOllama(t, "qwen2.5:7b")
	n := testBrainNode(t, ollama)
	t.Setenv("SHODAN_API_KEY", "my-shodan")
	n.Cfg.Brain.Keys.ShareEnvs = []string{"SHODAN_API_KEY"}
	t.Setenv("ZETOMESH_TEST_FRIEND_TOKEN", "friend-secret")

	var got map[string]string
	friend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Keys map[string]string `json:"keys"`
		}
		payload, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(payload, &in)
		got = in.Keys
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "exchanged", "accepted": len(in.Keys)})
	}))
	t.Cleanup(friend.Close)
	n.Cfg.Brain.Friends = []config.BrainFriendConfig{{
		Name: "gpu-friend", AdminURL: friend.URL, TokenEnv: "ZETOMESH_TEST_FRIEND_TOKEN",
	}}

	sent, err := n.ShareKeysWithFriend(context.Background(), "gpu-friend")
	if err != nil || sent != 1 {
		t.Fatalf("ShareKeysWithFriend: sent=%d err=%v", sent, err)
	}
	if got["SHODAN_API_KEY"] != "my-shodan" {
		t.Fatalf("the friend received %v", got)
	}
	// Nothing shareable means no call at all: the friend is not bothered.
	n.Cfg.Brain.Keys.ShareEnvs = nil
	if sent, err := n.ShareKeysWithFriend(context.Background(), "gpu-friend"); err != nil || sent != 0 {
		t.Fatalf("nothing to share: sent=%d err=%v", sent, err)
	}
}

// The catalog gossip is signed by the sender and verifiable from the peer id
// alone (peer ids are self-certifying), so a contribution is attributable and a
// forged one is refused.
func TestBrain_CatalogSyncSignVerify(t *testing.T) {
	dir := t.TempDir()
	ident, err := security.LoadOrGenerate(filepath.Join(dir, "peer.key"))
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cat := brain.NewCatalog(brain.CatalogConfig{Enabled: true, Sources: []string{"shodan"}},
		brain.CatalogKeys{Shodan: "k"}, logger)
	// Point the search engine at a local server, or the scan would walk out to
	// the real internet — a unit test must never leave the process.
	search := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"matches": []any{map[string]any{
				"ip_str": "203.0.113.10", "port": 11434, "transport": "tcp",
			}},
		})
	}))
	t.Cleanup(search.Close)
	cat.WithEndpoints(brain.CatalogEndpoints{Shodan: search.URL + "/shodan"})

	sync := brain.NewCatalogSync(ident.PeerID(), cat,
		func(ctx context.Context, payload []byte) ([]byte, error) {
			return ident.Sign(payload)
		}, logger)

	// An empty catalog announces nothing.
	if payload, err := sync.Payload(context.Background()); err != nil || payload != nil {
		t.Fatalf("empty catalog payload = %v err=%v", payload, err)
	}

	// Seed the catalog with a scan and announce it.
	if report := cat.Scan(context.Background()); report.Created != 1 {
		t.Fatalf("scan: %+v", report)
	}
	payload, err := sync.Payload(context.Background())
	if err != nil || payload == nil {
		t.Fatalf("Payload: %v err=%v", payload, err)
	}
	gossip, err := brain.UnmarshalCatalogGossip(payload)
	if err != nil {
		t.Fatalf("unmarshal own payload: %v", err)
	}
	if gossip.PeerID != ident.PeerID().String() {
		t.Fatalf("peer id = %q", gossip.PeerID)
	}
	if err := gossip.Verify(); err != nil {
		t.Fatalf("own signature did not verify: %v", err)
	}
	if len(gossip.Candidates) != 1 {
		t.Fatalf("gossip candidates = %d", len(gossip.Candidates))
	}

	// Tampering with the body invalidates the signature.
	gossip.Candidates[0].IP = "203.0.113.99"
	if err := gossip.Verify(); err == nil {
		t.Fatalf("a tampered announcement must not verify")
	}
}

// A second node's announcement folds into our catalog: the merge is additive and
// deduplicates by (ip, port, protocol), which is how every agent's scan results
// accumulate into one shared view of the Ollama landscape.
func TestBrain_CatalogSyncMergesReceived(t *testing.T) {
	dir := t.TempDir()
	ident, err := security.LoadOrGenerate(filepath.Join(dir, "peer.key"))
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cat := brain.NewCatalog(brain.CatalogConfig{Enabled: true, Sources: []string{"shodan"}},
		brain.CatalogKeys{Shodan: "k"}, logger)
	sync := brain.NewCatalogSync(ident.PeerID(), cat, nil, logger)

	gossip := brain.CatalogGossip{
		PeerID: ident.PeerID().String(),
		Candidates: []brain.Candidate{{
			ID: "cnd_from_friend", IP: "203.0.113.42", Port: 11434, Protocol: "tcp",
			Sources: []string{"shodan"}, RiskScore: 20, ObservedAt: time.Now().UTC(),
		}},
		Timestamp: time.Now().UTC().Unix(),
	}
	data, err := brain.MarshalCatalogGossip(gossip)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	sync.OnReceive(ident.PeerID(), data)

	cands := cat.List()
	if len(cands) != 1 || cands[0].ID != "cnd_from_friend" {
		t.Fatalf("catalog after merge = %+v", cands)
	}
	// The same candidate again unions its sources instead of adding a row.
	gossip.Candidates[0].Sources = []string{"zoomeye"}
	data, err = brain.MarshalCatalogGossip(gossip)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	sync.OnReceive(ident.PeerID(), data)
	if len(cat.List()) != 1 {
		t.Fatalf("dedup failure: catalog = %+v", cat.List())
	}
	merged := cat.List()[0]
	if len(merged.Sources) != 2 {
		t.Fatalf("sources not unioned: %v", merged.Sources)
	}
	// Unparseable data is dropped, not stored.
	sync.OnReceive(ident.PeerID(), []byte("not json"))
	if len(cat.List()) != 1 {
		t.Fatalf("garbage mutated the catalog")
	}
}

// The persisted catalog round-trips through a restart, so a node keeps its
// inventory without re-scanning.
func TestBrain_CatalogPersists(t *testing.T) {
	ollama := testOllama(t, "qwen2.5:7b")
	n := testBrainNode(t, ollama)
	firstCandidate(t, n)

	path := filepath.Join(t.TempDir(), "brain", "catalog.json")
	if err := n.BrainCatalog.Save(path); err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the catalog file was not written: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fresh := brain.NewCatalog(brain.CatalogConfig{Enabled: true}, brain.CatalogKeys{}, logger)
	if err := fresh.Load(path); err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fresh.List()) != 1 {
		t.Fatalf("restored catalog = %+v", fresh.List())
	}
}
