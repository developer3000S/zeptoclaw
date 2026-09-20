package p2p

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/security"
)

func testIdentity(t *testing.T) *security.Identity {
	t.Helper()
	cfg := config.Default()
	cfg.Node.DataDir = t.TempDir()
	cfg.Identity.KeyFile = t.TempDir() + "/peer.key"
	id, err := security.LoadOrGenerate(cfg.Identity.KeyFile)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	return id
}

func makeGater(t *testing.T, policy *security.Policy) *Gater {
	t.Helper()
	audit, err := security.OpenAudit("", nil)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	return NewGater(policy, audit)
}

func TestGater_AllowConnection_NilPolicy(t *testing.T) {
	g := makeGater(t, nil)
	pid := peer.ID("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")
	if !g.allow(pid, "secured") {
		t.Fatal("nil policy should allow all")
	}
}

func TestGater_AllowConnection_BlockedByPolicy(t *testing.T) {
	policy := security.NewPolicy(security.ModeLimited, security.TrustUntrusted)
	pid := peer.ID("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")
	policy.SetListed(pid, false)

	g := makeGater(t, policy)
	if g.allow(pid, "secured") {
		t.Fatal("deny-listed peer should be blocked")
	}
}

func TestGater_QuarantineBlocked_BlocksAllDirections(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	pid := peer.ID("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineBlocked, 0)

	g := makeGater(t, policy)
	dirs := []string{"dial_peer", "dial_addr", "secured", "upgraded"}
	for _, dir := range dirs {
		if g.allow(pid, dir) {
			t.Fatalf("QuarantineBlocked should block %s", dir)
		}
	}
}

func TestGater_QuarantineIsolated_BlocksAllDirections(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	pid := peer.ID("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineIsolated, 0)

	g := makeGater(t, policy)
	dirs := []string{"dial_peer", "dial_addr", "secured", "upgraded"}
	for _, dir := range dirs {
		if g.allow(pid, dir) {
			t.Fatalf("QuarantineIsolated should block %s", dir)
		}
	}
}

func TestGater_QuarantineChallenged_AllowsSecured(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	pid := peer.ID("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineChallenged, 0)

	g := makeGater(t, policy)
	if !g.allow(pid, "secured") {
		t.Fatal("QuarantineChallenged should allow secured connection")
	}
	if g.allow(pid, "dial_peer") {
		t.Fatal("QuarantineChallenged should block dial_peer")
	}
	if g.allow(pid, "dial_addr") {
		t.Fatal("QuarantineChallenged should block dial_addr")
	}
	if g.allow(pid, "upgraded") {
		t.Fatal("QuarantineChallenged should block upgraded")
	}
}

func TestGater_QuarantineMonitored_AllowsAll(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	pid := peer.ID("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineMonitored, 0)

	g := makeGater(t, policy)
	dirs := []string{"dial_peer", "dial_addr", "secured", "upgraded"}
	for _, dir := range dirs {
		if !g.allow(pid, dir) {
			t.Fatalf("QuarantineMonitored should allow %s", dir)
		}
	}
}

func TestGater_QuarantineTTL_Expiry(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	pid := peer.ID("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineBlocked, time.Nanosecond)
	time.Sleep(time.Millisecond)

	g := makeGater(t, policy)
	// Expired quarantine TTL should be ignored
	if !g.allow(pid, "secured") {
		t.Fatal("expired QuarantineBlocked should allow; gater checks IsExpired")
	}
}

func testGaterNode(t *testing.T, policy *security.Policy) (*Host, *Service, peer.AddrInfo) {
	t.Helper()
	cfg := config.Default()
	cfg.Node.Listen = []string{"/ip4/127.0.0.1/tcp/0"}
	cfg.Discovery.DHT = false
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	audit, err := security.OpenAudit("", nil)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	gater := NewGater(policy, audit)

	h, err := New(context.Background(), Options{Config: cfg, Key: key, Gater: gater, Logger: logger})
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h, NewService(h, Handlers{}, logger), h.AddrInfo()
}

// TestGater_Integration_QuarantineBlocked_DialFails applies quarantine to a peer
// and verifies that a *different* host cannot dial it.
func TestGater_Integration_QuarantineBlocked_DialFails(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	h1, _, addr := testGaterNode(t, policy)
	h2, _, _ := testGaterNode(t, policy)

	pid := h1.ID()
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineBlocked, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := h2.Connect(ctx, peer.AddrInfo{ID: pid, Addrs: addr.Addrs})
	if err == nil {
		t.Fatal("dial to QuarantineBlocked peer should fail")
	}
}

// TestGater_Integration_QuarantineIsolated_DialFails applies quarantine to a peer
// and verifies that a *different* host cannot dial it.
func TestGater_Integration_QuarantineIsolated_DialFails(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	h1, _, addr := testGaterNode(t, policy)
	h2, _, _ := testGaterNode(t, policy)

	pid := h1.ID()
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineIsolated, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := h2.Connect(ctx, peer.AddrInfo{ID: pid, Addrs: addr.Addrs})
	if err == nil {
		t.Fatal("dial to QuarantineIsolated peer should fail")
	}
}

// TestGater_Integration_QuarantineChallenged_DialFails applies quarantine to a peer
// and verifies that a *different* host cannot dial it (challenged blocks dial_peer).
func TestGater_Integration_QuarantineChallenged_DialFails(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	h1, _, addr := testGaterNode(t, policy)
	h2, _, _ := testGaterNode(t, policy)

	pid := h1.ID()
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineChallenged, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := h2.Connect(ctx, peer.AddrInfo{ID: pid, Addrs: addr.Addrs})
	if err == nil {
		t.Fatal("dial to QuarantineChallenged peer should fail")
	}
}
