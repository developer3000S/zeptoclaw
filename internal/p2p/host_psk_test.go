package p2p

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/developer3000S/zeptoclaw/internal/config"
)

func TestTCPOnlyAddrsDropsQuicVariants(t *testing.T) {
	in := []string{
		"/ip4/0.0.0.0/tcp/4001",
		"/ip4/0.0.0.0/udp/4001/quic-v1",
		"/ip4/0.0.0.0/udp/4001/quic",
		"/ip4/0.0.0.0/udp/4001/quic-v1/webtransport",
		"not-a-multiaddr", // kept: libp2p must report the parse error itself
	}
	got := tcpOnlyAddrs(in)
	want := []string{"/ip4/0.0.0.0/tcp/4001", "not-a-multiaddr"}
	if len(got) != len(want) {
		t.Fatalf("tcpOnlyAddrs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tcpOnlyAddrs[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func newPSK(t *testing.T) string {
	t.Helper()
	p, err := GeneratePSK()
	if err != nil {
		t.Fatalf("generate psk: %v", err)
	}
	return p
}

func pskNode(t *testing.T, psk string) *Host {
	t.Helper()
	cfg := config.Default()
	cfg.Node.Listen = []string{"/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/udp/0/quic-v1"}
	cfg.Node.PrivateNetwork = psk
	cfg.Discovery.DHT = false
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	h, err := New(context.Background(), Options{Config: cfg, Key: key,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("host under psk %q: %v", psk[:6], err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// TestPSKHostStartsOnTCPOnly is the regression for the live acceptance
// finding: with a PSK configured, the QUIC transport refused to build at all
// ("QUIC doesn't support private networks yet") and the node never started.
func TestPSKHostStartsOnTCPOnly(t *testing.T) {
	a := pskNode(t, newPSK(t))
	var hasQUIC bool
	for _, addr := range a.Addrs() {
		if strings.Contains(addr, "quic") {
			hasQUIC = true
		}
		if !strings.Contains(addr, "/tcp/") {
			t.Fatalf("unexpected non-TCP listen address under PSK: %s", addr)
		}
	}
	if hasQUIC {
		t.Fatal("QUIC address survived the PSK filter")
	}
}

// Two PSK peers must complete a handshake and talk; two different PSKs must
// never join, even on the same loopback (the E.6 acceptance claim).
func TestPSKPeersConnectAndSeparate(t *testing.T) {
	psk := newPSK(t)
	a, b := pskNode(t, psk), pskNode(t, psk)
	c := pskNode(t, newPSK(t))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.Connect(ctx, peer.AddrInfo{ID: b.ID(), Addrs: mustAddrs(t, b.Addrs())}); err != nil {
		t.Fatalf("same-psk connect: %v", err)
	}
	if len(a.Underlying().Network().ConnsToPeer(b.ID())) == 0 {
		t.Fatal("same-psk hosts report no established connections")
	}

	// Different PSK: dial must fail. Bound it short — the failure is at the
	// handshake, not at TCP connect, so a few seconds is generous.
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	err := a.Connect(dctx, peer.AddrInfo{ID: c.ID(), Addrs: mustAddrs(t, c.Addrs())})
	if err == nil {
		t.Fatal("hosts with different PSKs connected")
	}
}

func mustAddrs(t *testing.T, ss []string) []ma.Multiaddr {
	t.Helper()
	out := make([]ma.Multiaddr, 0, len(ss))
	for _, s := range ss {
		m, err := ma.NewMultiaddr(s)
		if err != nil {
			t.Fatalf("parse %s: %v", s, err)
		}
		out = append(out, m)
	}
	return out
}
