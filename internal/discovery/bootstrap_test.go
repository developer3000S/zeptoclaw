package discovery

import (
	"strings"
	"testing"
)

// TestParseAddrInfo_hasDialablePort verifies that ParseAddrInfo rejects
// addresses without a transport port. Without this check a portless
// bootstrap entry would silently fail on every retry and look like an
// unreachable host instead of a malformed entry.
func TestParseAddrInfo_hasDialablePort(t *testing.T) {
	cases := []struct {
		addr    string
		wantErr bool
	}{
		{"/ip4/127.0.0.1/tcp/4001", false},
		{"/dns4/host.example.com/tcp/4001", false},
		{"/ip4/127.0.0.1/udp/4001", false},
		{"/ip4/127.0.0.1", true},
		{"/dns4/host.example.com", true},
		{"/ipfs/QmTest", true},
		{"/p2p/12D3KooWStable", true},
		{"not-a-multiaddr", true},
	}

	for _, c := range cases {
		t.Run(c.addr, func(t *testing.T) {
			_, err := ParseAddrInfo(c.addr)
			if c.wantErr && err == nil {
				t.Fatal("expected error but got none")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestParseAddrInfo_knownPort_ok verifies that addresses with explicit
// transport ports still parse successfully, even without a p2p peer ID.
func TestParseAddrInfo_knownPort_ok(t *testing.T) {
	ok := []string{
		"/ip4/127.0.0.1/tcp/4001",
		"/dns4/node.example.com/tcp/4001",
		"/ip4/127.0.0.1/udp/4001",
	}
	for _, addr := range ok {
		t.Run(addr, func(t *testing.T) {
			_, err := ParseAddrInfo(addr)
			if err != nil {
				t.Fatalf("expected success, got: %v", err)
			}
		})
	}
}

// TestParseAddrInfo_rejects_portless_addresses verifies that addresses missing transport ports
// are properly rejected, preventing silent failures in bootstrap.
func TestParseAddrInfo_rejects_portless_addresses(t *testing.T) {
	portless := []string{
		"/ip4/127.0.0.1",
		"/dns4/host.example.com",
		"/p2p/12D3KooWStable",
		"/ipfs/QmTest",
		"/plaintext",
	}

	for _, addr := range portless {
		t.Run(addr, func(t *testing.T) {
			_, err := ParseAddrInfo(addr)
			if err == nil {
				t.Fatalf("expected error for portless address %q, got none", addr)
			}
		})
	}
}

// TestNewBootstrap_rejects_portless verifies that NewBootstrap rejects
// bootstrap entries without transport ports, providing early failure
// feedback instead of silent retry loops.
func TestNewBootstrap_rejects_portless(t *testing.T) {
	h := testHost(t).Underlying()
	cfg := []string{
		"/ip4/192.168.1.1/tcp/0", // valid port
		"/ip4/192.168.1.2",       // portless → should fail
	}
	_, err := NewBootstrap(h, cfg, 0, 0, nil)
	if err == nil {
		t.Fatal("expected error for portless bootstrap address")
	}
	if !strings.Contains(err.Error(), "no transport port") {
		t.Fatalf("expected 'no transport port' error, got: %v", err)
	}
}
