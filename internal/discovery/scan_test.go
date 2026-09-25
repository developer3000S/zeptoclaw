package discovery

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	ma "github.com/multiformats/go-multiaddr"
)

func TestMeshTCPPorts(t *testing.T) {
	ports := MeshTCPPorts([]string{
		"/ip4/0.0.0.0/tcp/4001",
		"/ip4/0.0.0.0/udp/4001/quic-v1",
		"/ip4/0.0.0.0/tcp/4001", // duplicate must collapse
		"/ip4/0.0.0.0/udp/9999/quic-v1",
		"/dns4/example.com/tcp/443",
		"not-a-multiaddr",
	})
	// 4001 from tcp, 443 from the dns4/tcp entry; the udp-only entries are
	// skipped because they are not TCP listeners.
	want := map[int]bool{4001: true, 443: true}
	if len(ports) != len(want) {
		t.Fatalf("ports = %v, want %v", ports, want)
	}
	for _, p := range ports {
		if !want[p] {
			t.Fatalf("unexpected port %d in %v", p, ports)
		}
	}
}

func TestMeshTCPPorts_EmptyAndBad(t *testing.T) {
	if got := MeshTCPPorts(nil); len(got) != 0 {
		t.Fatalf("nil listen should yield no ports, got %v", got)
	}
	if got := MeshTCPPorts([]string{"garbage"}); len(got) != 0 {
		t.Fatalf("garbage listen should yield no ports, got %v", got)
	}
}

func TestPrivateSubnets(t *testing.T) {
	got := PrivateSubnets()
	// The test runner's interfaces must surface at least one RFC1918 /24 on any
	// normal host (docker/CI included); a machine with only loopback would
	// legitimately return nothing, so this asserts shape, not presence.
	for _, s := range got {
		ip, _, err := net.ParseCIDR(s)
		if err != nil {
			t.Fatalf("bad subnet %q: %v", s, err)
		}
		if ip.To4() == nil {
			t.Fatalf("subnet %q is not IPv4", s)
		}
		if !privateIPv4Addr(ip.String()) {
			t.Fatalf("subnet %q is not RFC1918", s)
		}
		if !strings.HasSuffix(s, ".0/24") {
			t.Fatalf("subnet %q is not a /24 prefix", s)
		}
	}
}

func TestSubnetSweep_FindsPeer(t *testing.T) {
	// Two real libp2p hosts on loopback: the sweep probes the listening host's
	// TCP port and must report its address as an open mesh port.
	target := testHost(t)
	prober := testHost(t)

	portStr := ""
	var targetAddr ma.Multiaddr
	for _, a := range target.Underlying().Addrs() {
		ma.ForEach(a, func(c ma.Component) bool {
			if c.Protocol().Code == ma.P_TCP {
				portStr = c.Value()
			}
			return true
		})
		if portStr != "" {
			break
		}
	}
	if portStr == "" {
		t.Fatal("target has no TCP address")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("bad port %q: %v", portStr, err)
	}
	targetAddr, err = ma.NewMultiaddr("/ip4/127.0.0.1/tcp/" + portStr)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	found, probed := SubnetSweep(ctx, prober.Underlying(), []string{"127.0.0.0/24"}, []int{port},
		prober.ID(), 500*time.Millisecond, 8)
	if probed == 0 {
		t.Fatal("sweep probed no addresses")
	}
	var saw bool
	for _, ai := range found {
		for _, a := range ai.Addrs {
			if a.Equal(targetAddr) {
				saw = true
			}
		}
	}
	if !saw {
		t.Fatalf("sweep did not find target address %s among %d found", targetAddr, len(found))
	}
}

func TestSubnetSweep_NoHost(t *testing.T) {
	// A nil host or empty inputs must be a no-op, not a panic.
	if got, _ := SubnetSweep(context.Background(), nil, []string{"127.0.0.0/24"}, []int{1}, "", time.Second, 8); len(got) != 0 {
		t.Fatalf("nil host should find nothing, got %v", got)
	}
	h := testHost(t)
	if got, _ := SubnetSweep(context.Background(), h.Underlying(), nil, []int{1}, h.ID(), time.Second, 8); len(got) != 0 {
		t.Fatalf("nil subnets should find nothing, got %v", got)
	}
	if got, _ := SubnetSweep(context.Background(), h.Underlying(), []string{"127.0.0.0/24"}, nil, h.ID(), time.Second, 8); len(got) != 0 {
		t.Fatalf("nil ports should find nothing, got %v", got)
	}
}
