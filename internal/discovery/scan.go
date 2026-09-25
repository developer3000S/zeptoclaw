package discovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// Subnet scanner: the only truly active, probing discovery source in the mesh.
// Where mDNS, the registry and the DHT wait for peers to announce themselves,
// here the node walks its own LAN subnets and tries to reach the mesh port on
// every address. It exists because an announcement-based mesh can miss a
// neighbour whose mDNS/registry is off while the node is still reachable on
// the wire.
//
// A probe is a *candidate*, never a fact: the produced AddrInfo carries no
// /p2p/<id> (so libp2p learns the id on the security handshake) and the caller
// still runs Policy.AllowConnection plus the capabilities check before trusting
// the peer. Scanning a subnet sends traffic across the LAN, which is why it is
// off by default and gated by config.

// MeshTCPPorts returns the TCP ports this node listens on, in case the caller
// wants to probe a subnet on them. Zero entries are skipped.
func MeshTCPPorts(listen []string) []int {
	seen := make(map[int]bool)
	var out []int
	for _, l := range listen {
		m, err := ma.NewMultiaddr(l)
		if err != nil {
			continue
		}
		var tcp int
		ma.ForEach(m, func(c ma.Component) bool {
			if c.Protocol().Code == ma.P_TCP {
				fmt.Sscanf(c.Value(), "%d", &tcp)
			}
			return true
		})
		if tcp > 0 && !seen[tcp] {
			seen[tcp] = true
			out = append(out, tcp)
		}
	}
	return out
}

// PrivateSubnets returns the /24 prefixes of this host's local interfaces that
// fall in RFC1918 space, as "10.0.0.0/24"-style strings. Interface addresses
// that are down, non-private or non-IPv4 are skipped. A /24 is the default LAN
// sweep: scanning a whole /16 or /8 would flood the network.
func PrivateSubnets() []string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []string
	seen := make(map[string]bool)
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok || ipnet.IP.To4() == nil {
				continue
			}
			ip := ipnet.IP.To4()
			if !privateIPv4Addr(ip.String()) {
				continue
			}
			prefix := fmt.Sprintf("%d.%d.%d.0/24", ip[0], ip[1], ip[2])
			if !seen[prefix] {
				seen[prefix] = true
				out = append(out, prefix)
			}
		}
	}
	return out
}

func privateIPv4Addr(s string) bool {
	switch {
	case strings.HasPrefix(s, "10."), strings.HasPrefix(s, "192.168."):
		return true
	case strings.HasPrefix(s, "172.16."), strings.HasPrefix(s, "172.17."),
		strings.HasPrefix(s, "172.18."), strings.HasPrefix(s, "172.19."),
		strings.HasPrefix(s, "172.2"), strings.HasPrefix(s, "172.30."),
		strings.HasPrefix(s, "172.31."):
		return true
	default:
		return false
	}
}

// sweepWorkers bounds the parallelism of a subnet sweep: a /24 is 254 hosts, and
// probing them serially at ~0.5s each would take minutes, while unbounded
// parallelism would saturate the file-descriptor table.
const sweepWorkers = 32

// SubnetSweep walks the given private /24 subnets and probes the mesh TCP port
// on every host address (.1..254). It is a bounded, best-effort probe: one TCP
// attempt per address with its own short timeout, a fan-out cap so a large LAN
// cannot stall the node, and a global maxCandidates cap (0 = unlimited).
// Returns the addresses that answered and the number of addresses probed.
//
// This is the sweep's honest limit: libp2p peer ids are self-certifying, so no
// public API will dial an address that carries no /p2p/<id>. What the sweep can
// do is find the *open mesh port* on the wire — an address that is listening —
// and hand it to the caller, which then relies on the id-bearing sources
// (registry, mDNS, DHT, peer exchange, search topic) to adopt the peer. A found
// address is a candidate, never a trusted agent.
func SubnetSweep(ctx context.Context, h host.Host, subnets []string, ports []int,
	ownID peer.ID, perAddr time.Duration, maxCandidates int) ([]peer.AddrInfo, int) {

	if h == nil || len(subnets) == 0 || len(ports) == 0 {
		return nil, 0
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	found := make(map[string]peer.AddrInfo)
	probed := 0

	probe := func(ip string) {
		for _, port := range ports {
			mu.Lock()
			probed++
			full := maxCandidates > 0 && len(found) >= maxCandidates
			mu.Unlock()
			if full {
				cancel()
				return
			}
			addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ip, port))
			if err != nil {
				continue
			}
			// A raw socket probe is the only thing an address without a peer
			// id allows: it answers "is something listening", not "who". The
			// handshake that would prove identity needs an id to address.
			dctx, dcancel := context.WithTimeout(ctx, perAddr)
			conn, err := dialTCP(dctx, addr)
			dcancel()
			if err != nil || conn == nil {
				continue
			}
			_ = conn.Close()
			mu.Lock()
			if _, seen := found[ip]; !seen {
				found[ip] = peer.AddrInfo{Addrs: []ma.Multiaddr{addr}}
			}
			mu.Unlock()
			return // one open port per address is enough
		}
	}

	// Fan the host list out across a bounded worker pool.
	hosts := make(chan string, sweepWorkers)
	var wg sync.WaitGroup
	for i := 0; i < sweepWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ip := range hosts {
				select {
				case <-ctx.Done():
					return
				default:
					probe(ip)
				}
			}
		}()
	}

	for _, subnet := range subnets {
		base, _, err := net.ParseCIDR(subnet)
		if err != nil || base.To4() == nil {
			continue
		}
		ip := base.To4()
		// Hosts .1 .. .254 of a /24; .0 (network) and .255 (broadcast) are
		// skipped, and our own listener is filtered out as a self-connect.
		for i := 1; i < 255; i++ {
			select {
			case <-ctx.Done():
				break
			case hosts <- nextHost(ip, i):
			}
		}
	}
	close(hosts)
	wg.Wait()

	return foundSnapshot(found, &mu), probed
}

// dialTCP opens one TCP connection to a /ip4/<a>/tcp/<p> multiaddr. It is the
// probe primitive: no identity, no handshake, just "is the port open".
func dialTCP(ctx context.Context, m ma.Multiaddr) (net.Conn, error) {
	var host, port string
	ma.ForEach(m, func(c ma.Component) bool {
		switch c.Protocol().Code {
		case ma.P_IP4, ma.P_IP6:
			host = c.Value()
		case ma.P_TCP:
			port = c.Value()
		}
		return true
	})
	if host == "" || port == "" {
		return nil, errors.New("discovery: not a tcp multiaddr")
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
}

func nextHost(base net.IP, i int) string {
	c := append([]byte(nil), base...)
	c[3] = byte(i)
	return net.IP(c).String()
}

func foundSnapshot(m map[string]peer.AddrInfo, mu *sync.Mutex) []peer.AddrInfo {
	mu.Lock()
	defer mu.Unlock()
	out := make([]peer.AddrInfo, 0, len(m))
	for _, ai := range m {
		out = append(out, ai)
	}
	return out
}
