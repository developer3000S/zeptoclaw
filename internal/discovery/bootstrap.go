package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// Bootstrap connects to a static list of multiaddrs and keeps retrying them
// until the node has enough neighbours. Bootstrap peers are never required for
// a formed mesh to keep working — they only help a new node get in.
type Bootstrap struct {
	addrs    []peer.AddrInfo
	raw      []string
	h        host.Host
	log      *slog.Logger
	interval time.Duration
	maxDial  time.Duration

	mu      sync.Mutex
	healthy map[peer.ID]time.Time
}

// NewBootstrap parses the configured addresses. An entry that cannot be parsed
// is reported immediately rather than silently ignored, because a typo in a
// bootstrap list is otherwise invisible until the network fails to form.
//
// An entry without an embedded peer id is rejected for the same reason: libp2p
// dials a peer, not a socket, so such a line can never connect. Left in the
// list it would fail on every retry and read like an unreachable host rather
// than the malformed entry it is.
func NewBootstrap(h host.Host, addrs []string, interval, dialTimeout time.Duration, logger *slog.Logger) (*Bootstrap, error) {
	b := &Bootstrap{
		raw:      append([]string(nil), addrs...),
		h:        h,
		log:      logger,
		interval: interval,
		maxDial:  dialTimeout,
		healthy:  make(map[peer.ID]time.Time),
	}
	var bad []string
	for _, raw := range addrs {
		ai, err := ParseAddrInfo(raw)
		if err != nil {
			bad = append(bad, err.Error())
			continue
		}
		if ai.ID == "" {
			bad = append(bad, fmt.Sprintf("%q has no /p2p/<peer id> part and cannot be dialed", raw))
			continue
		}
		b.addrs = append(b.addrs, *ai)
	}
	if len(bad) > 0 {
		return nil, fmt.Errorf("discovery: bad bootstrap addrs: %s", strings.Join(bad, "; "))
	}
	if b.interval <= 0 {
		b.interval = 30 * time.Second
	}
	if b.maxDial <= 0 {
		b.maxDial = 10 * time.Second
	}
	return b, nil
}

// hasDialablePort reports whether m carries a transport port (tcp, udp with
// quic, etc.). A multiaddr without one — e.g. bare "/ip4/1.2.3.4" — is not
// dialable by libp2p: the port is mandatory on every reliable transport, and
// there is no safe way to guess it. Such an entry is rejected here rather than
// silently accepted, because a portless bootstrap never connects and otherwise
// reads as an unreachable host instead of a malformed entry.
func hasDialablePort(m ma.Multiaddr) bool {
	if m == nil {
		return false
	}
	for _, c := range m {
		switch c.Protocol().Code {
		case ma.P_TCP, ma.P_UDP, ma.P_QUIC_V1:
			return true
		}
	}
	return false
}

// ParseAddrInfo accepts "/ip4/1.2.3.4/tcp/4001/p2p/12D3KooW…" and the shorter
// "/dns4/host/tcp/4001" form (the latter yields no peer id, which is fine for
// dialing but means identity is only confirmed by the Noise handshake).
//
// An address without a transport port is rejected: libp2p cannot dial it, and
// there is no safe default port to guess — mesh ports are per-instance
// (4001+i). Callers that learn a peer solely by id must supply a port-bearing
// multiaddr from a discovery source (mDNS, DHT, peer-exchange, registry) rather
// than rely on one being synthesized here.
func ParseAddrInfo(s string) (*peer.AddrInfo, error) {
	m, err := ma.NewMultiaddr(s)
	if err != nil {
		return nil, fmt.Errorf("discovery: %q is not a multiaddr: %w", s, err)
	}
	if !hasDialablePort(m) {
		return nil, fmt.Errorf("discovery: %q has no transport port and cannot be dialed", s)
	}
	ai, err := peer.AddrInfoFromP2pAddr(m)
	if err != nil {
		// Fall back to address-only: dial by address, learn the id on connect.
		return &peer.AddrInfo{Addrs: []ma.Multiaddr{m}}, nil
	}
	return ai, nil
}

// Count is the number of configured bootstrap peers.
func (b *Bootstrap) Count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.addrs)
}

// targets snapshots the configured peers. Rename can retarget an entry while a
// dial loop is walking the list, so every reader takes a copy under the lock
// instead of iterating the shared slice.
func (b *Bootstrap) targets() []peer.AddrInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]peer.AddrInfo(nil), b.addrs...)
}

// Rename retargets a bootstrap entry whose embedded peer id was rotated into
// newID, keeping the network address.
//
// A bootstrap multiaddr carries an identity ("…/p2p/12D3…"), so the moment that
// key is retired the entry dials an address that will fail the Noise handshake —
// the address is still right, the id is not. Renaming in memory is what keeps a
// planned rotation from quietly breaking the entry point of the mesh; the
// operator still has to edit the YAML eventually, but the network does not
// depend on that happening before the next restart. Reports whether an entry
// was changed.
func (b *Bootstrap) Rename(oldID, newID peer.ID) bool {
	if oldID == "" || newID == "" || oldID == newID {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	changed := false
	for i, ai := range b.addrs {
		if ai.ID != oldID {
			continue
		}
		ai.ID = newID
		b.addrs[i] = ai
		if full, err := peer.AddrInfoToP2pAddrs(&ai); err == nil && len(full) > 0 {
			b.retainAddr(oldID.String(), full[0].String())
		}
		changed = true
	}
	if _, ok := b.healthy[oldID]; ok {
		delete(b.healthy, oldID)
		b.healthy[newID] = time.Now().UTC()
	}
	return changed
}

// retainAddr rewrites the string form kept for diagnostics, matching on the
// embedded peer id rather than on the whole address (a peer may be listed with
// several transports).
func (b *Bootstrap) retainAddr(oldIDStr, newAddr string) {
	for j, raw := range b.raw {
		if strings.HasSuffix(raw, "/p2p/"+oldIDStr) || strings.Contains(raw, "/p2p/"+oldIDStr+"/") {
			b.raw[j] = newAddr
		}
	}
}

// Raw returns the configured entries as strings (diagnostics, rotation notice).
func (b *Bootstrap) Raw() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.raw...)
}

// Dial connects to one bootstrap peer.
func (b *Bootstrap) Dial(ctx context.Context, ai peer.AddrInfo) error {
	dctx, cancel := context.WithTimeout(ctx, b.maxDial)
	defer cancel()
	if err := b.h.Connect(dctx, ai); err != nil {
		return fmt.Errorf("discovery: bootstrap %s: %w", ai.ID, err)
	}
	b.mu.Lock()
	b.healthy[ai.ID] = time.Now().UTC()
	b.mu.Unlock()
	if b.log != nil {
		b.log.Info("bootstrap_connected", "peer", ai.ID.String(), "addrs", len(ai.Addrs))
	}
	return nil
}

// DialAll tries every bootstrap peer, tolerating partial failure. It returns
// the number of peers reached.
func (b *Bootstrap) DialAll(ctx context.Context) int {
	ok := 0
	for _, ai := range b.targets() {
		if ctx.Err() != nil {
			return ok
		}
		if ai.ID == b.h.ID() {
			continue // self-configured bootstrap entry
		}
		if err := b.Dial(ctx, ai); err != nil {
			if b.log != nil {
				b.log.Debug("bootstrap_dial", "peer", ai.ID.String(), "err", err.Error())
			}
			continue
		}
		ok++
	}
	return ok
}

// Run dials on start and keeps retrying until the mesh is wide enough.
// needMore reports whether the caller still wants connections.
func (b *Bootstrap) Run(ctx context.Context, needMore func() bool) {
	if b.Count() == 0 {
		return
	}
	b.DialAll(ctx)
	t := time.NewTicker(b.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if needMore != nil && !needMore() {
				return
			}
			b.DialAll(ctx)
		}
	}
}

// Healthy lists bootstrap peers currently reachable.
func (b *Bootstrap) Healthy(window time.Duration) []peer.ID {
	cutoff := time.Now().UTC().Add(-window)
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []peer.ID
	for p, ts := range b.healthy {
		if ts.After(cutoff) {
			out = append(out, p)
		}
	}
	return out
}
