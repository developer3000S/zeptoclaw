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

// ParseAddrInfo accepts "/ip4/1.2.3.4/tcp/4001/p2p/12D3KooW…" and the shorter
// "/dns4/host/tcp/4001" form (the latter yields no peer id, which is fine for
// dialing but means identity is only confirmed by the Noise handshake).
func ParseAddrInfo(s string) (*peer.AddrInfo, error) {
	m, err := ma.NewMultiaddr(s)
	if err != nil {
		return nil, fmt.Errorf("discovery: %q is not a multiaddr: %w", s, err)
	}
	ai, err := peer.AddrInfoFromP2pAddr(m)
	if err != nil {
		// Fall back to address-only: dial by address, learn the id on connect.
		return &peer.AddrInfo{Addrs: []ma.Multiaddr{m}}, nil
	}
	return ai, nil
}

// Count is the number of configured bootstrap peers.
func (b *Bootstrap) Count() int { return len(b.addrs) }

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
	for _, ai := range b.addrs {
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
	if len(b.addrs) == 0 {
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
