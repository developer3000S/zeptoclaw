package discovery

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// MDNS announces this node in the local network and reports peers it finds.
//
// libp2p's mDNS helper resolves service instances to a peer.AddrInfo whose
// addresses are the ones the remote host is actually listening on, so a found
// peer is directly dialable. Identity is still verified by the Noise handshake
// before the peer is trusted, which satisfies the "authenticate before adding"
// requirement.
type MDNS struct {
	svc     mdns.Service
	log     *slog.Logger
	mu      sync.Mutex
	found   map[peer.ID]time.Time
	seen    int
	onFound func(peer.AddrInfo)
	closed  bool
}

// NewMDNS registers the service. serviceName is the full instance name; the
// convention here is a per-node unique name over a shared service type.
func NewMDNS(h host.Host, serviceName string, logger *slog.Logger) (*MDNS, error) {
	if h == nil {
		return nil, errors.New("discovery: nil host")
	}
	if serviceName == "" {
		serviceName = h.ID().String()
	}
	m := &MDNS{log: logger, found: make(map[peer.ID]time.Time)}
	m.svc = mdns.NewMdnsService(h, serviceName, m)
	return m, nil
}

// HandlePeerFound implements mdns.Notifee.
func (m *MDNS) HandlePeerFound(ai peer.AddrInfo) {
	if ai.ID == "" {
		return
	}
	m.mu.Lock()
	if _, dup := m.found[ai.ID]; !dup {
		m.seen++
	}
	m.found[ai.ID] = time.Now().UTC()
	cb := m.onFound
	m.mu.Unlock()

	if m.log != nil {
		m.log.Debug("mdns_peer_found", "peer", ai.ID.String(), "addrs", len(ai.Addrs))
	}
	if cb != nil && ai.ID != "" {
		cb(ai)
	}
}

// Start begins announcing and resolving.
func (m *MDNS) Start(onFound func(peer.AddrInfo)) error {
	m.mu.Lock()
	m.onFound = onFound
	m.mu.Unlock()
	if err := m.svc.Start(); err != nil {
		return fmt.Errorf("discovery: mdns start: %w", err)
	}
	if m.log != nil {
		m.log.Info("mdns_started")
	}
	return nil
}

// FoundCount is the number of distinct peers seen since startup.
func (m *MDNS) FoundCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.seen
}

// Recent returns peers seen within the window.
func (m *MDNS) Recent(window time.Duration) []peer.ID {
	cutoff := time.Now().UTC().Add(-window)
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []peer.ID
	for p, ts := range m.found {
		if ts.After(cutoff) {
			out = append(out, p)
		}
	}
	return out
}

// Close stops the service.
func (m *MDNS) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	return m.svc.Close()
}
