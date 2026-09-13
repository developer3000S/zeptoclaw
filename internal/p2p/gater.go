package p2p

import (
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/developer3000S/zeptoclaw/internal/security"
)

// Gater enforces the operator's peer lists at the transport level: a blocked
// peer never gets an authenticated stream, even for read-only protocols.
// It implements connmgr.ConnectionGater.
type Gater struct {
	policy *security.Policy
	audit  *security.Audit
}

// NewGater wires the gate to a trust policy. audit may be nil.
func NewGater(policy *security.Policy, audit *security.Audit) *Gater {
	return &Gater{policy: policy, audit: audit}
}

// InterceptPeerDial denies dials to blocked peers.
func (g *Gater) InterceptPeerDial(p peer.ID) bool {
	return g.allow(p, "dial_peer")
}

// InterceptAddrDial denies address+dial combinations for blocked peers.
func (g *Gater) InterceptAddrDial(p peer.ID, _ ma.Multiaddr) bool {
	return g.allow(p, "dial_addr")
}

// InterceptAccept allows every raw transport accept: identity is unknown
// pre-handshake, blocking happens once the peer is authenticated.
func (g *Gater) InterceptAccept(_ network.ConnMultiaddrs) bool { return true }

// InterceptSecured is the authoritative gate: after the Noise (or QUIC TLS)
// handshake the remote peer id is cryptographically verified.
func (g *Gater) InterceptSecured(_ network.Direction, p peer.ID, _ network.ConnMultiaddrs) bool {
	return g.allow(p, "secured")
}

// InterceptUpgraded gates the fully capable connection as a second line of
// defence (a transport may reach this point without InterceptSecured).
func (g *Gater) InterceptUpgraded(c network.Conn) (bool, control.DisconnectReason) {
	if c == nil {
		return true, 0
	}
	if g.allow(c.RemotePeer(), "upgraded") {
		return true, 0
	}
	return false, control.DisconnectReason(1)
}

func (g *Gater) allow(p peer.ID, dir string) bool {
	if g == nil || g.policy == nil {
		return true
	}
	if g.policy.AllowConnection(p) {
		return true
	}
	if g.audit != nil {
		g.audit.Log(security.AuditEvent{Event: "connection_gated", PeerID: p.String(), Reason: "blocked", Detail: dir})
	}
	return false
}
