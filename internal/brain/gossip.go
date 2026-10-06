package brain

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/developer3000S/zeptoclaw/internal/security"
)

// BrainSyncTopic is the pubsub topic agents use to share their candidate catalogs.
// Unlike a PeerState exchange where the topic carries the agent's own state, here
// the topic carries one agent's current view of discovered candidates — so every
// agent's catalog grows as peers contribute their own scan results.
const BrainSyncTopic = "/zeptomesh/brain-catalog/0.1"

// MaxGossipCandidates bounds one gossip announcement: a catalog that outgrows
// this is truncated to the newest entries, so a single publication can neither
// blow the pubsub message ceiling nor starve the freshest observations.
const MaxGossipCandidates = 256

// CatalogGossip carries one agent's candidate catalog snapshot. It is signed by the
// sender's identity so a receiver can attribute contributions to a specific peer:
// the signature covers the announcement body, and the receiver recovers the
// sender's public key from the claimed peer id (peer ids are self-certifying).
type CatalogGossip struct {
	PeerID     string       `json:"peer_id"`
	Candidates []Candidate  `json:"candidates"`
	Timestamp  int64        `json:"timestamp_unix"`
	Signature  string       `json:"signature,omitempty"` // base64, over the signed prefix
}

// signedPrefix returns the announcement without the signature field, i.e. the
// exact bytes the sender signed.
func (g CatalogGossip) signedPrefix() ([]byte, error) {
	g.Signature = ""
	return json.Marshal(g)
}

// Sign fills the Signature field with signer's signature over the body.
func (g *CatalogGossip) Sign(signer Signer) error {
	if signer == nil {
		return nil // unsigned announcements are accepted, just less attributable
	}
	body, err := g.signedPrefix()
	if err != nil {
		return err
	}
	sig, err := signer(context.Background(), body)
	if err != nil {
		return fmt.Errorf("brain: sign catalog gossip: %w", err)
	}
	g.Signature = base64Encode(sig)
	return nil
}

// Verify checks the signature against the claimed peer id. An unsigned
// announcement is valid (verified loosely); a malformed one is not.
func (g CatalogGossip) Verify() error {
	if g.Signature == "" {
		return nil
	}
	pid, err := peer.Decode(g.PeerID)
	if err != nil {
		return fmt.Errorf("brain: gossip peer id: %w", err)
	}
	body, err := g.signedPrefix()
	if err != nil {
		return err
	}
	sig, err := base64Decode(g.Signature)
	if err != nil {
		return fmt.Errorf("brain: gossip signature: %w", err)
	}
	return security.Verify(pid, body, sig, nil)
}

// CatalogSync enables agents to share discovered candidates over the mesh gossip
// plane. Each agent contributes its own scan results to its peers; received
// catalogs are merged into the local candidate set so every agent's catalog
// grows. This is how a decentralized mesh accumulates a shared view of the
// internet Ollama landscape without a central registry: one agent scans, shares,
// peers merge and relay, the catalog propagates epidemic-style.
//
// A catalog entry received over gossip is still a candidate — inventory
// metadata. Whether it becomes a used backend is decided by this node alone,
// through the same verified-probe promotion every other candidate goes through.
type CatalogSync struct {
	selfID     peer.ID
	local      *Catalog
	signer     Signer
	log        *slog.Logger
	sendPeriod time.Duration
}

// Signer abstracts the signing operation so the sync can use the node's identity
// without depending on the whole node package.
type Signer func(ctx context.Context, payload []byte) ([]byte, error)

// NewCatalogSync builds the gossip handler. signer is the node's identity signing
// function; it may be nil, which produces unsigned announcements.
func NewCatalogSync(selfID peer.ID, local *Catalog, signer Signer, log *slog.Logger) *CatalogSync {
	if log == nil {
		log = slog.Default()
	}
	return &CatalogSync{
		selfID:     selfID,
		local:      local,
		signer:     signer,
		log:        log,
		sendPeriod: 5 * time.Minute,
	}
}

// Topic is the pubsub topic the catalog rides.
func (cs *CatalogSync) Topic() string { return BrainSyncTopic }

// SendPeriod returns how often the catalog is announced to peers.
func (cs *CatalogSync) SendPeriod() time.Duration { return cs.sendPeriod }

// Payload returns a signed snapshot of the local catalog for gossip propagation,
// or nil when there is nothing to announce (an empty catalog publishes noise).
func (cs *CatalogSync) Payload(ctx context.Context) ([]byte, error) {
	cands := cs.local.List()
	if len(cands) == 0 {
		return nil, nil
	}
	if len(cands) > MaxGossipCandidates {
		cands = cands[:MaxGossipCandidates]
	}
	gossip := CatalogGossip{
		PeerID:     cs.selfID.String(),
		Candidates: cands,
		Timestamp:  time.Now().UTC().Unix(),
	}
	if err := gossip.Sign(cs.signer); err != nil {
		return nil, err
	}
	return MarshalCatalogGossip(gossip)
}

// OnReceive applies a received catalog to the local catalog: candidates from
// another agent are merged in (new ones added, existing ones updated with fresh
// observed_at and expanded sources list). The local catalog is the authoritative
// store; received data is always additive.
//
// A valid signature lets the receiver attribute the contribution; an unsigned
// contribution is accepted with an audit note, because a catalog entry is
// inventory metadata and the merge dedup key (ip, port, protocol) is verified
// independently when the candidate is later probed.
func (cs *CatalogSync) OnReceive(from peer.ID, data []byte) {
	gossip, err := UnmarshalCatalogGossip(data)
	if err != nil {
		cs.log.Debug("brain_catalog_parse_err", "from", from.String(), "err", err.Error())
		return
	}
	if len(gossip.Candidates) == 0 {
		return
	}
	if err := gossip.Verify(); err != nil {
		cs.log.Debug("brain_catalog_bad_signature", "from", from.String(), "err", err.Error())
		return
	}
	merged := 0
	for _, c := range gossip.Candidates {
		merged += cs.mergeCandidate(c)
	}
	if merged > 0 {
		cs.log.Info("brain_catalog_merged",
			"from", from.String(),
			"contributed", len(gossip.Candidates),
			"new_or_updated", merged)
	}
}

// mergeCandidate folds one candidate from a gossip message into the local catalog.
// It mirrors the FOA §4.4.2 dedup logic: same (ip, port, protocol) → merge sources.
func (cs *CatalogSync) mergeCandidate(c Candidate) int {
	return cs.local.mergeReceived(c)
}

// MarshalCatalogGossip encodes a CatalogGossip for wire transport.
func MarshalCatalogGossip(g CatalogGossip) ([]byte, error) {
	return json.Marshal(g)
}

// UnmarshalCatalogGossip decodes a CatalogGossip from wire data.
func UnmarshalCatalogGossip(data []byte) (CatalogGossip, error) {
	var g CatalogGossip
	if err := json.Unmarshal(data, &g); err != nil {
		return CatalogGossip{}, err
	}
	return g, nil
}

// PeerAuditEvents creates audit events for the gossip round so the audit journal
// records the contribution (required by the mesh's accountability model).
func (cs *CatalogSync) PeerAuditEvents(from peer.ID, g CatalogGossip) []security.AuditEvent {
	if len(g.Candidates) == 0 {
		return nil
	}
	return []security.AuditEvent{{
		Event:  "brain_catalog_received",
		PeerID: from.String(),
		Reason: "gossip",
		Detail: fmt.Sprintf("count=%d peer=%s", len(g.Candidates), g.PeerID),
	}}
}
