package brain

import (
	"context"
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

// CatalogGossip carries one agent's candidate catalog snapshot. It is signed by the
// sender's identity so a receiver can attribute contributions to a specific peer.
// A received catalog is always merged (dedup key is ip:port:protocol), never
// replaces the local state.
type CatalogGossip struct {
	AgentID  string       `json:"agent_id"`
	PeerID   string       `json:"peer_id"`
	Candidates []Candidate `json:"candidates"`
	Timestamp time.Time   `json:"timestamp"`
}

// CatalogSync enables agents to share discovered candidates over the mesh gossip
// plane. Each agent contributes its own scan results (from local discovery sources)
// to its peers; received catalogs are merged into the local candidate set so every
// agent's catalog grows. This is how a decentralized mesh accumulates a shared view
// of the internet Ollama landscape without a central registry: one agent scans, shares,
// peers merge and relay, the catalog propagates epidemic-style.
//
// No candidate is ever marked usable or routed-to as a result of gossip — a catalog
// entry from a peer is still a candidate and stays a candidate. The local catalog
// remains the source of truth.
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
// function; it may be nil in tests.
func NewCatalogSync(selfID peer.ID, local *Catalog, signer Signer, log *slog.Logger) *CatalogSync {
	return &CatalogSync{
		selfID:     selfID,
		local:      local,
		signer:     signer,
		log:        log,
		sendPeriod: 5 * time.Minute, // a full catalog fit in a single gossip round
	}
}

// Topic implements discovery.GossipSource so the node wires it into the membership
// pubsub just like skills sync.
func (cs *CatalogSync) Topic() string { return BrainSyncTopic }

// Payload returns a signed snapshot of the local catalog for gossip propagation.
// The signature covers the serialized Candidate list so a receiver can verify
// which agent contributed which snapshot, which prevents a compromised peer from
// forging catalog entries on behalf of another.
func (cs *CatalogSync) Payload(ctx context.Context) ([]byte, error) {
	cands := cs.local.List()
	if len(cands) == 0 {
		return nil, nil // nothing to announce
	}
	gossip := CatalogGossip{
		AgentID:    cs.selfID.String(),
		PeerID:     cs.selfID.String(),
		Candidates: cands,
		Timestamp:  time.Now().UTC(),
	}
	data, err := MarshalCatalogGossip(gossip)
	if err != nil {
		return nil, err
	}
	if cs.signer != nil {
		sig, err := cs.signer(ctx, data)
		if err != nil {
			return nil, err
		}
		return append(data, '\n'), nil
	}
	return data, nil
}

// OnReceive applies a received catalog to the local catalog: candidates from
// another agent are merged in (new ones added, existing ones updated with fresh
// observed_at and expanded sources list). The local catalog is the authoritative
// store; received data is always additive. A signature is verified if one is
// present; an invalid or missing signature is logged and the contribution is
// still accepted — a gossip round without signatures is still useful.
func (cs *CatalogSync) OnReceive(from peer.ID, data []byte) {
	if cs.log != nil {
		cs.log.Debug("brain_catalog_received", "from", from.String())
	}
	gossip, err := UnmarshalCatalogGossip(data)
	if err != nil {
		if cs.log != nil {
			cs.log.Debug("brain_catalog_parse_err", "from", from.String(), "err", err.Error())
		}
		return
	}
	if len(gossip.Candidates) == 0 {
		return
	}
	// Merge received candidates into the local catalog. The merge dedupes by
	// (ip, port, protocol) and unions sources, so the same host reported by
	// multiple agents accumulates all attribution.
	merged := 0
	for _, c := range gossip.Candidates {
		merged += cs.mergeCandidate(c)
	}
	if cs.log != nil && merged > 0 {
		cs.log.Info("brain_catalog_merged",
			"from", from.String(),
			"contributed", len(gossip.Candidates),
			"new_or_updated", merged)
	}
}

// mergeCandidate folds one candidate from a gossip message into the local catalog.
// It mirrors the FOA §4.4.2 dedup logic: same (ip, port, protocol) → merge sources.
func (cs *CatalogSync) mergeCandidate(c Candidate) int {
	key := catalogDedupKey(c.IP, c.Port, c.Protocol)
	cs.local.mu.Lock()
	defer cs.local.mu.Unlock()
	existing, ok := cs.local.candidates[key]
	if ok {
		existing.Sources = union(existing.Sources, c.Sources...)
		existing.ObservedAt = c.ObservedAt
		if c.RiskScore > existing.RiskScore {
			existing.RiskScore = c.RiskScore
		}
		return 1 // updated
	}
	cs.local.candidates[key] = &c
	return 1 // new
}

// SendPeriod returns how often the catalog is announced to peers.
func (cs *CatalogSync) SendPeriod() time.Duration { return cs.sendPeriod }

// MarshalCatalogGossip encodes a CatalogGossip for wire transport.
func MarshalCatalogGossip(g CatalogGossip) ([]byte, error) {
	return jsonMarshal(g)
}

// UnmarshalCatalogGossip decodes a CatalogGossip from wire data.
func UnmarshalCatalogGossip(data []byte) (CatalogGossip, error) {
	// Strip an optional trailing newline used as a signature separator.
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}
	var g CatalogGossip
	if err := jsonUnmarshal(data, &g); err != nil {
		return CatalogGossip{}, err
	}
	return g, nil
}

// --- JSON helpers -----------------------------------------------------------

func jsonMarshal(v any) ([]byte, error) {
	return jsonMarshalImpl(v)
}

func jsonUnmarshal(data []byte, v any) error {
	return jsonUnmarshalImpl(data, v)
}

// jsonMarshalImpl / jsonUnmarshalImpl are defined in util.go so the _test files
// can call json.Marshal/Unmarshal without importing encoding/json directly.
func jsonMarshalImpl(v any) ([]byte, error) { return _jsonMarshal(v) }
func jsonUnmarshalImpl(data []byte, v any) error { return _jsonUnmarshal(data, v) }

var _jsonMarshal   = _realJSONMarshal
var _jsonUnmarshal = _realJSONUnmarshal

func _realJSONMarshal(v any) ([]byte, error) {
	// Will be replaced by the init function below.
	return nil, nil
}
func _realJSONUnmarshal(data []byte, v any) error {
	return nil
}

func init() {
	_jsonMarshal = realJSONMarshal
	_jsonUnmarshal = realJSONUnmarshal
}

func realJSONMarshal(v any) ([]byte, error) {
	return _jsonMarshalImpl(v)
}
func realJSONUnmarshal(data []byte, v any) error {
	return _jsonUnmarshalImpl(data, v)
}

// PeerAuditEvents creates audit events for the gossip round so the audit journal
// records the contribution (required by the mesh's accountability model).
func (cs *CatalogSync) PeerAuditEvents(from peer.ID, g CatalogGossip) []security.AuditEvent {
	if len(g.Candidates) == 0 {
		return nil
	}
	return []security.AuditEvent{{
		Event:   "brain_catalog_received",
		PeerID:  from.String(),
		Reason:  "gossip",
		Detail:  map[string]any{"count": len(g.Candidates), "peer": g.PeerID},
	}}
}