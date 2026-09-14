package security

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	ic "github.com/libp2p/go-libp2p/core/crypto"
)

// RebindStore is the node-local ledger of identity handovers (ТЗ 11.2 п.3–4).
//
// A rotation is a self-certifying statement: it is signed by the retiring key
// AND the incoming key, so any node that can verify the old key can accept the
// new one without an operator touching anything. That is the only mechanism by
// which a rotated Peer ID stops being "an unknown peer" to the rest of the
// mesh. A revocation is the same statement with no successor: the identity is
// retired deliberately and must no longer be trusted.
//
// The ledger is persisted because a rebind that is accepted and then forgotten
// at restart would let an old, revoked identity look fresh again.
type RebindStore struct {
	mu sync.RWMutex

	path string
	log  *slog.Logger

	// byOld keeps the highest-sequence statement per retiring id.
	byOld map[string]*pb.KeyRebind
	// chain maps any retired id to its currently effective successor ("" for a
	// pure revocation), resolved transitively.
	chain map[string]string
	// revoked is the set of ids retired without a successor.
	revoked map[string]bool
}

// NewRebindStore opens (or creates) the ledger at path. path may be empty for
// an in-memory store, which is what tests use.
func NewRebindStore(path string, logger *slog.Logger) (*RebindStore, error) {
	s := &RebindStore{
		path:    path,
		log:     logger,
		byOld:   make(map[string]*pb.KeyRebind),
		chain:   make(map[string]string),
		revoked: make(map[string]bool),
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if path != "" {
		if err := s.load(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// persistedStatement is the JSON form; protobuf messages marshal to JSON with
// their field names, which is stable enough for a local ledger.
type persistedStatement struct {
	OldPeerID       string `json:"old_peer_id"`
	NewPeerID       string `json:"new_peer_id,omitempty"`
	NewPubkey       []byte `json:"new_pubkey,omitempty"`
	Sequence        int64  `json:"sequence"`
	IssuedAt        int64  `json:"issued_at"`
	Reason          string `json:"reason,omitempty"`
	OldSignature    []byte `json:"old_signature"`
	NewSignature    []byte `json:"new_signature,omitempty"`
	SignatureScheme string `json:"signature_scheme"`
}

type persistedLedger struct {
	Statements []persistedStatement `json:"statements"`
}

func (s *RebindStore) load() error {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("security: read rebind ledger: %w", err)
	}
	var p persistedLedger
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("security: parse rebind ledger: %w", err)
	}
	for _, st := range p.Statements {
		k := &pb.KeyRebind{
			OldPeerId: st.OldPeerID, NewPeerId: st.NewPeerID, NewPubkey: st.NewPubkey,
			Sequence: st.Sequence, IssuedAt: st.IssuedAt, Reason: st.Reason,
			OldSignature: st.OldSignature, NewSignature: st.NewSignature,
			SignatureScheme: st.SignatureScheme,
		}
		// Stored statements are re-verified on load: a tampered file must not
		// grant or revoke identities. Verification needs the new key's own
		// self-certification (Ed25519 peer ids are derived from the pubkey), so
		// no external lookup is required.
		if err := VerifyRebind(k, nil); err != nil {
			s.log.Warn("rebind_ledger_entry_untrusted", "old", st.OldPeerID, "err", err.Error())
			continue
		}
		s.applyLocked(k)
	}
	return nil
}

func (s *RebindStore) saveLocked() error {
	if s.path == "" {
		return nil
	}
	p := persistedLedger{}
	for _, k := range s.byOld {
		p.Statements = append(p.Statements, persistedStatement{
			OldPeerID: k.GetOldPeerId(), NewPeerID: k.GetNewPeerId(), NewPubkey: k.GetNewPubkey(),
			Sequence: k.GetSequence(), IssuedAt: k.GetIssuedAt(), Reason: k.GetReason(),
			OldSignature: k.GetOldSignature(), NewSignature: k.GetNewSignature(),
			SignatureScheme: k.GetSignatureScheme(),
		})
	}
	sort.Slice(p.Statements, func(i, j int) bool {
		if p.Statements[i].IssuedAt == p.Statements[j].IssuedAt {
			return p.Statements[i].OldPeerID < p.Statements[j].OldPeerID
		}
		return p.Statements[i].IssuedAt < p.Statements[j].IssuedAt
	})
	raw, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Apply verifies and records a handover statement. It reports applied=false
// when the statement is older than what is already held (a replay or a stale
// gossip copy), which is not an error.
//
// Revocation is terminal: once an identity has retired itself, no further
// statement signed by that key is honoured. Without this rule, a node that was
// revoked for compromise could simply issue a rotation with its still-held
// private key and walk the trust it had into a fresh identity — which would
// make the whole revocation mechanism decorative.
func (s *RebindStore) Apply(k *pb.KeyRebind) (bool, error) {
	if k == nil || k.GetOldPeerId() == "" {
		return false, errors.New("security: empty rebind")
	}
	if err := VerifyRebind(k, nil); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Known-or-newer first. Gossip republishes held statements epidemically, so
	// the same handover arrives again and again; answering that with an error
	// would fill the security journal with entries that describe nothing but a
	// duplicate delivery. A statement at or below the sequence already held is a
	// replay, and a replay of a revocation is exactly as true as the revocation.
	if cur, ok := s.byOld[k.GetOldPeerId()]; ok && cur.GetSequence() >= k.GetSequence() {
		return false, nil
	}
	// Only an attempt to move a revoked identity forward reaches this point —
	// which is the attack revocation exists to stop.
	if s.revoked[k.GetOldPeerId()] {
		return false, fmt.Errorf("security: identity %s is revoked and cannot rebind", k.GetOldPeerId())
	}
	s.applyLocked(k)
	if err := s.saveLocked(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *RebindStore) applyLocked(k *pb.KeyRebind) {
	s.byOld[k.GetOldPeerId()] = k
	if k.GetNewPeerId() == "" {
		s.revoked[k.GetOldPeerId()] = true
		delete(s.chain, k.GetOldPeerId())
		return
	}
	s.chain[k.GetOldPeerId()] = k.GetNewPeerId()
}

// Resolve follows rotation statements to the identity currently in effect for
// pid: the last one in its class. It returns (resolved, changed, revoked).
func (s *RebindStore) Resolve(pid peer.ID) (peer.ID, bool, bool) {
	cls := s.ClassOf(pid)
	if len(cls) == 0 {
		return pid, false, false
	}
	head := cls[len(cls)-1]
	return head, head != pid, s.ClassRevoked(pid)
}

// maxChainLinks bounds how far a handover chain is walked. Verification already
// precludes a cycle (each statement needs the retiring key), but a routing
// decision must not depend on that being true.
const maxChainLinks = 16

// ClassOf returns every identifier known to belong to the same node as pid, in
// succession order (earliest key first, current key last), pid included.
//
// A rotation statement does not merely point forward: it declares that two
// identities are one node. Trust therefore has to attach to the class, not to
// whichever key happens to be current. Resolving only forward would make a
// block escapable — deny a compromised id, watch its owner mint a fresh key and
// be welcomed as a stranger; resolving only backward would let a stolen key
// inherit an allow-list entry it never earned. Looking at the whole class keeps
// the operator's decision about the *node* authoritative in both directions.
func (s *RebindStore) ClassOf(pid peer.ID) []peer.ID {
	if pid == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.classOfLocked(pid)
}

func (s *RebindStore) classOfLocked(pid peer.ID) []peer.ID {
	// Walk to the head of the chain: the id that was never rotated into.
	cur := pid.String()
	for i := 0; i < maxChainLinks; i++ {
		prev, ok := s.predecessorLocked(cur)
		if !ok {
			break
		}
		cur = prev
		if i == maxChainLinks-1 {
			s.log.Warn("rebind_chain_too_deep", "peer", pid.String())
		}
	}

	// Then collect forward from the head, which yields the class in order.
	out := []peer.ID{}
	seen := map[string]bool{cur: true}
	node := cur
	for i := 0; i <= maxChainLinks; i++ {
		id, err := peer.Decode(node)
		if err != nil {
			break
		}
		out = append(out, id)
		next, ok := s.chain[node]
		if !ok || seen[next] {
			break
		}
		seen[next] = true
		node = next
	}
	return out
}

// predecessorLocked returns the id that rotated into the given id.
func (s *RebindStore) predecessorLocked(id string) (string, bool) {
	for old, k := range s.byOld {
		if k.GetNewPeerId() == id {
			return old, true
		}
	}
	return "", false
}

// ClassRevoked reports whether any identity in pid's class was retired by its
// own key without a successor — the ledger's statement that this node is gone
// (or was compromised), which must apply to keys it minted afterwards too.
func (s *RebindStore) ClassRevoked(pid peer.ID) bool {
	if pid == "" {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.classRevokedLocked(pid)
}

func (s *RebindStore) classRevokedLocked(pid peer.ID) bool {
	for _, m := range s.classOfLocked(pid) {
		if s.revoked[m.String()] {
			return true
		}
	}
	return false
}

// NextSequence returns the sequence number a statement for pid should carry:
// strictly above every held statement for that id. The retiring node asks for
// it before signing, so a replayed older statement can never overwrite a newer
// one on peers that already adopted it.
func (s *RebindStore) NextSequence(pid peer.ID) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if cur, ok := s.byOld[pid.String()]; ok {
		return cur.GetSequence() + 1
	}
	return 1
}

// Revoked reports whether pid was retired without a successor.
func (s *RebindStore) Revoked(pid peer.ID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revoked[pid.String()]
}

// SuccessorOf returns the id that replaced pid, if a statement exists.
func (s *RebindStore) SuccessorOf(pid peer.ID) (peer.ID, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	next, ok := s.chain[pid.String()]
	if !ok {
		return "", false
	}
	nid, err := peer.Decode(next)
	if err != nil {
		return "", false
	}
	return nid, true
}

// HoldsStatement reports whether the ledger already contains a statement that
// retired pid (either a rotation or a revocation).
func (s *RebindStore) HoldsStatement(pid peer.ID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.byOld[pid.String()]
	return ok
}

// PredecessorOf returns the id that was rotated into pid, if known.
func (s *RebindStore) PredecessorOf(pid peer.ID) (peer.ID, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for old, k := range s.byOld {
		if k.GetNewPeerId() == pid.String() {
			op, err := peer.Decode(old)
			if err != nil {
				continue
			}
			return op, true
		}
	}
	return "", false
}

// Statements returns every held statement, newest issuance first — the payload
// for gossip piggybacking and the admin view.
func (s *RebindStore) Statements() []*pb.KeyRebind {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.statementsLocked()
}

// StatementsFresh returns only statements issued within maxAge, for
// republication.
//
// The age bound is a transport rule, not a validity rule: a rotation performed
// last month remains true and must keep resolving trust locally, but there is
// nothing to teach a peer about it, and republishing it forever would have every
// recipient log it as too old to adopt.
func (s *RebindStore) StatementsFresh(maxAge time.Duration) []*pb.KeyRebind {
	return s.StatementsSince(time.Now().UTC().Add(-maxAge))
}

// StatementsSince returns the statements issued at or after the cutoff, newest
// first. Exposed as a cutoff rather than an age so callers and tests decide
// "now" themselves instead of racing a clock with one-second resolution.
func (s *RebindStore) StatementsSince(cutoff time.Time) []*pb.KeyRebind {
	c := cutoff.Unix()
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := s.statementsLocked()
	out := all[:0]
	for _, k := range all {
		if k.GetIssuedAt() >= c {
			out = append(out, k)
		}
	}
	return out
}

func (s *RebindStore) statementsLocked() []*pb.KeyRebind {
	out := make([]*pb.KeyRebind, 0, len(s.byOld))
	for _, k := range s.byOld {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].GetIssuedAt() == out[j].GetIssuedAt() {
			return out[i].GetOldPeerId() < out[j].GetOldPeerId()
		}
		return out[i].GetIssuedAt() > out[j].GetIssuedAt()
	})
	return out
}

// Len is the number of held statements.
func (s *RebindStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byOld)
}

// IssueRebind builds and double-signs a rotation statement from the retiring
// identity to the incoming one. Both keys sign the same canonical body, so a
// node that only ever saw the old id can still verify the handover.
func IssueRebind(oldID, newID *Identity, reason string, seq int64) (*pb.KeyRebind, error) {
	return IssueRebindAt(oldID, newID, reason, seq, time.Time{})
}

// IssueRebindAt issues a rotation stamped with an explicit issuance time.
//
// issued_at is covered by the signature, so peers cannot be told to re-date a
// statement they already hold; a caller that needs a handover dated otherwise
// (an archival record, or a test of the adoption window) has to say so at
// signing time.
func IssueRebindAt(oldID, newID *Identity, reason string, seq int64, issued time.Time) (*pb.KeyRebind, error) {
	if oldID == nil || newID == nil {
		return nil, errors.New("security: rebind needs both identities")
	}
	if seq <= 0 {
		seq = 1
	}
	if issued.IsZero() {
		issued = time.Now().UTC()
	}
	raw, err := ic.MarshalPublicKey(newID.PubKey())
	if err != nil {
		return nil, fmt.Errorf("security: rebind marshal pubkey: %w", err)
	}
	k := &pb.KeyRebind{
		OldPeerId: oldID.PeerID().String(),
		NewPeerId: newID.PeerID().String(),
		NewPubkey: raw,
		Sequence:  seq,
		IssuedAt:  issued.Unix(),
		Reason:    reason,
	}
	if err := NewSigner(oldID).SignRebindOld(k); err != nil {
		return nil, err
	}
	if err := NewSigner(newID).SignRebindNew(k); err != nil {
		return nil, err
	}
	if err := VerifyRebind(k, nil); err != nil {
		return nil, fmt.Errorf("security: self-check failed: %w", err)
	}
	return k, nil
}

// IssueRevocation retires an identity with no successor. Only the retiring key
// can authorise this; it is the "I am decommissioning this node" statement.
func IssueRevocation(old *Identity, reason string, seq int64) (*pb.KeyRebind, error) {
	if old == nil {
		return nil, errors.New("security: revocation needs an identity")
	}
	if seq <= 0 {
		seq = 1
	}
	k := &pb.KeyRebind{
		OldPeerId: old.PeerID().String(),
		Sequence:  seq,
		IssuedAt:  time.Now().UTC().Unix(),
		Reason:    reason,
	}
	if err := NewSigner(old).SignRebindOld(k); err != nil {
		return nil, err
	}
	k.SignatureScheme = SchemeRebind
	if err := VerifyRebind(k, nil); err != nil {
		return nil, fmt.Errorf("security: self-check failed: %w", err)
	}
	return k, nil
}
