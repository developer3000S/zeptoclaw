package security

import (
	"errors"
	"fmt"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/wire"
)

// Signature schemes. The name travels on the wire so a future format can be
// introduced without ambiguity.
const (
	SchemeTask         = "zeptomesh-task-v1"
	SchemeResult       = "zeptomesh-result-v1"
	SchemeResultWorker = "zeptomesh-result-worker-v1"
	SchemeCancel       = "zeptomesh-cancel-v1"
	SchemeCaps         = "zeptomesh-caps-v1"
	SchemeAck          = "zeptomesh-ack-v1"
	// SchemeTaskOrigin covers only the authoring body (wire.TaskContent), so it
	// survives every relay that re-signs the transport copy (ТЗ 6.6.4).
	SchemeTaskOrigin = "zeptomesh-task-origin-v1"
	// SchemeRebind is applied to a KeyRebind body and signed by BOTH identities
	// the statement names — neither key alone can forge the handover (ТЗ 11.2).
	SchemeRebind = "zeptomesh-rebind-v1"
	// SchemeSkillsSync signs a peer's disclosed skill descriptor set.
	SchemeSkillsSync = "zeptomesh-skills-sync-v1"
	// SchemeSearchRequest/SchemeSearchReply sign the epidemic search-topic
	// lookups (ТЗ 6.9.5 п.5). Separate schemes keep a request bytes from
	// verifying as a reply and vice versa even if their bodies collided.
	SchemeSearchRequest = "zeptomesh-search-request-v1"
	SchemeSearchReply   = "zeptomesh-search-reply-v1"
)

// KeyLookup resolves a public key for a peer whose id does not embed one.
type KeyLookup = func(peer.ID) (ic.PubKey, error)

// ErrUnsigned reports a message that carries no signature.
var ErrUnsigned = errors.New("security: message is unsigned")

// Signer binds an identity to the scheme constants.
type Signer struct {
	id *Identity
}

// NewSigner wraps an identity.
func NewSigner(id *Identity) *Signer { return &Signer{id: id} }

// PeerID is the signer's identity.
func (s *Signer) PeerID() peer.ID { return s.id.PeerID() }

// signTaskDigest returns the digest a task signature must cover.
func signTaskDigest(t *pb.TaskEnvelope) ([]byte, error) {
	body, err := wire.TaskBody(t)
	if err != nil {
		return nil, err
	}
	return wire.Digest(SchemeTask, body), nil
}

// SignTask signs an envelope in place.
func (s *Signer) SignTask(t *pb.TaskEnvelope) error {
	d, err := signTaskDigest(t)
	if err != nil {
		return err
	}
	sig, err := s.id.Sign(d)
	if err != nil {
		return err
	}
	t.Signature = sig
	t.SignatureScheme = SchemeTask
	return nil
}

// SignResult signs a result in place.
func (s *Signer) SignResult(r *pb.TaskResult) error {
	body, err := wire.ResultBody(r)
	if err != nil {
		return err
	}
	sig, err := s.id.Sign(wire.Digest(SchemeResult, body))
	if err != nil {
		return err
	}
	r.Signature = sig
	r.SignatureScheme = SchemeResult
	return nil
}

// SignWorkerResult additionally stamps the worker chain signature (ТЗ 6.10.3,
// 11.5). Unlike Signature, it is computed over the routing-independent content
// and is never rewritten by relays, so the origin can prove what the executing
// peer actually returned even after several hops re-signed the transport copy.
func (s *Signer) SignWorkerResult(r *pb.TaskResult) error {
	if err := s.SignResult(r); err != nil {
		return err
	}
	content, err := wire.ResultContent(r)
	if err != nil {
		return err
	}
	sig, err := s.id.Sign(wire.Digest(SchemeResultWorker, content))
	if err != nil {
		return err
	}
	r.WorkerSignature = sig
	return nil
}

// VerifyWorkerResult checks the chain signature against worker_peer_id. It is
// independent of the last-hop signature and therefore survives relaying.
func VerifyWorkerResult(r *pb.TaskResult, lookup KeyLookup) error {
	if r == nil {
		return errors.New("security: nil task result")
	}
	if len(r.GetWorkerSignature()) == 0 {
		return ErrUnsigned
	}
	pid, err := peer.Decode(r.GetWorkerPeerId())
	if err != nil {
		return fmt.Errorf("security: result worker: %w", err)
	}
	content, err := wire.ResultContent(r)
	if err != nil {
		return err
	}
	return Verify(pid, wire.Digest(SchemeResultWorker, content), r.GetWorkerSignature(), lookup)
}

// SignCancel signs a cancel request in place.
func (s *Signer) SignCancel(c *pb.CancelRequest) error {
	body, err := wire.CancelBody(c)
	if err != nil {
		return err
	}
	sig, err := s.id.Sign(wire.Digest(SchemeCancel, body))
	if err != nil {
		return err
	}
	c.Signature = sig
	return nil
}

// SignCaps signs a capabilities advertisement in place.
func (s *Signer) SignCaps(c *pb.Capabilities) error {
	body, err := wire.CapsBody(c)
	if err != nil {
		return err
	}
	sig, err := s.id.Sign(wire.Digest(SchemeCaps, body))
	if err != nil {
		return err
	}
	c.Signature = sig
	return nil
}

// SignAck signs a task acknowledgement in place.
func (s *Signer) SignAck(a *pb.TaskAck) error {
	body, err := wire.AckBody(a)
	if err != nil {
		return err
	}
	sig, err := s.id.Sign(wire.Digest(SchemeAck, body))
	if err != nil {
		return err
	}
	a.Signature = sig
	return nil
}

// SignTaskOrigin stamps the authoring signature of a task. It covers only
// wire.TaskContent — everything the origin wrote — so relays that rewrite
// sender/ttl/route_stack and re-sign `signature` leave it untouched, and the
// final consumer can still verify the origin's own words (ТЗ 6.6.4).
func (s *Signer) SignTaskOrigin(t *pb.TaskEnvelope) error {
	content, err := wire.TaskContent(t)
	if err != nil {
		return err
	}
	sig, err := s.id.Sign(wire.Digest(SchemeTaskOrigin, content))
	if err != nil {
		return err
	}
	t.OriginSignature = sig
	return nil
}

// VerifyTaskOrigin checks the authoring signature against origin_peer_id.
// Envelopes from builds that predate the field verify as "unsigned origin":
// callers decide whether that is fatal (it is not by default, for compat).
func VerifyTaskOrigin(t *pb.TaskEnvelope, lookup KeyLookup) error {
	if t == nil {
		return errors.New("security: nil task envelope")
	}
	if len(t.GetOriginSignature()) == 0 {
		return ErrUnsigned
	}
	pid, err := peer.Decode(t.GetOriginPeerId())
	if err != nil {
		return fmt.Errorf("security: task origin id: %w", err)
	}
	content, err := wire.TaskContent(t)
	if err != nil {
		return err
	}
	return Verify(pid, wire.Digest(SchemeTaskOrigin, content), t.GetOriginSignature(), lookup)
}

// SignRebindOld signs a key-rebind statement with the RETIRING identity. The
// caller must have s.id == the old identity.
func (s *Signer) SignRebindOld(k *pb.KeyRebind) error {
	return s.signRebind(k)
}

// SignRebindNew signs a key-rebind statement with the INCOMING identity.
func (s *Signer) SignRebindNew(k *pb.KeyRebind) error {
	return s.signRebind(k)
}

func (s *Signer) signRebind(k *pb.KeyRebind) error {
	body, err := wire.RebindBody(k)
	if err != nil {
		return err
	}
	d := wire.Digest(SchemeRebind, body)
	sig, err := s.id.Sign(d)
	if err != nil {
		return err
	}
	if k.GetOldPeerId() == s.id.PeerID().String() {
		k.OldSignature = sig
	} else if k.GetNewPeerId() == s.id.PeerID().String() {
		k.NewSignature = sig
	} else {
		return errors.New("security: rebind signer matches neither identity")
	}
	k.SignatureScheme = SchemeRebind
	return nil
}

// VerifyRebind checks both halves of a rebind statement: the retiring key's
// signature and (for a rotation) the incoming key's signature over the same
// body, plus that new_pubkey really hashes to new_peer_id. A revocation (empty
// new id) requires only the old key's signature. This is what lets any third
// node accept a rotation without ever having met the new key.
func VerifyRebind(k *pb.KeyRebind, lookup KeyLookup) error {
	if k == nil {
		return errors.New("security: nil rebind")
	}
	if err := checkScheme(k.GetSignatureScheme(), SchemeRebind); err != nil {
		return err
	}
	if len(k.GetOldSignature()) == 0 {
		return ErrUnsigned
	}
	old, err := peer.Decode(k.GetOldPeerId())
	if err != nil {
		return fmt.Errorf("security: rebind old id: %w", err)
	}
	body, err := wire.RebindBody(k)
	if err != nil {
		return err
	}
	d := wire.Digest(SchemeRebind, body)
	if err := Verify(old, d, k.GetOldSignature(), lookup); err != nil {
		return fmt.Errorf("security: rebind old signature: %w", err)
	}
	if k.GetNewPeerId() == "" {
		return nil // pure revocation
	}
	if len(k.GetNewSignature()) == 0 {
		return ErrUnsigned
	}
	newID, err := peer.Decode(k.GetNewPeerId())
	if err != nil {
		return fmt.Errorf("security: rebind new id: %w", err)
	}
	pk, err := ic.UnmarshalPublicKey(k.GetNewPubkey())
	if err != nil {
		return fmt.Errorf("security: rebind new pubkey: %w", err)
	}
	derived, err := peer.IDFromPublicKey(pk)
	if err != nil || derived != newID {
		return errors.New("security: rebind new_pubkey does not match new_peer_id")
	}
	if err := Verify(newID, d, k.GetNewSignature(), lookup); err != nil {
		return fmt.Errorf("security: rebind new signature: %w", err)
	}
	return nil
}

// SignSkillsSync signs a skill-descriptor disclosure.
func (s *Signer) SignSkillsSync(r *pb.SkillsSyncResponse) error {
	body, err := wire.SkillsSyncBody(r)
	if err != nil {
		return err
	}
	sig, err := s.id.Sign(wire.Digest(SchemeSkillsSync, body))
	if err != nil {
		return err
	}
	r.Signature = sig
	r.SignatureScheme = SchemeSkillsSync
	return nil
}

// VerifySkillsSync checks a disclosure against the authenticated sender.
func VerifySkillsSync(r *pb.SkillsSyncResponse, sender peer.ID, lookup KeyLookup) error {
	if r == nil {
		return errors.New("security: nil skills sync")
	}
	if len(r.GetSignature()) == 0 {
		return ErrUnsigned
	}
	if r.GetPeerId() != sender.String() {
		return errors.New("security: skills sync peer id does not match stream identity")
	}
	body, err := wire.SkillsSyncBody(r)
	if err != nil {
		return err
	}
	return Verify(sender, wire.Digest(SchemeSkillsSync, body), r.GetSignature(), lookup)
}

// checkScheme rejects an unexpected scheme, treating an empty one as expected.
func checkScheme(got, want string) error {
	if got == "" || got == want {
		return nil
	}
	return fmt.Errorf("security: unsupported signature scheme %q (want %q)", got, want)
}

// VerifyTask checks an envelope signature against its claimed sender.
func VerifyTask(t *pb.TaskEnvelope, lookup KeyLookup) error {
	if t == nil {
		return errors.New("security: nil task envelope")
	}
	if len(t.GetSignature()) == 0 {
		return ErrUnsigned
	}
	if err := checkScheme(t.GetSignatureScheme(), SchemeTask); err != nil {
		return err
	}
	pid, err := peer.Decode(t.GetSenderPeerId())
	if err != nil {
		return fmt.Errorf("security: sender peer id: %w", err)
	}
	body, err := wire.TaskBody(t)
	if err != nil {
		return err
	}
	return Verify(pid, wire.Digest(SchemeTask, body), t.GetSignature(), lookup)
}

// VerifyResult checks a result signature against its claimed sender.
func VerifyResult(r *pb.TaskResult, lookup KeyLookup) error {
	if r == nil {
		return errors.New("security: nil task result")
	}
	if len(r.GetSignature()) == 0 {
		return ErrUnsigned
	}
	if err := checkScheme(r.GetSignatureScheme(), SchemeResult); err != nil {
		return err
	}
	pid, err := peer.Decode(r.GetSenderPeerId())
	if err != nil {
		return fmt.Errorf("security: result sender: %w", err)
	}
	body, err := wire.ResultBody(r)
	if err != nil {
		return err
	}
	return Verify(pid, wire.Digest(SchemeResult, body), r.GetSignature(), lookup)
}

// VerifyCancel checks a cancel request signature.
func VerifyCancel(c *pb.CancelRequest, lookup KeyLookup) error {
	if c == nil {
		return errors.New("security: nil cancel request")
	}
	if len(c.GetSignature()) == 0 {
		return ErrUnsigned
	}
	pid, err := peer.Decode(c.GetSenderPeerId())
	if err != nil {
		return fmt.Errorf("security: cancel sender: %w", err)
	}
	body, err := wire.CancelBody(c)
	if err != nil {
		return err
	}
	return Verify(pid, wire.Digest(SchemeCancel, body), c.GetSignature(), lookup)
}

// VerifyCaps checks a capabilities advertisement against its claimed peer id.
func VerifyCaps(c *pb.Capabilities, lookup KeyLookup) error {
	if c == nil {
		return errors.New("security: nil capabilities")
	}
	if len(c.GetSignature()) == 0 {
		return ErrUnsigned
	}
	pid, err := peer.Decode(c.GetPeerId())
	if err != nil {
		return fmt.Errorf("security: caps peer id: %w", err)
	}
	body, err := wire.CapsBody(c)
	if err != nil {
		return err
	}
	return Verify(pid, wire.Digest(SchemeCaps, body), c.GetSignature(), lookup)
}

// VerifyAck checks an acknowledgement signature.
func VerifyAck(a *pb.TaskAck, lookup KeyLookup) error {
	if a == nil {
		return errors.New("security: nil ack")
	}
	if len(a.GetSignature()) == 0 {
		return ErrUnsigned
	}
	pid, err := peer.Decode(a.GetAcceptedBy())
	if err != nil {
		return fmt.Errorf("security: ack signer: %w", err)
	}
	body, err := wire.AckBody(a)
	if err != nil {
		return err
	}
	return Verify(pid, wire.Digest(SchemeAck, body), a.GetSignature(), lookup)
}

// SignSearchRequest signs a search-topic request in place. The requester signs
// with its own key; the receiver checks that against the authenticated pubsub
// sender, not the claim inside the message (the same rule Membership follows).
func (s *Signer) SignSearchRequest(r *pb.SearchRequest) error {
	body, err := wire.SearchRequestBody(r)
	if err != nil {
		return err
	}
	sig, err := s.id.Sign(wire.Digest(SchemeSearchRequest, body))
	if err != nil {
		return err
	}
	r.Signature = sig
	r.SignatureScheme = SchemeSearchRequest
	return nil
}

// VerifySearchRequest checks a request against the sender of the pubsub
// message that carried it.
func VerifySearchRequest(r *pb.SearchRequest, sender peer.ID, lookup KeyLookup) error {
	return verifySearch(SchemeSearchRequest, r.GetRequestId(), r.GetRequesterPeerId(),
		r.GetSignatureScheme(), r.GetSignature(), sender, lookup,
		func() ([]byte, error) { return wire.SearchRequestBody(r) }, "search request")
}

// SignSearchReply signs a search-topic answer in place.
func (s *Signer) SignSearchReply(r *pb.SearchReply) error {
	body, err := wire.SearchReplyBody(r)
	if err != nil {
		return err
	}
	sig, err := s.id.Sign(wire.Digest(SchemeSearchReply, body))
	if err != nil {
		return err
	}
	r.Signature = sig
	r.SignatureScheme = SchemeSearchReply
	return nil
}

// VerifySearchReply checks an answer against the sender of the pubsub message
// that carried it.
func VerifySearchReply(r *pb.SearchReply, sender peer.ID, lookup KeyLookup) error {
	return verifySearch(SchemeSearchReply, r.GetRequestId(), r.GetResponderPeerId(),
		r.GetSignatureScheme(), r.GetSignature(), sender, lookup,
		func() ([]byte, error) { return wire.SearchReplyBody(r) }, "search reply")
}

// verifySearch is the shared shape of the two checks: a search message is only
// worth anything if the authenticated transport sender is the peer it claims to
// be, so the claimed id is compared before any crypto runs.
func verifySearch(scheme, requestID, claimed, gotScheme string, sig []byte, sender peer.ID,
	lookup KeyLookup, body func() ([]byte, error), what string) error {
	if requestID == "" {
		return fmt.Errorf("security: %s without a request id", what)
	}
	if len(sig) == 0 {
		return ErrUnsigned
	}
	if claimed != sender.String() {
		return fmt.Errorf("security: %s claims %s but arrived from %s", what, claimed, sender)
	}
	if err := checkScheme(gotScheme, scheme); err != nil {
		return err
	}
	raw, err := body()
	if err != nil {
		return err
	}
	return Verify(sender, wire.Digest(scheme, raw), sig, lookup)
}
