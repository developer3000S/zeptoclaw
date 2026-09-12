package security

import (
	"errors"
	"fmt"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	pb "github.com/zeptoclaw/zeptomesh/gen/zeptomesh/v1"
	"github.com/zeptoclaw/zeptomesh/internal/wire"
)

// Signature schemes. The name travels on the wire so a future format can be
// introduced without ambiguity.
const (
	SchemeTask   = "zeptomesh-task-v1"
	SchemeResult = "zeptomesh-result-v1"
	SchemeCancel = "zeptomesh-cancel-v1"
	SchemeCaps   = "zeptomesh-caps-v1"
	SchemeAck    = "zeptomesh-ack-v1"
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
