// Package wire holds the canonical encoders that both task digests and message
// signatures are computed over. Keeping them in one place means the digest a
// node stores and the digest it signs can never drift apart.
//
// The encoding is length-prefixed and field-ordered, so no combination of field
// values can shift another field's bytes (the classic "a|b" vs "ab|" collision).
package wire

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// ErrNilMessage is returned for a nil input rather than encoding a zero value.
var ErrNilMessage = errors.New("wire: nil message")

// Digest returns the domain-separated sha256 over a scheme and a body.
func Digest(scheme string, body []byte) []byte {
	h := sha256.New()
	h.Write([]byte("zeptomesh/"))
	h.Write([]byte(scheme))
	h.Write([]byte{0})
	h.Write(body)
	return h.Sum(nil)
}

// TaskContent encodes everything the origin authored: identity, skills,
// payload and constraints. It excludes the mutable routing fields (sender, ttl,
// route_stack) so any node on the path can recompute it.
func TaskContent(env *pb.TaskEnvelope) ([]byte, error) {
	if env == nil {
		return nil, ErrNilMessage
	}
	e := newEncoder()
	e.str(env.GetTaskId())
	e.str(env.GetParentTaskId())
	e.str(env.GetOriginPeerId())
	e.varint(env.GetCreatedAt())
	e.varint(int64(env.GetPriority()))
	e.strList(env.GetRequiredSkills())

	pl := env.GetPayload()
	e.str(pl.GetInstruction())
	e.str(pl.GetContextDigest())
	e.uvarint(uint64(len(pl.GetAttachments())))
	for _, a := range pl.GetAttachments() {
		e.str(a.GetName())
		e.str(a.GetHash())
		e.varint(a.GetSize())
		e.str(a.GetMediaType())
	}
	e.strMap(pl.GetLabels())

	c := env.GetConstraints()
	e.varint(int64(c.GetMaxDurationSeconds()))
	e.boolean(c.GetAllowNetworkTools())
	e.boolean(c.GetAllowShell())
	e.boolean(c.GetAllowDelegation())
	e.boolean(c.GetAllowSubtasks())
	return e.out(), nil
}

// TaskBody encodes the full signing body of an envelope: the content digest
// plus the routing fields a relaying node mutates.
func TaskBody(env *pb.TaskEnvelope) ([]byte, error) {
	content, err := TaskContent(env)
	if err != nil {
		return nil, err
	}
	e := newEncoder()
	e.raw(content)
	e.str(env.GetSenderPeerId())
	e.varint(int64(env.GetTtl()))
	e.strList(env.GetRouteStack())
	return e.out(), nil
}

// ResultBody encodes the signing body of a task result.
func ResultBody(r *pb.TaskResult) ([]byte, error) {
	if r == nil {
		return nil, ErrNilMessage
	}
	content, err := ResultContent(r)
	if err != nil {
		return nil, err
	}
	e := newEncoder()
	e.raw(content)
	e.str(r.GetSenderPeerId())
	e.strList(r.GetRouteStack())
	return e.out(), nil
}

// ResultContent encodes the part of a result the worker authored. Relays
// rewrite sender and route_stack, so the worker's chain signature covers this
// routing-independent body and keeps verifying after any number of hops.
func ResultContent(r *pb.TaskResult) ([]byte, error) {
	if r == nil {
		return nil, ErrNilMessage
	}
	e := newEncoder()
	e.str(r.GetTaskId())
	e.str(r.GetWorkerPeerId())
	e.varint(int64(r.GetStatus()))
	e.str(r.GetText())
	e.raw(r.GetResultDigest())
	e.str(r.GetErrorMessage())
	e.varint(r.GetStartedAt())
	e.varint(r.GetFinishedAt())
	e.uvarint(uint64(len(r.GetArtifacts())))
	for _, a := range r.GetArtifacts() {
		e.str(a.GetName())
		e.str(a.GetHash())
		e.varint(a.GetSize())
		e.str(a.GetMediaType())
	}
	e.varint(int64(r.GetErrorClass()))
	e.boolean(r.GetAggregated())
	return e.out(), nil
}

// ResultDigest computes the content digest of a result: sha256 over the
// worker-authored body (text, artifacts, status, timings) with the digest
// field itself treated as empty, so the definition is unambiguous no matter
// when it is called. It is what the signature covers and what the task journal
// stores, so an operator can verify a stored answer against the signed one
// without the worker's key material.
func ResultDigest(r *pb.TaskResult) ([]byte, error) {
	if r == nil {
		return nil, ErrNilMessage
	}
	clone := proto.Clone(r).(*pb.TaskResult)
	clone.ResultDigest = nil
	content, err := ResultContent(clone)
	if err != nil {
		return nil, err
	}
	return Digest("result-content", content), nil
}

// CancelBody encodes the signing body of a cancel request.
func CancelBody(c *pb.CancelRequest) ([]byte, error) {
	if c == nil {
		return nil, ErrNilMessage
	}
	e := newEncoder()
	e.str(c.GetTaskId())
	e.str(c.GetOriginPeerId())
	e.str(c.GetSenderPeerId())
	e.str(c.GetReason())
	return e.out(), nil
}

// CapsBody encodes the signing body of a capabilities advertisement.
func CapsBody(c *pb.Capabilities) ([]byte, error) {
	if c == nil {
		return nil, ErrNilMessage
	}
	e := newEncoder()
	e.str(c.GetPeerId())
	e.str(c.GetNodeName())
	e.str(c.GetVersion())
	e.strList(c.GetSkills())
	e.strList(c.GetModels())
	e.varint(int64(c.GetMaxParallelTasks()))
	e.varint(int64(c.GetRunningTasks()))
	e.str(formatFloat(c.GetLoad()))
	e.boolean(c.GetAcceptExternalTasks())
	e.boolean(c.GetAllowShell())
	e.boolean(c.GetRelayCapable())
	e.str(c.GetResourceClass())
	e.strList(c.GetListenAddrs())
	e.varint(c.GetTimestamp())
	// The skill epoch and its descriptors are covered by the same signature:
	// a peer that swaps a descriptor or bumps the version in transit breaks
	// verification, so skill exchange cannot be poisoned on the way (ТЗ 6.3).
	e.varint(c.GetSkillsVersion())
	e.uvarint(uint64(len(c.GetSkillDocs())))
	for _, d := range skillDocsSorted(c.GetSkillDocs()) {
		raw, err := SkillDescriptorBody(d)
		if err != nil {
			return nil, err
		}
		e.raw(raw)
	}
	return e.out(), nil
}

// SkillDescriptorBody encodes one skill descriptor canonically. Attributes are
// map data, so they are sorted by key — the same rule strMap applies elsewhere.
func SkillDescriptorBody(d *pb.SkillDescriptor) ([]byte, error) {
	if d == nil {
		return nil, ErrNilMessage
	}
	e := newEncoder()
	e.str(d.GetName())
	e.varint(d.GetVersion())
	e.varint(d.GetUpdatedAt())
	e.str(d.GetDescription())
	e.strList(d.GetModels())
	e.strMap(d.GetAttributes())
	return e.out(), nil
}

// SkillDescriptorDigest is the "sha256:<hex>" content id of a descriptor. It
// deliberately excludes the descriptor's own digest field (which is where the
// value is stored), so the definition is unambiguous.
func SkillDescriptorDigest(d *pb.SkillDescriptor) (string, error) {
	body, err := SkillDescriptorBody(d)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", Digest("skill-descriptor-v1", body)), nil
}

// skillDocsSorted returns descriptors ordered by name for deterministic
// encoding (protobuf repeated fields keep insertion order, but the encoder
// must not depend on whoever built the message).
func skillDocsSorted(in []*pb.SkillDescriptor) []*pb.SkillDescriptor {
	out := make([]*pb.SkillDescriptor, 0, len(in))
	for _, d := range in {
		if d != nil {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetName() < out[j].GetName() })
	return out
}

// RebindBody encodes the signing body of a key rebind/revocation statement.
// Both the old and the new key sign exactly this body, so the statement binds
// the two identities together with no third party's involvement (ТЗ 11.2).
func RebindBody(k *pb.KeyRebind) ([]byte, error) {
	if k == nil {
		return nil, ErrNilMessage
	}
	e := newEncoder()
	e.str(k.GetOldPeerId())
	e.str(k.GetNewPeerId())
	e.raw(k.GetNewPubkey())
	e.varint(k.GetSequence())
	e.varint(k.GetIssuedAt())
	e.str(k.GetReason())
	return e.out(), nil
}

// SkillsSyncBody encodes what a responder signs about its skill set.
func SkillsSyncBody(r *pb.SkillsSyncResponse) ([]byte, error) {
	if r == nil {
		return nil, ErrNilMessage
	}
	e := newEncoder()
	e.str(r.GetPeerId())
	e.varint(r.GetSkillsVersion())
	e.uvarint(uint64(len(r.GetSkills())))
	for _, d := range skillDocsSorted(r.GetSkills()) {
		raw, err := SkillDescriptorBody(d)
		if err != nil {
			return nil, err
		}
		e.raw(raw)
	}
	return e.out(), nil
}

// AckBody encodes the signing body of a task acknowledgement.
func AckBody(a *pb.TaskAck) ([]byte, error) {
	if a == nil {
		return nil, ErrNilMessage
	}
	e := newEncoder()
	e.str(a.GetTaskId())
	e.varint(int64(a.GetStatus()))
	e.str(a.GetReason())
	e.str(a.GetAcceptedBy())
	e.varint(a.GetTimestamp())
	return e.out(), nil
}

// encoder accumulates a canonical byte encoding.
type encoder struct{ buf []byte }

func newEncoder() *encoder { return &encoder{buf: make([]byte, 0, 128)} }

func (e *encoder) out() []byte  { return e.buf }
func (e *encoder) raw(p []byte) { e.buf = append(e.buf, p...) }
func (e *encoder) str(s string) {
	e.uvarint(uint64(len(s)))
	e.buf = append(e.buf, s...)
}
func (e *encoder) varint(v int64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], v)
	e.buf = append(e.buf, tmp[:n]...)
}
func (e *encoder) uvarint(v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	e.buf = append(e.buf, tmp[:n]...)
}
func (e *encoder) boolean(v bool) {
	if v {
		e.buf = append(e.buf, 1)
		return
	}
	e.buf = append(e.buf, 0)
}
func (e *encoder) strList(vals []string) {
	e.uvarint(uint64(len(vals)))
	for _, v := range vals {
		e.str(v)
	}
}
func (e *encoder) strMap(m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	e.uvarint(uint64(len(keys)))
	for _, k := range keys {
		e.str(k)
		e.str(m[k])
	}
}

// formatFloat renders a float64 deterministically without strconv's exponent
// surprises for the small load values the mesh reports.
func formatFloat(f float64) string {
	if f != f { // NaN
		return "nan"
	}
	if f > 1 {
		f = 1
	}
	if f < 0 {
		f = 0
	}
	i := int64(f*10000 + 0.5)
	return "bp:" + itoa(i)
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var digits [20]byte
	i := len(digits)
	for v > 0 {
		i--
		digits[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}
