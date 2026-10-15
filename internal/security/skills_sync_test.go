package security

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

func skillDoc(name string, version int64) *pb.SkillDescriptor {
	return &pb.SkillDescriptor{
		Name: name, Version: version, UpdatedAt: time.Now().UTC().Unix(),
		Description: name + " skill", Models: []string{"m1"},
		Attributes: map[string]string{"tier": "a"},
	}
}

// A skill disclosure is believed only because the responder's own key signed it
// and the claimed peer id is the identity that actually signed.
func TestSkillsSyncSignatureRoundTrip(t *testing.T) {
	id := ephemeralIdentity(t)
	resp := &pb.SkillsSyncResponse{
		PeerId: id.PeerID().String(), SkillsVersion: 7,
		Skills: []*pb.SkillDescriptor{skillDoc("coding", 1), skillDoc("research", 3)},
	}
	if err := NewSigner(id).SignSkillsSync(resp); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if err := VerifySkillsSync(resp, id.PeerID(), nil); err != nil {
		t.Fatalf("verify: %v", err)
	}

	tampered := &pb.SkillsSyncResponse{
		PeerId: resp.PeerId, SkillsVersion: resp.SkillsVersion, Signature: resp.Signature,
		SignatureScheme: resp.SignatureScheme,
		Skills:          []*pb.SkillDescriptor{skillDoc("coding", 99), resp.Skills[1]},
	}
	if err := VerifySkillsSync(tampered, id.PeerID(), nil); err == nil {
		t.Fatal("a raised skill version must break the disclosure signature")
	}
}

// Without the sender match the signature is worthless: any valid disclosure can
// be replayed as if it came from another node.
func TestSkillsSyncRejectsSenderMismatch(t *testing.T) {
	signer := ephemeralIdentity(t)
	resp := &pb.SkillsSyncResponse{
		PeerId: signer.PeerID().String(), SkillsVersion: 1,
		Skills: []*pb.SkillDescriptor{skillDoc("coding", 1)},
	}
	if err := NewSigner(signer).SignSkillsSync(resp); err != nil {
		t.Fatal(err)
	}
	if err := VerifySkillsSync(resp, ephemeralIdentity(t).PeerID(), nil); err == nil {
		t.Fatal("a disclosure must not verify against a different authenticated sender")
	}

	// The peer id inside the body is signed over and must equal the authenticated
	// sender, so neither can be swapped without breaking verification.
	lying := proto.Clone(resp).(*pb.SkillsSyncResponse)
	lying.PeerId = ephemeralIdentity(t).PeerID().String()
	if err := VerifySkillsSync(lying, signer.PeerID(), nil); err == nil {
		t.Fatal("a disclosure must not vouch for another peer's skills")
	}
}

func TestSkillsSyncRejectsUnsigned(t *testing.T) {
	id := ephemeralIdentity(t)
	resp := &pb.SkillsSyncResponse{PeerId: id.PeerID().String(), SkillsVersion: 1}
	if err := VerifySkillsSync(resp, id.PeerID(), nil); err == nil {
		t.Fatal("an unsigned disclosure must be refused")
	}
	// An empty answer is still a statement by the responder ("nothing newer", as
	// opposed to "not telling you"), so it has to be signed too.
	signed := &pb.SkillsSyncResponse{PeerId: id.PeerID().String(), SkillsVersion: 1,
		Reason: "disclosure_policy"}
	if err := NewSigner(id).SignSkillsSync(signed); err != nil {
		t.Fatal(err)
	}
	if err := VerifySkillsSync(signed, id.PeerID(), nil); err != nil {
		t.Fatalf("an empty but signed disclosure must verify: %v", err)
	}
}

// The origin signature is what makes authorship survive relaying: a relay
// rewrites sender, not origin, and cannot mint the origin's seal.
func TestTaskOriginSignatureSurvivesRelay(t *testing.T) {
	author := ephemeralIdentity(t)
	relay := ephemeralIdentity(t)
	env := &pb.TaskEnvelope{
		TaskId: "t1", OriginPeerId: author.PeerID().String(),
		SenderPeerId: author.PeerID().String(), CreatedAt: time.Now().Unix(), Ttl: 4,
		Payload: &pb.TaskPayload{Instruction: "do the thing"},
	}
	if err := NewSigner(author).SignTask(env); err != nil {
		t.Fatal(err)
	}
	if err := NewSigner(author).SignTaskOrigin(env); err != nil {
		t.Fatalf("sign origin: %v", err)
	}
	if err := VerifyTaskOrigin(env, nil); err != nil {
		t.Fatalf("verify origin: %v", err)
	}

	// A relay forwards it: the hop changes sender, and routing fields are outside
	// the signed body, so authorship must still check out.
	env.SenderPeerId = relay.PeerID().String()
	env.RouteStack = []string{author.PeerID().String()}
	env.Ttl = 3
	if err := VerifyTaskOrigin(env, nil); err != nil {
		t.Fatalf("relay must not break the origin seal: %v", err)
	}
	if err := VerifyTask(env, nil); err == nil {
		t.Fatal("the sender signature must still be the relay's to make")
	}
}

func TestTaskOriginSignatureRejectsRewrites(t *testing.T) {
	author := ephemeralIdentity(t)
	newBody := func() *pb.TaskEnvelope {
		env := &pb.TaskEnvelope{
			TaskId: "t2", OriginPeerId: author.PeerID().String(),
			SenderPeerId: author.PeerID().String(), Payload: &pb.TaskPayload{Instruction: "real"},
		}
		if err := NewSigner(author).SignTaskOrigin(env); err != nil {
			t.Fatal(err)
		}
		return env
	}
	cases := []struct {
		name   string
		mutate func(*pb.TaskEnvelope)
	}{
		{"instruction rewritten", func(e *pb.TaskEnvelope) { e.Payload.Instruction = "evil" }},
		{"origin claimed by another node", func(e *pb.TaskEnvelope) {
			e.OriginPeerId = ephemeralIdentity(t).PeerID().String()
		}},
		{"origin signature dropped", func(e *pb.TaskEnvelope) { e.OriginSignature = nil }},
	}
	for _, tc := range cases {
		env := newBody()
		tc.mutate(env)
		if err := VerifyTaskOrigin(env, nil); err == nil {
			t.Errorf("%s: must be rejected", tc.name)
		}
	}
}

// A stranger cannot attest authorship for someone else's task, which is the
// whole point of signing the content with the origin key.
func TestTaskOriginSignatureRejectsForeignSigner(t *testing.T) {
	author, attacker := ephemeralIdentity(t), ephemeralIdentity(t)
	env := &pb.TaskEnvelope{
		TaskId: "t3", OriginPeerId: author.PeerID().String(),
		SenderPeerId: author.PeerID().String(), Payload: &pb.TaskPayload{Instruction: "x"},
	}
	if err := NewSigner(attacker).SignTaskOrigin(env); err != nil {
		t.Fatal(err)
	}
	if err := VerifyTaskOrigin(env, nil); err == nil {
		t.Fatal("a task sealed by a node that is not its origin must be refused")
	}
}
