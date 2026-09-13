package security

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

func ephemeralIdentity(t *testing.T) *Identity {
	t.Helper()
	id, err := NewEphemeral()
	if err != nil {
		t.Fatalf("NewEphemeral: %v", err)
	}
	return id
}

func TestLoadOrGeneratePersistsIdentity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "peer.key")

	first, err := LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("key file not written: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("key file mode %o, want 600", perm)
	}

	second, err := LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if first.PeerID() != second.PeerID() {
		t.Fatalf("peer id changed across reload: %s != %s", first.PeerID(), second.PeerID())
	}
}

func TestLoadOrGenerateRejectsGarbage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peer.key")
	if err := os.WriteFile(path, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrGenerate(path); err == nil {
		t.Fatal("expected error for malformed key file")
	}
}

func TestTaskSignVerifyRoundTrip(t *testing.T) {
	id := ephemeralIdentity(t)
	env := &pb.TaskEnvelope{
		TaskId: "t1", OriginPeerId: id.PeerID().String(), SenderPeerId: id.PeerID().String(),
		CreatedAt: time.Now().Unix(), Ttl: 3, Priority: 5,
		Payload: &pb.TaskPayload{Instruction: "do it"},
	}
	s := NewSigner(id)
	if err := s.SignTask(env); err != nil {
		t.Fatalf("SignTask: %v", err)
	}
	if err := VerifyTask(env, nil); err != nil {
		t.Fatalf("VerifyTask: %v", err)
	}

	tampered := proto.Clone(env).(*pb.TaskEnvelope)
	tampered.Priority = 9
	if err := VerifyTask(tampered, nil); err == nil {
		t.Fatal("tampered task must fail verification")
	}

	// Route changes must invalidate the signature (replay onto another path).
	rerouted := proto.Clone(env).(*pb.TaskEnvelope)
	rerouted.RouteStack = []string{"12D3KooWEvil"}
	if err := VerifyTask(rerouted, nil); err == nil {
		t.Fatal("route_stack must be covered by the signature")
	}
}

func TestVerifyRejectsUnsigned(t *testing.T) {
	env := &pb.TaskEnvelope{TaskId: "x", SenderPeerId: "12D3KooWSelf"}
	if err := VerifyTask(env, nil); err == nil {
		t.Fatal("unsigned task must be rejected")
	}
}

func TestResultAndCapsSignatures(t *testing.T) {
	id := ephemeralIdentity(t)
	s := NewSigner(id)

	res := &pb.TaskResult{TaskId: "t1", WorkerPeerId: id.PeerID().String(), SenderPeerId: id.PeerID().String(), Status: pb.TaskStatus_TASK_STATUS_COMPLETED, Text: "done"}
	if err := s.SignResult(res); err != nil {
		t.Fatal(err)
	}
	if err := VerifyResult(res, nil); err != nil {
		t.Fatalf("VerifyResult: %v", err)
	}
	res.Text = "lies"
	if err := VerifyResult(res, nil); err == nil {
		t.Fatal("tampered result must fail")
	}

	caps := &pb.Capabilities{PeerId: id.PeerID().String(), Skills: []string{"coding"}, Timestamp: time.Now().Unix()}
	if err := s.SignCaps(caps); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCaps(caps, nil); err != nil {
		t.Fatalf("VerifyCaps: %v", err)
	}
}

func TestVerifyFailsForWrongSigner(t *testing.T) {
	real := ephemeralIdentity(t)
	other := ephemeralIdentity(t)
	env := &pb.TaskEnvelope{TaskId: "t", SenderPeerId: other.PeerID().String(), Payload: &pb.TaskPayload{Instruction: "x"}}
	if err := NewSigner(real).SignTask(env); err != nil {
		t.Fatal(err)
	}
	// Re-declared sender does not match the key that actually signed.
	if err := VerifyTask(env, nil); err == nil {
		t.Fatal("signature from a different key must not verify")
	}
}

func TestPeerstoreLookupForUnknownKey(t *testing.T) {
	id := ephemeralIdentity(t)
	env := &pb.TaskEnvelope{TaskId: "t", SenderPeerId: id.PeerID().String(), Payload: &pb.TaskPayload{Instruction: "x"}}
	if err := NewSigner(id).SignTask(env); err != nil {
		t.Fatal(err)
	}
	lookupCalled := false
	lookup := func(peer.ID) (ic.PubKey, error) { lookupCalled = true; return nil, nil }
	// Ed25519 peer ids carry their public key; a lookup is unnecessary.
	if err := VerifyTask(env, lookup); err != nil {
		t.Fatalf("self-certifying key must verify without lookup: %v", err)
	}
	if lookupCalled {
		t.Fatal("lookup must not be consulted when the peer id embeds the key")
	}
}

func TestTrustPolicy(t *testing.T) {
	a, err := NewEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	pa, pbID := a.PeerID(), b.PeerID()

	p := NewPolicy(ModeLimited, TrustLimited)
	p.SetListed(pa, true)
	p.SetListed(pbID, false)

	if got := p.TrustOf(pa); got != TrustKnown {
		t.Fatalf("allowed peer trust = %v, want known", got)
	}
	if !p.AllowTasksFrom(pa) {
		t.Fatal("allowed peer must pass tasks")
	}
	if p.AllowTasksFrom(pbID) {
		t.Fatal("blocked peer must not pass tasks")
	}
	if p.AllowConnection(pbID) {
		t.Fatal("blocked peer must not connect")
	}
	if p.AllowDelegationTo(pbID) {
		t.Fatal("delegation to blocked peer must be refused")
	}

	// Private mode: only explicit allow-list entries are usable.
	priv := NewPolicy(ModePrivate, TrustKnown)
	priv.SetListed(pa, true)
	if !priv.AllowTasksFrom(pa) {
		t.Fatal("private mode must accept allow-listed peers")
	}
	if priv.AllowTasksFrom(ephemeralIdentity(t).PeerID()) {
		t.Fatal("private mode must reject strangers")
	}
}

func TestObserveNeverDowngrades(t *testing.T) {
	id := ephemeralIdentity(t)
	p := NewPolicy(ModeLimited, TrustLimited)
	p.Observe(id.PeerID(), TrustKnown)
	p.Observe(id.PeerID(), TrustUntrusted)
	if got := p.TrustOf(id.PeerID()); got != TrustKnown {
		t.Fatalf("trust = %v after worse observation, want known", got)
	}
	p.Observe(id.PeerID(), TrustTrusted)
	if got := p.TrustOf(id.PeerID()); got != TrustTrusted {
		t.Fatalf("trust = %v, want upgrade to trusted", got)
	}
}

func TestLoadPeerFiles(t *testing.T) {
	dir := t.TempDir()
	id := ephemeralIdentity(t)
	path := filepath.Join(dir, "allow.txt")
	content := "# comment\n" + id.PeerID().String() + "\n\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	p := NewPolicy(ModeLimited, TrustLimited)
	n, err := p.LoadPeerFile(path, true)
	if err != nil {
		t.Fatalf("LoadPeerFile: %v", err)
	}
	if n != 1 {
		t.Fatalf("loaded %d peers, want 1", n)
	}
	if !p.AllowTasksFrom(id.PeerID()) {
		t.Fatal("peer from file must be allowed")
	}
	// Missing file is not an error.
	if n, err := p.LoadPeerFile(filepath.Join(dir, "nope.txt"), false); err != nil || n != 0 {
		t.Fatalf("missing file: n=%d err=%v", n, err)
	}
	bad := filepath.Join(dir, "bad.txt")
	if err := os.WriteFile(bad, []byte("not-a-peer-id"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := p.LoadPeerFile(bad, true); err == nil {
		t.Fatal("malformed peer id must be reported")
	}
}

func TestLimiter(t *testing.T) {
	l := NewLimiter(100, 5)
	for i := 0; i < 5; i++ {
		if !l.Allow("peer-a") {
			t.Fatalf("token %d denied within burst", i)
		}
	}
	if l.Allow("peer-a") {
		t.Fatal("burst exceeded but Allow returned true")
	}
	if !l.Allow("peer-b") {
		t.Fatal("different peer must have its own bucket")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.Wait(ctx, "peer-a"); err == nil {
		t.Fatal("Wait must fail for an exhausted bucket")
	}
}

func TestAuditWritesJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit", "security.jsonl")
	a, err := OpenAudit(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.Log(AuditEvent{Event: "test", PeerID: "12D3", Reason: "x"})
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"event":"test"`)) {
		t.Fatalf("audit line missing event: %s", b)
	}
}
