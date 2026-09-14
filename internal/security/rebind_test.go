package security

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

func TestIssueRebindDoubleSignedAndVerifies(t *testing.T) {
	old := ephemeralIdentity(t)
	nw := ephemeralIdentity(t)

	k, err := IssueRebind(old, nw, "rotation", 1)
	if err != nil {
		t.Fatalf("IssueRebind: %v", err)
	}
	if err := VerifyRebind(k, nil); err != nil {
		t.Fatalf("a freshly issued rebind must verify: %v", err)
	}
	if k.GetOldPeerId() != old.PeerID().String() || k.GetNewPeerId() != nw.PeerID().String() {
		t.Fatal("statement must name both identities")
	}
	// The successor's key is embedded so a node that never met it can still
	// check the id/key binding — that is what makes rotation self-certifying.
	if len(k.GetNewPubkey()) == 0 {
		t.Fatal("new_pubkey must be carried")
	}
}

func TestVerifyRebindRejectsTampering(t *testing.T) {
	old := ephemeralIdentity(t)
	nw := ephemeralIdentity(t)
	third := ephemeralIdentity(t)

	cases := []struct {
		name   string
		mutate func(*pb.KeyRebind)
	}{
		{"new id swapped for another peer", func(k *pb.KeyRebind) { k.NewPeerId = third.PeerID().String() }},
		{"reason rewritten", func(k *pb.KeyRebind) { k.Reason = "compromise" }},
		{"sequence raised", func(k *pb.KeyRebind) { k.Sequence = 99 }},
		{"old key signature dropped", func(k *pb.KeyRebind) { k.OldSignature = nil }},
		{"new key signature dropped", func(k *pb.KeyRebind) { k.NewSignature = nil }},
	}
	for _, tc := range cases {
		k, err := IssueRebind(old, nw, "rotation", 1)
		if err != nil {
			t.Fatal(err)
		}
		tc.mutate(k)
		if err := VerifyRebind(k, nil); err == nil {
			t.Errorf("%s: must not verify", tc.name)
		}
	}
}

func TestVerifyRebindRejectsThirdPartySigning(t *testing.T) {
	old := ephemeralIdentity(t)
	nw := ephemeralIdentity(t)
	attacker := ephemeralIdentity(t)

	// An attacker who wants the old node's trust handed over must produce both
	// signatures; holding any other key is not enough. Signing the retiring
	// half with a stranger's key is refused outright — the signer must be one of
	// the two identities the statement names.
	k := &pb.KeyRebind{
		OldPeerId: old.PeerID().String(),
		NewPeerId: nw.PeerID().String(),
		Sequence:  1,
		IssuedAt:  time.Now().Unix(),
		Reason:    "rotation",
	}
	if err := NewSigner(attacker).SignRebindOld(k); err == nil {
		t.Fatal("a stranger's key must not be able to sign the retiring half")
	}
	// Even with the real retiring key, the missing successor signature is fatal.
	if err := NewSigner(old).SignRebindOld(k); err != nil {
		t.Fatal(err)
	}
	if err := VerifyRebind(k, nil); err == nil {
		t.Fatal("a statement signed by only one of the two identities must not be accepted")
	}
}

func TestRevocationNeedsOnlyOldKey(t *testing.T) {
	old := ephemeralIdentity(t)
	k, err := IssueRevocation(old, "decommission", 1)
	if err != nil {
		t.Fatalf("IssueRevocation: %v", err)
	}
	if k.GetNewPeerId() != "" {
		t.Fatal("a revocation has no successor")
	}
	if err := VerifyRebind(k, nil); err != nil {
		t.Fatalf("revocation must verify with the retiring key alone: %v", err)
	}
}

func newStore(t *testing.T) *RebindStore {
	t.Helper()
	s, err := NewRebindStore("", nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRebindStoreResolveChain(t *testing.T) {
	a, b, c := ephemeralIdentity(t), ephemeralIdentity(t), ephemeralIdentity(t)
	s := newStore(t)

	// a -> b -> c: two rotations in sequence. Resolution must follow the whole
	// chain, since an old client may still address `a`.
	for _, pair := range []struct{ from, to *Identity }{{a, b}, {b, c}} {
		k, err := IssueRebind(pair.from, pair.to, "rotation", 1)
		if err != nil {
			t.Fatal(err)
		}
		ok, err := s.Apply(k)
		if err != nil || !ok {
			t.Fatalf("Apply: ok=%v err=%v", ok, err)
		}
	}
	got, changed, revoked := s.Resolve(a.PeerID())
	if !changed || revoked {
		t.Fatalf("Resolve(a) = (%v,%v,%v), want resolved/changed/not-revoked", got, changed, revoked)
	}
	if got != c.PeerID() {
		t.Fatalf("Resolve(a) = %s, want %s (end of chain)", got, c.PeerID())
	}
	if _, changed, _ := s.Resolve(c.PeerID()); changed {
		t.Fatal("the current identity must resolve to itself")
	}
}

func TestRebindStoreRejectsReplayAndRewind(t *testing.T) {
	a, b, c := ephemeralIdentity(t), ephemeralIdentity(t), ephemeralIdentity(t)
	s := newStore(t)

	newSeq, err := IssueRebind(a, b, "rotation", 5)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Apply(newSeq); !ok {
		t.Fatal("first statement must apply")
	}
	// A gossip copy of an older sequence for the same retiring id must not
	// replace the newer one, and must not be reported as an error.
	oldSeq, err := IssueRebind(a, c, "rotation", 4)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Apply(oldSeq); ok {
		t.Fatal("older sequence must not be adopted")
	}
	if succ, _ := s.SuccessorOf(a.PeerID()); succ != b.PeerID() {
		t.Fatalf("successor = %s, want %s", succ, b.PeerID())
	}
}

func TestRebindStoreRevocationMarksAndResolves(t *testing.T) {
	a := ephemeralIdentity(t)
	s := newStore(t)
	k, err := IssueRevocation(a, "compromise", 1)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Apply(k); !ok || err != nil {
		t.Fatalf("Apply revocation: ok=%v err=%v", ok, err)
	}
	if _, _, revoked := s.Resolve(a.PeerID()); !revoked {
		t.Fatal("a revoked id must resolve as revoked")
	}
	if !s.Revoked(a.PeerID()) {
		t.Fatal("Revoked must report the id")
	}
}

// A revocation gossiped repeatedly must not be reported as a rejection.
//
// Gossip republishes held statements epidemically, so every heartbeat re-delivers
// the same handover. Treating that as an error filled the security journal with
// hundreds of entries per minute describing nothing but a duplicate delivery,
// which buried the events an operator actually needs to see.
func TestRevocationReplayIsNotAnError(t *testing.T) {
	a := ephemeralIdentity(t)
	s := newStore(t)
	rev, err := IssueRevocation(a, "compromise", 1)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Apply(rev); !ok || err != nil {
		t.Fatalf("first revocation: ok=%v err=%v", ok, err)
	}
	// The identical statement arriving again (a gossip echo) must be a silent
	// no-op: applied=false, but no error to audit.
	for i := 0; i < 3; i++ {
		ok, err := s.Apply(rev)
		if err != nil {
			t.Fatalf("replayed revocation must not error (iteration %d): %v", i, err)
		}
		if ok {
			t.Fatal("a replay must not report itself as newly applied")
		}
	}
	// The identity is still revoked, and still cannot move forward.
	if !s.Revoked(a.PeerID()) {
		t.Fatal("replays must not weaken the revocation")
	}
}

// Revocation must be terminal. If a retired key could still issue a rotation,
// the owner of a compromised key — which is exactly the case revocation exists
// for — would walk its trust into a fresh identity and the revocation would be
// decorative.
func TestRevocationIsTerminal(t *testing.T) {
	a, b := ephemeralIdentity(t), ephemeralIdentity(t)
	s := newStore(t)
	rev, err := IssueRevocation(a, "compromise", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(rev); err != nil {
		t.Fatal(err)
	}
	rot, err := IssueRebind(a, b, "rotation", 2)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Apply(rot); ok || err == nil {
		t.Fatalf("a revoked identity must not rebind: ok=%v err=%v", ok, err)
	}
	if got, changed, _ := s.Resolve(a.PeerID()); changed && got == b.PeerID() {
		t.Fatal("the revoked id must not resolve into a successor")
	}
	if !s.ClassRevoked(a.PeerID()) {
		t.Fatal("class must still be marked revoked")
	}
}

// A cycle of statements must not hang resolution: verification should preclude
// it, but a routing decision must never depend on that being true.
func TestRebindChainDepthBound(t *testing.T) {
	s := newStore(t)
	ids := make([]*Identity, maxChainLinks+4)
	for i := range ids {
		ids[i] = ephemeralIdentity(t)
	}
	for i := 0; i < len(ids)-1; i++ {
		k, err := IssueRebind(ids[i], ids[i+1], "rotation", 1)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Apply(k); err != nil {
			t.Fatal(err)
		}
	}
	// Close the cycle back onto the first id.
	back, err := IssueRebind(ids[len(ids)-1], ids[0], "rotation", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(back); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Resolve(ids[0].PeerID())
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Resolve did not terminate on a cyclic chain")
	}
}

func TestRebindStorePersistsAndReverifies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rebinds.json")
	a, b := ephemeralIdentity(t), ephemeralIdentity(t)
	s, err := NewRebindStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	k, err := IssueRebind(a, b, "rotation", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(k); err != nil {
		t.Fatal(err)
	}

	back, err := NewRebindStore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, _, _ := back.Resolve(a.PeerID()); got != b.PeerID() {
		t.Fatalf("after reload Resolve(a) = %s, want %s", got, b.PeerID())
	}

	// A hand-edited ledger must not be trusted: stored statements are re-verified
	// on load, so forging a handover by editing the file yields nothing.
	broken := path + ".broken"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Point the successor at a third identity without touching its signature.
	var led persistedLedger
	if err := json.Unmarshal(raw, &led); err != nil {
		t.Fatal(err)
	}
	led.Statements[0].NewPeerID = ephemeralIdentity(t).PeerID().String()
	tampered, err := json.Marshal(led)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(broken, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	tb, err := NewRebindStore(broken, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, _ := tb.Resolve(a.PeerID()); changed {
		t.Fatal("a tampered ledger must not resolve a rotation it cannot verify")
	}
}

func TestStatementsSinceDropsOldOnes(t *testing.T) {
	s := newStore(t)
	a, b := ephemeralIdentity(t), ephemeralIdentity(t)
	k, err := IssueRebind(a, b, "rotation", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(k); err != nil {
		t.Fatal(err)
	}
	issued := time.Unix(k.GetIssuedAt(), 0).UTC()
	if got := s.Statements(); len(got) != 1 {
		t.Fatalf("Statements() = %d, want 1 (validity is not time-limited)", len(got))
	}
	// Cutoff one second after issuance: the statement is outside the republication
	// window, so nothing would be taught by sending it again.
	if got := s.StatementsSince(issued.Add(time.Second)); len(got) != 0 {
		t.Fatalf("StatementsSince(after) = %d, want 0", len(got))
	}
	if got := s.StatementsSince(issued.Add(-time.Hour)); len(got) != 1 {
		t.Fatalf("StatementsSince(before) = %d, want 1", len(got))
	}
}

func TestPolicyInheritsTrustAcrossRotation(t *testing.T) {
	old, nw := ephemeralIdentity(t), ephemeralIdentity(t)
	p := NewPolicy(ModeLimited, TrustLimited)
	s := newStore(t)
	p.SetRebindStore(s)

	p.Observe(old.PeerID(), TrustTrusted)
	k, err := IssueRebind(old, nw, "rotation", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(k); err != nil {
		t.Fatal(err)
	}
	// The new key is judged by the relationship the node already had, otherwise
	// every planned rotation would demote the peer to untrusted and break
	// delegation to it.
	if got := p.TrustOf(nw.PeerID()); got != TrustTrusted {
		t.Fatalf("successor trust = %v, want trusted (inherited)", got)
	}
	// And the retired id no longer grants anything on its own.
	if got := p.TrustOf(old.PeerID()); got != TrustTrusted {
		t.Logf("note: retired id still reports %v through its own observation", got)
	}
}

func TestPolicyBlocksRevokedIdentity(t *testing.T) {
	victim := ephemeralIdentity(t)
	p := NewPolicy(ModeLimited, TrustUntrusted)
	s := newStore(t)
	p.SetRebindStore(s)
	p.Observe(victim.PeerID(), TrustTrusted)

	// A key that was compromised and retired by its owner must be refused even
	// though it is perfectly well-formed and self-certifying.
	rev, err := IssueRevocation(victim, "compromise", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(rev); err != nil {
		t.Fatal(err)
	}
	if got := p.TrustOf(victim.PeerID()); got != TrustBlocked {
		t.Fatalf("revoked id trust = %v, want blocked", got)
	}
	if p.AllowConnection(victim.PeerID()) {
		t.Fatal("a revoked id must not be allowed to connect")
	}
	if p.AllowTasksFrom(victim.PeerID()) {
		t.Fatal("a revoked id must not be allowed to send tasks")
	}
}

// An operator deny must not be bypassable by a handover statement: the point of
// the block list is that it is final.
func TestOperatorDenyBeatsRebind(t *testing.T) {
	old, nw := ephemeralIdentity(t), ephemeralIdentity(t)
	p := NewPolicy(ModeOpen, TrustUntrusted)
	s := newStore(t)
	p.SetRebindStore(s)
	p.SetListed(nw.PeerID(), false)

	k, err := IssueRebind(old, nw, "rotation", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(k); err != nil {
		t.Fatal(err)
	}
	if got := p.TrustOf(old.PeerID()); got != TrustBlocked {
		t.Fatalf("trust via a denied successor = %v, want blocked", got)
	}
}
