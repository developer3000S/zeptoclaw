package discovery

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/p2p"
	"github.com/developer3000S/zeptoclaw/internal/security"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testIdentity(t *testing.T) *security.Identity {
	t.Helper()
	id, err := security.NewEphemeral()
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	return id
}

func testHost(t *testing.T) *p2p.Host {
	t.Helper()
	cfg := config.Default()
	cfg.Node.Listen = []string{"/ip4/127.0.0.1/tcp/0"}
	cfg.Discovery.DHT = false
	h, err := p2p.New(context.Background(), p2p.Options{
		Config: cfg, Logger: quietLogger(), Key: testIdentity(t).PrivKey(),
	})
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func gossipCfg() config.GossipConfig {
	return config.GossipConfig{
		Enabled: true, Topic: "/zeptomesh/test/0.1.0",
		Heartbeat: config.Duration(50 * time.Millisecond),
		FullSync:  config.Duration(time.Hour), FailureTimeout: config.Duration(30 * time.Second),
	}
}

func openAudit(t *testing.T, name string) *security.Audit {
	t.Helper()
	a, err := security.OpenAudit(filepath.Join(t.TempDir(), name), quietLogger())
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// limitedPolicy trusts nobody, so "may this relay route for us" stays a real
// question instead of a formality.
func limitedPolicy() *security.Policy {
	return security.NewPolicy(security.ModeLimited, security.TrustKnown)
}

func newMembership(t *testing.T, h *p2p.Host, audit string) *Membership {
	t.Helper()
	au := openAudit(t, audit)
	ps, err := NewPubSub(context.Background(), h.Underlying(), limitedPolicy(), au, quietLogger())
	if err != nil {
		t.Fatalf("pubsub: %v", err)
	}
	m, err := NewMembership(context.Background(), h.Underlying(), ps, gossipCfg(),
		limitedPolicy(), au, quietLogger())
	if err != nil {
		t.Fatalf("membership: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func selfState(h *p2p.Host) func() *pb.PeerState {
	return func() *pb.PeerState {
		return &pb.PeerState{PeerId: h.ID().String(), Addrs: h.Addrs(), Timestamp: time.Now().UTC().Unix()}
	}
}

func rebindOf(t *testing.T) (oldID, newID *security.Identity, statement *pb.KeyRebind) {
	t.Helper()
	oldID, newID = testIdentity(t), testIdentity(t)
	k, err := security.IssueRebind(oldID, newID, "rotation", 1)
	if err != nil {
		t.Fatalf("issue rebind: %v", err)
	}
	return oldID, newID, k
}

func awaitRebind(t *testing.T, ch <-chan *pb.KeyRebind) *pb.KeyRebind {
	t.Helper()
	select {
	case k := <-ch:
		return k
	case <-time.After(15 * time.Second):
		t.Fatal("no handover statement arrived")
	}
	return nil
}

// TestGossipCarriesRebindToUnacquaintedPeer is the property the rotation design
// rests on: a node that never met either identity accepts the handover because
// the statement proves its own authorship.
func TestGossipCarriesRebindToUnacquaintedPeer(t *testing.T) {
	a, b := testHost(t), testHost(t)
	ctx := context.Background()
	oldID, newID, statement := rebindOf(t)

	mA, mB := newMembership(t, a, "a.jsonl"), newMembership(t, b, "b.jsonl")
	got := make(chan *pb.KeyRebind, 8)
	// B holds no statements and trusts nobody; A republishes the handover with
	// its heartbeats.
	mB.SetRebindSource(func() []*pb.KeyRebind { return nil }, func(k *pb.KeyRebind) { got <- k })
	mA.SetRebindSource(func() []*pb.KeyRebind { return []*pb.KeyRebind{statement} },
		func(*pb.KeyRebind) { t.Error("A must not receive its own statement back") })
	mA.SetSelfStateFunc(selfState(a))
	mB.SetSelfStateFunc(selfState(b))

	if err := mA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := mB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Connect(ctx, b.AddrInfo()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	k := awaitRebind(t, got)
	if k.GetOldPeerId() != oldID.PeerID().String() || k.GetNewPeerId() != newID.PeerID().String() {
		t.Fatalf("B adopted %s -> %s, want %s -> %s",
			k.GetOldPeerId(), k.GetNewPeerId(), oldID.PeerID(), newID.PeerID())
	}
	// The handover arrived from a peer that is not routing-trusted. A third-party
	// PeerState under the same conditions would have been refused, which is the
	// asymmetry that makes rotation operator-free.
	if _, ok := mB.State(newID.PeerID()); ok {
		t.Fatal("the successor must not appear as a routing state; only the handover is learned")
	}
}

// TestIngestAppliesRebindWithoutUsableStates pins the ordering inside ingest: a
// message whose PeerStates were all refused must still deliver its handovers.
func TestIngestAppliesRebindWithoutUsableStates(t *testing.T) {
	h, relay := testHost(t), testHost(t)
	_, newID, statement := rebindOf(t)
	m := newMembership(t, h, "ingest.jsonl")

	got := make(chan *pb.KeyRebind, 4)
	m.SetRebindSource(func() []*pb.KeyRebind { return nil }, func(k *pb.KeyRebind) { got <- k })

	// Untrusted relay carrying a third-party state (must be dropped) plus a valid
	// handover (must be kept).
	raw, err := proto.Marshal(&pb.MembershipGossip{
		States: []*pb.PeerState{{
			PeerId: newID.PeerID().String(), Addrs: relay.Addrs(),
			Timestamp: time.Now().UTC().Unix(),
		}},
		FromPeerId: relay.ID().String(),
		Rebinds:    []*pb.KeyRebind{statement},
	})
	if err != nil {
		t.Fatal(err)
	}
	m.ingest(relay.ID(), raw)

	select {
	case k := <-got:
		if k.GetOldPeerId() != statement.GetOldPeerId() {
			t.Fatalf("wrong statement adopted: %s", k.GetOldPeerId())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a handover in a message with unusable states must still be applied")
	}
	if _, ok := m.State(newID.PeerID()); ok {
		t.Fatal("third-party state from an untrusted relay must not be stored")
	}
}

// TestIngestDropsForgedRebind: rewriting the successor breaks the double
// signature, so a forged handover never reaches the ledger.
func TestIngestDropsForgedRebind(t *testing.T) {
	h, relay, stranger := testHost(t), testHost(t), testHost(t)
	_, _, statement := rebindOf(t)
	m := newMembership(t, h, "forged.jsonl")

	got := make(chan *pb.KeyRebind, 4)
	m.SetRebindSource(func() []*pb.KeyRebind { return nil }, func(k *pb.KeyRebind) { got <- k })

	forged := proto.Clone(statement).(*pb.KeyRebind)
	forged.NewPeerId = stranger.ID().String()
	for _, k := range []*pb.KeyRebind{forged, statement} {
		raw, err := proto.Marshal(&pb.MembershipGossip{
			Rebinds: []*pb.KeyRebind{k}, FromPeerId: relay.ID().String()})
		if err != nil {
			t.Fatal(err)
		}
		m.ingest(relay.ID(), raw)
	}
	k := awaitRebind(t, got)
	if k.GetOldPeerId() != statement.GetOldPeerId() {
		t.Fatalf("only the valid statement may be adopted, got %s", k.GetOldPeerId())
	}
	select {
	case extra := <-got:
		t.Fatalf("forged statement was also adopted: %s/%s", extra.GetOldPeerId(), extra.GetNewPeerId())
	case <-time.After(300 * time.Millisecond):
	}
}

// TestIngestDropsStaleRebind bounds adoption by age: an arbitrarily old
// statement is history, and accepting it would let a captured message re-open a
// long-retired identity.
func TestIngestDropsStaleRebind(t *testing.T) {
	h, relay := testHost(t), testHost(t)
	oldID, newID, _ := rebindOf(t)
	m := newMembership(t, h, "stale.jsonl")

	// Validity must not be in question: issued_at is signed over, so a stale
	// statement has to be issued with its timestamp from the start.
	stale, err := security.IssueRebindAt(oldID, newID, "rotation", 1,
		time.Now().UTC().Add(-(rebindMaxAgeSeconds+3600)*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := security.VerifyRebind(stale, nil); err != nil {
		t.Fatalf("the statement must be valid — only its age is wrong: %v", err)
	}
	got := make(chan *pb.KeyRebind, 4)
	m.SetRebindSource(func() []*pb.KeyRebind { return nil }, func(k *pb.KeyRebind) { got <- k })

	raw, err := proto.Marshal(&pb.MembershipGossip{
		Rebinds: []*pb.KeyRebind{stale}, FromPeerId: relay.ID().String()})
	if err != nil {
		t.Fatal(err)
	}
	m.ingest(relay.ID(), raw)
	select {
	case k := <-got:
		t.Fatalf("a stale or signature-broken statement must not be adopted: %s", k.GetOldPeerId())
	case <-time.After(300 * time.Millisecond):
	}
}

// TestIngestDropsFutureRebind covers the other side of the age bound.
func TestIngestDropsFutureRebind(t *testing.T) {
	h, relay := testHost(t), testHost(t)
	oldID, newID, _ := rebindOf(t)
	m := newMembership(t, h, "future.jsonl")

	ahead, err := security.IssueRebindAt(oldID, newID, "rotation", 1,
		time.Now().UTC().Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := security.VerifyRebind(ahead, nil); err != nil {
		t.Fatalf("the statement is valid; only its clock is wrong: %v", err)
	}

	got := make(chan *pb.KeyRebind, 4)
	m.SetRebindSource(func() []*pb.KeyRebind { return nil }, func(k *pb.KeyRebind) { got <- k })

	raw, err := proto.Marshal(&pb.MembershipGossip{
		Rebinds: []*pb.KeyRebind{ahead}, FromPeerId: relay.ID().String()})
	if err != nil {
		t.Fatal(err)
	}
	m.ingest(relay.ID(), raw)
	select {
	case k := <-got:
		t.Fatalf("a statement from the future must not be adopted: %s", k.GetOldPeerId())
	case <-time.After(300 * time.Millisecond):
	}
}

// TestGossipCarriesRevocationSeparately checks that the revocation field reaches
// a peer too, since a node retired without a successor is the case where
// refusing matters most.
func TestGossipCarriesRevocationSeparately(t *testing.T) {
	a, b := testHost(t), testHost(t)
	ctx := context.Background()
	victim := testIdentity(t)
	rev, err := security.IssueRevocation(victim, "compromise", 1)
	if err != nil {
		t.Fatal(err)
	}

	mA, mB := newMembership(t, a, "ra.jsonl"), newMembership(t, b, "rb.jsonl")
	got := make(chan *pb.KeyRebind, 8)
	mB.SetRebindSource(func() []*pb.KeyRebind { return nil }, func(k *pb.KeyRebind) { got <- k })
	// Revocations ride their own field, matching how PublishFull splits them.
	mA.SetRebindSource(func() []*pb.KeyRebind { return nil }, nil)
	mA.SetSelfStateFunc(selfState(a))
	mB.SetSelfStateFunc(selfState(b))
	if err := mA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := mB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Connect(ctx, b.AddrInfo()); err != nil {
		t.Fatal(err)
	}
	raw, err := proto.Marshal(&pb.MembershipGossip{
		Revocations: []*pb.KeyRebind{rev}, FromPeerId: a.ID().String()})
	if err != nil {
		t.Fatal(err)
	}
	mB.ingest(a.ID(), raw)

	k := awaitRebind(t, got)
	if k.GetNewPeerId() != "" {
		t.Fatalf("a revocation has no successor, got %s", k.GetNewPeerId())
	}
	if k.GetOldPeerId() != victim.PeerID().String() {
		t.Fatalf("revoked %s, want %s", k.GetOldPeerId(), victim.PeerID())
	}
}
