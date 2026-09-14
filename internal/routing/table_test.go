package routing

import (
	"crypto/rand"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/storage"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func peerID(t *testing.T) peer.ID {
	t.Helper()
	k, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := peer.IDFromPublicKey(k.GetPublic())
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func openStore(t *testing.T) *storage.Store {
	t.Helper()
	st, err := storage.Open(storage.Options{
		InMemory: true, ArtifactsDir: filepath.Join(t.TempDir(), "artifacts"),
	})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// The neighbour table cannot express trust at all.
//
// Trust is a judgement this node makes about a peer, and security.Policy is the
// only thing that computes it. When Neighbor carried a copy, every
// observation-shaped Upsert — "these are the peer's skills", "it answered on
// this address" — left the zero value in place, and the zero value of
// security.Trust is TrustTrusted, the most privileged level. Since peer ids are
// self-certifying, a gossiped stranger was promoted to trusted by whichever
// bookkeeping write touched it last, and the promotion was persisted: the
// Restore rule "trust is re-earned per process" was undone by the first partial
// upsert after a restart.
//
// TestRestoreDoesNotResurrectTrust is the surviving half of that story: what
// comes back from disk is the history (skills, epoch, scores), never a standing.
func TestRestoreDoesNotResurrectTrust(t *testing.T) {
	st := openStore(t)
	pid := peerID(t)

	// A policy that believes this peer is the network's most trusted node.
	trusted := security.NewPolicy(security.ModeLimited, security.TrustTrusted)
	trusted.SetListed(pid, true)
	trusted.Observe(pid, security.TrustTrusted)

	write := NewTable(config.NeighborsConfig{}, st, quietLog(), trusted)
	nb := &Neighbor{
		PeerID: pid, Addrs: []string{"/ip4/127.0.0.1/tcp/4001"}, Skills: []string{"coding"},
		Connected: true, LastSeen: time.Now().UTC(),
	}
	write.Upsert(nb)
	if got := write.TrustOf(pid); got != security.TrustTrusted {
		t.Fatalf("with the operator's allow entry, trust = %s, want trusted", got)
	}

	recs, err := st.ListPeers()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("persisted peers = %d, want 1", len(recs))
	}
	// The persisted record carries the standing as a diagnostic label only.
	if recs[0].Trust != security.TrustTrusted.String() {
		t.Fatalf("persisted label = %q, want trusted", recs[0].Trust)
	}

	// A fresh process starts without the operator's entry: the policy has to
	// re-earn the peer, so the same disk contents must yield no trust at all.
	restored := NewTable(config.NeighborsConfig{}, st, quietLog(),
		security.NewPolicy(security.ModeLimited, security.TrustTrusted))
	if _, err := restored.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got, ok := restored.Get(pid)
	if !ok {
		t.Fatal("peer must survive Restore")
	}
	if tr := restored.TrustOf(pid); tr != security.TrustUntrusted {
		t.Fatalf("restored trust = %s, want untrusted (trust is re-earned per process)", tr)
	}
	if got.Connected {
		t.Fatal("restored connected = true, want false (connections do not survive a restart)")
	}
	// Everything that is genuinely history must still be carried forward.
	if len(got.Skills) != 1 || got.Skills[0] != "coding" {
		t.Fatalf("restored skills = %v, want [coding]", got.Skills)
	}
}

// A peer that is an operator stranger must never be scored as trusted, however
// many partial observations accumulate about it. This is the regression: the
// writes below are all bookkeeping, and before the fix each one was also,
// silently, a promotion.
func TestPartialObservationsNeverPromoteAPeer(t *testing.T) {
	st := openStore(t)
	pid := peerID(t)

	// Open mode so the peer is a candidate at all: the point is what it is
	// *scored* as, and a posture that refuses strangers would hide that.
	tbl := NewTable(config.NeighborsConfig{}, st, quietLog(),
		security.NewPolicy(security.ModeOpen, security.TrustUntrusted))
	// The gossip/research/registry paths as they actually call in: a peer id and
	// one field of news about it, nothing about the relationship.
	tbl.Upsert(&Neighbor{PeerID: pid, Skills: []string{"coding"}})
	tbl.Upsert(&Neighbor{PeerID: pid, Addrs: []string{"/ip4/127.0.0.1/tcp/4001"}})
	tbl.Upsert(&Neighbor{PeerID: pid, SkillsVersion: 3})
	tbl.SetConnected(pid, true)
	tbl.RecordRTT(pid, time.Millisecond)
	tbl.RecordSuccess(pid)

	if got := tbl.TrustOf(pid); got != security.TrustUntrusted {
		t.Fatalf("after 6 partial observations trust = %s, want untrusted", got)
	}
	// Select is where a promoted stranger did damage: scored on trust it never
	// earned, and offered as a delegate ahead of a peer that did.
	cands := tbl.Select([]string{"coding"}, tbl.policy, "", 5, nil)
	if len(cands) != 1 {
		t.Fatalf("candidates = %d, want 1", len(cands))
	}
	var quoted string
	for _, r := range cands[0].Reasons {
		if strings.HasPrefix(r, "trust=") {
			quoted = r
		}
	}
	if quoted != "trust=untrusted" {
		t.Fatalf("Select scored an operator stranger as %q, want its real standing", quoted)
	}

	// A verified handshake is what actually earns standing, and the table must
	// reflect it at once — proving the score reads the policy, not a stale copy.
	tbl.policy.Observe(pid, security.TrustKnown)
	if got := tbl.TrustOf(pid); got != security.TrustKnown {
		t.Fatalf("after the handshake trust = %s, want known", got)
	}
	known := tbl.Select([]string{"coding"}, tbl.policy, "", 5, nil)
	if len(known) != 1 || known[0].Score <= cands[0].Score {
		t.Fatalf("a peer that earned trust is not scored above a stranger: %v vs %v",
			known, cands[0].Score)
	}

	// And the operator's block is visible immediately, without any write: the
	// table answers from the policy, so it cannot lag behind a decision.
	tbl.policy.SetListed(pid, false)
	if got := tbl.TrustOf(pid); got != security.TrustBlocked {
		t.Fatalf("after a block trust = %s, want blocked", got)
	}
	if len(tbl.Select([]string{"coding"}, tbl.policy, "", 5, nil)) != 0 {
		t.Fatal("a blocked peer is still offered as a delegation candidate")
	}
}

// Skill documentation is knowledge, not a relationship: unlike trust it must
// survive a restart so the node does not re-sync what it already holds.
func TestRestoreKeepsSkillsVersion(t *testing.T) {
	st := openStore(t)
	pid := peerID(t)

	write := NewTable(config.NeighborsConfig{}, st, quietLog(), nil)
	write.Upsert(&Neighbor{PeerID: pid, Skills: []string{"research"}, SkillsVersion: 7})

	restored := NewTable(config.NeighborsConfig{}, st, quietLog(), nil)
	if _, err := restored.Restore(); err != nil {
		t.Fatal(err)
	}
	got, ok := restored.Get(pid)
	if !ok || got.SkillsVersion != 7 {
		t.Fatalf("restored epoch = %d (ok=%v), want 7", got.SkillsVersion, ok)
	}
}
