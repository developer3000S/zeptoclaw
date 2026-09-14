package storage

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

func openStore(t *testing.T) *Store {
	t.Helper()
	// badger logs to stdout by default; a test suite wants its own output only.
	s, err := Open(Options{InMemory: true, ArtifactsDir: filepath.Join(t.TempDir(), "art"), Logger: quietLogger{}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

type quietLogger struct{}

func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Warningf(string, ...any) {
}
func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Debugf(string, ...any) {}

// The reverse index must describe what a peer advertises *now*, not what it has
// ever advertised. A one-way index is the bug this pins: a peer that drops a
// skill stays discoverable for it forever, and nothing ever reclaims the key —
// Prune touches only the task journal.
func TestPutPeerRebuildsSkillIndex(t *testing.T) {
	s := openStore(t)

	if err := s.PutPeer(&PeerRecord{PeerID: "p1", Skills: []string{"ocr", "coding"}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := s.PeersBySkill("ocr")
	if err != nil || len(got) != 1 || got[0] != "p1" {
		t.Fatalf("after first write: peers(ocr) = %v, err %v", got, err)
	}

	// Same peer, one skill fewer: the dropped skill must stop naming it.
	if err := s.PutPeer(&PeerRecord{PeerID: "p1", Skills: []string{"coding"}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got, _ := s.PeersBySkill("ocr"); len(got) != 0 {
		t.Fatalf("stale ocr index after the peer stopped advertising it: %v", got)
	}
	if got, _ := s.PeersBySkill("coding"); len(got) != 1 {
		t.Fatalf("coding index lost its peer: %v", got)
	}

	// No skills at all empties the peer's index entries but keeps its record.
	if err := s.PutPeer(&PeerRecord{PeerID: "p1"}); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if got, _ := s.PeersBySkill("coding"); len(got) != 0 {
		t.Fatalf("cleared peer still indexed: %v", got)
	}
	if rec, err := s.GetPeer("p1"); err != nil || rec == nil {
		t.Fatalf("peer record vanished with its index: %v", err)
	}
}

// Two peers under one skill, and one peer losing it, must leave the other
// intact: the rebuild is per peer, not per skill.
func TestSkillIndexIsScopedToOnePeer(t *testing.T) {
	s := openStore(t)
	if err := s.PutPeer(&PeerRecord{PeerID: "p1", Skills: []string{"ocr"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutPeer(&PeerRecord{PeerID: "p2", Skills: []string{"ocr"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutPeer(&PeerRecord{PeerID: "p1"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.PeersBySkill("ocr")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "p2" {
		t.Fatalf("peers(ocr) = %v, want only p2", got)
	}
}

// Every other skill comparison in the mesh normalises (tasks.SkillsMatch
// lowercases and trims). An index that stores names verbatim disagrees with
// routing: "OCR" on the wire would be invisible to a lookup for "ocr".
func TestSkillIndexNormalizesNames(t *testing.T) {
	s := openStore(t)
	if err := s.PutPeer(&PeerRecord{PeerID: "p1", Skills: []string{"  OCR  ", "Coding"}}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		skill string
		n     int
	}{
		{"ocr", 1}, {"OCR", 1}, {" ocr ", 1}, {"coding", 1}, {"missing", 0},
	} {
		got, err := s.PeersBySkill(want.skill)
		if err != nil {
			t.Fatalf("peers(%q): %v", want.skill, err)
		}
		if len(got) != want.n {
			t.Fatalf("peers(%q) = %v, want %d", want.skill, got, want.n)
		}
	}
}

// An empty skill name would create the key "s:/<peer>", matching every lookup
// prefix that starts with a slash. It must not be indexed at all.
func TestSkillIndexSkipsEmptySkillNames(t *testing.T) {
	s := openStore(t)
	if err := s.PutPeer(&PeerRecord{PeerID: "p1", Skills: []string{"", "   ", "ocr"}}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.PeersBySkill(""); err != nil || len(got) != 0 {
		t.Fatalf("empty skill indexed as %v (err %v)", got, err)
	}
	// The junk key must not exist even under a prefix scan.
	var junk []string
	if err := s.iterate(pfxSkill, func(k, _ []byte) error {
		junk = append(junk, string(k))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, k := range junk {
		if strings.HasPrefix(k, pfxSkill+"/") {
			t.Fatalf("junk key with an empty skill name: %q (all: %v)", k, junk)
		}
	}
	if len(junk) != 1 || junk[0] != "s:ocr/p1" {
		t.Fatalf("index keys = %v, want exactly [s:ocr/p1]", junk)
	}
}

// A record stored by an older build (index keyed verbatim, e.g. "s:OCR/p1")
// must be reclaimed by the first write after the upgrade, leaving exactly one
// normalised key. This is the only reachable legacy shape: the old PutPeer
// always wrote the record and its index together.
func TestPutPeerCleansLegacyCaseVariant(t *testing.T) {
	s := openStore(t)
	// Simulate the pre-normalisation write by hand: record + verbatim index key.
	if err := s.putJSON(pfxPeer+"p1", &PeerRecord{PeerID: "p1", Skills: []string{"OCR"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("s:OCR/p1"), []byte{1})
	}); err != nil {
		t.Fatal(err)
	}
	// The legacy verbatim key is already unreachable through a normalised query
	// — that is precisely the garbage the rewrite has to reclaim.
	if got, _ := s.PeersBySkill("ocr"); len(got) != 0 {
		t.Fatalf("normalised lookup matched the verbatim legacy key: %v", got)
	}

	if err := s.PutPeer(&PeerRecord{PeerID: "p1", Skills: []string{"OCR"}}); err != nil {
		t.Fatal(err)
	}
	var keys []string
	if err := s.iterate(pfxSkill, func(k, _ []byte) error {
		keys = append(keys, string(k))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "s:ocr/p1" {
		t.Fatalf("index keys = %v, want the single normalised [s:ocr/p1]", keys)
	}
	if got, _ := s.PeersBySkill("ocr"); len(got) != 1 {
		t.Fatalf("lookups after the rewrite: %v", got)
	}
}

// A dropped skill whose record is unreadable must not wedge writes: the
// rewrite still lands, and the peer is simply re-indexed from the new record.
func TestPutPeerSurvivesUnreadablePreviousRecord(t *testing.T) {
	s := openStore(t)
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(pfxPeer+"p1"), []byte("not json"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutPeer(&PeerRecord{PeerID: "p1", Skills: []string{"ocr"}}); err != nil {
		t.Fatalf("write over a corrupt record: %v", err)
	}
	if got, _ := s.PeersBySkill("ocr"); len(got) != 1 || got[0] != "p1" {
		t.Fatalf("peer not re-indexed after the rewrite: %v", got)
	}
}

func TestPutPeerRejectsUselessRecords(t *testing.T) {
	s := openStore(t)
	if err := s.PutPeer(nil); err == nil {
		t.Fatal("nil record accepted")
	}
	if err := s.PutPeer(&PeerRecord{}); err == nil {
		t.Fatal("record without a peer id accepted")
	}
}

// ТЗ 6.6.5: dedup is a claim on the task id for a window. The first delivery
// executes, the rest do not, and the window restarts from the first sighting —
// so a late duplicate cannot be mistaken for a fresh task.
func TestClaimDedupWindow(t *testing.T) {
	s := openStore(t)
	const win = time.Minute

	first, prev, err := s.ClaimDedup("t1", win)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !first {
		t.Fatal("the first delivery was told it is a duplicate")
	}
	if prev.Count != 0 {
		t.Fatalf("previous count = %d, want 0 for a new id", prev.Count)
	}

	for i := 0; i < 3; i++ {
		again, p, err := s.ClaimDedup("t1", win)
		if err != nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
		if again {
			t.Fatalf("repeat %d re-executed a task inside the dedup window", i)
		}
		// The returned record is the stored claim as it stood before this
		// delivery: Count counts claims of this id, not deliveries of it.
		if p.Count != 1 {
			t.Fatalf("repeat %d: stored count = %d, want 1 (a duplicate must not re-claim)", i, p.Count)
		}
		if p.FirstSeenUnix == 0 {
			t.Fatalf("repeat %d: first-seen stamp lost", i)
		}
	}

	// Once the window expires the id is claimable again, which is what lets a
	// resubmitted task (new delivery of a journal entry) run after 15 minutes
	// rather than being suppressed forever.
	if ok, p, err := s.ClaimDedup("t1", time.Millisecond); err != nil || !ok {
		t.Fatalf("claim after the window must succeed: claimed=%v err=%v", ok, err)
	} else if p.Count != 1 {
		t.Fatalf("re-claim: previous count = %d, want the 1 from the expired claim", p.Count)
	}
	if ok, p, err := s.ClaimDedup("t1", time.Hour); err != nil {
		t.Fatal(err)
	} else if ok || p.Count != 2 {
		t.Fatalf("re-claim inside the fresh window: claimed=%v count=%d", ok, p.Count)
	}

	// A different task id is never suppressed by another one's claim.
	other, _, err := s.ClaimDedup("t2", win)
	if err != nil || !other {
		t.Fatalf("unrelated id suppressed: claimed=%v err=%v", other, err)
	}

	if _, _, err := s.ClaimDedup("", win); err == nil {
		t.Fatal("empty task id claimed")
	}
}

// Zero window means "no protection": every delivery is a fresh claim. That is
// how an operator turns dedup off, so it must not become "never execute".
func TestClaimDedupZeroWindowClaimsEveryTime(t *testing.T) {
	s := openStore(t)
	for i := 0; i < 2; i++ {
		ok, _, err := s.ClaimDedup("t1", 0)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("delivery %d suppressed with a zero window", i)
		}
	}
}

func TestTaskAndResultRoundTrip(t *testing.T) {
	s := openStore(t)
	now := time.Now().UTC()
	rec := &TaskRecord{
		TaskID: "t1", OriginPeerID: "o", SenderPeerID: "o", Status: "RUNNING",
		RequiredSkills: []string{"ocr"}, ReceivedAt: now, Attempts: 1, TTL: 60, Priority: 5,
	}
	if err := s.PutTask(rec); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := s.GetTask("t1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.UpdatedAt == 0 {
		t.Fatal("UpdatedAt not stamped: ListTasks orders by it")
	}
	if got.Status != "RUNNING" || len(got.RequiredSkills) != 1 {
		t.Fatalf("round trip changed the record: %+v", got)
	}

	if err := s.PutResult(&ResultRecord{TaskID: "t1", Status: "COMPLETED", Text: "done", Model: "llm-a"}); err != nil {
		t.Fatalf("put result: %v", err)
	}
	res, err := s.GetResult("t1")
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	if res.Model != "llm-a" {
		t.Fatalf("the model answered by this node did not survive the journal: %+v", res)
	}

	for _, miss := range []func() error{
		func() error { _, err := s.GetTask("nope"); return err },
		func() error { _, err := s.GetResult("nope"); return err },
		func() error { _, err := s.GetPeer("nope"); return err },
	} {
		if err := miss(); !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	}
}

func TestPutRejectsRecordsWithoutID(t *testing.T) {
	s := openStore(t)
	for name, err := range map[string]error{
		"task nil":   s.PutTask(nil),
		"task no id": s.PutTask(&TaskRecord{}),
		"result":     s.PutResult(&ResultRecord{}),
		"result nil": s.PutResult(nil),
	} {
		if err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
}

// The admin list is the operator's view of what is stuck, so ordering is part
// of the contract: newest first, and the limit must cut the *oldest*.
func TestListTasksOrdersNewestFirstAndFilters(t *testing.T) {
	s := openStore(t)
	for i, st := range []string{"COMPLETED", "FAILED", "RUNNING", "FAILED"} {
		rec := &TaskRecord{TaskID: string(rune('a' + i)), Status: st}
		if err := s.PutTask(rec); err != nil {
			t.Fatal(err)
		}
		// UpdatedAt is stamped from the clock; space the writes out so the order
		// under test is unambiguous.
		if i < 3 {
			time.Sleep(1100 * time.Millisecond)
		}
	}
	all, err := s.ListTasks(0, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 {
		t.Fatalf("listed %d, want 4", len(all))
	}
	if all[0].TaskID != "d" || all[3].TaskID != "a" {
		t.Fatalf("order = %v, want newest first", idsOf(all))
	}
	failed, err := s.ListTasks(0, "FAILED")
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 2 {
		t.Fatalf("FAILED filter returned %v", idsOf(failed))
	}
	limited, err := s.ListTasks(1, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 1 || limited[0].TaskID != "d" {
		t.Fatalf("limit cut the wrong end: %v", idsOf(limited))
	}
}

func idsOf(recs []*TaskRecord) []string {
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.TaskID)
	}
	return out
}

// Subtask edges are how a restarted node finds children it still owes an
// aggregation for, so both directions have to survive the write.
func TestLinkChildEdgesAreBidirectional(t *testing.T) {
	s := openStore(t)
	for _, c := range []string{"c1", "c2"} {
		if err := s.LinkChild("p1", c); err != nil {
			t.Fatalf("link %s: %v", c, err)
		}
	}
	children, err := s.ChildrenOf("p1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(children, ",") != "c1,c2" {
		t.Fatalf("children = %v", children)
	}
	parent, err := s.ParentOf("c2")
	if err != nil || parent != "p1" {
		t.Fatalf("parent of c2 = %q, err %v", parent, err)
	}
	if _, err := s.ParentOf("orphan"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphan parent: %v", err)
	}
	if _, err := s.ChildrenOf("none"); err != nil {
		t.Fatalf("parent without children must list empty, not fail: %v", err)
	}
}

// Content-addressed storage must be idempotent (the same bytes are one object),
// reject a hash it cannot resolve, and never let a crafted hash escape the root.
func TestArtifactsAreContentAddressed(t *testing.T) {
	s := openStore(t)
	data := []byte("the answer")
	hash, size, err := s.StoreArtifact(data)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("size = %d", size)
	}
	if !strings.HasPrefix(hash, "sha256:") || len(hash) != len("sha256:")+64 {
		t.Fatalf("hash shape wrong: %q", hash)
	}
	again, _, err := s.StoreArtifact(data)
	if err != nil || again != hash {
		t.Fatalf("same bytes got a second identity: %q vs %q (err %v)", again, hash, err)
	}
	back, err := s.LoadArtifact(hash)
	if err != nil || !bytes.Equal(back, data) {
		t.Fatalf("round trip: %q, err %v", back, err)
	}

	other, _, err := s.StoreArtifact([]byte("different"))
	if err != nil || other == hash {
		t.Fatalf("different bytes share a hash: %q vs %q", other, hash)
	}

	if _, err := s.LoadArtifact("sha256:deadbeef"); err == nil {
		t.Fatal("malformed hash resolved")
	}
	if _, err := s.LoadArtifact("md5:" + strings.Repeat("a", 32)); err == nil {
		t.Fatal("hash of another algorithm resolved")
	}
	if p, err := s.ArtifactPath("../../escape"); err == nil {
		t.Fatalf("crafted hash escaped the root: %q", p)
	}
	if _, err := s.LoadArtifact("sha256:" + strings.Repeat("f", 64)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("absent artifact: %v", err)
	}
}

func TestMarkSeenOnlyForKnownPeers(t *testing.T) {
	s := openStore(t)
	if err := s.MarkSeen("ghost"); err != nil {
		t.Fatalf("MarkSeen on an unknown peer failed: %v", err)
	}
	if err := s.PutPeer(&PeerRecord{PeerID: "p1"}); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetPeer("p1")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := s.MarkSeen("p1"); err != nil {
		t.Fatal(err)
	}
	after, err := s.GetPeer("p1")
	if err != nil {
		t.Fatal(err)
	}
	if after.SeenAt <= before.SeenAt {
		t.Fatalf("SeenAt not refreshed: %d -> %d", before.SeenAt, after.SeenAt)
	}
}

func TestMetaRoundTrip(t *testing.T) {
	s := openStore(t)
	if _, err := s.GetMeta("watermark"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("fresh key: %v", err)
	}
	if err := s.PutMeta("watermark", []byte("42")); err != nil {
		t.Fatal(err)
	}
	v, err := s.GetMeta("watermark")
	if err != nil || string(v) != "42" {
		t.Fatalf("meta = %q, err %v", v, err)
	}
}

// Retention is what keeps a long-lived node's disk bounded: finished tasks go,
// everything still in flight stays, and its result goes with it.
func TestPruneDropsOnlyFinishedRecords(t *testing.T) {
	s := openStore(t)
	old := time.Now().UTC().Add(-48 * time.Hour)
	if err := s.PutTask(&TaskRecord{TaskID: "done", Status: "COMPLETED", FinishedAt: old}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutResult(&ResultRecord{TaskID: "done", Status: "COMPLETED"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutTask(&TaskRecord{TaskID: "running", Status: "RUNNING"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutTask(&TaskRecord{TaskID: "fresh", Status: "COMPLETED", FinishedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ClaimDedup("done", time.Hour); err != nil {
		t.Fatal(err)
	}

	removed, err := s.Prune(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if removed < 1 {
		t.Fatalf("removed = %d, want at least the finished task", removed)
	}
	if _, err := s.GetTask("done"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("finished task survived pruning: %v", err)
	}
	if _, err := s.GetResult("done"); !errors.Is(err, ErrNotFound) {
		t.Fatal("result outlived its task record")
	}
	if _, err := s.GetTask("running"); err != nil {
		t.Fatalf("unfinished task was pruned: %v", err)
	}
	if _, err := s.GetTask("fresh"); err != nil {
		t.Fatalf("recent task was pruned: %v", err)
	}
}

func TestStatsCountsRecords(t *testing.T) {
	s := openStore(t)
	if err := s.PutTask(&TaskRecord{TaskID: "t1", Status: "RUNNING"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutPeer(&PeerRecord{PeerID: "p1"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ClaimDedup("t1", time.Hour); err != nil {
		t.Fatal(err)
	}
	st, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Tasks != 1 || st.Peers != 1 || st.DedupKeys != 1 {
		t.Fatalf("stats = %+v, want 1/1/1", st)
	}
	if st.OpenedAt.IsZero() {
		t.Fatal("OpenedAt not set")
	}
}

// A disk-backed store must be re-readable: the peer table and the journal are
// the node's memory across a restart.
func TestStoreReopensFromDisk(t *testing.T) {
	dir := t.TempDir()
	art := filepath.Join(dir, "art")
	s, err := Open(Options{Dir: filepath.Join(dir, "db"), ArtifactsDir: art, Logger: quietLogger{}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.PutPeer(&PeerRecord{PeerID: "p1", Skills: []string{"ocr"}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.StoreArtifact([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := Open(Options{Dir: filepath.Join(dir, "db"), ArtifactsDir: art, Logger: quietLogger{}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if _, err := s2.GetPeer("p1"); err != nil {
		t.Fatalf("peer lost across restart: %v", err)
	}
	got, err := s2.PeersBySkill("ocr")
	if err != nil || len(got) != 1 {
		t.Fatalf("skill index lost across restart: %v, err %v", got, err)
	}
}

func TestOpenCreatesDirectories(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: filepath.Join(dir, "nested", "db"), ArtifactsDir: filepath.Join(dir, "nested", "art"), Logger: quietLogger{}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	// Writing an artifact proves the tree exists and is usable, not just created.
	if _, _, err := s.StoreArtifact([]byte("payload")); err != nil {
		t.Fatalf("artifact after nested-dir open: %v", err)
	}
}
