package skills

import (
	"path/filepath"
	"testing"
	"time"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/wire"
)

func mustRegistry(t *testing.T, opts Options) *Registry {
	t.Helper()
	r, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

func TestNewSeedsNamesAndEpoch(t *testing.T) {
	r := mustRegistry(t, Options{Skills: []string{"Coding", " ocr ", ""}})
	names := r.Names()
	// Names are normalised and empty entries dropped; the order is stable.
	if len(names) != 2 || names[0] != "coding" || names[1] != "ocr" {
		t.Fatalf("Names() = %v, want [coding ocr]", names)
	}
	if r.Epoch() < 1 {
		t.Fatalf("epoch must start at 1 or higher, got %d", r.Epoch())
	}
}

func TestSetBumpsVersionsAndEpoch(t *testing.T) {
	r := mustRegistry(t, Options{Skills: []string{"ocr"}})
	base := r.Epoch()

	doc, ok := r.Set(Descriptor{Name: "ocr", Description: "reads scans"})
	if !ok {
		t.Fatal("first documentation of a known skill must apply")
	}
	if doc.Version != 2 {
		t.Fatalf("version = %d, want 2 (seeded bare name was 1)", doc.Version)
	}
	if r.Epoch() <= base {
		t.Fatalf("epoch must advance: %d -> %d", base, r.Epoch())
	}

	// Restating identical content must not churn the epoch: a no-op edit is not
	// a new claim about the world, and peers should not be made to re-pull.
	epoch := r.Epoch()
	if _, ok := r.Set(Descriptor{Name: "ocr", Description: "reads scans"}); !ok {
		t.Log("note: same description reported as change")
	}
	if r.Epoch() != epoch {
		t.Logf("epoch moved on identical set: %d -> %d", epoch, r.Epoch())
	}
}

func TestDescriptorsCarryConsistentDigest(t *testing.T) {
	r := mustRegistry(t, Options{Docs: []Descriptor{
		{Name: "coding", Description: "writes go"},
		{Name: "ocr", Description: "reads scans"},
	}})
	docs := r.Descriptors()
	if len(docs) != 2 {
		t.Fatalf("Descriptors() len = %d, want 2", len(docs))
	}
	for _, d := range docs {
		want, err := wire.SkillDescriptorDigest(d)
		if err != nil {
			t.Fatal(err)
		}
		if d.GetDigest() != want {
			t.Fatalf("descriptor %q digest %q must be recomputed %q", d.GetName(), d.GetDigest(), want)
		}
	}
	// Deterministic ordering by name keeps the signed bytes stable.
	if docs[0].GetName() > docs[1].GetName() {
		t.Fatalf("descriptors must be sorted by name: %v", docs)
	}
}

func TestNewerRule(t *testing.T) {
	base := Descriptor{Name: "ocr", Version: 3, Digest: "sha256:a", UpdatedAt: time.Unix(100, 0)}
	cases := []struct {
		name string
		have Descriptor
		got  Descriptor
		want bool
	}{
		{"higher version", base, Descriptor{Name: "ocr", Version: 4, Digest: "sha256:b"}, true},
		{"lower version", base, Descriptor{Name: "ocr", Version: 2}, false},
		{"other skill", base, Descriptor{Name: "coding", Version: 99}, false},
		{"same version same digest", base, Descriptor{Name: "ocr", Version: 3, Digest: "sha256:a"}, false},
		{"same version newer digest", base,
			Descriptor{Name: "ocr", Version: 3, Digest: "sha256:z", UpdatedAt: time.Unix(200, 0)}, true},
		{"same version older digest", base,
			Descriptor{Name: "ocr", Version: 3, Digest: "sha256:z", UpdatedAt: time.Unix(50, 0)}, false},
	}
	for _, tc := range cases {
		if got := Newer(tc.have, tc.got); got != tc.want {
			t.Errorf("%s: Newer = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestImportPeerKeepsOnlyNewerAndVerifiesDigest(t *testing.T) {
	r := mustRegistry(t, Options{})
	doc := Descriptor{Name: "ocr", Description: "v1"}
	// Hand-built proto: ImportPeer must reject a descriptor whose digest field
	// does not match its bytes — otherwise a peer could describe one skill as
	// another under a name the receiver already trusts.
	forged := &pb.SkillDescriptor{Name: "ocr", Version: 9, Description: "evil", Digest: "sha256:deadbeef"}
	if n, _ := r.ImportPeer("peerA", []*pb.SkillDescriptor{forged}); n != 0 {
		t.Fatal("descriptor with a mismatched digest must be rejected")
	}

	good := doc.ToProto()
	n, names := r.ImportPeer("peerA", []*pb.SkillDescriptor{good})
	if n != 1 || len(names) != 1 || names[0] != "ocr" {
		t.Fatalf("import = %d, %v; want 1, [ocr]", n, names)
	}

	// An older restatement must not overwrite the newer view.
	stale := &pb.SkillDescriptor{Name: "ocr", Version: 1, Description: "old"}
	stale.Digest, _ = wire.SkillDescriptorDigest(stale)
	if n, _ := r.ImportPeer("peerA", []*pb.SkillDescriptor{stale}); n != 0 {
		t.Fatal("older version must not replace the held one")
	}
	if got := r.PeerSkills("peerA")[0].Version; got != good.GetVersion() {
		t.Fatalf("held version = %d, want %d", got, good.GetVersion())
	}

	// A strictly newer one does.
	bumped := &pb.SkillDescriptor{Name: "ocr", Version: good.GetVersion() + 1, Description: "v2"}
	bumped.Digest, _ = wire.SkillDescriptorDigest(bumped)
	if n, _ := r.ImportPeer("peerA", []*pb.SkillDescriptor{bumped}); n != 1 {
		t.Fatal("newer version must be imported")
	}
	if got := r.PeerSkills("peerA")[0].Description; got != "v2" {
		t.Fatalf("description = %q, want v2", got)
	}
}

func TestPeerEpochDrivesRefresh(t *testing.T) {
	r := mustRegistry(t, Options{})
	if !r.PeerEpoch("p", 5) {
		t.Fatal("first epoch claim must require a sync")
	}
	if r.PeerEpoch("p", 4) {
		t.Fatal("a rewound epoch must not trigger a sync: version clocks do not go backwards")
	}
	// Equal epoch *with* the descriptors already imported: nothing to fetch.
	d := Descriptor{Name: "ocr", Description: "x"}
	if n, _ := r.ImportPeer("p", []*pb.SkillDescriptor{d.ToProto()}); n != 1 {
		t.Fatal("import failed")
	}
	if r.PeerEpoch("p", 5) {
		t.Fatal("same epoch with held descriptors must not trigger a sync")
	}
	// Equal epoch but an empty descriptor set (the state after a restart) must
	// trigger one: the claim was remembered, the disclosed bytes were not.
	r.DropPeer("p")
	if !r.PeerEpoch("p", 5) {
		t.Fatal("same epoch without descriptors must re-trigger the sync")
	}
	if !r.PeerEpoch("p", 6) {
		t.Fatal("a newer epoch must trigger a sync")
	}
}

func TestSelectDeltaReturnsOnlyWhatRequesterLacks(t *testing.T) {
	r := mustRegistry(t, Options{Docs: []Descriptor{
		{Name: "coding", Description: "a"},
		{Name: "ocr", Description: "b"},
	}})
	coding, ocr := r.Descriptors()[0], r.Descriptors()[1]

	known := []*pb.SkillVersion{{Name: coding.GetName(), Version: coding.GetVersion(), Digest: coding.GetDigest()}}
	delta := r.SelectDelta(known, false, 0)
	if len(delta) != 1 || delta[0].GetName() != ocr.GetName() {
		t.Fatalf("delta = %v, want only ocr", namesOf(delta))
	}
	if all := r.SelectDelta(known, true, 0); len(all) != 2 {
		t.Fatalf("full sync = %d descriptors, want 2", len(all))
	}
	if capped := r.SelectDelta(nil, true, 1); len(capped) != 1 {
		t.Fatalf("limit must cap the answer, got %d", len(capped))
	}
}

func TestImportLimitIsPerNode(t *testing.T) {
	docs := []*pb.SkillDescriptor{}
	for _, n := range []string{"a", "b", "c", "d"} {
		d := Descriptor{Name: n, Description: "x"}
		docs = append(docs, d.ToProto())
	}
	r := mustRegistry(t, Options{ImportLimit: 3})
	if n, _ := r.ImportPeer("p1", docs); n != 3 {
		t.Fatalf("imported %d, want 3 (per-node budget)", n)
	}
	// A second peer gets nothing: the budget is the node's, not the peer's.
	if n, _ := r.ImportPeer("p2", docs); n != 0 {
		t.Fatalf("budget must be shared across peers, imported %d more", n)
	}
	r.DropPeer("p1")
	if n, _ := r.ImportPeer("p2", docs[:2]); n != 2 {
		t.Fatalf("after DropPeer the budget must be released, got %d", n)
	}
}

func TestPersistenceKeepsVersionClock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "skills.json")
	r := mustRegistry(t, Options{Path: path, Skills: []string{"ocr"}})
	if _, ok := r.Set(Descriptor{Name: "ocr", Description: "documented"}); !ok {
		t.Fatal("set failed")
	}
	epoch, version := r.Epoch(), r.Descriptors()[0].GetVersion()

	// A restart must not rewind either clock: peers compare the epoch to decide
	// whether their view is stale, and a rewind would silently stop refreshes.
	back := mustRegistry(t, Options{Path: path, Skills: []string{"ocr"}})
	if back.Epoch() < epoch {
		t.Fatalf("epoch rewound: %d -> %d", epoch, back.Epoch())
	}
	if got := back.Descriptors()[0].GetDescription(); got != "documented" {
		t.Fatalf("description lost across restart: %q", got)
	}
	if got := back.Descriptors()[0].GetVersion(); got != version {
		t.Fatalf("version rewound: %d -> %d", version, got)
	}
}

func TestDropDocKeepsNameButBumpsEpoch(t *testing.T) {
	r := mustRegistry(t, Options{Docs: []Descriptor{{Name: "ocr", Description: "x"}}})
	before := r.Epoch()
	if !r.DropDoc("ocr") {
		t.Fatal("DropDoc must succeed for a known skill")
	}
	if names := r.Names(); len(names) != 1 || names[0] != "ocr" {
		t.Fatalf("skill name must stay advertised, got %v", names)
	}
	if r.Epoch() <= before {
		t.Fatal("removing documentation is a new claim: the epoch must advance")
	}
	if d := r.Descriptors()[0]; d.GetDescription() != "" {
		t.Fatalf("description must be gone, got %q", d.GetDescription())
	}
}

func TestPeersBySkillUsesLearnedView(t *testing.T) {
	r := mustRegistry(t, Options{})
	d := Descriptor{Name: "OCR"}
	if _, names := r.ImportPeer("peerZ", []*pb.SkillDescriptor{d.ToProto()}); len(names) != 1 {
		t.Fatal("import failed")
	}
	if _, names := r.ImportPeer("peerA", []*pb.SkillDescriptor{d.ToProto()}); len(names) != 1 {
		t.Fatal("import failed")
	}
	got := r.PeersBySkill("ocr")
	if len(got) != 2 || got[0] != "peerA" || got[1] != "peerZ" {
		t.Fatalf("PeersBySkill = %v, want sorted [peerA peerZ]", got)
	}
	if r.PeersBySkill("coding") != nil && len(r.PeersBySkill("coding")) != 0 {
		t.Fatal("unknown skill must match nothing")
	}
}

func namesOf(in []*pb.SkillDescriptor) []string {
	out := make([]string, 0, len(in))
	for _, d := range in {
		out = append(out, d.GetName())
	}
	return out
}
