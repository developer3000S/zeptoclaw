package tasks

import (
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/zeptoclaw/zeptomesh/gen/zeptomesh/v1"
)

func TestSkillsCover(t *testing.T) {
	cases := []struct {
		name string
		have []string
		want []string
		ok   bool
	}{
		{"exact", []string{"ocr", "translate"}, []string{"ocr"}, true},
		{"missing", []string{"ocr"}, []string{"ocr", "translate"}, false},
		{"wildcard general", []string{"general"}, []string{"anything"}, true},
		{"wildcard any", []string{"any"}, []string{"a", "b"}, true},
		{"empty want", []string{"ocr"}, nil, true},
		{"case insensitive", []string{"OCR"}, []string{" ocr "}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := skillsCover(tc.have, tc.want); got != tc.ok {
				t.Fatalf("skillsCover(%v, %v) = %v, want %v", tc.have, tc.want, got, tc.ok)
			}
		})
	}
}

func TestDedupRecordsMergesViews(t *testing.T) {
	// Two relay branches answer about the same peer; the first has only an
	// address, the second only the skills. The merge must keep both.
	in := []*pb.PeerRecord{
		{PeerId: "A", Addrs: []string{"/ip4/10.0.0.1/tcp/4001"}, SeenAt: 100},
		{PeerId: "B", Addrs: []string{"/ip4/10.0.0.2/tcp/4001"}, Skills: []string{"ocr"}, SeenAt: 200},
		{PeerId: "A", Skills: []string{"ocr", "translate"}, SeenAt: 300},
		nil,
		{PeerId: ""},
	}
	out := dedupRecords(in)
	if len(out) != 2 {
		t.Fatalf("dedupRecords returned %d records, want 2: %+v", len(out), out)
	}
	if out[0].PeerId != "A" || out[1].PeerId != "B" {
		t.Fatalf("records not sorted by id: %s, %s", out[0].PeerId, out[1].PeerId)
	}
	a := out[0]
	if len(a.Addrs) != 1 || a.Addrs[0] != "/ip4/10.0.0.1/tcp/4001" {
		t.Fatalf("A addrs not filled from the second sighting: %v", a.Addrs)
	}
	if len(a.Skills) != 2 {
		t.Fatalf("A skills not filled from the second sighting: %v", a.Skills)
	}
	if a.SeenAt != 300 {
		t.Fatalf("A SeenAt = %d, want freshest 300", a.SeenAt)
	}
	// Input must not be mutated: the caller may keep using the raw answers.
	if len(in[0].Skills) != 0 {
		t.Fatalf("dedupRecords mutated its input: %+v", in[0])
	}
}

func TestDedupRecordsEmpty(t *testing.T) {
	if got := dedupRecords(nil); got != nil {
		t.Fatalf("dedupRecords(nil) = %+v, want nil", got)
	}
}

func TestNormalizeSkillsStableOrdering(t *testing.T) {
	got := NormalizeSkills([]string{" Translate ", "ocr", "OCR", "", "translate"})
	want := []string{"ocr", "translate"}
	if len(got) != len(want) {
		t.Fatalf("NormalizeSkills = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("NormalizeSkills = %v, want %v", got, want)
		}
	}
}

// TestSkillLookupRequestWireRoundTrip pins the fields the search relay depends
// on: a responder that cannot see full_refresh or relay_budget silently degrades
// the mesh to single-hop routing.
func TestSkillLookupRequestWireRoundTrip(t *testing.T) {
	req := &pb.SkillLookupRequest{
		Skills:      []string{"ocr"},
		FullRefresh: true,
		RelayBudget: 2,
		Visited:     []string{"QmSender", "QmHop"},
	}
	raw, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got pb.SkillLookupRequest
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.GetFullRefresh() {
		t.Fatal("full_refresh lost on the wire")
	}
	if got.GetRelayBudget() != 2 {
		t.Fatalf("relay_budget = %d, want 2", got.GetRelayBudget())
	}
	if len(got.GetVisited()) != 2 {
		t.Fatalf("visited = %v, want 2 entries", got.GetVisited())
	}

	resp := &pb.SkillLookupResponse{Peers: []*pb.PeerRecord{{PeerId: "QmA"}}, Partial: true, ResponderRefreshed: true}
	raw, err = proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	var gotResp pb.SkillLookupResponse
	if err := proto.Unmarshal(raw, &gotResp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !gotResp.GetPartial() || !gotResp.GetResponderRefreshed() {
		t.Fatalf("response flags lost on the wire: %+v", &gotResp)
	}
}
