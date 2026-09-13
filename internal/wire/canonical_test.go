package wire

import (
	"bytes"
	"encoding/hex"
	"testing"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

func sampleTask() *pb.TaskEnvelope {
	return &pb.TaskEnvelope{
		TaskId:         "018f0b1c-0000-7000-8000-000000000001",
		OriginPeerId:   "12D3KooWOrigin",
		SenderPeerId:   "12D3KooWSender",
		CreatedAt:      1773000000,
		Ttl:            5,
		Priority:       7,
		RequiredSkills: []string{"coding", "research"},
		Payload: &pb.TaskPayload{
			Instruction: "summarise the changelog",
			Attachments: []*pb.ArtifactRef{{Name: "log.txt", Hash: "sha256:aa", Size: 10}},
			Labels:      map[string]string{"b": "2", "a": "1"},
		},
		Constraints: &pb.TaskConstraints{MaxDurationSeconds: 60, AllowNetworkTools: true, AllowDelegation: true},
		RouteStack:  []string{"12D3KooWRelay"},
	}
}

func TestTaskContentStable(t *testing.T) {
	a, err := TaskContent(sampleTask())
	if err != nil {
		t.Fatal(err)
	}
	b, err := TaskContent(sampleTask())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("TaskContent is not deterministic")
	}
}

func TestTaskContentIgnoresRoutingFields(t *testing.T) {
	base := sampleTask()
	x, err := TaskContent(base)
	if err != nil {
		t.Fatal(err)
	}
	mutated := sampleTask()
	mutated.SenderPeerId = "12D3KooWEvil"
	mutated.Ttl = 1
	mutated.RouteStack = []string{"a", "b", "c"}
	y, err := TaskContent(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(x, y) {
		t.Fatal("TaskContent must not depend on sender/ttl/route")
	}
	tb, err := TaskBody(base)
	if err != nil {
		t.Fatal(err)
	}
	tb2, err := TaskBody(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(tb, tb2) {
		t.Fatal("TaskBody must depend on routing fields")
	}
}

func TestEncodingBlocksSeparatorShifts(t *testing.T) {
	// Payload labels: {"a|b":"c"} must not encode identically to {"a":"b|c"}.
	mk := func(env *pb.TaskEnvelope) []byte {
		b, err := TaskContent(env)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	a := sampleTask()
	a.Payload.Labels = map[string]string{"a|b": "c"}
	b := sampleTask()
	b.Payload.Labels = map[string]string{"a": "b|c"}
	if bytes.Equal(mk(a), mk(b)) {
		t.Fatal("length-prefixing failed to prevent separator collision")
	}
}

func TestDigestDomainSeparation(t *testing.T) {
	d1 := Digest("zeptomesh-task-v1", []byte("x"))
	d2 := Digest("zeptomesh-result-v1", []byte("x"))
	if bytes.Equal(d1, d2) {
		t.Fatal("schemes must not collide")
	}
	if len(d1) != 32 {
		t.Fatalf("digest length %d, want 32", len(d1))
	}
}

func TestNilMessagesRejected(t *testing.T) {
	if _, err := TaskContent(nil); err == nil {
		t.Fatal("expected error for nil task")
	}
	if _, err := ResultBody(nil); err == nil {
		t.Fatal("expected error for nil result")
	}
	if _, err := AckBody(nil); err == nil {
		t.Fatal("expected error for nil ack")
	}
}

func TestFloatEncodingDeterministic(t *testing.T) {
	seen := map[string]bool{}
	for _, f := range []float64{0, 0.0001, 0.33333, 1, 1.5, -1} {
		s := formatFloat(f)
		h := hex.EncodeToString([]byte(s))
		if seen[h] && s != formatFloat(f) {
			t.Fatalf("unstable float encoding %v -> %s/%s", f, s, h)
		}
		seen[h] = true
		if formatFloat(f) != s {
			t.Fatalf("float %v encodes unstably: %s vs %s", f, s, formatFloat(f))
		}
	}
	if formatFloat(2.0) != formatFloat(1.0) {
		t.Fatal("load must clamp to [0,1]")
	}
}
