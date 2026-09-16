package tasks

import (
	"context"
	"testing"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// TestParseStatusRoundTripsStrings pins the inverse of Status.String for every
// value the journal stores; the terminal guards in forward's accounting and
// updateStatus are only as correct as this mapping.
func TestParseStatusRoundTripsStrings(t *testing.T) {
	for _, s := range []Status{Received, Validating, Evaluating, Accepted, Rejected,
		Forwarded, Running, WaitingSubtasks, Aggregating, Completed, Failed, TimedOut, Canceled} {
		if got := ParseStatus(s.String()); got != s {
			t.Fatalf("ParseStatus(%q) = %v, want %v", s.String(), got, s)
		}
	}
	// Unknown spellings stay non-terminal by contract: the guarded writers then
	// behave as they did before the guards existed, instead of freezing.
	if ParseStatus("garbage").Terminal() || ParseStatus("").Terminal() {
		t.Fatal("unparseable status must map to non-terminal")
	}
}

// TestJournalStatusNeverResurrected is the regression for the race a fast
// executor exposes: the stub completes in single-digit milliseconds, so the
// post-ack accounting (updateStatus / forward's FORWARDED write) can land
// AFTER recordOutcome already wrote a terminal status. The terminal record is
// the truth; the bookkeeping must leave it alone and only add its fields.
func TestJournalStatusNeverResurrected(t *testing.T) {
	m := newLocalManager(t)
	id, err := m.Submit(context.Background(), SubmitRequest{Instruction: "journal me", TTL: 3})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := m.AwaitResult(context.Background(), id)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if res.GetStatus() != pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Fatalf("task status = %s, want COMPLETED", res.GetStatus())
	}
	rec, err := m.store.GetTask(id)
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	if rec.Status != Completed.String() {
		t.Fatalf("journal status = %s, want %s", rec.Status, Completed.String())
	}

	// The same sequence the race produces: a late non-terminal update lands on
	// an already-COMPLETED record.
	m.updateStatus(id, Running)
	after, err := m.store.GetTask(id)
	if err != nil {
		t.Fatalf("journal after late update: %v", err)
	}
	if after.Status != Completed.String() {
		t.Fatalf("late updateStatus resurrected the record: %s, want %s", after.Status, Completed.String())
	}
}
