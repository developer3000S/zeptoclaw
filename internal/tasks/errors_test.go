package tasks

import (
	"testing"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// ErrorClass decides whether a relay retries, so the class must survive a
// refusal message that quotes a remote peer's own words. Those words are about
// whatever the peer chose to say — "allow_network_tools" or "untrusted" in a
// no-worker answer must not turn a retryable failure into a security refusal.
func TestErrorClassNoWorkerWinsOverQuotedReasons(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want pb.TaskErrorClass
	}{
		{
			"plain", "no eligible peers reachable",
			pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER,
		},
		{
			"quoting a constraint refusal",
			"no eligible peers reachable: 12D3KooWPeer: allow_network_tools not permitted",
			pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER,
		},
		{
			"quoting a trust refusal",
			"no eligible peers reachable: 12D3KooWPeer: peer not trusted for tasks",
			pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER,
		},
		{
			"empty neighbour view",
			"no neighbour advertises skills [research,coding]",
			pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER,
		},
		{
			"no capability plus refusal",
			"no capability; ttl exhausted: one more hop would spend it",
			pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER,
		},
		// Still classified by their own words when nothing shadows them.
		{
			"bare security failure", "signature: security: bad signature",
			pb.TaskErrorClass_TASK_ERROR_CLASS_SECURITY,
		},
		{
			"bare transport failure", "connection reset by peer",
			pb.TaskErrorClass_TASK_ERROR_CLASS_NETWORK,
		},
		{
			"bare limits failure", "queue is full",
			pb.TaskErrorClass_TASK_ERROR_CLASS_LIMITS,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ErrorClass(pb.TaskStatus_TASK_STATUS_FAILED, tc.msg); got != tc.want {
				t.Fatalf("ErrorClass(%q) = %s, want %s", tc.msg, got, tc.want)
			}
		})
	}
}

// A rejected task carries the refusal onward: the operator reading the task
// record must see why the mesh could not run it, not just that it could not.
func TestErrorResultCarriesReason(t *testing.T) {
	m := newLocalManager(t)

	env := &pb.TaskEnvelope{TaskId: "t-why", RequiredSkills: []string{"research"}}
	res := m.errorResult(env, "no eligible peers reachable: 12D3KooWPeer: rate limit exceeded",
		pb.TaskStatus_TASK_STATUS_FAILED)
	if res.GetErrorMessage() == "" {
		t.Fatal("error message must be carried")
	}
	if res.GetErrorClass() != pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER {
		t.Fatalf("error class = %s, want NO_WORKER despite the quoted limit message", res.GetErrorClass())
	}
}

// The class is what a relay reads when it decides to retry, and that decision is
// usually taken after the task has settled — AwaitResult, `get` and resubmit all
// serve the answer out of the journal rather than out of memory. So the class
// must survive the round trip through storage, including for records written by
// an older node that had no such field.
func TestErrorClassSurvivesJournalRoundTrip(t *testing.T) {
	m := newLocalManager(t)
	env := &pb.TaskEnvelope{TaskId: "t-journal", RequiredSkills: []string{"research"}}

	cases := []struct {
		name string
		st   pb.TaskStatus
		msg  string
		want pb.TaskErrorClass
	}{
		{"lost downstream", pb.TaskStatus_TASK_STATUS_TIMEOUT,
			"downstream peer disconnected before returning a result",
			pb.TaskErrorClass_TASK_ERROR_CLASS_TIMEOUT},
		{"no worker", pb.TaskStatus_TASK_STATUS_FAILED,
			"no eligible peers reachable: no neighbour advertises skills [quantum-annealing]",
			pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER},
		{"transport", pb.TaskStatus_TASK_STATUS_FAILED, "connection reset by peer",
			pb.TaskErrorClass_TASK_ERROR_CLASS_NETWORK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := m.errorResult(env, tc.msg, tc.st)
			if res.GetErrorClass() != tc.want {
				t.Fatalf("live class = %s, want %s", res.GetErrorClass(), tc.want)
			}
			back := resultFromRecord(recordFromResult(res))
			if back.GetErrorClass() != tc.want {
				t.Fatalf("class after a journal round trip = %s, want %s", back.GetErrorClass(), tc.want)
			}
			if back.GetErrorMessage() != res.GetErrorMessage() {
				t.Fatalf("message lost: %q", back.GetErrorMessage())
			}
		})
	}

	// A legacy record without the field must still be classified, exactly the
	// way the live path does it.
	legacy := recordFromResult(m.errorResult(env, "no eligible peers reachable",
		pb.TaskStatus_TASK_STATUS_FAILED))
	legacy.ErrorClass = ""
	if got := resultFromRecord(legacy).GetErrorClass(); got != pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER {
		t.Fatalf("legacy record class = %s, want NO_WORKER derived from the message", got)
	}
	// An unparseable name is treated as absent, never as a silent VALIDATION.
	legacy.ErrorClass = "TASK_ERROR_CLASS_BOGUS"
	if got := resultFromRecord(legacy).GetErrorClass(); got != pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER {
		t.Fatalf("unknown class name gave %s, want the derived NO_WORKER", got)
	}
}
