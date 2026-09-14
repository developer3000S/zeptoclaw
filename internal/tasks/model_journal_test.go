package tasks

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/picoclaw"
)

// fakeExecutor stands for the local agent. `disclose` is the model the agent
// reports having answered with (what PicoClaw puts in `model_name`), `report` is
// the model the node configured — the two sources the journal has to rank.
type fakeExecutor struct {
	disclose string
	report   string
}

func (e *fakeExecutor) Name() string { return "fake-cli" }

func (e *fakeExecutor) Execute(context.Context, picoclaw.Request) (*picoclaw.Response, error) {
	return &picoclaw.Response{
		Text:       "answered",
		Model:      e.disclose,
		StartedAt:  time.Now().UTC().Add(-time.Second),
		FinishedAt: time.Now().UTC(),
	}, nil
}

func (e *fakeExecutor) Healthy(context.Context) error { return nil }

func (e *fakeExecutor) Capabilities(context.Context) ([]string, error) { return nil, nil }

func (e *fakeExecutor) Close() error { return nil }

// Model makes this a picoclaw.ModelReporter, like the CLI adapter.
func (e *fakeExecutor) Model() string { return e.report }

var (
	_ picoclaw.Adapter       = (*fakeExecutor)(nil)
	_ picoclaw.ModelReporter = (*fakeExecutor)(nil)
)

func TestExecutedModelPriorities(t *testing.T) {
	cases := []struct {
		name     string
		disclose string
		report   string
		want     string
	}{
		{"agent disclosure wins over the configured default", "llm-actual", "llm-asked", "llm-actual"},
		{"configured default is the fallback", "", "llm-asked", "llm-asked"},
		{"whitespace in a disclosure is not a model", "   ", "llm-asked", "llm-asked"},
		{"nothing known stays unknown", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newLocalManagerWith(t, &fakeExecutor{report: tc.report})
			if got := m.executedModel(&picoclaw.Response{Model: tc.disclose}); got != tc.want {
				t.Fatalf("executedModel = %q, want %q", got, tc.want)
			}
		})
	}
	// A failed execution can hand back no response at all; the configured model is
	// still the honest answer about what the node asked for.
	m := newLocalManagerWith(t, &fakeExecutor{report: "llm-asked"})
	if got := m.executedModel(nil); got != "llm-asked" {
		t.Fatalf("executedModel(nil) = %q, want llm-asked", got)
	}
}

// The journal names the model that answered work this node executed and stays
// silent about work a peer executed: TaskResult carries no model field (the
// protocol is frozen), so a value for a remote result would be a guess about
// somebody else's machine.
func TestResultJournalRecordsTheModel(t *testing.T) {
	ctx := context.Background()

	t.Run("local execution records what answered", func(t *testing.T) {
		m := newLocalManagerWith(t, &fakeExecutor{disclose: "llm-actual", report: "llm-asked"})
		id, err := m.Submit(ctx, SubmitRequest{Instruction: "do it", TTL: 5})
		if err != nil {
			t.Fatalf("submit: %v", err)
		}
		res, err := m.AwaitResult(ctx, id)
		if err != nil {
			t.Fatalf("await: %v", err)
		}
		if res.GetStatus() != pb.TaskStatus_TASK_STATUS_COMPLETED {
			t.Fatalf("status = %s (%s)", res.GetStatus(), res.GetErrorMessage())
		}
		rec, err := m.store.GetResult(id)
		if err != nil {
			t.Fatalf("journal read: %v", err)
		}
		if rec.Model != "llm-actual" {
			t.Fatalf("journal model = %q, want the disclosed llm-actual (not the configured one)", rec.Model)
		}
		// The result that left the node on the wire knows nothing about it.
		if strings.Contains(res.String(), "model") {
			t.Fatalf("TaskResult grew a model field; that is a protocol change:\n%s", res)
		}
	})

	t.Run("a configured model is journalled without a disclosure", func(t *testing.T) {
		m := newLocalManagerWith(t, &fakeExecutor{report: "llm-asked"})
		id, err := m.Submit(ctx, SubmitRequest{Instruction: "do it", TTL: 5})
		if err != nil {
			t.Fatalf("submit: %v", err)
		}
		if _, err := m.AwaitResult(ctx, id); err != nil {
			t.Fatalf("await: %v", err)
		}
		rec, err := m.store.GetResult(id)
		if err != nil {
			t.Fatalf("journal read: %v", err)
		}
		if rec.Model != "llm-asked" {
			t.Fatalf("journal model = %q, want llm-asked", rec.Model)
		}
	})

	t.Run("a peer's answer records nothing", func(t *testing.T) {
		m := newLocalManagerWith(t, &fakeExecutor{disclose: "llm-actual", report: "llm-asked"})
		delegate := foreignIdentity(t)
		env := Build(m.self.String(), NewID(), BuildRequest{Instruction: "do it", TTL: 5}, time.Now().UTC())
		if err := m.signer.SignTask(env); err != nil {
			t.Fatalf("sign: %v", err)
		}
		h := &handle{env: env, deadline: time.Now().UTC().Add(time.Minute),
			waiter:   make(chan *pb.TaskResult, 1),
			expected: map[peer.ID]bool{delegate.PeerID(): true}}
		if !m.register(env.GetTaskId(), h) {
			t.Fatal("register")
		}
		ack, err := m.OnResult(ctx, delegate.PeerID(), workerResult(t, delegate, env, "answered elsewhere"))
		if err != nil || !ack.GetAccepted() {
			t.Fatalf("delegate result refused: ack=%v err=%v", ack, err)
		}
		select {
		case got := <-h.waiter:
			if got == nil {
				t.Fatal("waiter got nil")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waiter never settled")
		}
		rec, err := m.store.GetResult(env.GetTaskId())
		if err != nil {
			t.Fatalf("journal read: %v", err)
		}
		if rec.Model != "" {
			t.Fatalf("journal model = %q for a task executed by a peer", rec.Model)
		}
	})
}
