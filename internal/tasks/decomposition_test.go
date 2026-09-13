package tasks

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/p2p"
	"github.com/developer3000S/zeptoclaw/internal/picoclaw"
	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/storage"
	"github.com/developer3000S/zeptoclaw/internal/wire"
)

// newLocalManager wires a fully functional Manager on an ephemeral libp2p
// host with the offline stub adapter, mirroring how node.go builds it. The
// routing table is empty on purpose: everything must resolve locally, so a
// routing mistake (forwarding to nobody) fails fast instead of hanging.
func newLocalManager(t *testing.T) *Manager {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Default()
	dir := t.TempDir()
	cfg.Node.DataDir = dir
	cfg.Node.Listen = []string{"/ip4/127.0.0.1/tcp/0"}
	cfg.Discovery.DHT = false
	cfg.Identity.KeyFile = filepath.Join(dir, "peer.key")
	cfg.Tasks.Forwarding.SearchRelay.Enabled = false
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validate: %v", err)
	}
	id, err := security.LoadOrGenerate(cfg.Identity.KeyFile)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	store, err := storage.Open(storage.Options{
		InMemory:     true,
		ArtifactsDir: filepath.Join(dir, "artifacts"),
	})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	h, err := p2p.New(context.Background(), p2p.Options{Config: cfg, Key: id.PrivKey(), Logger: logger})
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	svc := p2p.NewService(h, p2p.Handlers{}, logger)
	m, err := NewManager(Options{
		Config:   cfg,
		Identity: id,
		Policy:   security.NewPolicy(security.ModeOpen, security.TrustTrusted),
		Store:    store,
		Table:    routing.NewTable(cfg.Neighbors, store, logger),
		Adapter:  picoclaw.NewStub(config.StubConfig{Echo: true}, cfg.Capabilities.Skills),
		Service:  svc,
		Logger:   logger,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	t.Cleanup(func() { cancel(); m.Stop() })
	return m
}

// selfPub verifies signatures made by the manager's own node — the same
// peerstore lookup the production result path uses.
func selfPub(m *Manager) security.KeyLookup { return m.keyLookup() }

// TestDecompositionEndToEnd is the ТЗ 6.9.1 + 6.10.5 happy path: a parent
// task with a subtask plan runs each child through the normal local pipeline,
// folds the results in plan order into one signed, digested aggregate and
// delivers it to the origin waiter exactly once.
func TestDecompositionEndToEnd(t *testing.T) {
	m := newLocalManager(t)
	ctx := context.Background()

	id, err := m.Submit(ctx, SubmitRequest{
		Instruction: "prepare the report",
		TTL:         4,
		Subtasks: []*SubtaskRequest{
			{Instruction: "collect the data", RequiredSkills: []string{"research"}},
			{Instruction: "summarize findings"},
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := m.AwaitResult(ctx, id)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if res.GetStatus() != pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Fatalf("aggregate status = %s (%s), want COMPLETED", res.GetStatus(), res.GetErrorMessage())
	}
	if !res.GetAggregated() {
		t.Fatal("aggregate flag not set")
	}
	if res.GetWorkerPeerId() != m.self.String() {
		t.Fatalf("aggregate worker = %s, want the aggregating node %s", res.GetWorkerPeerId(), m.self)
	}
	for _, want := range []string{"### subtask 0", "### subtask 1", "collect the data", "summarize findings"} {
		if !strings.Contains(res.GetText(), want) {
			t.Fatalf("aggregate text missing %q:\n%s", want, res.GetText())
		}
	}
	if strings.Index(res.GetText(), "subtask 0") > strings.Index(res.GetText(), "subtask 1") {
		t.Fatal("subtask sections are not in plan order")
	}
	if len(res.GetResultDigest()) == 0 {
		t.Fatal("aggregate carries no result digest")
	}
	if err := security.VerifyWorkerResult(res, selfPub(m)); err != nil {
		t.Fatalf("worker signature must verify: %v", err)
	}
	// The digest must survive a round through the wire definition: recomputed
	// over the delivered content with the digest field cleared.
	clone := proto.Clone(res).(*pb.TaskResult)
	clone.ResultDigest = nil
	if want, err := wire.ResultDigest(clone); err != nil || string(want) != string(res.GetResultDigest()) {
		t.Fatalf("aggregate digest mismatch (err %v)", err)
	}
	// Journal side: parent recorded aggregated, children linked both ways.
	rec, rrec, err := m.TaskStatus(id)
	if err != nil {
		t.Fatalf("journal parent: %v", err)
	}
	if rec.Status != Completed.String() || rrec == nil || !rrec.Aggregated {
		t.Fatalf("parent journal = %s, result aggregated=%v", rec.Status, rrec != nil && rrec.Aggregated)
	}
	kids, err := m.store.ChildrenOf(id)
	if err != nil || len(kids) != 2 {
		t.Fatalf("ChildrenOf = %v (err %v), want 2 children", kids, err)
	}
	for _, k := range kids {
		if par, err := m.store.ParentOf(k); err != nil || par != id {
			t.Fatalf("ParentOf(%s) = %q (err %v), want %s", k, par, err, id)
		}
	}
	// Nothing may remain in flight once the parent resolves.
	if n := m.TrackedCount(); n != 0 {
		t.Fatalf("%d handles still tracked after aggregation", n)
	}
}

// TestAggregateResultFailureAndDedup pins the merge rules of ТЗ 6.10.5 п.4:
// completed only when every child completed, the first failed child describes
// the parent error, artifacts deduplicated by content hash.
func TestAggregateResultFailureAndDedup(t *testing.T) {
	m := newLocalManager(t)
	env := Build(m.self.String(), NewID(), BuildRequest{Instruction: "root", TTL: 3}, time.Now().UTC())
	art := &pb.ArtifactRef{Name: "a.txt", Hash: "sha256:aa", Size: 2}
	ok := &pb.TaskResult{
		TaskId: NewID(), WorkerPeerId: "worker-1", Status: pb.TaskStatus_TASK_STATUS_COMPLETED,
		Text: "first output", Artifacts: []*pb.ArtifactRef{art},
	}
	bad := &pb.TaskResult{
		TaskId: NewID(), WorkerPeerId: "worker-2", Status: pb.TaskStatus_TASK_STATUS_FAILED,
		Text: "partial", ErrorMessage: "no eligible peers reachable",
		ErrorClass: pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER,
		Artifacts:  []*pb.ArtifactRef{art}, // duplicate of the first child's artifact
	}
	res := m.aggregateResult(env, []*pb.TaskResult{ok, bad})
	if res.GetStatus() != pb.TaskStatus_TASK_STATUS_FAILED {
		t.Fatalf("status = %s, want FAILED", res.GetStatus())
	}
	if res.GetErrorClass() != pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER {
		t.Fatalf("error class = %s, want NO_WORKER", res.GetErrorClass())
	}
	if !strings.Contains(res.GetErrorMessage(), "subtask 1") {
		t.Fatalf("error message must name the failed slot: %q", res.GetErrorMessage())
	}
	if len(res.GetArtifacts()) != 1 {
		t.Fatalf("artifacts = %d, want 1 after dedup", len(res.GetArtifacts()))
	}
	if !res.GetAggregated() {
		t.Fatal("aggregate flag not set")
	}
	if err := security.VerifyWorkerResult(res, selfPub(m)); err != nil {
		t.Fatalf("aggregate signature: %v", err)
	}
	// A missing error class is derived from status+message, not left UNSPECIFIED.
	bad.ErrorClass = pb.TaskErrorClass_TASK_ERROR_CLASS_UNSPECIFIED
	bad.ErrorMessage = "connect refused"
	res2 := m.aggregateResult(env, []*pb.TaskResult{bad})
	if res2.GetErrorClass() != pb.TaskErrorClass_TASK_ERROR_CLASS_NETWORK {
		t.Fatalf("derived error class = %s, want NETWORK", res2.GetErrorClass())
	}
}

// TestSubtaskRetryOnce is ТЗ 6.10.5 п.3: a retryable child failure is
// re-injected exactly once and the retried child settles the parent; a
// non-retryable class settles immediately without re-injection.
func TestSubtaskRetryOnce(t *testing.T) {
	m := newLocalManager(t)
	par := Build(m.self.String(), NewID(), BuildRequest{Instruction: "root", TTL: 3}, time.Now().UTC())
	if err := m.signer.SignTask(par); err != nil {
		t.Fatalf("sign parent: %v", err)
	}
	f := newFanout(par, time.Now().UTC().Add(time.Minute),
		[]*SubtaskRequest{{Instruction: "only child"}})
	ph := &handle{env: par, deadline: f.deadline, waiter: make(chan *pb.TaskResult, 1), fan: f}
	if !m.register(par.GetTaskId(), ph) {
		t.Fatal("register parent")
	}
	// The slot stands for an already-lost child (no handle, never executed):
	// foldChild must burn exactly one retry and re-inject a live replacement.
	f.mu.Lock()
	f.live[0] = "lost-child"
	f.mu.Unlock()
	boom := &pb.TaskResult{
		TaskId: "lost-child", WorkerPeerId: m.self.String(),
		Status:       pb.TaskStatus_TASK_STATUS_FAILED,
		ErrorClass:   pb.TaskErrorClass_TASK_ERROR_CLASS_NETWORK,
		ErrorMessage: "connect reset by peer",
	}
	m.foldChild(f, "lost-child", boom)

	f.mu.Lock()
	attempts, waiting := f.attempts[0], f.waiting
	repl := f.live[0]
	f.mu.Unlock()
	if attempts != maxSubtaskRetries {
		t.Fatalf("attempts = %d, want %d", attempts, maxSubtaskRetries)
	}
	if waiting != 1 {
		t.Fatalf("waiting = %d, retried child must still be open", waiting)
	}
	if repl == "lost-child" || repl == "" {
		t.Fatalf("retry did not replace the live child id (%q)", repl)
	}

	// The re-injected child executes locally; its aggregate is the parent's
	// one and only delivery.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var res *pb.TaskResult
	select {
	case res = <-ph.waiter:
	case <-ctx.Done():
		t.Fatal("retried child never settled the parent")
	}
	if res.GetStatus() != pb.TaskStatus_TASK_STATUS_COMPLETED || !res.GetAggregated() {
		t.Fatalf("retry outcome = %s aggregated=%v, want COMPLETED", res.GetStatus(), res.GetAggregated())
	}
	f.mu.Lock()
	attemptsAfter := f.attempts[0]
	f.mu.Unlock()
	if attemptsAfter != maxSubtaskRetries {
		t.Fatalf("attempts after delivery = %d, want exactly %d (no retry storm)", attemptsAfter, maxSubtaskRetries)
	}

	// A non-retryable class must settle its slot without any re-injection.
	par2 := Build(m.self.String(), NewID(), BuildRequest{Instruction: "root2", TTL: 3}, time.Now().UTC())
	f2 := newFanout(par2, time.Now().UTC().Add(time.Minute), []*SubtaskRequest{{Instruction: "second"}})
	f2.mu.Lock()
	f2.live[0] = "lost-2"
	f2.mu.Unlock()
	sec := &pb.TaskResult{
		TaskId: "lost-2", Status: pb.TaskStatus_TASK_STATUS_FAILED,
		ErrorClass: pb.TaskErrorClass_TASK_ERROR_CLASS_SECURITY, ErrorMessage: "peer not trusted for tasks",
	}
	m.foldChild(f2, "lost-2", sec)
	f2.mu.Lock()
	attempts2, waiting2, live2 := f2.attempts[0], f2.waiting, f2.live[0]
	f2.mu.Unlock()
	if attempts2 != 0 || waiting2 != 0 || live2 != "lost-2" {
		t.Fatalf("security failure must settle without retry: attempts=%d waiting=%d live=%q",
			attempts2, waiting2, live2)
	}
	rec2, err := m.store.GetResult(par2.GetTaskId())
	if err != nil {
		t.Fatalf("f2 parent journal: %v", err)
	}
	if rec2.Status != Failed.String() {
		t.Fatalf("f2 aggregate = %s, want FAILED", rec2.Status)
	}
}

// TestDecompositionRejectsBadPlans pins Submit-side validation: oversize,
// empty and self-contradicting plans must be refused before any child exists,
// and a plan that cannot start must fail the parent rather than hang.
func TestDecompositionRejectsBadPlans(t *testing.T) {
	m := newLocalManager(t)
	ctx := context.Background()

	specs := make([]*SubtaskRequest, MaxSubtasks+1)
	for i := range specs {
		specs[i] = &SubtaskRequest{Instruction: "x"}
	}
	if _, err := m.Submit(ctx, SubmitRequest{Instruction: "root", Subtasks: specs}); err == nil {
		t.Fatal("oversized plan accepted")
	}
	if _, err := m.Submit(ctx, SubmitRequest{Instruction: "root",
		Subtasks: []*SubtaskRequest{{Instruction: "  "}}}); err == nil {
		t.Fatal("empty subtask instruction accepted")
	}
	no := false
	if _, err := m.Submit(ctx, SubmitRequest{Instruction: "root", AllowSubtasks: &no,
		Subtasks: []*SubtaskRequest{{Instruction: "work"}}}); err == nil {
		t.Fatal("plan accepted with allow_subtasks=false")
	}
	// A ttl that leaves no room for children must fail the parent instead of
	// hanging in WAITING_SUBTASKS forever.
	id, err := m.Submit(ctx, SubmitRequest{Instruction: "root", TTL: 1,
		Subtasks: []*SubtaskRequest{{Instruction: "work"}}})
	if err != nil {
		t.Fatalf("submit with exhausted ttl: %v", err)
	}
	res, err := m.AwaitResult(ctx, id)
	if err != nil {
		t.Fatalf("await failing parent: %v", err)
	}
	if res.GetStatus() != pb.TaskStatus_TASK_STATUS_FAILED || !strings.Contains(res.GetErrorMessage(), "decomposition") {
		t.Fatalf("parent result = %s %q, want FAILED decomposition", res.GetStatus(), res.GetErrorMessage())
	}
}
