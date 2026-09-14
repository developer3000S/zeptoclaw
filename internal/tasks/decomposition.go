package tasks

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/wire"
)

// Subtask decomposition (ТЗ 6.9.1) and result aggregation (ТЗ 6.10.5).
//
// The node that originates a root task may split it into subtasks, route each
// one independently through the normal delegation machinery (so a subtask can
// execute locally or be forwarded to whichever peer is best for its skills),
// wait for all child results, retry a failed child once, merge the outcomes
// and return one signed aggregate to the caller.
//
// Decomposition is origin-side only: the wire TaskEnvelope carries no subtask
// plan, because inventing one at a relay would let an intermediate node push
// work to its own peers, which the security model of the ТЗ forbids. The
// requester supplies the plan (admin API/CLI); the mesh executes and
// aggregates it.

// MaxSubtasks bounds one decomposition plan — the fan-out limit of ТЗ 6.9.4
// applied to subtasks rather than candidates.
const MaxSubtasks = 16

// SubtaskRequest is one planned child task.
type SubtaskRequest struct {
	Instruction    string
	RequiredSkills []string
	TTL            int32
	Priority       int32
}

// maxSubtaskRetries bounds re-injection of a failed child (ТЗ 6.10.5 п.3
// "при необходимости выполнить повтор" — one honest retry, no storm).
const maxSubtaskRetries = 1

// fanout is the live state of one decomposed parent task.
type fanout struct {
	mu       sync.Mutex
	parentID string
	env      *pb.TaskEnvelope
	deadline time.Time
	specs    []*SubtaskRequest
	// attempts[i] counts re-injections of child i; live[i] is its current id.
	attempts []int
	live     []string
	results  []*pb.TaskResult
	waiting  int
	aggreg   bool // aggregate already delivered
}

func newFanout(env *pb.TaskEnvelope, deadline time.Time, specs []*SubtaskRequest) *fanout {
	return &fanout{
		parentID: env.GetTaskId(),
		env:      env,
		deadline: deadline,
		specs:    specs,
		attempts: make([]int, len(specs)),
		live:     make([]string, len(specs)),
		results:  make([]*pb.TaskResult, len(specs)),
		waiting:  len(specs),
	}
}

func (f *fanout) subRequest(i int) BuildRequest {
	s := f.specs[i]
	ttl := s.TTL
	if ttl <= 0 {
		ttl = f.env.GetTtl() - 1
	}
	prio := s.Priority
	if prio <= 0 {
		prio = f.env.GetPriority()
	}
	return BuildRequest{
		Instruction:    s.Instruction,
		RequiredSkills: s.RequiredSkills,
		TTL:            ttl,
		Priority:       prio,
		TimeoutSeconds: f.env.GetConstraints().GetMaxDurationSeconds(),
		AllowShell:     f.env.GetConstraints().GetAllowShell(),
		AllowNetwork:   f.env.GetConstraints().GetAllowNetworkTools(),
		// A child may be forwarded but never further decomposed: the plan is
		// owned by the origin and recursion would double-count fan-out.
		AllowDeleg:    boolPtr(true),
		AllowSubtasks: boolPtr(false),
	}
}

// decompose injects every child of f and marks the parent as waiting. It is
// called with f already attached to the parent handle.
func (m *Manager) decompose(f *fanout) error {
	if len(f.specs) == 0 {
		return errors.New("tasks: empty subtask plan")
	}
	if len(f.specs) > MaxSubtasks {
		return fmt.Errorf("tasks: %d subtasks exceeds the limit of %d", len(f.specs), MaxSubtasks)
	}
	if f.env.GetTtl()-1 < 1 {
		return fmt.Errorf("tasks: ttl %d is too small to decompose", f.env.GetTtl())
	}
	for i := range f.specs {
		if err := m.injectChild(f, i); err != nil {
			return err
		}
	}
	m.updateStatus(f.parentID, WaitingSubtasks)
	return nil
}

func (m *Manager) injectChild(f *fanout, i int) error {
	childID := NewID()
	req := f.subRequest(i)
	child := DeriveSubtask(f.env, childID, req, time.Now().UTC())
	if err := Validate(child, m.cfg.Tasks.MaxPayloadBytes, time.Now().UTC()); err != nil {
		return fmt.Errorf("tasks: subtask %d invalid: %w", i, err)
	}
	if err := m.signer.SignTask(child); err != nil {
		return err
	}
	if err := m.signAuthorship(child); err != nil {
		return err
	}
	cdeadline := f.deadline
	if d := time.Now().UTC().Add(m.totalBudget(child)); d.Before(cdeadline) {
		cdeadline = d
	}
	h := &handle{env: child, deadline: cdeadline, parentID: f.parentID}
	if !m.register(childID, h) {
		return fmt.Errorf("tasks: subtask %s already tracked", childID)
	}
	m.recordJournal(child, Received, false)
	_ = m.store.LinkChild(f.parentID, childID)
	m.countReceived()

	f.mu.Lock()
	f.live[i] = childID
	f.mu.Unlock()

	go m.routeOrigin(child, cdeadline)
	return nil
}

// liveChildren snapshots the currently tracked child ids.
func (f *fanout) liveChildren() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.live))
	for _, id := range f.live {
		if id != "" {
			out = append(out, id)
		}
	}
	return out
}

// settleChild journals one terminal result of a locally injected subtask,
// releases its handle and folds it into the parent's fanout.
func (m *Manager) settleChild(h *handle, res *pb.TaskResult) {
	m.recordOutcome(res)
	m.resolve(res.GetTaskId())
	m.foldIfChild(h, res)
}

// foldIfChild routes a terminal result of a locally injected subtask into its
// parent's fanout. It reports whether the handle was a child (the caller then
// skips the waiter/upstream paths). The result must already be journalled and
// the child handle resolved.
func (m *Manager) foldIfChild(h *handle, res *pb.TaskResult) bool {
	if h == nil || h.parentID == "" {
		return false
	}
	ph := m.handleOf(h.parentID)
	if ph == nil || ph.fan == nil {
		// The parent resolved first (deadline or cancel): nothing waits for
		// this child anymore, its outcome stays in the journal.
		m.log.Debug("subtask_orphaned", "task_id", res.GetTaskId(),
			"parent_task_id", h.parentID)
		return true
	}
	m.foldChild(ph.fan, res.GetTaskId(), res)
	return true
}

// foldChild absorbs one terminal child result. When every child is settled it
// aggregates and delivers the parent result; a retryable failure is
// re-injected once (ТЗ 6.10.5 п.3) before the parent settles.
func (m *Manager) foldChild(f *fanout, childID string, res *pb.TaskResult) {
	f.mu.Lock()
	if f.aggreg {
		f.mu.Unlock()
		return
	}
	idx := -1
	for i, id := range f.live {
		if id == childID {
			idx = i
			break
		}
	}
	if idx < 0 {
		f.mu.Unlock()
		return
	}
	settled := pb.TaskStatus_TASK_STATUS_COMPLETED
	retryable := res.GetStatus() != settled &&
		res.GetErrorClass() != pb.TaskErrorClass_TASK_ERROR_CLASS_SECURITY &&
		res.GetErrorClass() != pb.TaskErrorClass_TASK_ERROR_CLASS_VALIDATION &&
		res.GetErrorClass() != pb.TaskErrorClass_TASK_ERROR_CLASS_CANCELED &&
		time.Now().UTC().Before(f.deadline)

	if retryable && f.attempts[idx] < maxSubtaskRetries {
		f.attempts[idx]++
		attempt := f.attempts[idx]
		f.mu.Unlock()
		m.log.Info("subtask_retry", "task_id", childID, "parent_task_id", f.parentID,
			"attempt", attempt, "class", res.GetErrorClass().String())
		if err := m.injectChild(f, idx); err != nil {
			m.log.Warn("subtask_retry_failed", "task_id", childID,
				"parent_task_id", f.parentID, "err", err.Error())
			// Could not re-inject: settle this slot with the original failure.
			f.mu.Lock()
			m.settleChildLocked(f, idx, res)
			done := f.waiting == 0
			f.mu.Unlock()
			if done {
				m.finishFanout(f)
			}
		}
		return
	}
	m.settleChildLocked(f, idx, res)
	done := f.waiting == 0
	f.mu.Unlock()
	if !done {
		return
	}
	m.finishFanout(f)
}

// settleChildLocked stores one settled child result; f.mu must be held.
func (m *Manager) settleChildLocked(f *fanout, idx int, res *pb.TaskResult) {
	f.results[idx] = res
	f.waiting--
}

// finishFanout builds the signed aggregate and delivers it through the parent
// handle, which always carries a waiter: decomposition only happens on the
// node that submitted the root task.
func (m *Manager) finishFanout(f *fanout) {
	f.mu.Lock()
	if f.aggreg {
		f.mu.Unlock()
		return
	}
	f.aggreg = true
	parts := make([]*pb.TaskResult, 0, len(f.results))
	for _, r := range f.results {
		if r != nil {
			parts = append(parts, r)
		}
	}
	planned := len(f.results)
	f.mu.Unlock()

	if len(parts) < planned {
		m.log.Warn("subtask_missing", "task_id", f.parentID, "settled", len(parts), "planned", planned)
	}
	agg := m.aggregateResult(f.env, parts)
	if ph := m.handleOf(f.parentID); ph != nil {
		m.absorbResult(agg)
		m.deliverWaiter(ph, agg)
		m.resolve(f.parentID)
		return
	}
	// Parent already resolved (deadline or cancel): journal only.
	m.recordOutcome(agg)
}

// aggregateResult merges child results in plan order into one signed parent
// result (ТЗ 6.10.5 п.4-6): completed only when every child completed, the
// first non-completed error class describes the failure, texts and artifacts
// are concatenated, and the whole thing is digest+worker signed by this node.
func (m *Manager) aggregateResult(env *pb.TaskEnvelope, parts []*pb.TaskResult) *pb.TaskResult {
	res := m.baseResult(env, pb.TaskStatus_TASK_STATUS_COMPLETED)
	res.WorkerPeerId = m.self.String()
	res.Aggregated = true

	var sb strings.Builder
	seen := make(map[string]bool)
	failed := false
	for i, p := range parts {
		if p.GetStatus() != pb.TaskStatus_TASK_STATUS_COMPLETED && !failed {
			failed = true
			res.Status = p.GetStatus()
			ec := p.GetErrorClass()
			if ec == pb.TaskErrorClass_TASK_ERROR_CLASS_UNSPECIFIED {
				ec = ErrorClass(p.GetStatus(), p.GetErrorMessage())
			}
			res.ErrorClass = ec
			res.ErrorMessage = capText(fmt.Sprintf("subtask %d (worker %s): %s",
				i, orDash(p.GetWorkerPeerId()), p.GetErrorMessage()), 1024)
		}
		sb.WriteString(fmt.Sprintf("### subtask %d — worker %s — %s\n",
			i, orDash(p.GetWorkerPeerId()), p.GetStatus().String()))
		if t := strings.TrimSpace(p.GetText()); t != "" {
			sb.WriteString(t)
			sb.WriteByte('\n')
		}
		for _, a := range p.GetArtifacts() {
			if a == nil || seen[a.GetHash()] {
				continue
			}
			seen[a.GetHash()] = true
			res.Artifacts = append(res.Artifacts, a)
		}
	}
	res.Text = capText(sb.String(), halfMax(m.cfg))
	if d, err := wire.ResultDigest(res); err == nil {
		res.ResultDigest = d
	} else {
		m.log.Warn("aggregate_digest_failed", "task_id", env.GetTaskId(), "err", err.Error())
	}
	if err := m.signer.SignWorkerResult(res); err != nil {
		m.log.Error("aggregate_sign_failed", "task_id", env.GetTaskId(), "err", err.Error())
	}
	return res
}

func boolPtr(v bool) *bool { return &v }

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
