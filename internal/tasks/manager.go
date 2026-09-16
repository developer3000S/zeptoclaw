package tasks

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/metrics"
	"github.com/developer3000S/zeptoclaw/internal/p2p"
	"github.com/developer3000S/zeptoclaw/internal/picoclaw"
	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/storage"
	"github.com/developer3000S/zeptoclaw/internal/tracing"
	"github.com/developer3000S/zeptoclaw/internal/wire"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// SkillSource is the slice of membership/discovery the manager needs to answer
// control-plane queries. The node wires a composite over membership, DHT and
// the neighbour table into it.
type SkillSource interface {
	PeersBySkill(want []string) []peer.ID
	State(pid peer.ID) (*pb.PeerState, bool)
	// Refresh reloads the full skill view from every live source (gossip
	// membership, DHT provider records, peer-exchange data) and folds it into
	// the neighbour table. complete reports whether the resulting view can be
	// treated as authoritative for the requested skills — false means the
	// caller may still be missing executors it has never heard of.
	Refresh(ctx context.Context, want []string) (found int, complete bool)
	// Adopt folds peer records learned from a search relay into the neighbour
	// table. A relay answer is a claim by a third party, so the implementation
	// must dial each peer and verify its signed capabilities before trusting
	// the advertised skills. It returns how many peers became routable.
	Adopt(ctx context.Context, recs []*pb.PeerRecord) int
	// SearchTopic runs the epidemic lookup of the last resort: a signed,
	// skills-only request on the shared search topic (ТЗ 6.9.5 п.5), answering
	// until ctx is done. It returns candidate records — claims that still go
	// through Adopt — or nil when the plane is not enabled. Naming no task id
	// or instruction keeps a lookup on a shared channel from disclosing what
	// is being executed.
	SearchTopic(ctx context.Context, want []string) []*pb.PeerRecord
}

// SkillView is the manager's window onto the node's own skill registry, used
// to answer a peer's descriptor-sync request. It is a narrow interface rather
// than the concrete *skills.Registry so the tasks package keeps its dependency
// graph acyclic and tests can stub it.
type SkillView interface {
	// Epoch is this node's advertised skill epoch.
	Epoch() int64
	// SelectDelta returns the local descriptors the requester does not have
	// (or has older). full ignores the diff; limit caps the answer (<=0: all).
	SelectDelta(known []*pb.SkillVersion, full bool, limit int) []*pb.SkillDescriptor
}

// Manager owns the task lifecycle on this node: acceptance, validation,
// local execution, delegation, relay of results and the durable journal.
//
// Relay semantics (v1): every hop records its *upstream* neighbour (the
// authenticated sender of the envelope) as the destination for the result.
// A hop re-signs an onward-relayed result with its own identity; the original
// worker's signature is not chained — see docs/PROTOCOLS.md.
type Manager struct {
	cfg     *config.Config
	self    peer.ID
	signer  *security.Signer
	policy  *security.Policy
	limiter *security.Limiter
	audit   *security.Audit
	store   *storage.Store
	table   *routing.Table
	adapter picoclaw.Adapter
	svc     *p2p.Service
	known   SkillSource
	sview   SkillView
	rebind  func(*pb.KeyRebind) (bool, string)
	mets    *metrics.Collector
	log     *slog.Logger
	caps    func() *pb.Capabilities
	tracer  oteltrace.Tracer

	slots  chan struct{}
	queued chan struct{} // bounds queue depth beyond running slots

	mu       sync.Mutex
	inflight map[string]*handle
	canceled map[string]bool
	wg       sync.WaitGroup

	// journalMu serialises the read-modify-write on a task journal record made
	// by forward's post-ack accounting, updateStatus and recordOutcome. Without
	// it the Terminal() guards are check-then-write across a window: a result
	// landing inside that window got its COMPLETED overwritten by a stale
	// FORWARDED read taken before it.
	journalMu sync.Mutex
}

// handle is the manager's bookkeeping for one task this node is responsible
// for: as origin (waiter set), as executor, or as relay (upstream set).
type handle struct {
	env      *pb.TaskEnvelope
	deadline time.Time
	// waiter is set only on the node that originated the task.
	waiter chan *pb.TaskResult
	// upstream is where the result must be sent back to (empty for origin).
	upstream peer.ID
	// downstream records the peer that accepted the task when we forwarded it;
	// cancel propagation follows it.
	downstream peer.ID
	// expected is the set of peers we actually handed this task to, and the only
	// ones allowed to answer. A signature proves who authored a result, not that
	// we asked them: without this binding anyone who learned a task id could
	// deliver their own "COMPLETED" for work they were never given. It is a set
	// rather than one peer because forwarding races several candidates in
	// parallel and every one of them may legitimately answer.
	expected map[peer.ID]bool
	running  bool
	cancelFn context.CancelFunc
	// parentID marks this task as a subtask injected by this node (ТЗ 6.10.5):
	// its result folds into the aggregator's fanout instead of a waiter or an
	// upstream relay.
	parentID string
	// fan is set on the parent handle while decomposition is in flight.
	fan *fanout
	// model records which model answered a task executed on *this* node
	// (ТЗ 10.3). It is journal-only: TaskResult carries no model field, so a
	// remotely produced result leaves it empty rather than guessing.
	model string
}

// Options wires the manager to its collaborators.
type Options struct {
	Config   *config.Config
	Identity *security.Identity
	Policy   *security.Policy
	Limiter  *security.Limiter
	Audit    *security.Audit
	Store    *storage.Store
	Table    *routing.Table
	Adapter  picoclaw.Adapter
	Service  *p2p.Service
	Known    SkillSource
	// SkillView answers peers' skill-descriptor sync requests (ТЗ 6.3 skill
	// exchange). Nil disables the endpoint's disclosure side.
	SkillView SkillView
	// OnRebind hands a control-plane identity-handover statement to the node
	// ledger. The node owns the ledger and the trust policy, so the manager only
	// transports; it returns whether the statement was accepted and why not.
	OnRebind func(*pb.KeyRebind) (bool, string)
	Metrics  *metrics.Collector
	Logger   *slog.Logger
}

// NewManager builds the task manager.
func NewManager(opts Options) (*Manager, error) {
	if opts.Config == nil || opts.Identity == nil || opts.Store == nil || opts.Service == nil {
		return nil, errors.New("tasks: config, identity, store and service are required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	slots := opts.Config.Tasks.MaxParallelTasks
	if slots <= 0 {
		slots = 1
	}
	return &Manager{
		cfg:      opts.Config,
		self:     opts.Identity.PeerID(),
		signer:   security.NewSigner(opts.Identity),
		policy:   opts.Policy,
		limiter:  opts.Limiter,
		audit:    opts.Audit,
		store:    opts.Store,
		table:    opts.Table,
		adapter:  opts.Adapter,
		svc:      opts.Service,
		known:    opts.Known,
		sview:    opts.SkillView,
		rebind:   opts.OnRebind,
		mets:     opts.Metrics,
		log:      logger,
		tracer:   tracing.Tracer("tasks"),
		slots:    make(chan struct{}, slots),
		queued:   make(chan struct{}, slots*4),
		inflight: make(map[string]*handle),
		canceled: make(map[string]bool),
	}, nil
}

// SetCapabilitiesFunc supplies the signed-by-caller self description used to
// answer the capabilities RPC.
func (m *Manager) SetCapabilitiesFunc(fn func() *pb.Capabilities) { m.caps = fn }

// Start launches background maintenance (deadline janitor).
func (m *Manager) Start(ctx context.Context) {
	m.wg.Add(2)
	go m.janitor(ctx)
	go m.sandboxSweeper(ctx)
	m.log.Info("task_manager_started", "max_parallel", cap(m.slots))
}

// Stop waits for background goroutines to drain. Task executions are bound to
// the manager's own per-task contexts; node shutdown cancels them.
func (m *Manager) Stop() {
	m.wg.Wait()
}

// ---------- public surface for node/admin ----------

// SubmitRequest is the admin-level description of a new root task.
type SubmitRequest struct {
	Instruction    string
	RequiredSkills []string
	TTL            int32
	Priority       int32
	TimeoutSeconds int32
	AllowShell     bool
	AllowNetwork   bool
	AllowDeleg     *bool
	AllowSubtasks  *bool
	Labels         map[string]string
	// Subtasks is the decomposition plan (ТЗ 6.9.1). When non-empty the task is
	// not executed as a whole: each entry becomes an independently routed
	// subtask and the results come back as one signed aggregate (ТЗ 6.10.5).
	Subtasks []*SubtaskRequest
}

// ---------- tracing (ТЗ 14.3) ----------

// startTaskSpan opens one span of a task's lifecycle, tagged with the
// cross-cutting task identifiers. When tracing is not installed the API's no-op
// tracer hands back an inert span and every call below costs an interface check.
func (m *Manager) startTaskSpan(ctx context.Context, name string, env *pb.TaskEnvelope) (context.Context, oteltrace.Span) {
	return m.tracer.Start(ctx, name, oteltrace.WithAttributes(tracing.TaskAttrs(env)...))
}

// injectTraceContext stamps the active span's W3C trace context into the
// envelope's labels. The caller must recompute the content digest afterwards:
// labels are author-signed content, so stamping them after the digest would
// break verification on every hop. This is why only the author of an envelope
// can carry a trace into it — and why a relay cannot splice its own trace into
// someone else's task.
func injectTraceContext(ctx context.Context, env *pb.TaskEnvelope) {
	payload := env.GetPayload()
	if payload == nil {
		return
	}
	labels := make(map[string]string, len(payload.GetLabels())+2)
	for k, v := range payload.GetLabels() {
		labels[k] = v
	}
	tracing.Inject(ctx, labels)
	env.Payload.Labels = labels
}

// spanOnly keeps the trace of ctx while dropping its cancellation and deadline:
// a task lives in the mesh far longer than the HTTP request that submitted it,
// and its routing is bounded by the envelope deadline, not by a caller context.
func spanOnly(ctx context.Context) context.Context {
	return oteltrace.ContextWithSpan(context.Background(), oteltrace.SpanFromContext(ctx))
}

// ---------- submission ----------

// Submit injects a root task authored by this node, starts routing it and
// returns its id.
func (m *Manager) Submit(ctx context.Context, req SubmitRequest) (string, error) {
	if strings.TrimSpace(req.Instruction) == "" {
		return "", errors.New("tasks: empty instruction")
	}
	if len(req.Subtasks) > MaxSubtasks {
		return "", fmt.Errorf("tasks: %d subtasks exceeds the limit of %d", len(req.Subtasks), MaxSubtasks)
	}
	if len(req.Subtasks) > 0 && req.AllowSubtasks != nil && !*req.AllowSubtasks {
		return "", errors.New("tasks: subtask plan given while allow_subtasks=false")
	}
	for i, s := range req.Subtasks {
		if s == nil || strings.TrimSpace(s.Instruction) == "" {
			return "", fmt.Errorf("tasks: subtask %d has an empty instruction", i)
		}
	}
	if req.TTL <= 0 {
		req.TTL = int32(m.cfg.Tasks.DefaultTTL)
	}
	if req.TTL > int32(m.cfg.Tasks.MaxTTL) {
		req.TTL = int32(m.cfg.Tasks.MaxTTL)
	}
	env := Build(m.self.String(), "", BuildRequest{
		Instruction:    req.Instruction,
		RequiredSkills: req.RequiredSkills,
		TTL:            req.TTL,
		Priority:       req.Priority,
		TimeoutSeconds: req.TimeoutSeconds,
		AllowShell:     req.AllowShell,
		AllowNetwork:   req.AllowNetwork,
		AllowDeleg:     req.AllowDeleg,
		AllowSubtasks:  req.AllowSubtasks,
		Labels:         req.Labels,
	}, time.Now().UTC())
	// The trace starts here, at the author, and rides to every later hop inside
	// the signed labels — hence the digest recomputation right after the stamp.
	sctx, submitSpan := m.startTaskSpan(ctx, tracing.SpanSubmit, env)
	defer submitSpan.End()
	injectTraceContext(sctx, env)
	env.ContextDigest = ContentDigest(env)
	if err := Validate(env, m.cfg.Tasks.MaxPayloadBytes, time.Now().UTC()); err != nil {
		submitSpan.RecordError(err)
		submitSpan.SetStatus(codes.Error, err.Error())
		return "", err
	}
	if err := m.signer.SignTask(env); err != nil {
		return "", err
	}
	if err := m.signAuthorship(env); err != nil {
		return "", err
	}

	deadline := time.Now().UTC().Add(m.totalBudget(env))
	h := &handle{env: env, deadline: deadline, waiter: make(chan *pb.TaskResult, 1)}
	var fan *fanout
	if len(req.Subtasks) > 0 {
		fan = newFanout(env, deadline, req.Subtasks)
		h.fan = fan
	}
	if !m.register(env.TaskId, h) {
		return "", fmt.Errorf("tasks: task %s already tracked", env.TaskId)
	}
	m.recordJournal(env, Received, false)
	m.countReceived()

	if fan != nil {
		go func() {
			if err := m.decompose(spanOnly(sctx), fan); err != nil {
				// The plan never got off the ground (bad ttl, rejected child):
				// fail the parent instead of leaving a task that waits forever.
				res := m.errorResult(env, "decomposition: "+err.Error(), pb.TaskStatus_TASK_STATUS_FAILED)
				if serr := m.signer.SignResult(res); serr == nil {
					m.absorbResult(res)
				}
				m.deliverWaiter(h, res)
				m.resolve(env.GetTaskId())
			}
		}()
		return env.GetTaskId(), nil
	}
	go m.routeOrigin(spanOnly(sctx), env, deadline)
	return env.GetTaskId(), nil
}

// signAuthorship stamps the authoring signature over everything this node wrote
// into the envelope (wire.TaskContent: instruction, skills, constraints, ids).
// Relays rewrite sender/ttl/route_stack and re-sign the transport copy, so this
// keeps the author's content verifiable end to end (ТЗ 6.6.4, 11.5). Children
// of a decomposition are authored by the node that planned them — which in this
// mesh is always the root origin, so they are signed too.
func (m *Manager) signAuthorship(env *pb.TaskEnvelope) error {
	if env.GetOriginPeerId() != m.self.String() {
		return nil // authored elsewhere: nothing of ours to sign
	}
	return m.signer.SignTaskOrigin(env)
}

// verifyAuthorship checks the authoring claim of a received envelope. A
// signature that is present but wrong is always fatal: someone on the path
// edited the author's content, or claimed authorship they do not hold.
//
// Subtask envelopes are exempt: their author is the node that decomposed the
// plan, which is by construction their sender, and that fact is already covered
// by the sender signature (parent_task_id sits inside the signed body, so a
// relay cannot mislabel a root task as a child without breaking its own
// signature). Requiring an origin signature there would reject legitimate
// children whose root origin is a third node.
//
// A missing signature on a root task is a compatibility decision, not a
// forgery: security.require_origin_signature opts a mesh in once every node
// speaks this build (ТЗ 6.6.4 otherwise holds via the sender signature).
func (m *Manager) verifyAuthorship(env *pb.TaskEnvelope) error {
	if env.GetParentTaskId() != "" {
		return nil
	}
	if len(env.GetOriginSignature()) == 0 {
		if m.cfg.Security.RequireOriginSignature {
			return security.ErrUnsigned
		}
		return nil
	}
	return security.VerifyTaskOrigin(env, m.keyLookup())
}

// AwaitResult blocks until an originated task resolves, ctx ends, or its
// deadline plus slack passes.
func (m *Manager) AwaitResult(ctx context.Context, taskID string) (*pb.TaskResult, error) {
	m.mu.Lock()
	h := m.inflight[taskID]
	m.mu.Unlock()
	if h == nil || h.waiter == nil {
		rec, err := m.store.GetResult(taskID)
		if err != nil {
			return nil, fmt.Errorf("tasks: unknown task %s", taskID)
		}
		return resultFromRecord(rec), nil
	}
	timer := time.NewTimer(time.Until(h.deadline) + 30*time.Second)
	defer timer.Stop()
	select {
	case res := <-h.waiter:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("tasks: wait deadline exceeded for %s", taskID)
	}
}

// TaskStatus returns the journal view of one task.
func (m *Manager) TaskStatus(taskID string) (*storage.TaskRecord, *storage.ResultRecord, error) {
	rec, err := m.store.GetTask(taskID)
	if err != nil {
		return nil, nil, err
	}
	res, rerr := m.store.GetResult(taskID)
	if rerr != nil {
		return rec, nil, nil
	}
	return rec, res, nil
}

// ListTasks returns recent journal records.
func (m *Manager) ListTasks(limit int, statusFilter string) ([]*storage.TaskRecord, error) {
	return m.store.ListTasks(limit, statusFilter)
}

// Cancel aborts a task authored on this node and propagates the request to
// the downstream peer currently holding it.
func (m *Manager) Cancel(ctx context.Context, taskID, reason string) error {
	m.mu.Lock()
	h := m.inflight[taskID]
	m.mu.Unlock()
	if h == nil || h.env.GetOriginPeerId() != m.self.String() {
		return fmt.Errorf("tasks: cannot cancel unknown or remote task %s", taskID)
	}
	req := &pb.CancelRequest{
		TaskId:       taskID,
		OriginPeerId: m.self.String(),
		SenderPeerId: m.self.String(),
		Reason:       reason,
	}
	if err := m.signer.SignCancel(req); err != nil {
		return err
	}
	m.applyCancelLocal(taskID, "canceled by origin: "+reason)
	if h.fan != nil {
		// A decomposed parent owns children on this node; cancelling the plan
		// must cancel every outstanding child too, not just the parent record.
		for _, cid := range h.fan.liveChildren() {
			if ch := m.handleOf(cid); ch != nil {
				cr := proto.Clone(req).(*pb.CancelRequest)
				cr.TaskId = cid
				if err := m.signer.SignCancel(cr); err == nil {
					m.applyCancelLocal(cid, "canceled with parent: "+reason)
					if ch.downstream != "" {
						cctx, cancel := context.WithTimeout(ctx, m.svc.Timeout())
						_, _ = m.svc.RPC(cctx, ch.downstream, &pb.RpcRequest{Kind: &pb.RpcRequest_Cancel{Cancel: cr}})
						cancel()
					}
				}
			}
		}
		return nil
	}
	if h.downstream != "" {
		cctx, cancel := context.WithTimeout(ctx, m.svc.Timeout())
		defer cancel()
		_, _ = m.svc.RPC(cctx, h.downstream, &pb.RpcRequest{Kind: &pb.RpcRequest_Cancel{Cancel: req}})
	}
	return nil
}

// RunningCount is the number of local executions in progress.
func (m *Manager) RunningCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, h := range m.inflight {
		if h.running {
			n++
		}
	}
	return n
}

// TrackedCount is the number of tasks this node still considers live.
func (m *Manager) TrackedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.inflight)
}

// OriginalInstruction recovers a task's instruction from the content-addressed
// artifact written when the task was journalled. It backs the admin API's
// resubmit flow.
func (m *Manager) OriginalInstruction(taskID string) (string, error) {
	rec, err := m.store.GetTask(taskID)
	if err != nil {
		return "", fmt.Errorf("tasks: %w", err)
	}
	if rec.PayloadHash == "" {
		return "", errors.New("tasks: no stored payload for this task")
	}
	data, err := m.store.LoadArtifact(rec.PayloadHash)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Load is running/max, clamped to [0,1]; feeds gossip and neighbour scoring.
func (m *Manager) Load() float64 {
	max := m.cfg.Tasks.MaxParallelTasks
	if max <= 0 {
		max = 1
	}
	load := float64(m.RunningCount()) / float64(max)
	if load > 1 {
		return 1
	}
	return load
}

// PeerID is this node's identity, exposed for status output.
func (m *Manager) PeerID() peer.ID { return m.self }

// Prune applies the retention policy to the journal and task sandboxes.
func (m *Manager) Prune(ctx context.Context) (int, error) {
	return m.store.Prune(ctx, m.cfg.Tasks.Retention.D())
}

// ---------- protocol handlers ----------

// OnTask handles an inbound envelope and produces the ack for the sender.
// The checks run cheapest-first so a hostile peer cannot make us do expensive
// work before we have rejected it.
func (m *Manager) OnTask(ctx context.Context, remote peer.ID, env *pb.TaskEnvelope) (*pb.TaskAck, error) {
	m.countReceived()

	// A hop that cannot even validate the envelope produces no span: rejected
	// traffic must not be able to grow a collector's storage.
	if err := Validate(env, m.cfg.Tasks.MaxPayloadBytes, time.Now().UTC()); err != nil {
		m.securityEvent("task_invalid", remote, env.GetTaskId(), err.Error())
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_REJECTED, "validation: "+err.Error()), nil
	}
	// The trace continues from the author's labels, not from this stream's own
	// context (each libp2p request is a trace of its own).
	rctx, recvSpan := m.startTaskSpan(tracing.Extract(ctx, env), tracing.SpanReceive, env)
	defer recvSpan.End()
	rctx = spanOnly(rctx)
	if env.GetSenderPeerId() != remote.String() {
		m.securityEvent("task_sender_mismatch", remote, env.GetTaskId(), env.GetSenderPeerId())
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_REJECTED, "sender_peer_id does not match stream identity"), nil
	}
	if m.cfg.Security.RequireTaskSignature {
		if err := security.VerifyTask(env, m.keyLookup()); err != nil {
			m.securityEvent("task_bad_signature", remote, env.GetTaskId(), err.Error())
			return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_REJECTED, "signature: "+err.Error()), nil
		}
	}
	if err := m.verifyAuthorship(env); err != nil {
		m.securityEvent("task_origin_signature", remote, env.GetTaskId(), err.Error())
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_REJECTED, "origin signature: "+err.Error()), nil
	}
	if err := CheckRoute(env, m.self.String()); err != nil {
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_REJECTED, err.Error()), nil
	}
	deadline := time.Unix(env.GetCreatedAt(), 0).UTC().Add(m.totalBudget(env))
	if time.Now().UTC().After(deadline) {
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_REJECTED, "task deadline already passed"), nil
	}
	if m.policy != nil && !m.policy.AllowTasksFrom(remote) {
		m.securityEvent("task_untrusted", remote, env.GetTaskId(), m.policy.TrustOf(remote).String())
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_REJECTED, "peer not trusted for tasks"), nil
	}
	if m.limiter != nil && !m.limiter.Allow(remote.String()) {
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_REJECTED, "rate limit exceeded"), nil
	}
	// Per-sender concurrency cap (tasks.max_parallel_tasks_per_peer): one busy
	// origin must not occupy every local slot. Checked before registration so a
	// refused task never occupies the inflight map or the dedup claim.
	if limit := m.cfg.Tasks.MaxParallelPerPeer; limit > 0 && m.senderInflight(remote) >= limit {
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_REJECTED, "per-peer parallel limit reached"), nil
	}

	claimed, _, err := m.store.ClaimDedup(env.GetTaskId(), m.cfg.Tasks.DedupWindow.D())
	if err != nil {
		return nil, err
	}
	if !claimed || m.tracked(env.GetTaskId()) {
		m.countDuplicate()
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_DUPLICATE, "already seen on this node"), nil
	}

	h := &handle{env: env, deadline: deadline, upstream: remote}
	if !m.register(env.GetTaskId(), h) {
		m.countDuplicate()
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_DUPLICATE, "already tracked"), nil
	}
	m.recordJournal(env, Evaluating, false)

	if m.canExecute(env) {
		go m.runLocal(rctx, env, deadline)
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_QUEUED, ""), nil
	}
	// A relay node gets one attempt: if this hop cannot place the task, the
	// upstream is better placed to retry than we are, so no exclusion set is
	// carried.
	ok, reason := m.forward(rctx, env, deadline, nil)
	if ok {
		return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_FORWARDED, ""), nil
	}
	// Nothing we can do: refuse and let the upstream retry elsewhere. The peers'
	// own reasons ride along, because this node is the only place that can tell
	// the submitter why the mesh could not run the task. Capped: the text goes
	// into a signed ack crossing a network.
	reason = capText("no capability; "+reason, 512)
	// We release our dedup claim by finishing the journal entry as rejected.
	m.recordOutcome(&pb.TaskResult{
		TaskId: env.GetTaskId(), WorkerPeerId: m.self.String(), SenderPeerId: m.self.String(),
		Status: pb.TaskStatus_TASK_STATUS_REJECTED, ErrorMessage: reason,
		FinishedAt: time.Now().UTC().Unix(),
	})
	m.resolve(env.GetTaskId())
	return m.ack(env.GetTaskId(), pb.AckStatus_ACK_STATUS_REJECTED, reason), nil
}

// OnResult handles an inbound TaskResult: absorbed at the origin, relayed at
// intermediate hops.
func (m *Manager) OnResult(ctx context.Context, remote peer.ID, res *pb.TaskResult) (*pb.ResultAck, error) {
	if err := security.VerifyResult(res, m.keyLookup()); err != nil {
		m.securityEvent("result_bad_signature", remote, res.GetTaskId(), err.Error())
		return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: false, Reason: "signature: " + err.Error()}, nil
	}
	m.mu.Lock()
	h := m.inflight[res.GetTaskId()]
	// The membership test belongs inside the lock: forward adds to this map while
	// other attempts are in flight, and reading it after unlocking would race a
	// concurrent write (fatal "concurrent map read and map write"). The audit
	// write is deliberately left for after the unlock.
	var asked int
	undelegated := false
	if h != nil {
		asked = len(h.expected)
		undelegated = !h.expected[remote]
	}
	m.mu.Unlock()

	// A valid signature proves who authored the result, not that we asked them.
	// Without this binding any peer that learned a task id could hand the origin
	// its own "COMPLETED" for work it was never given — including a peer we
	// rejected as unfit, or a third party replaying an id scraped from logs. So
	// only the peers this node itself delegated to may answer.
	if undelegated {
		m.securityEvent("result_unexpected_sender", remote, res.GetTaskId(),
			fmt.Sprintf("task was delegated to %d peer(s), this one is not among them", asked))
		return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: false,
			Reason: "task was not delegated to this peer"}, nil
	}

	switch {
	case h == nil:
		rec, err := m.store.GetResult(res.GetTaskId())
		if err == nil && rec.Status == StatusFromProto(res.GetStatus()).String() {
			return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: true, Reason: "duplicate, already stored"}, nil
		}
		return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: false, Reason: "unknown task"}, nil

	case h.waiter != nil: // we are the origin: absorb and deliver
		// The transport signature only proves who forwarded the result; the
		// worker chain signature proves what the executing peer actually
		// returned. A relay that edited the answer fails here (ТЗ 11.5).
		if err := security.VerifyWorkerResult(res, m.keyLookup()); err != nil {
			m.securityEvent("result_worker_signature", remote, res.GetTaskId(), err.Error())
			return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: false, Reason: "worker signature: " + err.Error()}, nil
		}
		m.absorbResult(res)
		m.deliverWaiter(h, res)
		m.resolve(res.GetTaskId())
		return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: true}, nil

	case h.parentID != "": // a subtask we injected: settle it into the parent fanout
		if err := security.VerifyWorkerResult(res, m.keyLookup()); err != nil {
			m.securityEvent("subtask_worker_signature", remote, res.GetTaskId(), err.Error())
			return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: false, Reason: "worker signature: " + err.Error()}, nil
		}
		m.absorbResult(res)
		m.resolve(res.GetTaskId())
		m.foldIfChild(h, res)
		return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: true}, nil

	default: // intermediate hop: relay upstream under our own signature
		if h.upstream == "" {
			return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: false, Reason: "no upstream path"}, nil
		}
		relay := proto.Clone(res).(*pb.TaskResult)
		relay.SenderPeerId = m.self.String()
		if err := m.signer.SignResult(relay); err != nil {
			return nil, err
		}
		rctx, cancel := context.WithTimeout(context.Background(), m.svc.Timeout())
		defer cancel()
		if _, err := m.svc.SendResult(rctx, h.upstream, relay); err != nil {
			m.log.Warn("result_relay_failed", "task_id", res.GetTaskId(), "to", h.upstream.String(), "err", err.Error())
			return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: false, Reason: "relay failed"}, nil
		}
		m.recordOutcome(relay)
		m.resolve(res.GetTaskId())
		return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: true}, nil
	}
}

// OnCancel processes a cancel request; only the task's origin may cancel.
func (m *Manager) OnCancel(ctx context.Context, remote peer.ID, req *pb.CancelRequest) *pb.CancelResponse {
	m.mu.Lock()
	h := m.inflight[req.GetTaskId()]
	m.mu.Unlock()
	if h == nil {
		return &pb.CancelResponse{Accepted: false, Reason: "unknown task"}
	}
	if err := security.VerifyCancel(req, m.keyLookup()); err != nil {
		m.securityEvent("cancel_bad_signature", remote, req.GetTaskId(), err.Error())
		return &pb.CancelResponse{Accepted: false, Reason: "bad signature"}
	}
	if remote.String() != h.env.GetOriginPeerId() || req.GetOriginPeerId() != h.env.GetOriginPeerId() {
		m.securityEvent("cancel_not_origin", remote, req.GetTaskId(), req.GetOriginPeerId())
		return &pb.CancelResponse{Accepted: false, Reason: "only the origin may cancel"}
	}
	m.applyCancelLocal(req.GetTaskId(), "canceled: "+req.GetReason())
	if h.downstream != "" { // propagate toward the worker
		cp := proto.Clone(req).(*pb.CancelRequest)
		cp.SenderPeerId = m.self.String()
		_ = m.signer.SignCancel(cp)
		cctx, cancel := context.WithTimeout(ctx, m.svc.Timeout())
		defer cancel()
		_, _ = m.svc.RPC(cctx, h.downstream, &pb.RpcRequest{Kind: &pb.RpcRequest_Cancel{Cancel: cp}})
	}
	return &pb.CancelResponse{Accepted: true}
}

// OnRPC dispatches control-plane requests.
func (m *Manager) OnRPC(ctx context.Context, remote peer.ID, req *pb.RpcRequest) (*pb.RpcResponse, error) {
	if m.limiter != nil && !m.limiter.Allow("rpc:"+remote.String()) {
		return nil, errors.New("tasks: rpc rate limit")
	}
	switch k := req.GetKind().(type) {
	case *pb.RpcRequest_Ping:
		return &pb.RpcResponse{Kind: &pb.RpcResponse_Ping{Ping: &pb.PingResponse{
			Nonce: k.Ping.GetNonce(), PeerTime: time.Now().UTC().Unix(),
		}}}, nil
	case *pb.RpcRequest_Capabilities:
		if m.caps == nil {
			return &pb.RpcResponse{}, nil
		}
		return &pb.RpcResponse{Kind: &pb.RpcResponse_Capabilities{Capabilities: &pb.CapabilitiesResponse{
			Capabilities: m.caps(),
		}}}, nil
	case *pb.RpcRequest_PeerExchange:
		want := int(k.PeerExchange.GetCount())
		if want <= 0 || want > 32 {
			want = 16
		}
		var recs []*pb.PeerRecord
		for _, n := range m.table.Sample(want * 2) {
			// A blocked or departed peer is never disclosed — an address answer
			// is an inducement to connect, so it obeys the same policy gate as a
			// skill-lookup response.
			if n.Left || (m.policy != nil && !m.policy.AllowConnection(n.PeerID)) {
				continue
			}
			recs = append(recs, &pb.PeerRecord{
				PeerId: n.PeerID.String(), Addrs: n.Addrs, Skills: n.Skills, SeenAt: n.LastSeen.Unix(),
			})
			if len(recs) >= want {
				break
			}
		}
		return &pb.RpcResponse{Kind: &pb.RpcResponse_PeerExchange{PeerExchange: &pb.PeerExchangeResponse{Peers: recs}}}, nil
	case *pb.RpcRequest_Cancel:
		return &pb.RpcResponse{Kind: &pb.RpcResponse_Cancel{Cancel: m.OnCancel(ctx, remote, k.Cancel)}}, nil
	case *pb.RpcRequest_SkillLookup:
		return &pb.RpcResponse{Kind: &pb.RpcResponse_SkillLookup{
			SkillLookup: m.onSkillLookup(ctx, remote, k.SkillLookup),
		}}, nil
	case *pb.RpcRequest_SkillsSync:
		return &pb.RpcResponse{Kind: &pb.RpcResponse_SkillsSync{
			SkillsSync: m.onSkillsSync(remote, k.SkillsSync),
		}}, nil
	case *pb.RpcRequest_Rebind:
		return &pb.RpcResponse{Kind: &pb.RpcResponse_Rebind{
			Rebind: m.onRebind(k.Rebind),
		}}, nil
	default:
		return nil, fmt.Errorf("tasks: unsupported rpc kind %T", k)
	}
}

// ---------- search relay ----------

// onSkillsSync answers a peer's request for skill descriptors this node can
// disclose. Skill documentation is operator metadata about this node, so
// disclosure is policy-gated (capabilities.skill_exchange.disclose_to) rather
// than automatic — and the answer is signed by this node's key so the peer can
// be sure the descriptors were not edited in transit.
func (m *Manager) onSkillsSync(remote peer.ID, req *pb.SkillsSyncRequest) *pb.SkillsSyncResponse {
	ex := m.cfg.Capabilities.SkillExchange
	// An empty answer is still a claim by this node ("nothing newer" versus "not
	// telling you"), and the requester verifies every answer — so refusals are
	// signed too. An unsigned refusal reads as a protocol violation and audits a
	// security event on the peer that was merely being told "no".
	empty := func(reason string) *pb.SkillsSyncResponse {
		if m.mets != nil {
			m.mets.SkillsRefused.Inc()
		}
		resp := &pb.SkillsSyncResponse{PeerId: m.self.String(), SkillsVersion: m.skillsEpoch(), Reason: reason}
		if err := m.signer.SignSkillsSync(resp); err != nil {
			m.log.Warn("skills_sync_sign_failed", "peer", remote.String(), "err", err.Error())
		}
		return resp
	}
	if !ex.Enabled || m.sview == nil {
		return empty("exchange_disabled")
	}
	if want, ok := disclosureFloor(ex.DiscloseTo); ok && m.policy != nil {
		if !m.policy.TrustOf(remote).AtLeast(want) {
			return empty("disclosure_policy")
		}
	}
	docs := m.sview.SelectDelta(req.GetKnown(), req.GetFull(), ex.MaxDescriptors)
	resp := &pb.SkillsSyncResponse{PeerId: m.self.String(), SkillsVersion: m.skillsEpoch(), Skills: docs}
	if err := m.signer.SignSkillsSync(resp); err != nil {
		m.log.Warn("skills_sync_sign_failed", "peer", remote.String(), "err", err.Error())
		return empty("signing_failed")
	}
	return resp
}

// onRebind receives an identity-handover statement over the control RPC.
//
// Verification happens here rather than relying on the sender: the statement is
// self-authenticating (both named keys signed it), so the manager can judge it
// without trusting who relayed it. This path exists because gossip is optional —
// a mesh running with discovery.gossip.enabled=false still has to learn that an
// identity was rotated or retired (ТЗ 11.2).
func (m *Manager) onRebind(req *pb.RebindRequest) *pb.RebindResponse {
	k := req.GetRebind()
	if k == nil {
		return &pb.RebindResponse{Accepted: false, Reason: "empty statement"}
	}
	if err := security.VerifyRebind(k, m.keyLookup()); err != nil {
		m.securityEvent("rebind_bad_signature", m.self, k.GetOldPeerId(), err.Error())
		return &pb.RebindResponse{Accepted: false, Reason: "signature: " + err.Error()}
	}
	if m.rebind == nil {
		return &pb.RebindResponse{Accepted: false, Reason: "not accepted here"}
	}
	ok, why := m.rebind(k)
	return &pb.RebindResponse{Accepted: ok, Reason: why}
}

// skillsEpoch reports the advertised skill epoch, from the view when present.
func (m *Manager) skillsEpoch() int64 {
	if m.sview == nil {
		return 0
	}
	return m.sview.Epoch()
}

// disclosureFloor maps the config knob onto a trust floor. ok=false means
// "disclose to anyone authenticated".
func disclosureFloor(s string) (security.Trust, bool) {
	switch s {
	case "trusted":
		return security.TrustTrusted, true
	case "known":
		return security.TrustKnown, true
	default:
		return 0, false
	}
}

// onSkillLookup answers a skill lookup, honouring a full-refresh request.
//
// When full_refresh is set the responder must not answer from a cached table:
// it reloads its complete skill view from every source it holds and runs its
// own routing Select over that view. That is what makes the mesh work when a
// neighbour's local view is limited — a peer that never received the gossip
// about the only executor with a given skill cannot recommend it, so the
// instruction to reload is sent instead of accepting the empty answer.
//
// If the refreshed view is still partial and the request carries relay budget,
// the lookup is fanned out to other neighbours (never back to `remote`, and
// never to a peer already in `visited`), and their answers are merged.
func (m *Manager) onSkillLookup(ctx context.Context, remote peer.ID, req *pb.SkillLookupRequest) *pb.SkillLookupResponse {
	want := NormalizeSkills(req.GetSkills())
	if len(want) == 0 {
		return &pb.SkillLookupResponse{Partial: true}
	}
	// The requester's claim about its own id is not trusted for loop control;
	// `remote` is the authenticated sender and is always excluded.
	visited := make(map[peer.ID]bool, len(req.GetVisited())+1)
	visited[remote] = true
	for _, s := range req.GetVisited() {
		if pid, err := peer.Decode(s); err == nil {
			visited[pid] = true
		}
	}

	refreshed := false
	authoritative := false
	if req.GetFullRefresh() && m.known != nil {
		refreshed = true
		_, authoritative = m.known.Refresh(ctx, want)
		if m.mets != nil {
			m.mets.FullRefreshes.Inc()
		}
	}

	peers := m.matchingPeers(want)
	if len(peers) > 0 {
		// A positive answer needs no widening, even on a partial view: the
		// origin will fall back to a relay itself if delegation fails.
		return &pb.SkillLookupResponse{Peers: peers, Partial: !authoritative, ResponderRefreshed: refreshed}
	}

	// Nothing local. Widen the search if we are allowed to.
	budget := int(req.GetRelayBudget())
	if budget > 0 && m.cfg.Tasks.Forwarding.SearchRelay.Enabled && m.cfg.Tasks.Forwarding.SearchRelay.MaxDepth > 0 {
		if budget > m.cfg.Tasks.Forwarding.SearchRelay.MaxDepth {
			budget = m.cfg.Tasks.Forwarding.SearchRelay.MaxDepth
		}
		merged, gotRefresh := m.relaySkillLookup(ctx, want, budget, visited, true)
		peers = append(peers, merged...)
		refreshed = refreshed || gotRefresh
	}
	return &pb.SkillLookupResponse{
		Peers:              dedupRecords(peers),
		Partial:            !authoritative,
		ResponderRefreshed: refreshed,
	}
}

// matchingPeers renders every routable peer in the local view that covers want.
// Blocked peers are never disclosed; the trust policy still decides who may be
// *recommended* for delegation, because an answer is an inducement to send work.
func (m *Manager) matchingPeers(want []string) []*pb.PeerRecord {
	byID := make(map[peer.ID]*pb.PeerRecord)
	add := func(pid peer.ID, addrs, skills []string, seen int64) {
		if pid == "" || pid == m.self || !m.policy.AllowConnection(pid) {
			return
		}
		if _, ok := byID[pid]; ok {
			return
		}
		byID[pid] = &pb.PeerRecord{PeerId: pid.String(), Addrs: addrs, Skills: skills, SeenAt: seen}
	}
	for _, c := range m.table.Select(want, m.policy, m.self, 0, nil) {
		add(c.Neighbor.PeerID, append([]string(nil), c.Neighbor.Addrs...),
			append([]string(nil), c.Neighbor.Skills...), c.Neighbor.LastSeen.Unix())
	}
	if m.known != nil {
		for _, pid := range m.known.PeersBySkill(want) {
			st, _ := m.known.State(pid)
			if st == nil {
				continue
			}
			add(pid, append([]string(nil), st.GetAddrs()...), append([]string(nil), st.GetSkills()...), st.GetTimestamp())
		}
	}
	if len(byID) == 0 {
		return nil
	}
	out := make([]*pb.PeerRecord, 0, len(byID))
	for _, r := range byID {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SeenAt == out[j].SeenAt {
			return out[i].PeerId < out[j].PeerId
		}
		return out[i].SeenAt > out[j].SeenAt
	})
	// Bound the disclosure so one lookup cannot exfiltrate the whole table.
	if len(out) > 16 {
		out = out[:16]
	}
	return out
}

// relaySkillLookup fans the lookup out to neighbours and merges their answers.
// It returns the collected records and whether any hop reported a full refresh.
func (m *Manager) relaySkillLookup(
	ctx context.Context,
	want []string,
	budget int,
	visited map[peer.ID]bool,
	askFullRefresh bool,
) ([]*pb.PeerRecord, bool) {
	sr := m.cfg.Tasks.Forwarding.SearchRelay
	fanout := sr.Fanout
	if fanout <= 0 {
		fanout = 1
	}
	targets := m.relayTargets(want, fanout, visited)
	if len(targets) == 0 {
		return nil, false
	}

	// Every hop pays for itself: cap the fan-out round by the configured
	// request timeout so a slow branch cannot hold the caller's stream open.
	deadline := sr.RequestTimeout.D()
	if deadline <= 0 {
		deadline = 8 * time.Second
	}
	rctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	selfID := m.self.String()
	next := make([]string, 0, len(visited)+1)
	next = append(next, selfID)
	for pid := range visited {
		if pid != m.self {
			next = append(next, pid.String())
		}
	}

	type answer struct {
		recs      []*pb.PeerRecord
		refreshed bool
	}
	ch := make(chan answer, len(targets))
	var wg sync.WaitGroup
	for _, t := range targets {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := m.svc.RPC(rctx, t, &pb.RpcRequest{Kind: &pb.RpcRequest_SkillLookup{
				SkillLookup: &pb.SkillLookupRequest{
					Skills: want, FullRefresh: askFullRefresh,
					RelayBudget: int32(budget - 1), Visited: next,
				},
			}})
			if err != nil {
				m.table.RecordFailure(t)
				return
			}
			m.table.RecordSuccess(t)
			sl := resp.GetSkillLookup()
			if sl == nil {
				return
			}
			ch <- answer{recs: sl.GetPeers(), refreshed: sl.GetResponderRefreshed()}
		}()
	}
	go func() { wg.Wait(); close(ch) }()

	var (
		out       []*pb.PeerRecord
		refreshed bool
	)
	for a := range ch {
		out = append(out, a.recs...)
		refreshed = refreshed || a.refreshed
	}
	if m.mets != nil {
		m.mets.SearchRelays.Inc()
	}
	return dedupRecords(out), refreshed
}

// relayTargets picks which neighbours to ask. Preference goes to peers that
// already cover the skills (they are the likeliest to know more about them),
// then to relays we are connected to; blocked and visited peers are skipped.
func (m *Manager) relayTargets(want []string, fanout int, visited map[peer.ID]bool) []peer.ID {
	type rank struct {
		pid   peer.ID
		score int
	}
	var ranked []rank
	for _, n := range m.table.List() {
		pid := n.PeerID
		if pid == m.self || (visited != nil && visited[pid]) {
			continue
		}
		if !m.policy.AllowDelegationTo(pid) || !m.policy.AllowConnection(pid) {
			continue
		}
		if !n.Connected || n.Left {
			continue
		}
		score := 0
		if skillsCover(n.Skills, want) {
			score = 2
		} else if len(n.Skills) > 0 {
			score = 1
		}
		ranked = append(ranked, rank{pid: pid, score: score})
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		return ranked[i].pid.String() < ranked[j].pid.String()
	})
	out := make([]peer.ID, 0, fanout)
	for _, r := range ranked {
		if len(out) >= fanout {
			break
		}
		out = append(out, r.pid)
	}
	return out
}

// searchRelay is the origin-side escape hatch: the local view produced no
// delegation candidate, so ask the mesh — instructing each responder to reload
// its full skill view and run its own Select — then adopt whatever is returned.
// It returns true when the widened view yielded at least one routable peer.
// taskID names the task the search is for, so a widening can be tied back to the
// journal record that provoked it.
//
// The widening is a ladder, and each rung has its own switch: the addressed
// part (full refresh + bounded relay RPC) belongs to search_relay.enabled;
// the epidemic topic is search_relay.topic.enabled and runs even when the
// addressed part is off — its absence is exactly the case the topic plane is
// for (nobody reachable knows where the executor is).
func (m *Manager) searchRelay(ctx context.Context, want []string, deadline time.Time, taskID string) bool {
	sr := m.cfg.Tasks.Forwarding.SearchRelay
	if m.known == nil || len(want) == 0 {
		return false
	}
	if !sr.Enabled && !sr.Topic.Enabled {
		return false
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	budget := sr.RequestTimeout.D()
	if budget <= 0 || budget > remaining {
		budget = remaining
	}
	adopted := 0
	found := 0
	if sr.Enabled {
		rctx, cancel := context.WithTimeout(ctx, budget)
		// Start from our own complete view: a refresh may be all that is missing.
		var authoritative bool
		found, authoritative = m.known.Refresh(rctx, want)
		if m.mets != nil {
			m.mets.FullRefreshes.Inc()
		}
		if found == 0 && !authoritative {
			recs, _ := m.relaySkillLookup(rctx, want, sr.MaxDepth, map[peer.ID]bool{}, true)
			adopted = m.known.Adopt(rctx, recs)
		}
		cancel()
	}
	ok := m.table.AnyWithSkills(want)
	// The epidemic plane is the last widening step, taken only when the
	// addressed ladder came up empty (ТЗ 6.9.5 п.5). Publishing on a shared
	// topic reaches every subscriber, so the request names skills and nothing
	// else; answers stay claims until Adopt dials and verifies each peer.
	if !ok && sr.Topic.Enabled {
		if remaining := time.Until(deadline); remaining > 0 {
			tb := budget
			if sr.Topic.RequestTTL.D() > 0 && sr.Topic.RequestTTL.D() < tb {
				tb = sr.Topic.RequestTTL.D()
			}
			tctx, tcancel := context.WithTimeout(ctx, tb)
			recs := m.known.SearchTopic(tctx, want)
			if len(recs) > 0 {
				adopted += m.known.Adopt(tctx, recs)
			}
			tcancel()
			ok = m.table.AnyWithSkills(want)
		}
	}
	if m.mets != nil && ok {
		m.mets.SearchRelayHits.Inc()
	}
	if ok {
		m.log.Info("search_relay_widened_view", "task_id", taskID,
			"skills", strings.Join(want, ","),
			"refresh_found", found, "adopted", adopted)
	}
	return ok
}

// ---------- routing internals ----------

// routeOrigin is the origin-side decision loop: execute if we can, delegate
// with bounded retries otherwise.
func (m *Manager) routeOrigin(ctx context.Context, env *pb.TaskEnvelope, deadline time.Time) {
	if m.canExecute(env) {
		m.runLocal(ctx, env, deadline)
		return
	}
	retries := m.cfg.Tasks.Forwarding.MaxRetries
	if retries < 0 {
		retries = 0
	}
	// Peers that refused, were unreachable, or already hold this task are never
	// offered it again. Without this the retry loop re-sends the task to the same
	// node that just declined it: Select is deterministic, and RecordFailure moves
	// the success-rate weight (0.05) far too little to change the order — so
	// "retry" meant "ask the same peer again", and a second, capable peer stayed
	// unused (ТЗ 6.12.1).
	excluded := make(map[peer.ID]bool)
	// why is the last refusal any candidate gave. The generic "no eligible peers
	// reachable" is what an operator sees today, and it sends them hunting for a
	// connectivity fault when the real reason is usually a constraint the peers
	// will not honour. Keeping the peer's own words makes the result actionable.
	why := "no eligible peers reachable"
	for attempt := 0; attempt <= retries; attempt++ {
		if time.Now().UTC().After(deadline) {
			break
		}
		ok, reason := m.forward(ctx, env, deadline, excluded)
		if ok {
			return
		}
		if reason != "" {
			why = "no eligible peers reachable: " + reason
		}
		if attempt < retries {
			select {
			case <-time.After(m.cfg.Tasks.Forwarding.RetryInterval.D()):
			case <-time.After(time.Until(deadline)):
			}
		}
	}
	res := m.errorResult(env, why, pb.TaskStatus_TASK_STATUS_FAILED)
	if err := m.signer.SignResult(res); err == nil {
		m.absorbResult(res)
	}
	if h := m.handleOf(env.GetTaskId()); h != nil {
		if h.parentID != "" {
			m.resolve(env.GetTaskId())
			m.foldIfChild(h, res)
			return
		}
		m.deliverWaiter(h, res)
		m.resolve(env.GetTaskId())
	}
}

// forward delegates the envelope to the top-scoring neighbours, up to
// max_parallel_candidates concurrently. It reports whether a peer accepted the
// task; the first acceptance wins and is recorded on the handle. The second
// return value explains a refusal — "why did my task not run" is the single
// most common question an operator asks, and without the peers' own words the
// only answer available is a generic "no eligible peers reachable".
//
// exclude, when non-nil, is both read and written: it keeps the candidates this
// call asked and could not hand the task to out of the next attempt's selection
// (ТЗ 6.12.1). Callers that make a single attempt pass nil.
func (m *Manager) forward(ctx context.Context, env *pb.TaskEnvelope, deadline time.Time, exclude map[peer.ID]bool) (bool, string) {
	fctx, fwdSpan := m.startTaskSpan(ctx, tracing.SpanForward, env)
	fwdCtx := spanOnly(fctx)
	defer fwdSpan.End()
	fwdSpan.SetAttributes(attribute.String("zeptomesh.self_peer_id", m.self.String()))
	if env.GetTtl() <= 1 {
		fwdSpan.SetAttributes(attribute.String("zeptomesh.forward.outcome", "ttl_exhausted"))
		return false, "ttl exhausted: one more hop would spend it"
	}
	// The origin's right to keep the task local is binding (ТЗ 6.4.3): a node
	// that cannot execute a no-delegation task must refuse it, not forward it.
	if !env.GetConstraints().GetAllowDelegation() {
		fwdSpan.SetAttributes(attribute.String("zeptomesh.forward.outcome", "delegation_forbidden"))
		return false, "delegation not allowed by task constraints"
	}
	fanout := m.cfg.Tasks.Forwarding.MaxFanout
	cands := m.table.Select(env.GetRequiredSkills(), m.policy, m.self, fanout, exclude)
	if len(cands) == 0 {
		// The local neighbour view is not enough: widen it with a bounded
		// skill-lookup relay before giving up (ТЗ 6.5.3, multi-hop delegation).
		relayCtx, cancel := context.WithDeadline(fwdCtx, deadline)
		widened := m.searchRelay(relayCtx, env.GetRequiredSkills(), deadline, env.GetTaskId())
		cancel()
		if !widened {
			if m.mets != nil {
				m.mets.Forward("no_candidates")
			}
			return false, m.noCandidateReason(env)
		}
		cands = m.table.Select(env.GetRequiredSkills(), m.policy, m.self, fanout, exclude)
		if len(cands) == 0 {
			if m.mets != nil {
				m.mets.Forward("no_candidates")
			}
			return false, m.noCandidateReason(env)
		}
	}
	parallel := m.cfg.Tasks.Forwarding.MaxParallelCandidates
	if parallel > len(cands) {
		parallel = len(cands)
	}
	// Record who is about to be asked before asking them: a fast candidate can
	// answer while the loop is still dialing the others, and OnResult must not
	// reject its answer for arriving too early. Repeated forwards (a retried
	// subtask) add to the set rather than replace it, so a late answer from the
	// first attempt stays acceptable.
	m.mu.Lock()
	if h := m.inflight[env.GetTaskId()]; h != nil {
		if h.expected == nil {
			h.expected = make(map[peer.ID]bool, parallel)
		}
		for _, c := range cands[:parallel] {
			h.expected[c.Neighbor.PeerID] = true
		}
	}
	m.mu.Unlock()
	asked := make([]string, 0, parallel)
	for _, c := range cands[:parallel] {
		asked = append(asked, c.Neighbor.PeerID.String())
	}
	fwdSpan.SetAttributes(attribute.StringSlice("zeptomesh.candidates", asked))

	type outcome struct {
		to  peer.ID
		ack *pb.TaskAck
	}
	ch := make(chan outcome, parallel)
	var wg sync.WaitGroup
	// Rejection reasons from the candidates that answered "no". Without them a
	// delegation failure reads as "no eligible peers reachable", which sends the
	// operator looking for a connectivity problem when the real answer is usually
	// a constraint the peers will not honour.
	var rejects sync.Map // peer.ID -> reason
	for _, c := range cands[:parallel] {
		cand := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			hop := cloneEnv(env)
			hop.Ttl = env.GetTtl() - 1
			hop.SenderPeerId = m.self.String()
			AppendRoute(hop, m.self.String())
			if err := m.signer.SignTask(hop); err != nil {
				m.log.Warn("task_sign_for_hop_failed", "task_id", env.GetTaskId(),
					"peer", cand.Neighbor.PeerID.String(), "err", err.Error())
				return
			}
			ai := peer.AddrInfo{ID: cand.Neighbor.PeerID}
			for _, a := range cand.Neighbor.Addrs {
				if parsed, err := ma.NewMultiaddr(a); err == nil {
					ai.Addrs = append(ai.Addrs, parsed)
				}
			}
			dctx, cancel := context.WithTimeout(context.Background(), m.svc.Timeout())
			defer cancel()
			ack, err := m.svc.SendTask(dctx, ai, hop)
			if err != nil {
				m.table.RecordFailure(cand.Neighbor.PeerID)
				m.countForward("error")
				rejects.Store(cand.Neighbor.PeerID, "transport: "+err.Error())
				return
			}
			switch ack.GetStatus() {
			case pb.AckStatus_ACK_STATUS_QUEUED, pb.AckStatus_ACK_STATUS_FORWARDED:
				ch <- outcome{to: cand.Neighbor.PeerID, ack: ack}
			case pb.AckStatus_ACK_STATUS_DUPLICATE:
				m.countForward("duplicate")
			default:
				m.countForward("rejected")
				m.table.RecordFailure(cand.Neighbor.PeerID)
				reason := ack.GetReason()
				if reason == "" {
					reason = ack.GetStatus().String()
				}
				rejects.Store(cand.Neighbor.PeerID, reason)
			}
		}()
	}
	go func() { wg.Wait(); close(ch) }()

	first, ok := <-ch
	if !ok || first.ack == nil {
		m.countForward("no_accept")
		// Everyone we just asked declined, was unreachable, or already holds this
		// task. `channel closed` means all dispatch goroutines are done, so no
		// other writer can be touching exclude here. Retrying the same set would
		// only burn the attempt budget, so they leave the candidate pool (ТЗ
		// 6.12.1); the ones we never reached stay in it.
		if exclude != nil {
			for _, c := range cands[:parallel] {
				exclude[c.Neighbor.PeerID] = true
			}
		}
		return false, m.rejectSummary(env, &rejects)
	}
	m.mu.Lock()
	if h := m.inflight[env.GetTaskId()]; h != nil {
		h.downstream = first.to
	}
	m.mu.Unlock()
	m.journalMu.Lock()
	defer m.journalMu.Unlock()
	if rec, err := m.store.GetTask(env.GetTaskId()); err == nil {
		// A fast candidate can complete before this accounting write lands (the
		// stub does it in single-digit ms). The terminal record is the truth:
		// downgrade it to FORWARD and A's journal contradicts the result it
		// already holds. The delegation itself is still recorded in DelegatedTo.
		if !ParseStatus(rec.Status).Terminal() {
			rec.Status = Forwarded.String()
		}
		rec.DelegatedTo = append(rec.DelegatedTo, first.to.String())
		rec.Attempts++
		_ = m.store.PutTask(rec)
	}
	m.countForward("accepted")
	if m.mets != nil {
		m.mets.TasksDelegated.Inc()
	}
	m.log.Info("task_delegated", "task_id", env.GetTaskId(), "to", first.to.String(), "ack", first.ack.GetStatus().String())
	return true, ""
}

// noCandidateReason explains an empty candidate set. The skills are named
// because that is the answer 90% of the time: nobody in the local view
// advertises what the task asked for.
func (m *Manager) noCandidateReason(env *pb.TaskEnvelope) string {
	want := strings.Join(env.GetRequiredSkills(), ",")
	if want == "" {
		want = "(none requested)"
	}
	return fmt.Sprintf("no neighbour advertises skills [%s]", want)
}

// rejectSummary logs and renders the candidates' refusal reasons, so the
// operator sees the peers' own words in the task result instead of a generic
// "no eligible peers reachable".
func (m *Manager) rejectSummary(env *pb.TaskEnvelope, rejects *sync.Map) string {
	var reasons []string
	rejects.Range(func(k, v any) bool {
		pid, _ := k.(peer.ID)
		reasons = append(reasons, pid.String()+": "+fmt.Sprint(v))
		return true
	})
	if len(reasons) == 0 {
		m.log.Info("delegation_no_answer", "task_id", env.GetTaskId(),
			"skills", strings.Join(env.GetRequiredSkills(), ","))
		return "candidates did not answer"
	}
	sort.Strings(reasons)
	m.log.Warn("delegation_rejected", "task_id", env.GetTaskId(),
		"skills", strings.Join(env.GetRequiredSkills(), ","),
		"reasons", strings.Join(reasons, "; "))
	return strings.Join(reasons, "; ")
}

// canExecute reports capability under operator policy (not load): queueing is
// bounded separately so that admission control is explicit.
func (m *Manager) canExecute(env *pb.TaskEnvelope) bool {
	if !m.cfg.Capabilities.AcceptExternalTasks && env.GetOriginPeerId() != m.self.String() {
		return false
	}
	if _, ok := SkillsMatch(m.cfg.EffectiveSkills(), env.GetRequiredSkills()); !ok {
		return false
	}
	c := env.GetConstraints()
	// The task may demand less than the node allows, never more: a task
	// asking for shell on a no-shell node is not executable *faithfully*.
	if c.GetAllowShell() && !m.cfg.Capabilities.AllowShell {
		return false
	}
	if c.GetAllowNetworkTools() && !m.cfg.Capabilities.AllowNetworkTools {
		return false
	}
	return true
}

// runLocal executes the task through the PicoClaw adapter and delivers the
// signed result: to the local waiter when we originated it, upstream otherwise.
func (m *Manager) runLocal(ctx context.Context, env *pb.TaskEnvelope, deadline time.Time) {
	// Bounded queueing: a slot token first, then the semaphore itself.
	select {
	case m.queued <- struct{}{}:
		defer func() { <-m.queued }()
	case <-time.After(time.Until(deadline)):
		m.fail(env, "queue is full past task deadline", pb.TaskStatus_TASK_STATUS_TIMEOUT)
		return
	}
	acquire := time.NewTimer(time.Until(deadline))
	defer acquire.Stop()
	select {
	case m.slots <- struct{}{}:
		defer func() { <-m.slots }()
	case <-acquire.C:
		m.fail(env, "deadline expired while waiting for a free slot", pb.TaskStatus_TASK_STATUS_TIMEOUT)
		return
	}

	h := m.handleOf(env.GetTaskId())
	if h == nil {
		return // canceled or already resolved
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		m.fail(env, "deadline passed before execution", pb.TaskStatus_TASK_STATUS_TIMEOUT)
		return
	}
	// The span opens once a slot is in hand: queueing behind a busy adapter is
	// the node's backlog, not this task's execution, and conflating the two
	// would make a saturated worker look like slow agents. The execution
	// context is derived from it, so a timeout shows inside the span too.
	xctx, execSpan := m.startTaskSpan(ctx, tracing.SpanExecute, env)
	xctx = spanOnly(xctx)
	defer execSpan.End()
	execSpan.SetAttributes(attribute.String("zeptomesh.worker_peer_id", m.self.String()))
	timeout := time.Duration(timeoutSeconds(env, m.cfg)) * time.Second
	if timeout > remaining {
		timeout = remaining
	}
	ectx, cancel := context.WithTimeout(xctx, timeout)
	defer cancel()

	m.mu.Lock()
	if h := m.inflight[env.GetTaskId()]; h != nil {
		h.running = true
		h.cancelFn = cancel
	} else {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	m.updateStatus(env.GetTaskId(), Running)
	if m.mets != nil {
		m.mets.TasksRunning.Inc()
		m.mets.PicoClawRunning.Inc()
		defer m.mets.TasksRunning.Dec()
		defer m.mets.PicoClawRunning.Dec()
	}
	m.log.Info("task_executing", "task_id", env.GetTaskId(), "origin", env.GetOriginPeerId())

	home, workspace, err := m.prepareSandbox(env)
	if err != nil {
		m.fail(env, "sandbox: "+err.Error(), pb.TaskStatus_TASK_STATUS_FAILED)
		return
	}

	resp, execErr := m.adapter.Execute(ectx, picoclaw.Request{
		TaskID:            env.GetTaskId(),
		Instruction:       env.GetPayload().GetInstruction(),
		Skills:            append([]string(nil), env.GetRequiredSkills()...),
		Workspace:         workspace,
		Home:              home,
		AllowShell:        env.GetConstraints().GetAllowShell() && m.cfg.Capabilities.AllowShell,
		AllowNetwork:      env.GetConstraints().GetAllowNetworkTools() && m.cfg.Capabilities.AllowNetworkTools,
		Timeout:           timeout,
		MaxWorkspaceBytes: m.cfg.Tasks.MaxWorkspaceBytes,
		MaxMemoryBytes:    m.cfg.Tasks.MaxTaskMemoryBytes,
		SessionKey:        "mesh:" + env.GetTaskId(),
	})

	res := m.baseResult(env, pb.TaskStatus_TASK_STATUS_COMPLETED)
	if h := m.handleOf(env.GetTaskId()); h != nil {
		m.mu.Lock()
		h.model = m.executedModel(resp)
		m.mu.Unlock()
	}
	switch {
	case execErr != nil:
		execSpan.RecordError(execErr)
		if m.mets != nil {
			m.mets.PicoClawErrors.Inc()
		}
		res.Status = pb.TaskStatus_TASK_STATUS_FAILED
		if errors.Is(execErr, picoclaw.ErrTimeout) || errors.Is(execErr, context.DeadlineExceeded) {
			res.Status = pb.TaskStatus_TASK_STATUS_TIMEOUT
		}
		res.ErrorMessage = sanitizeErr(execErr)
		execSpan.SetStatus(codes.Error, res.ErrorMessage)
		if resp != nil && strings.TrimSpace(resp.Text) != "" {
			res.Text = capText(resp.Text, halfMax(m.cfg))
		}
	default:
		res.Text = capText(resp.Text, halfMax(m.cfg))
		res.Artifacts = m.archiveArtifacts(env, resp)
	}
	m.finishExecution(env, res)
}

// finishExecution delivers a locally produced result: origin absorbs,
// executor relays upstream.
func (m *Manager) finishExecution(env *pb.TaskEnvelope, res *pb.TaskResult) {
	// Content digest first: it commits text and artifact hashes, and both the
	// transport signature and the task journal record cover it (ТЗ 9.2).
	if d, err := wire.ResultDigest(res); err == nil {
		res.ResultDigest = d
	} else {
		m.log.Warn("result_digest_failed", "task_id", env.GetTaskId(), "err", err.Error())
	}
	// The worker signature is the chain anchor (ТЗ 6.10.3): relays overwrite
	// the transport signature with their own, but this one covers only the
	// worker-authored content and lets the origin detect an intermediate node
	// tampering with the answer.
	if err := m.signer.SignWorkerResult(res); err != nil {
		m.log.Error("result_sign_failed", "task_id", env.GetTaskId(), "err", err.Error())
		return
	}
	m.mu.Lock()
	h := m.inflight[env.GetTaskId()]
	m.mu.Unlock()
	if h == nil {
		// Canceled meanwhile; journal what we have.
		m.recordOutcome(res)
		return
	}
	if h.waiter != nil {
		m.absorbResult(res)
		m.deliverWaiter(h, res)
		m.resolve(env.GetTaskId())
		return
	}
	if h.parentID != "" {
		// Executed locally as part of our own decomposition plan.
		m.recordOutcome(res)
		m.resolve(env.GetTaskId())
		m.foldIfChild(h, res)
		return
	}
	m.recordOutcome(res)
	if h.upstream != "" {
		dctx, cancel := context.WithTimeout(context.Background(), m.svc.Timeout())
		defer cancel()
		if _, err := m.svc.SendResult(dctx, h.upstream, res); err != nil {
			m.log.Warn("result_send_failed", "task_id", env.GetTaskId(), "to", h.upstream.String(), "err", err.Error())
		} else {
			m.table.RecordSuccess(h.upstream)
		}
	}
	m.resolve(env.GetTaskId())
}

// fail records a synthetic failure result and routes it like a completion.
func (m *Manager) fail(env *pb.TaskEnvelope, msg string, st pb.TaskStatus) {
	m.finishExecution(env, m.errorResult(env, msg, st))
}

func (m *Manager) absorbResult(res *pb.TaskResult) {
	m.recordOutcome(res)
}

// executedModel names the model behind a local execution for the journal
// (ТЗ 10.3). The agent's own disclosure wins: a configured default is only what
// the node asked for, and the answer is evidence of what actually replied.
func (m *Manager) executedModel(resp *picoclaw.Response) string {
	if resp != nil && strings.TrimSpace(resp.Model) != "" {
		return strings.TrimSpace(resp.Model)
	}
	return picoclaw.ModelOf(m.adapter)
}

// recordOutcome journals a terminal result once per node.
func (m *Manager) recordOutcome(res *pb.TaskResult) {
	rr := recordFromResult(res)
	if h := m.handleOf(res.GetTaskId()); h != nil {
		m.mu.Lock()
		rr.Model = h.model
		m.mu.Unlock()
	}
	_ = m.store.PutResult(rr)
	m.journalMu.Lock()
	defer m.journalMu.Unlock()
	rec, err := m.store.GetTask(res.GetTaskId())
	if err != nil {
		return
	}
	rec.Status = StatusFromProto(res.GetStatus()).String()
	if !time.Unix(res.GetFinishedAt(), 0).IsZero() {
		rec.FinishedAt = time.Unix(res.GetFinishedAt(), 0).UTC()
	} else {
		rec.FinishedAt = time.Now().UTC()
	}
	rec.WorkerPeerID = res.GetWorkerPeerId()
	rec.Error = res.GetErrorMessage()
	rec.ResultDigest = fmt.Sprintf("%x", res.GetResultDigest())
	_ = m.store.PutTask(rec)

	if m.mets == nil {
		return
	}
	switch res.GetStatus() {
	case pb.TaskStatus_TASK_STATUS_COMPLETED:
		m.mets.TasksCompleted.Inc()
	case pb.TaskStatus_TASK_STATUS_TIMEOUT:
		m.mets.TasksTimedOut.Inc()
	case pb.TaskStatus_TASK_STATUS_REJECTED:
		m.mets.TasksRejected.Inc()
	default:
		m.mets.TasksFailed.Inc()
	}
	m.mets.TaskDuration.Observe(float64(res.GetFinishedAt() - res.GetStartedAt()))
	m.mets.TaskRouteHops.Observe(float64(len(res.GetRouteStack())))
}

func (m *Manager) applyCancelLocal(taskID, reason string) {
	m.mu.Lock()
	h := m.inflight[taskID]
	if h != nil {
		if h.cancelFn != nil {
			h.cancelFn()
		}
	}
	m.mu.Unlock()
	if h == nil {
		return
	}
	res := m.errorResult(h.env, reason, pb.TaskStatus_TASK_STATUS_CANCELED)
	if err := m.signer.SignResult(res); err == nil {
		switch {
		case h.waiter != nil:
			m.absorbResult(res)
			m.deliverWaiter(h, res)
		case h.parentID != "":
			m.recordOutcome(res)
			m.resolve(taskID)
			m.foldIfChild(h, res)
			m.countCanceled()
			return
		default:
			m.recordOutcome(res)
			if h.upstream != "" {
				dctx, cancel := context.WithTimeout(context.Background(), m.svc.Timeout())
				_, _ = m.svc.SendResult(dctx, h.upstream, res)
				cancel()
			}
		}
	}
	m.resolve(taskID)
	m.countCanceled()
}

// janitor enforces deadlines: tracked tasks whose result never arrived
// (crashed downstream, lost route) are timed out and, when we were relaying,
// the timeout is sent upstream.
func (m *Manager) janitor(ctx context.Context) {
	defer m.wg.Done()
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now().UTC()
		m.mu.Lock()
		var due []*handle
		for id, h := range m.inflight {
			if now.After(h.deadline) {
				due = append(due, h)
				delete(m.inflight, id)
			}
		}
		m.mu.Unlock()
		for _, h := range due {
			res := m.errorResult(h.env, "deadline exceeded with no result from downstream", pb.TaskStatus_TASK_STATUS_TIMEOUT)
			if err := m.signer.SignResult(res); err == nil {
				switch {
				case h.waiter != nil:
					m.absorbResult(res)
					select {
					case h.waiter <- res:
					default:
					}
				case h.parentID != "":
					m.recordOutcome(res)
					m.foldIfChild(h, res)
				default:
					m.recordOutcome(res)
					if h.upstream != "" {
						dctx, cancel := context.WithTimeout(context.Background(), m.svc.Timeout())
						_, _ = m.svc.SendResult(dctx, h.upstream, res)
						cancel()
					}
				}
			}
			m.countTimeout()
		}
	}
}

// sandboxSweeper removes task scratch directories older than retention.
func (m *Manager) sandboxSweeper(ctx context.Context) {
	defer m.wg.Done()
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	cutoff := func() time.Time {
		return time.Now().UTC().Add(-m.cfg.Tasks.Retention.D())
	}
	sweep := func() {
		root := m.cfg.TasksDir()
		entries, err := os.ReadDir(root)
		if err != nil {
			return
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			info, err := e.Info()
			if err != nil || info.ModTime().After(cutoff()) {
				continue
			}
			m.mu.Lock()
			tracked := m.inflight != nil && m.hasTaskNamed(e.Name())
			m.mu.Unlock()
			if tracked {
				continue
			}
			_ = os.RemoveAll(filepath.Join(root, e.Name()))
		}
		if n, err := m.store.Prune(ctx, m.cfg.Tasks.Retention.D()); err == nil && n > 0 {
			m.log.Info("journal_pruned", "records_removed", n)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sweep()
		}
	}
}

func (m *Manager) hasTaskNamed(dirname string) bool {
	for id := range m.inflight {
		if sanitizeID(id) == dirname {
			return true
		}
	}
	return false
}

// ---------- bookkeeping helpers ----------

func (m *Manager) register(taskID string, h *handle) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.inflight[taskID]; dup {
		return false
	}
	m.inflight[taskID] = h
	return true
}

func (m *Manager) tracked(taskID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.inflight[taskID]
	return ok
}

func (m *Manager) handleOf(taskID string) *handle {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inflight[taskID]
}

// senderInflight counts tasks accepted from one origin peer that have not
// settled yet; it backs tasks.max_parallel_tasks_per_peer.
func (m *Manager) senderInflight(remote peer.ID) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, h := range m.inflight {
		if h.env.GetOriginPeerId() == remote.String() {
			n++
		}
	}
	return n
}

func (m *Manager) deliverWaiter(h *handle, res *pb.TaskResult) {
	if h == nil || h.waiter == nil || res == nil {
		return
	}
	select {
	case h.waiter <- res:
	default:
	}
}

func (m *Manager) resolve(taskID string) {
	m.mu.Lock()
	delete(m.inflight, taskID)
	m.mu.Unlock()
}

func (m *Manager) updateStatus(taskID string, s Status) {
	m.journalMu.Lock()
	defer m.journalMu.Unlock()
	if rec, err := m.store.GetTask(taskID); err == nil {
		// Same rule as forward's accounting: a non-terminal update must not
		// resurrect a record a concurrent cancel or fast result already closed.
		if !ParseStatus(rec.Status).Terminal() {
			rec.Status = s.String()
			_ = m.store.PutTask(rec)
		}
	}
}

func (m *Manager) countReceived() {
	if m.mets != nil {
		m.mets.TasksReceived.Inc()
	}
}

func (m *Manager) countDuplicate() {
	if m.mets != nil {
		m.mets.TasksDuplicate.Inc()
	}
}

func (m *Manager) countCanceled() {
	if m.mets != nil {
		m.mets.Security("task_canceled")
	}
}

func (m *Manager) countTimeout() {
	if m.mets != nil {
		m.mets.TasksTimedOut.Inc()
	}
}

func (m *Manager) countForward(outcome string) {
	if m.mets != nil {
		m.mets.Forward(outcome)
	}
}

func (m *Manager) ack(taskID string, st pb.AckStatus, reason string) *pb.TaskAck {
	a := &pb.TaskAck{
		TaskId:     taskID,
		Status:     st,
		Reason:     reason,
		AcceptedBy: m.self.String(),
		Timestamp:  time.Now().UTC().Unix(),
	}
	if err := m.signer.SignAck(a); err != nil {
		m.log.Error("ack_sign_failed", "task_id", taskID, "err", err.Error())
	}
	if st == pb.AckStatus_ACK_STATUS_REJECTED && m.mets != nil {
		m.mets.TasksRejected.Inc()
	}
	return a
}

func (m *Manager) recordJournal(env *pb.TaskEnvelope, s Status, delegated bool) {
	now := time.Now().UTC()
	hash, _, _ := m.store.StoreArtifact([]byte(env.GetPayload().GetInstruction()))
	rec := &storage.TaskRecord{
		TaskID:         env.GetTaskId(),
		ParentTaskID:   env.GetParentTaskId(),
		OriginPeerID:   env.GetOriginPeerId(),
		SenderPeerID:   env.GetSenderPeerId(),
		Status:         s.String(),
		RequiredSkills: append([]string(nil), env.GetRequiredSkills()...),
		ReceivedAt:     now,
		TTL:            env.GetTtl(),
		Priority:       env.GetPriority(),
		PayloadHash:    hash,
	}
	if env.GetParentTaskId() != "" {
		_ = m.store.LinkChild(env.GetParentTaskId(), env.GetTaskId())
	}
	_ = m.store.PutTask(rec)
}

// totalBudget bounds the whole task: execution timeout plus per-hop routing
// slack plus one minute of clock tolerance.
func (m *Manager) totalBudget(env *pb.TaskEnvelope) time.Duration {
	base := time.Duration(timeoutSeconds(env, m.cfg)) * time.Second
	ttl := int64(env.GetTtl())
	if ttl < 1 {
		ttl = 1
	}
	return base + time.Duration(ttl)*m.cfg.Tasks.Forwarding.AttemptTimeout.D() + time.Minute
}

func timeoutSeconds(env *pb.TaskEnvelope, cfg *config.Config) int {
	return TimeoutSeconds(env, cfg.Tasks.DefaultTimeoutSeconds, cfg.Tasks.MaxTimeoutSeconds)
}

func (m *Manager) prepareSandbox(env *pb.TaskEnvelope) (home, workspace string, err error) {
	// Disk guard first (ТЗ 6.8.4): refusing a task while the volume still has
	// headroom beats starting it and failing halfway on ENOSPC. The check is
	// best-effort — an unsupported filesystem must not block execution.
	if min := m.cfg.Tasks.MaxDiskFreeBytes; min > 0 {
		if free, derr := diskFree(m.cfg.TasksDir()); derr == nil && free < uint64(min) {
			return "", "", fmt.Errorf("free disk %d bytes is below the configured minimum of %d", free, min)
		}
	}
	base := filepath.Join(m.cfg.TasksDir(), sanitizeID(env.GetTaskId()))
	home = filepath.Join(base, "home")
	workspace = filepath.Join(base, "workspace")
	if err := os.MkdirAll(home, 0o700); err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		return "", "", err
	}
	// A mesh-owned config isolates the instance from the operator's ~/.picoclaw
	// and pins the workspace to the per-task directory. Credentials come from
	// the environment (PicoClaw reads PICOCLAW_* overrides).
	cfgPath := filepath.Join(home, "config.json")
	if _, statErr := os.Stat(cfgPath); errors.Is(statErr, os.ErrNotExist) {
		content := fmt.Sprintf(
			`{"version":3,"agents":{"defaults":{"workspace":%q}},"gateway":{"host":"127.0.0.1","port":0}}`,
			workspace) + "\n"
		if werr := os.WriteFile(cfgPath, []byte(content), 0o600); werr != nil {
			return "", "", werr
		}
	}
	return home, workspace, nil
}

// archiveArtifacts stores adapter outputs content-addressed; oversized or
// unreadable files are skipped rather than failing the task.
func (m *Manager) archiveArtifacts(env *pb.TaskEnvelope, resp *picoclaw.Response) []*pb.ArtifactRef {
	if resp == nil {
		return nil
	}
	var out []*pb.ArtifactRef
	for _, a := range resp.Artifacts {
		data, err := os.ReadFile(a.Path)
		if err != nil || int64(len(data)) > m.cfg.Tasks.MaxArtifactBytes {
			continue
		}
		hash, size, serr := m.store.StoreArtifact(data)
		if serr != nil {
			continue
		}
		out = append(out, &pb.ArtifactRef{Name: a.Name, Hash: hash, Size: size})
	}
	return out
}

func (m *Manager) keyLookup() security.KeyLookup {
	h := m.svc.Host().Underlying()
	return func(p peer.ID) (ic.PubKey, error) {
		pk := h.Peerstore().PubKey(p)
		if pk == nil {
			return nil, fmt.Errorf("tasks: no public key in peerstore for %s", p)
		}
		return pk, nil
	}
}

func (m *Manager) securityEvent(event string, p peer.ID, taskID, detail string) {
	if m.audit != nil {
		m.audit.Log(security.AuditEvent{
			Event: event, PeerID: p.String(), TaskID: taskID, Detail: capText(detail, 300),
		})
	}
}

// ---------- pure helpers ----------

func (m *Manager) baseResult(env *pb.TaskEnvelope, st pb.TaskStatus) *pb.TaskResult {
	now := time.Now().UTC()
	return &pb.TaskResult{
		TaskId:       env.GetTaskId(),
		WorkerPeerId: m.self.String(),
		SenderPeerId: m.self.String(),
		Status:       st,
		StartedAt:    env.GetCreatedAt(),
		FinishedAt:   now.Unix(),
		RouteStack:   append([]string(nil), env.GetRouteStack()...),
	}
}

func (m *Manager) errorResult(env *pb.TaskEnvelope, msg string, st pb.TaskStatus) *pb.TaskResult {
	res := m.baseResult(env, st)
	res.ErrorMessage = capText(msg, 1024)
	res.ErrorClass = ErrorClass(st, msg)
	return res
}

func cloneEnv(env *pb.TaskEnvelope) *pb.TaskEnvelope {
	return proto.Clone(env).(*pb.TaskEnvelope)
}

func recordFromResult(res *pb.TaskResult) *storage.ResultRecord {
	rec := &storage.ResultRecord{
		TaskID:          res.GetTaskId(),
		Status:          StatusFromProto(res.GetStatus()).String(),
		Text:            res.GetText(),
		ErrorMessage:    res.GetErrorMessage(),
		WorkerPeerID:    res.GetWorkerPeerId(),
		StartedAt:       time.Unix(res.GetStartedAt(), 0).UTC(),
		FinishedAt:      time.Unix(res.GetFinishedAt(), 0).UTC(),
		Signature:       append([]byte(nil), res.GetSignature()...),
		WorkerSignature: append([]byte(nil), res.GetWorkerSignature()...),
		ResultDigest:    fmt.Sprintf("%x", res.GetResultDigest()),
		Aggregated:      res.GetAggregated(),
		ErrorClass:      errorClassName(res.GetErrorClass()),
	}
	for _, a := range res.GetArtifacts() {
		rec.Artifacts = append(rec.Artifacts, storage.ArtifactInfo{Name: a.GetName(), Hash: a.GetHash(), Size: a.GetSize()})
	}
	return rec
}

func resultFromRecord(rec *storage.ResultRecord) *pb.TaskResult {
	res := &pb.TaskResult{
		TaskId:          rec.TaskID,
		Status:          statusToProtoByName(rec.Status),
		Text:            rec.Text,
		ErrorMessage:    rec.ErrorMessage,
		WorkerPeerId:    rec.WorkerPeerID,
		SenderPeerId:    rec.WorkerPeerID,
		StartedAt:       rec.StartedAt.Unix(),
		FinishedAt:      rec.FinishedAt.Unix(),
		Signature:       rec.Signature,
		WorkerSignature: rec.WorkerSignature,
		Aggregated:      rec.Aggregated,
	}
	if d, err := hex.DecodeString(rec.ResultDigest); err == nil && rec.ResultDigest != "" {
		res.ResultDigest = d
	}
	// The class is read back as stored; a journal written before the field
	// existed (or a hand-edited record) falls back to deriving it, the same way
	// the aggregator does, so a retryable failure never degrades to UNSPECIFIED.
	res.ErrorClass = errorClassByName(rec.ErrorClass)
	if res.ErrorClass == pb.TaskErrorClass_TASK_ERROR_CLASS_UNSPECIFIED &&
		res.GetErrorMessage() != "" {
		res.ErrorClass = ErrorClass(res.GetStatus(), res.GetErrorMessage())
	}
	for _, a := range rec.Artifacts {
		res.Artifacts = append(res.Artifacts, &pb.ArtifactRef{Name: a.Name, Hash: a.Hash, Size: a.Size})
	}
	return res
}

func statusToProtoByName(s string) pb.TaskStatus {
	for _, st := range []Status{Completed, Failed, TimedOut, Canceled, Accepted, Rejected, Running, Forwarded} {
		if st.String() == s {
			return st.ToProto()
		}
	}
	return pb.TaskStatus_TASK_STATUS_UNSPECIFIED
}

func sanitizeID(s string) string {
	return strings.NewReplacer("/", "-", ":", "-", "\\", "-", "..", "-").Replace(s)
}

func capText(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n] + "…[truncated]"
}

func halfMax(cfg *config.Config) int {
	if cfg.Tasks.MaxPayloadBytes <= 0 {
		return 1 << 19
	}
	return int(cfg.Tasks.MaxPayloadBytes / 2)
}

func sanitizeErr(err error) string {
	return capText(strings.TrimSpace(err.Error()), 512)
}

// OnPeerDisconnected fails every task whose accepted downstream holder just
// dropped the connection (ТЗ 17.2.5: a worker crashing mid-task must not
// leave the origin waiting until the deadline). Tasks executing locally are
// unaffected. The failure is routed exactly like a normal result: origin
// waiter, subtask fanout, or upstream relay.
func (m *Manager) OnPeerDisconnected(p peer.ID) {
	m.mu.Lock()
	var victims []*handle
	for _, h := range m.inflight {
		if h.downstream == p && !h.running {
			victims = append(victims, h)
			h.downstream = "" // the cancel path must not chase a dead peer
		}
	}
	m.mu.Unlock()

	for _, h := range victims {
		res := m.errorResult(h.env, "downstream peer disconnected before returning a result",
			pb.TaskStatus_TASK_STATUS_TIMEOUT)
		if err := m.signer.SignResult(res); err != nil {
			m.recordOutcome(res)
			m.resolve(h.env.GetTaskId())
			continue
		}
		switch {
		case h.waiter != nil:
			m.absorbResult(res)
			m.deliverWaiter(h, res)
		case h.parentID != "":
			m.resolve(h.env.GetTaskId())
			m.foldIfChild(h, res)
			continue
		case h.upstream != "":
			m.recordOutcome(res)
			dctx, cancel := context.WithTimeout(context.Background(), m.svc.Timeout())
			if _, err := m.svc.SendResult(dctx, h.upstream, res); err != nil {
				m.log.Warn("failure_relay_failed", "task_id", h.env.GetTaskId(), "err", err.Error())
			}
			cancel()
		default:
			m.recordOutcome(res)
		}
		m.resolve(h.env.GetTaskId())
		m.countTimeout()
		m.log.Info("task_failed_peer_gone", "task_id", h.env.GetTaskId(), "peer", p.String())
	}
}
