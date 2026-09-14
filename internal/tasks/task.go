// Package tasks models the unit of work exchanged by mesh nodes and drives its
// lifecycle: validation, local execution, delegation and aggregation.
package tasks

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/wire"
)

// Status is the task lifecycle state, mirroring pb.TaskStatus.
type Status int32

// Lifecycle states.
const (
	Received Status = iota + 1
	Validating
	Evaluating
	Accepted
	Rejected
	Forwarded
	Running
	WaitingSubtasks
	Aggregating
	Completed
	Failed
	TimedOut
	Canceled
)

// String implements fmt.Stringer.
func (s Status) String() string {
	switch s {
	case Received:
		return "RECEIVED"
	case Validating:
		return "VALIDATING"
	case Evaluating:
		return "EVALUATING"
	case Accepted:
		return "ACCEPTED"
	case Rejected:
		return "REJECTED"
	case Forwarded:
		return "FORWARDED"
	case Running:
		return "RUNNING"
	case WaitingSubtasks:
		return "WAITING_SUBTASKS"
	case Aggregating:
		return "AGGREGATING"
	case Completed:
		return "COMPLETED"
	case Failed:
		return "FAILED"
	case TimedOut:
		return "TIMEOUT"
	case Canceled:
		return "CANCELED"
	default:
		return "UNKNOWN"
	}
}

// Terminal reports whether no further transitions are expected.
func (s Status) Terminal() bool {
	switch s {
	case Completed, Failed, TimedOut, Canceled, Rejected:
		return true
	}
	return false
}

// ToProto maps to the wire enum.
func (s Status) ToProto() pb.TaskStatus {
	switch s {
	case Completed:
		return pb.TaskStatus_TASK_STATUS_COMPLETED
	case Failed:
		return pb.TaskStatus_TASK_STATUS_FAILED
	case TimedOut:
		return pb.TaskStatus_TASK_STATUS_TIMEOUT
	case Canceled:
		return pb.TaskStatus_TASK_STATUS_CANCELED
	case Accepted:
		return pb.TaskStatus_TASK_STATUS_ACCEPTED
	case Rejected:
		return pb.TaskStatus_TASK_STATUS_REJECTED
	case Running:
		return pb.TaskStatus_TASK_STATUS_RUNNING
	case Forwarded:
		return pb.TaskStatus_TASK_STATUS_FORWARDED
	case WaitingSubtasks:
		return pb.TaskStatus_TASK_STATUS_WAITING_SUBTASKS
	case Aggregating:
		return pb.TaskStatus_TASK_STATUS_AGGREGATING
	default:
		return pb.TaskStatus_TASK_STATUS_UNSPECIFIED
	}
}

// StatusFromProto maps the wire enum back to Status.
func StatusFromProto(s pb.TaskStatus) Status {
	switch s {
	case pb.TaskStatus_TASK_STATUS_COMPLETED:
		return Completed
	case pb.TaskStatus_TASK_STATUS_FAILED:
		return Failed
	case pb.TaskStatus_TASK_STATUS_TIMEOUT:
		return TimedOut
	case pb.TaskStatus_TASK_STATUS_CANCELED:
		return Canceled
	case pb.TaskStatus_TASK_STATUS_ACCEPTED:
		return Accepted
	case pb.TaskStatus_TASK_STATUS_REJECTED:
		return Rejected
	case pb.TaskStatus_TASK_STATUS_RUNNING:
		return Running
	case pb.TaskStatus_TASK_STATUS_FORWARDED:
		return Forwarded
	case pb.TaskStatus_TASK_STATUS_WAITING_SUBTASKS:
		return WaitingSubtasks
	case pb.TaskStatus_TASK_STATUS_AGGREGATING:
		return Aggregating
	default:
		return Received
	}
}

// Errors returned by validation.
var (
	ErrEmptyTaskID     = errors.New("task: empty task_id")
	ErrMissingOrigin   = errors.New("task: missing origin_peer_id")
	ErrMissingSender   = errors.New("task: missing sender_peer_id")
	ErrNoInstruction   = errors.New("task: empty instruction")
	ErrTTLExhausted    = errors.New("task: ttl exhausted")
	ErrRouteLoop       = errors.New("task: route loop detected")
	ErrPayloadTooLarge = errors.New("task: payload exceeds limit")
	ErrTooManySkills   = errors.New("task: too many required_skills")
	ErrBadPriority     = errors.New("task: priority out of range 1..9")
	ErrStale           = errors.New("task: created too far in the past")
	ErrTooFarAhead     = errors.New("task: created too far in the future")
	ErrRouteTooLong    = errors.New("task: route_stack too long")
	ErrDigestMismatch  = errors.New("task: context_digest does not match payload")
)

// Limits that hold regardless of configuration.
const (
	MaxRequiredSkills = 16
	MaxRouteStack     = 64
	MaxInstructionLen = 1 << 20
	// ClockSkewTolerance bounds how far a peer's clock may drift before its
	// tasks look like replays.
	ClockSkewTolerance = 5 * time.Minute
	// MaxAge is the oldest a task may be when first seen.
	MaxAge = 24 * time.Hour
)

// NewID returns a time-ordered UUIDv7 task id.
func NewID() string {
	u, err := uuid.NewV7()
	if err != nil {
		// NewV7 only fails if the OS entropy source fails; fall back to v4
		// rather than panicking inside a long-running daemon.
		return uuid.NewString()
	}
	return u.String()
}

// BuildRequest is the input for constructing a root task envelope.
type BuildRequest struct {
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
	Attachments    []*pb.ArtifactRef
}

// Build creates an unsigned root envelope. Signing is the caller's job so that
// key material stays outside this package.
func Build(originPeer, id string, req BuildRequest, now time.Time) *pb.TaskEnvelope {
	if id == "" {
		id = NewID()
	}
	ttl := req.TTL
	if ttl <= 0 {
		ttl = 5
	}
	prio := req.Priority
	if prio <= 0 {
		prio = 5
	}
	allowDeleg := true
	if req.AllowDeleg != nil {
		allowDeleg = *req.AllowDeleg
	}
	allowSub := false
	if req.AllowSubtasks != nil {
		allowSub = *req.AllowSubtasks
	}
	env := &pb.TaskEnvelope{
		TaskId:         id,
		OriginPeerId:   originPeer,
		SenderPeerId:   originPeer,
		CreatedAt:      now.UTC().Unix(),
		Ttl:            ttl,
		Priority:       prio,
		RequiredSkills: NormalizeSkills(req.RequiredSkills),
		Payload: &pb.TaskPayload{
			Instruction: req.Instruction,
			Attachments: req.Attachments,
			Labels:      req.Labels,
		},
		Constraints: &pb.TaskConstraints{
			MaxDurationSeconds: req.TimeoutSeconds,
			AllowNetworkTools:  req.AllowNetwork,
			AllowShell:         req.AllowShell,
			AllowDelegation:    allowDeleg,
			AllowSubtasks:      allowSub,
		},
	}
	env.ContextDigest = ContentDigest(env)
	return env
}

// ContentDigest is the stable hash of everything the origin authored. It is
// deliberately independent of the mutable routing fields (sender, ttl,
// route_stack) so any node on the path can recompute and verify it.
func ContentDigest(env *pb.TaskEnvelope) string {
	body, err := wire.TaskContent(env)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(append([]byte("zeptomesh/content\x00"), body...))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// VerifyDigest recomputes the content digest and compares it with the field.
func VerifyDigest(env *pb.TaskEnvelope) error {
	want := ContentDigest(env)
	got := env.GetContextDigest()
	if got == "" {
		return fmt.Errorf("%w: missing", ErrDigestMismatch)
	}
	if !strings.EqualFold(want, got) {
		return ErrDigestMismatch
	}
	return nil
}

// ValidationError reports every rule an envelope broke. It unwraps to its
// causes so a caller can classify a rejection with errors.Is, while its message
// names them all — an operator fixing a rejected task should not have to
// rediscover one violation per round trip.
type ValidationError struct {
	Reasons []error
}

func (e *ValidationError) Error() string {
	msgs := make([]string, 0, len(e.Reasons))
	for _, r := range e.Reasons {
		msgs = append(msgs, r.Error())
	}
	return "task: invalid: " + strings.Join(msgs, "; ")
}

// Unwrap lets errors.Is/errors.As reach each individual rule.
func (e *ValidationError) Unwrap() []error { return e.Reasons }

// Validate checks structural and policy invariants of an envelope.
func Validate(env *pb.TaskEnvelope, maxPayload int64, now time.Time) error {
	if env == nil {
		return errors.New("task: nil envelope")
	}
	var errs []error
	require := func(cond bool, err error) {
		if !cond {
			errs = append(errs, err)
		}
	}
	require(env.GetTaskId() != "", ErrEmptyTaskID)
	require(env.GetOriginPeerId() != "", ErrMissingOrigin)
	require(env.GetSenderPeerId() != "", ErrMissingSender)
	// Blank in the whitespace sense is blank: the adapter refuses such a prompt,
	// so accepting it here would spend a task slot to fail the job later instead
	// of refusing it at the boundary with a reason.
	require(strings.TrimSpace(env.GetPayload().GetInstruction()) != "", ErrNoInstruction)
	require(len(env.GetPayload().GetInstruction()) <= MaxInstructionLen, ErrPayloadTooLarge)
	require(env.GetTtl() >= 0, ErrTTLExhausted)
	require(env.GetPriority() >= 1 && env.GetPriority() <= 9, ErrBadPriority)
	require(len(env.GetRequiredSkills()) <= MaxRequiredSkills, ErrTooManySkills)
	require(len(env.GetRouteStack()) <= MaxRouteStack, ErrRouteTooLong)

	if maxPayload > 0 {
		require(int64(proto.Size(env)) <= maxPayload, ErrPayloadTooLarge)
	}
	if created := env.GetCreatedAt(); created > 0 {
		age := now.UTC().Unix() - created
		require(age <= int64(MaxAge.Seconds()), ErrStale)
		require(age >= -int64(ClockSkewTolerance.Seconds()), ErrTooFarAhead)
	}
	if err := VerifyDigest(env); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return &ValidationError{Reasons: errs}
	}
	return nil
}

// CheckRoute rejects a task that already visited this peer or whose ttl is spent.
func CheckRoute(env *pb.TaskEnvelope, self string) error {
	if env.GetTtl() <= 0 {
		return ErrTTLExhausted
	}
	for _, hop := range env.GetRouteStack() {
		if hop == self {
			return ErrRouteLoop
		}
	}
	return nil
}

// AppendRoute records a hop. It is idempotent for the same peer.
func AppendRoute(env *pb.TaskEnvelope, p string) {
	if p == "" {
		return
	}
	stack := env.GetRouteStack()
	if len(stack) > 0 && stack[len(stack)-1] == p {
		return
	}
	env.RouteStack = append(env.RouteStack, p)
}

// PreviousHop resolves the peer to relay a result back to.
func PreviousHop(env *pb.TaskEnvelope) (string, bool) {
	stack := env.GetRouteStack()
	if len(stack) == 0 {
		return "", false
	}
	return stack[len(stack)-1], true
}

// DeriveSubtask builds a child envelope from a parent. The child inherits the
// origin and one less ttl; its constraints are the intersection of the parent's
// and the caller's request, so a subtask can never gain rights.
func DeriveSubtask(parent *pb.TaskEnvelope, childID string, req BuildRequest, now time.Time) *pb.TaskEnvelope {
	child := Build(parent.GetOriginPeerId(), childID, req, now)
	child.ParentTaskId = parent.GetTaskId()
	child.CreatedAt = now.UTC().Unix()

	ttl := req.TTL
	if ttl <= 0 {
		ttl = parent.GetTtl() - 1
	}
	if max := parent.GetTtl() - 1; ttl > max {
		ttl = max
	}
	if ttl < 0 {
		ttl = 0
	}
	child.Ttl = ttl

	pc := parent.GetConstraints()
	cc := child.Constraints
	cc.AllowShell = pc.GetAllowShell() && cc.AllowShell
	cc.AllowNetworkTools = pc.GetAllowNetworkTools() && cc.AllowNetworkTools
	cc.AllowDelegation = pc.GetAllowDelegation() && cc.AllowDelegation
	cc.AllowSubtasks = pc.GetAllowSubtasks() && cc.AllowSubtasks
	if pc.GetMaxDurationSeconds() > 0 {
		if cc.GetMaxDurationSeconds() == 0 || cc.GetMaxDurationSeconds() > pc.GetMaxDurationSeconds() {
			cc.MaxDurationSeconds = pc.GetMaxDurationSeconds()
		}
	}
	child.Priority = parent.GetPriority()
	child.ContextDigest = ContentDigest(child)
	return child
}

// TimeoutSeconds resolves the effective execution deadline for a task: the
// requested value when set, otherwise the node default — always capped by the
// operator's ceiling (ТЗ 6.8.3). Without the cap a remote requester could pin
// a worker slot for an arbitrarily long time by asking for a huge timeout.
func TimeoutSeconds(env *pb.TaskEnvelope, def, max int) int {
	t := def
	if c := env.GetConstraints().GetMaxDurationSeconds(); c > 0 {
		t = int(c)
	}
	if t <= 0 {
		t = 600
	}
	if max > 0 && t > max {
		t = max
	}
	return t
}

// ErrorClass maps a status and message onto the machine-readable taxonomy of
// ТЗ 6.12. Relays use it to decide whether retrying makes sense: network and
// no-worker failures are retryable, security/validation ones never are.
func ErrorClass(st pb.TaskStatus, msg string) pb.TaskErrorClass {
	switch st {
	case pb.TaskStatus_TASK_STATUS_CANCELED:
		return pb.TaskErrorClass_TASK_ERROR_CLASS_CANCELED
	case pb.TaskStatus_TASK_STATUS_TIMEOUT:
		return pb.TaskErrorClass_TASK_ERROR_CLASS_TIMEOUT
	case pb.TaskStatus_TASK_STATUS_REJECTED:
		return pb.TaskErrorClass_TASK_ERROR_CLASS_VALIDATION
	case pb.TaskStatus_TASK_STATUS_FAILED:
	default:
		return pb.TaskErrorClass_TASK_ERROR_CLASS_UNSPECIFIED
	}
	low := strings.ToLower(msg)
	// The no-worker cases are checked before the keyword scan: their messages
	// quote a remote peer's refusal verbatim, and those words could otherwise
	// flip the class (a "no eligible peers reachable: … allow_network_tools …"
	// would read as SECURITY, and a relay would then refuse to retry a failure
	// that is retryable elsewhere).
	switch {
	case strings.Contains(low, "no eligible"), strings.Contains(low, "no candidate"),
		strings.Contains(low, "no neighbour"), strings.Contains(low, "no capability"),
		strings.Contains(low, "skills"):
		return pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER
	case strings.Contains(low, "signature"), strings.Contains(low, "trust"),
		strings.Contains(low, "blocked"), strings.Contains(low, "allow"):
		return pb.TaskErrorClass_TASK_ERROR_CLASS_SECURITY
	case strings.Contains(low, "digest"), strings.Contains(low, "valid"),
		strings.Contains(low, "ttl"), strings.Contains(low, "unknown task"):
		return pb.TaskErrorClass_TASK_ERROR_CLASS_VALIDATION
	case strings.Contains(low, "queue"), strings.Contains(low, "rate"),
		strings.Contains(low, "disk"), strings.Contains(low, "memory"),
		strings.Contains(low, "limit"):
		return pb.TaskErrorClass_TASK_ERROR_CLASS_LIMITS
	case strings.Contains(low, "connect"), strings.Contains(low, "reset"),
		strings.Contains(low, "unreachable"), strings.Contains(low, "timeout "):
		return pb.TaskErrorClass_TASK_ERROR_CLASS_NETWORK
	default:
		return pb.TaskErrorClass_TASK_ERROR_CLASS_EXECUTION
	}
}

// errorClassName renders a class for the journal as its short form ("TIMEOUT"
// rather than "TASK_ERROR_CLASS_TIMEOUT"), so stored records stay readable and
// the prefix is not repeated in every task history entry. UNSPECIFIED renders
// as empty: a completed task has no class to record, and storing the word would
// make every successful entry look like it carries an error.
func errorClassName(ec pb.TaskErrorClass) string {
	if ec == pb.TaskErrorClass_TASK_ERROR_CLASS_UNSPECIFIED {
		return ""
	}
	return strings.TrimPrefix(ec.String(), "TASK_ERROR_CLASS_")
}

// errorClassByName is the inverse of errorClassName. An unknown or empty name
// yields UNSPECIFIED, which callers treat as "not recorded" and derive again.
func errorClassByName(s string) pb.TaskErrorClass {
	if s == "" {
		return pb.TaskErrorClass_TASK_ERROR_CLASS_UNSPECIFIED
	}
	if v, ok := pb.TaskErrorClass_value["TASK_ERROR_CLASS_"+s]; ok {
		return pb.TaskErrorClass(v)
	}
	return pb.TaskErrorClass_TASK_ERROR_CLASS_UNSPECIFIED
}

// NormalizeSkills lowercases, trims, deduplicates and sorts a skill list.
func NormalizeSkills(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.ToLower(strings.TrimSpace(s))
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// Describe renders a one-line task summary for logs.
func Describe(env *pb.TaskEnvelope) string {
	if env == nil {
		return "<nil>"
	}
	ins := env.GetPayload().GetInstruction()
	if len(ins) > 60 {
		ins = ins[:60] + "…"
	}
	return fmt.Sprintf("task %s ttl=%d skills=%v %q", env.GetTaskId(), env.GetTtl(),
		env.GetRequiredSkills(), ins)
}

// SkillsMatch reports whether a node offering haveSkills can serve wantSkills.
// The synthetic skills "general" and "any" match every requirement. Skill names
// arriving from the network or from a config file are matched case- and
// space-insensitively, mirroring NormalizeSkills.
func SkillsMatch(haveSkills, wantSkills []string) (matched int, ok bool) {
	norm := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
	have := make(map[string]bool, len(haveSkills))
	for _, s := range haveSkills {
		have[norm(s)] = true
	}
	if have["general"] || have["any"] {
		return len(wantSkills), true
	}
	for _, w := range wantSkills {
		if have[norm(w)] {
			matched++
		}
	}
	if len(wantSkills) == 0 {
		return 0, true
	}
	return matched, matched == len(wantSkills)
}

// skillsCover reports whether have contains every want, honouring the
// synthetic "general"/"any" wildcard skills.
func skillsCover(have, want []string) bool {
	_, ok := SkillsMatch(have, want)
	return ok
}

// dedupRecords merges peer records by id, keeping the first sighting of each
// peer but filling in any address or skill the first one lacked. Answers from
// several relay branches routinely overlap, and the branch that saw the peer
// first is not necessarily the one that knows the most about it.
func dedupRecords(in []*pb.PeerRecord) []*pb.PeerRecord {
	if len(in) == 0 {
		return nil
	}
	byID := make(map[string]*pb.PeerRecord, len(in))
	order := make([]string, 0, len(in))
	for _, r := range in {
		if r == nil || r.GetPeerId() == "" {
			continue
		}
		cur, ok := byID[r.GetPeerId()]
		if !ok {
			cp := proto.Clone(r).(*pb.PeerRecord)
			byID[cp.PeerId] = cp
			order = append(order, cp.PeerId)
			continue
		}
		if len(cur.Addrs) == 0 {
			cur.Addrs = append([]string(nil), r.GetAddrs()...)
		}
		if len(cur.Skills) == 0 {
			cur.Skills = append([]string(nil), r.GetSkills()...)
		}
		if cur.SeenAt < r.GetSeenAt() {
			cur.SeenAt = r.GetSeenAt()
		}
	}
	sort.Strings(order)
	out := make([]*pb.PeerRecord, 0, len(order))
	for _, id := range order {
		out = append(out, byID[id])
	}
	return out
}
