package triggers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"
)

// Submit injects a scheduled job as a task. It is supplied by the node — the
// only place where triggers and the task pipeline meet — and returns the id of
// the task created, or the reason nothing was created.
type Submit func(ctx context.Context, job Job) (taskID string, err error)

// Options configures a Scheduler.
type Options struct {
	Store  Saver
	Submit Submit
	Logger *slog.Logger

	// Interval is how often the schedule is checked. One minute is the finest
	// resolution cron offers, so anything smaller only wastes wakeups.
	Interval time.Duration

	// Now and Sleep make the firing loop testable without wall-clock waiting.
	Now   func() time.Time
	Sleep func(context.Context, time.Duration) error

	// Config holds triggers declared in the node's configuration. They are
	// replaced wholesale by SetConfigTriggers on a config reload, which is what
	// makes this section hot rather than restart-only.
	Config []*Trigger
}

// Scheduler runs cron-declared triggers, the fourth task source of ТЗ 6.6.1.
//
// Missed runs are skipped, not replayed: a job scheduled for 09:00 that the node
// was down for is not started at 14:00, when the operator is long gone and the
// reason for the schedule may have expired. The trade-off is stated in the
// trigger's own record — LastFire/LastError — so the gap is visible.
type Scheduler struct {
	st       Saver
	submit   Submit
	log      *slog.Logger
	interval time.Duration
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error

	// cfgMu guards the config-declared schedules, which are replaced on a
	// reload while the firing loop may be reading them.
	cfgMu sync.RWMutex
	extra []*Trigger

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	running bool

	// tickMu serializes scheduling passes; mu guards the loop's lifecycle. They
	// are separate because Stop waits for the loop, which is inside a tick.
	tickMu sync.Mutex
}

// New builds the scheduler. Store and Submit are required: a scheduler that
// cannot persist its bookkeeping would re-fire on every restart.
func New(opts Options) (*Scheduler, error) {
	if opts.Store == nil {
		return nil, errors.New("triggers: store is required")
	}
	if opts.Submit == nil {
		return nil, errors.New("triggers: submit is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Interval <= 0 {
		opts.Interval = time.Minute
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if opts.Sleep == nil {
		opts.Sleep = sleepTo
	}
	s := &Scheduler{
		st: opts.Store, submit: opts.Submit, log: opts.Logger,
		interval: opts.Interval, now: opts.Now, sleep: opts.Sleep,
	}
	if err := s.SetConfigTriggers(opts.Config); err != nil {
		return nil, err
	}
	return s, nil
}

// SetConfigTriggers replaces the config-declared schedules. It is the live
// owner the config reload writes through, which is what lets an operator edit
// `triggers:` in YAML and have it take effect without a restart. Invalid
// expressions are rejected as a group: a half-applied schedule list would leave
// the node running some of the edits and not others.
//
// Config triggers are not stored, so the scheduler's own objects are their book
// keeping: an entry whose schedule and job are unchanged keeps its live object.
// Replacing it on every reload would reset LastFire and let a "* * * * *"
// schedule fire twice in the same minute whenever an unrelated config line is
// edited.
func (s *Scheduler) SetConfigTriggers(list []*Trigger) error {
	prepared := make([]*Trigger, 0, len(list))
	seen := make(map[string]bool, len(list))
	for _, t := range list {
		if t == nil {
			continue
		}
		if seen[t.ID] {
			return fmt.Errorf("triggers: schedule id %q is declared twice", t.ID)
		}
		seen[t.ID] = true
		if old := s.configTrigger(t.ID); old != nil && old.Schedule == t.Schedule && reflect.DeepEqual(old.Job, t.Job) {
			prepared = append(prepared, old)
			continue
		}
		if err := t.Validate(); err != nil {
			return err
		}
		cp := t.Clone()
		cp.fromConfig = true
		if _, err := cp.Parsed(); err != nil {
			return err
		}
		prepared = append(prepared, cp)
	}
	s.cfgMu.Lock()
	s.extra = prepared
	s.cfgMu.Unlock()
	return nil
}

// configTrigger finds a live config schedule by id, if any.
func (s *Scheduler) configTrigger(id string) *Trigger {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	for _, t := range s.extra {
		if t.ID == id {
			return t
		}
	}
	return nil
}

// configTriggers snapshots the config-declared schedules for one pass.
func (s *Scheduler) configTriggers() []*Trigger {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return append([]*Trigger(nil), s.extra...)
}

// Start launches the firing loop. It is a no-op when a loop is already running.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	s.mu.Unlock()

	s.log.Info("trigger_scheduler_started", "interval", s.interval.String())
	go s.loop(runCtx)
}

// Stop ends the firing loop and waits for it.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	cancel := s.cancel
	done := s.done
	s.mu.Unlock()

	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errors.New("triggers: scheduler stop timed out: " + ctx.Err().Error())
	}
}

func (s *Scheduler) loop(ctx context.Context) {
	defer close(s.done)
	// Align the first check to a minute boundary: cron semantics are minute
	// based, and waking mid-minute would either fire late by up to a minute or
	// double-check the same minute.
	for {
		if err := s.sleep(ctx, untilBoundary(s.now(), s.interval)); err != nil {
			s.log.Info("trigger_scheduler_stopped", "reason", err.Error())
			return
		}
		if ctx.Err() != nil {
			return
		}
		s.Tick(ctx)
	}
}

// untilBoundary returns how long to wait before the next check.
func untilBoundary(now time.Time, interval time.Duration) time.Duration {
	if interval <= 0 {
		interval = time.Minute
	}
	next := now.Truncate(interval).Add(interval)
	if d := next.Sub(now); d > 0 {
		return d
	}
	return interval
}

// Tick performs one scheduling pass, firing everything due at the current
// minute. Exported for tests and for an operator-initiated flush.
func (s *Scheduler) Tick(ctx context.Context) []Result {
	return s.tickAt(ctx, s.now())
}

// Result reports one firing decision for a trigger.
type Result struct {
	ID     string
	Fired  bool
	TaskID string
	Err    error
}

// tickAt performs one scheduling pass evaluated at now.
//
// Ticks are serialized: the loop runs them one at a time, but Tick is exported
// so an operator can flush schedules from another goroutine. Two overlapping
// passes would each read the same LastFire and start the same job twice, which
// is the one failure mode a scheduler must not have.
func (s *Scheduler) tickAt(ctx context.Context, now time.Time) []Result {
	s.tickMu.Lock()
	defer s.tickMu.Unlock()
	stored, err := List(s.st)
	if err != nil {
		s.log.Warn("trigger_load_failed", "err", err.Error())
		return nil
	}
	// Config-declared triggers are merged over the stored ones by id, so an
	// operator can pin a schedule in YAML that the API cannot delete. The merge
	// yields copies: mutating the values returned by List would overwrite the
	// store with a list that silently dropped every config trigger in it.
	list := mergeByID(stored, s.configTriggers())

	minute := now.Truncate(time.Minute)
	var results []Result
	for _, t := range list {
		res := s.consider(ctx, t, now, minute)
		if res != nil {
			results = append(results, *res)
		}
	}
	if len(results) > 0 {
		if err := Save(s.st, storedOnly(list)); err != nil {
			s.log.Warn("trigger_state_not_saved", "err", err.Error())
		}
	}
	return results
}

// consider decides on one trigger and fires it when due.
func (s *Scheduler) consider(ctx context.Context, t *Trigger, now, minute time.Time) *Result {
	sched, err := t.Parsed()
	if err != nil {
		// Surfaced through View/status; a bad expression is reported once per
		// tick at debug level rather than as a warning storm.
		s.log.Debug("trigger_schedule_invalid", "trigger_id", t.ID, "err", err.Error())
		return nil
	}
	if !t.EnabledOrDefault() {
		return nil
	}
	if t.MaxRuns > 0 && t.RunCount >= t.MaxRuns {
		return nil
	}
	// The minute must match the expression AND not already have been used.
	if !sched.Matches(minute) || !t.LastFire.Before(minute) {
		return nil
	}
	t.LastFire = minute
	t.RunCount++
	t.LastRunAt = now

	taskID, err := s.submitTask(ctx, t)
	t.LastTaskID, t.LastError = taskID, ""
	if err != nil {
		t.LastError = err.Error()
		s.log.Warn("trigger_run_failed", "trigger_id", t.ID, "schedule", t.Schedule,
			"task_id", taskID, "err", err.Error())
		return &Result{ID: t.ID, Fired: true, TaskID: taskID, Err: err}
	}
	s.log.Info("trigger_fired", "trigger_id", t.ID, "schedule", t.Schedule,
		"task_id", taskID, "run_count", t.RunCount)
	return &Result{ID: t.ID, Fired: true, TaskID: taskID}
}

// submitTask hands the job to the node. A panic in the submission path must not
// take the daemon down at 03:00 with nobody watching, but it must also not be
// recorded as a success: the recovered panic becomes the run's error.
func (s *Scheduler) submitTask(ctx context.Context, t *Trigger) (taskID string, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			msg := asText(rec)
			s.log.Error("trigger_submit_panicked", "trigger_id", t.ID, "err", msg)
			taskID, err = "", fmt.Errorf("trigger submit panicked: %s", msg)
		}
	}()
	// The job is labelled with its trigger so a task in the journal reads as
	// scheduled work, not as something an operator submitted by hand. The label
	// map is copied: the trigger's own Job must not grow keys every run.
	job := t.Job
	if job.Labels == nil {
		job.Labels = map[string]string{triggerLabel: t.ID}
	} else {
		cp := make(map[string]string, len(job.Labels)+1)
		for k, v := range job.Labels {
			cp[k] = v
		}
		cp[triggerLabel] = t.ID
		job.Labels = cp
	}
	return s.submit(ctx, job)
}

// triggerLabel names the schedule that produced a task, so a journal entry
// reads as scheduled work rather than something submitted by hand.
const triggerLabel = "trigger"

// Views renders every trigger the scheduler knows — stored and config-declared
// — at the given instant. It takes the tick lock because reading a trigger's
// bookkeeping while a pass mutates it would report a torn state.
func (s *Scheduler) Views(now time.Time) ([]View, error) {
	s.tickMu.Lock()
	defer s.tickMu.Unlock()
	stored, err := List(s.st)
	if err != nil {
		return nil, err
	}
	list := mergeByID(stored, s.configTriggers())
	out := make([]View, 0, len(list))
	for _, t := range list {
		out = append(out, t.View(now))
	}
	return out, nil
}

// Add validates and stores a trigger, returning the resulting stored list. An id
// that the node config already declares is rejected: the config definition wins
// in the merge, so accepting the write would leave the caller's version
// permanently shadowed.
func (s *Scheduler) Add(t *Trigger) ([]*Trigger, error) {
	if t != nil && s.configTrigger(t.ID) != nil {
		return nil, fmt.Errorf("triggers: id %q is declared in the node config; edit that file instead", t.ID)
	}
	return Upsert(s.st, t)
}

// Delete removes a stored trigger by id, reporting whether anything was
// removed. Config-declared schedules are not stored and cannot be deleted here.
func (s *Scheduler) Delete(id string) (bool, error) {
	return Remove(s.st, id)
}

func asText(v any) string {
	if err, ok := v.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("%v", v)
}

// mergeByID overlays config triggers onto stored ones, keeping stored
// bookkeeping when both know the same id.
func mergeByID(stored, extra []*Trigger) []*Trigger {
	if len(extra) == 0 {
		return stored
	}
	byID := make(map[string]*Trigger, len(stored))
	out := make([]*Trigger, 0, len(stored)+len(extra))
	for _, t := range stored {
		byID[t.ID] = t
		out = append(out, t)
	}
	for _, t := range extra {
		// Not cloned: for a config trigger this object *is* its bookkeeping,
		// since config schedules are not written to the store.
		if prev, ok := byID[t.ID]; ok {
			t.LastFire, t.RunCount = prev.LastFire, prev.RunCount
			t.CreatedAt = prev.CreatedAt
			for i, existing := range out {
				if existing.ID == t.ID {
					out[i] = t
				}
			}
			continue
		}
		if t.CreatedAt.IsZero() {
			t.CreatedAt = time.Now().UTC()
		}
		byID[t.ID] = t
		out = append(out, t)
	}
	return out
}

// storedOnly drops config-declared triggers from a persistence write: they
// belong to the YAML file, and storing them would make an edited config look
// like a phantom duplicate schedule after a restart.
func storedOnly(list []*Trigger) []*Trigger {
	out := make([]*Trigger, 0, len(list))
	for _, t := range list {
		if t.fromConfig {
			continue
		}
		out = append(out, t)
	}
	return out
}

// sleepTo waits d or returns the context error.
func sleepTo(ctx context.Context, d time.Duration) error {
	if d < 0 {
		d = 0
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
