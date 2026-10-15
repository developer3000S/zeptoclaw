package triggers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// memStore is the Saver slice the scheduler needs, kept in memory.
type memStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemStore() *memStore { return &memStore{data: map[string][]byte{}} }

func (m *memStore) PutMeta(key string, val []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(val))
	copy(cp, val)
	m.data[key] = cp
	return nil
}

func (m *memStore) GetMeta(key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return nil, ErrNoRecord
	}
	return append([]byte(nil), v...), nil
}

func enabled(v bool) *bool { return &v }

type submitted struct {
	mu   sync.Mutex
	jobs []Job
	ids  map[string]int
	err  error
}

func (s *submitted) Submit(ctx context.Context, job Job) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return "", s.err
	}
	if s.ids == nil {
		s.ids = map[string]int{}
	}
	s.ids[job.Instruction]++
	id := fmt.Sprintf("task-%d-%d", len(s.jobs)+1, s.ids[job.Instruction])
	s.jobs = append(s.jobs, job)
	return id, nil
}

func (s *submitted) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.jobs)
}

func newTestScheduler(t *testing.T, st Saver, sub Submit, opts ...func(*Options)) *Scheduler {
	t.Helper()
	o := Options{Store: st, Submit: sub, Logger: quietLog(), Interval: time.Minute}
	for _, f := range opts {
		f(&o)
	}
	s, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// A trigger fires once per matching minute, at most, even if the tick runs
// twice inside that minute (a slow Submit can let the next tick overlap).
func TestTickFiresOncePerMinute(t *testing.T) {
	st := newMemStore()
	sub := &submitted{}
	tr := &Trigger{ID: "nightly", Schedule: "0 3 * * *", Job: Job{Instruction: "vacuum"}}
	if _, err := Upsert(st, tr); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	s := newTestScheduler(t, st, sub.Submit)

	// The minute before the schedule: nothing.
	now := at(2026, 5, 1, 2, 59)
	if got := s.tickAt(context.Background(), now); len(got) != 0 {
		t.Fatalf("fired early: %+v", got)
	}
	// The matching minute: exactly one task.
	now = at(2026, 5, 1, 3, 0)
	results := s.tickAt(context.Background(), now)
	if len(results) != 1 || !results[0].Fired {
		t.Fatalf("results = %+v, want one firing", results)
	}
	if sub.count() != 1 {
		t.Fatalf("submitted %d tasks, want 1", sub.count())
	}
	// A second tick in the same minute must not repeat the work.
	if got := s.tickAt(context.Background(), now.Add(10*time.Second)); len(got) != 0 {
		t.Fatalf("double-fired within the same minute: %+v", got)
	}
	if sub.count() != 1 {
		t.Fatalf("submitted %d tasks after a repeated tick, want 1", sub.count())
	}
}

// Reconciled minutes must not be replayed after a restart: the node was not
// running, and a 03:00 job started at 14:00 is not what the operator asked for.
func TestMissedRunsAreSkippedNotReplayed(t *testing.T) {
	st := newMemStore()
	sub := &submitted{}
	if _, err := Upsert(st, &Trigger{ID: "daily", Schedule: "0 3 * * *", Job: Job{Instruction: "report"}}); err != nil {
		t.Fatal(err)
	}
	s := newTestScheduler(t, st, sub.Submit)

	// Node down over 03:00; it comes back at 14:05.
	missing := at(2026, 5, 1, 3, 0)
	stored, err := List(st)
	if err != nil || len(stored) != 1 {
		t.Fatalf("list: %v (%d)", err, len(stored))
	}
	// Simulate having fired yesterday, so LastFire is one day old.
	stored[0].LastFire = missing.AddDate(0, 0, -1)
	if err := Save(st, stored); err != nil {
		t.Fatal(err)
	}
	if got := s.tickAt(context.Background(), at(2026, 5, 1, 14, 5)); len(got) != 0 {
		t.Fatalf("replayed a missed minute: %+v", got)
	}
	if sub.count() != 0 {
		t.Fatalf("submitted %d jobs for a skipped window", sub.count())
	}
	// The next scheduled minute still fires — the skip must not disable it.
	if got := s.tickAt(context.Background(), at(2026, 5, 2, 3, 0)); len(got) != 1 {
		t.Fatalf("did not fire on schedule after a skipped window: %+v", got)
	}
}

// Bookkeeping has to survive the process: LastFire, RunCount and the outcome of
// the last run are what the status endpoint shows and what prevents a replay.
func TestFiringStatePersists(t *testing.T) {
	st := newMemStore()
	sub := &submitted{}
	if _, err := Upsert(st, &Trigger{ID: "sync", Schedule: "*/10 * * * *", Job: Job{Instruction: "sync"}}); err != nil {
		t.Fatal(err)
	}
	s := newTestScheduler(t, st, sub.Submit)
	s.tickAt(context.Background(), at(2026, 5, 1, 12, 0))
	s.tickAt(context.Background(), at(2026, 5, 1, 12, 20))

	list, err := List(st)
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v (%d)", err, len(list))
	}
	tr := list[0]
	if tr.RunCount != 2 {
		t.Fatalf("RunCount = %d, want 2", tr.RunCount)
	}
	want := at(2026, 5, 1, 12, 20)
	if !tr.LastFire.Equal(want) {
		t.Fatalf("LastFire = %v, want %v", tr.LastFire, want)
	}
	if tr.LastTaskID == "" {
		t.Fatal("LastTaskID not recorded")
	}

	// A fresh scheduler over the same store must not re-run those minutes.
	sub2 := &submitted{}
	s2 := newTestScheduler(t, st, sub2.Submit)
	if got := s2.tickAt(context.Background(), want); len(got) != 0 {
		t.Fatalf("new process replayed a fired minute: %+v", got)
	}
}

func TestDisabledAndExhaustedTriggersNeverFire(t *testing.T) {
	cases := []struct {
		name string
		tr   *Trigger
	}{
		{"disabled", &Trigger{ID: "off", Schedule: "* * * * *", Enabled: enabled(false), Job: Job{Instruction: "i"}}},
		{"exhausted", &Trigger{ID: "done", Schedule: "* * * * *", RunCount: 3, MaxRuns: 3, Job: Job{Instruction: "i"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := newMemStore()
			sub := &submitted{}
			if _, err := Upsert(st, tc.tr); err != nil {
				t.Fatal(err)
			}
			s := newTestScheduler(t, st, sub.Submit)
			if got := s.tickAt(context.Background(), at(2026, 5, 1, 12, 0)); len(got) != 0 {
				t.Fatalf("fired a %s trigger: %+v", tc.name, got)
			}
			if sub.count() != 0 {
				t.Fatalf("%s trigger submitted %d jobs", tc.name, sub.count())
			}
		})
	}
}

// MaxRuns=1 is how an operator expresses "run this once tomorrow at 09:00";
// after that the trigger must stop rather than repeat every day.
func TestMaxRunsStopsTheTrigger(t *testing.T) {
	st := newMemStore()
	sub := &submitted{}
	if _, err := Upsert(st, &Trigger{ID: "once", Schedule: "0 3 15 5 *", MaxRuns: 1, Job: Job{Instruction: "one-off"}}); err != nil {
		t.Fatal(err)
	}
	s := newTestScheduler(t, st, sub.Submit)
	now := at(2026, 5, 15, 3, 0)
	if got := s.tickAt(context.Background(), now); len(got) != 1 {
		t.Fatalf("first run did not fire: %+v", got)
	}
	list, _ := List(st)
	if len(list) != 1 || list[0].RunCount != 1 {
		t.Fatalf("state after one run: %+v", list)
	}
	// Next year's May 15 must stay silent: the budget is spent.
	if got := s.tickAt(context.Background(), at(2027, 5, 15, 3, 0)); len(got) != 0 {
		t.Fatalf("fired past max_runs: %+v", got)
	}
}

// A Submit failure is recorded, not swallowed: a schedule that silently stopped
// working is worse than a broken one an operator can see.
func TestSubmitFailureIsRecordedAndCounted(t *testing.T) {
	st := newMemStore()
	sub := &submitted{err: errors.New("adapter is down")}
	if _, err := Upsert(st, &Trigger{ID: "broken", Schedule: "0 3 * * *", Job: Job{Instruction: "i"}}); err != nil {
		t.Fatal(err)
	}
	s := newTestScheduler(t, st, sub.Submit)
	now := at(2026, 5, 1, 3, 0)
	got := s.tickAt(context.Background(), now)
	if len(got) != 1 || got[0].Err == nil {
		t.Fatalf("results = %+v, want the failure surfaced", got)
	}
	list, _ := List(st)
	if list[0].LastError == "" {
		t.Fatal("LastError not recorded")
	}
	if list[0].RunCount != 1 {
		t.Fatalf("RunCount = %d, want the attempt counted", list[0].RunCount)
	}
	// A failed minute is still a used minute; the next one gets its chance.
	sub.err = nil
	if got := s.tickAt(context.Background(), at(2026, 5, 2, 3, 0)); len(got) != 1 || got[0].Err != nil {
		t.Fatalf("recovery on the next minute failed: %+v", got)
	}
}

// Editing the schedule must not make the new expression retroactive: with the
// old LastFire kept, "every minute" would fire for minutes already elapsed.
func TestScheduleEditResetsAccounting(t *testing.T) {
	st := newMemStore()
	tr := &Trigger{ID: "edit", Schedule: "0 3 * * *", Job: Job{Instruction: "i"}}
	if _, err := Upsert(st, tr); err != nil {
		t.Fatal(err)
	}
	sub := &submitted{}
	s := newTestScheduler(t, st, sub.Submit)
	s.tickAt(context.Background(), at(2026, 5, 1, 3, 0))

	updated := &Trigger{ID: "edit", Schedule: "* * * * *", Job: Job{Instruction: "i"}}
	if _, err := Upsert(st, updated); err != nil {
		t.Fatalf("update: %v", err)
	}
	list, _ := List(st)
	if !list[0].LastFire.IsZero() {
		t.Fatalf("LastFire carried over to a new schedule: %v", list[0].LastFire)
	}
	if list[0].RunCount != 0 {
		t.Fatalf("RunCount carried over: %d", list[0].RunCount)
	}
	// Now "every minute" starts from now rather than replaying the day.
	if got := s.tickAt(context.Background(), at(2026, 5, 1, 9, 0)); len(got) != 1 {
		t.Fatalf("edited schedule did not fire: %+v", got)
	}
}

// Changing the job but keeping the schedule must not reset the accounting — the
// next minute is unchanged either way.
func TestJobEditKeepsAccounting(t *testing.T) {
	st := newMemStore()
	if _, err := Upsert(st, &Trigger{ID: "keep", Schedule: "0 3 * * *", Job: Job{Instruction: "old"}}); err != nil {
		t.Fatal(err)
	}
	s := newTestScheduler(t, st, (&submitted{}).Submit)
	s.tickAt(context.Background(), at(2026, 5, 1, 3, 0))
	if _, err := Upsert(st, &Trigger{ID: "keep", Schedule: "0 3 * * *", Job: Job{Instruction: "new"}}); err != nil {
		t.Fatal(err)
	}
	list, _ := List(st)
	if list[0].RunCount != 1 || list[0].LastFire.IsZero() {
		t.Fatalf("bookkeeping lost on a job-only edit: %+v", list[0])
	}
}

// Config-declared triggers cannot be deleted through the API and are not
// written back to the store — otherwise an edited YAML line would resurrect as
// a duplicate row after a restart.
func TestConfigTriggersAreNotPersisted(t *testing.T) {
	st := newMemStore()
	sub := &submitted{}
	cfg := []*Trigger{{ID: "yaml", Schedule: "* * * * *", Job: Job{Instruction: "from yaml"}, fromConfig: true}}
	s := newTestScheduler(t, st, sub.Submit, func(o *Options) { o.Config = cfg })

	if got := s.tickAt(context.Background(), at(2026, 5, 1, 12, 0)); len(got) != 1 {
		t.Fatalf("config trigger did not fire: %+v", got)
	}
	list, err := List(st)
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range list {
		if tr.ID == "yaml" {
			t.Fatal("config trigger leaked into stored state")
		}
	}
	// A stored trigger in the same store survives the merge untouched.
	if _, err := Upsert(st, &Trigger{ID: "stored", Schedule: "* * * * *", Job: Job{Instruction: "i"}}); err != nil {
		t.Fatal(err)
	}
	if got := s.tickAt(context.Background(), at(2026, 5, 1, 12, 1)); len(got) != 2 {
		t.Fatalf("want config+stored firing together, got %+v", got)
	}
}

// A config trigger with the same id overrides the stored definition but keeps
// its accounting, so editing YAML does not double-run anything.
func TestConfigOverridesStoredDefinition(t *testing.T) {
	st := newMemStore()
	if _, err := Upsert(st, &Trigger{ID: "x", Schedule: "0 3 * * *", Job: Job{Instruction: "stored"}}); err != nil {
		t.Fatal(err)
	}
	s := newTestScheduler(t, st, (&submitted{}).Submit)
	s.tickAt(context.Background(), at(2026, 5, 1, 3, 0))

	sub := &submitted{}
	override := []*Trigger{{ID: "x", Schedule: "* * * * *", Job: Job{Instruction: "from yaml"}, fromConfig: true}}
	s2 := newTestScheduler(t, st, sub.Submit, func(o *Options) { o.Config = override })
	if got := s2.tickAt(context.Background(), at(2026, 5, 1, 9, 0)); len(got) != 1 {
		t.Fatalf("override did not fire: %+v", got)
	}
	if sub.jobs[0].Instruction != "from yaml" {
		t.Fatalf("stored job ran instead of the config one: %+v", sub.jobs)
	}
}

func TestRemoveAndNotFound(t *testing.T) {
	st := newMemStore()
	if _, err := Upsert(st, &Trigger{ID: "rm", Schedule: "* * * * *", Job: Job{Instruction: "i"}}); err != nil {
		t.Fatal(err)
	}
	ok, err := Remove(st, "rm")
	if err != nil || !ok {
		t.Fatalf("remove: ok=%v err=%v", ok, err)
	}
	if ok, err := Remove(st, "rm"); err != nil || ok {
		t.Fatalf("second remove must report nothing removed: ok=%v err=%v", ok, err)
	}
}

func TestUpsertValidates(t *testing.T) {
	st := newMemStore()
	cases := []struct {
		name string
		tr   *Trigger
	}{
		{"empty instruction", &Trigger{ID: "a", Schedule: "* * * * *"}},
		{"bad schedule", &Trigger{ID: "b", Schedule: "nope", Job: Job{Instruction: "i"}}},
		{"empty id", &Trigger{ID: "", Schedule: "* * * * *", Job: Job{Instruction: "i"}}},
		{"negative max runs", &Trigger{ID: "c", Schedule: "* * * * *", MaxRuns: -1, Job: Job{Instruction: "i"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Upsert(st, tc.tr); err == nil {
				t.Fatal("accepted an invalid trigger")
			}
		})
	}
	if _, err := Upsert(st, nil); err == nil {
		t.Fatal("accepted a nil trigger")
	}
}

// A bad schedule that slipped into the store (hand-edited file) must be visible
// in the status view, not silently inert.
func TestViewReportsBrokenSchedule(t *testing.T) {
	st := newMemStore()
	tr := &Trigger{ID: "bad", Schedule: "0 3 * * *", Job: Job{Instruction: "i"}}
	// Write it directly, bypassing Upsert's validation.
	raw, _ := json.Marshal([]*Trigger{tr})
	if err := st.PutMeta(metaKey, raw); err != nil {
		t.Fatal(err)
	}
	tr.Schedule = "99 * * * *"
	list, err := List(st)
	if err != nil {
		t.Fatal(err)
	}
	v := tr.View(at(2026, 5, 1, 12, 0))
	if v.BadSchedule == "" {
		t.Fatalf("broken schedule not reported: %+v (list %d)", v, len(list))
	}
	if !v.NextFire.IsZero() {
		t.Fatal("a broken schedule must not promise a next fire")
	}
}

// NextFire is what the status endpoint shows; it must respect the switch and
// the budget rather than the raw expression.
func TestNextFireRespectsState(t *testing.T) {
	now := at(2026, 5, 1, 12, 0)
	plain := &Trigger{ID: "p", Schedule: "0 3 * * *", Job: Job{Instruction: "i"}}
	next, ok := plain.NextFire(now)
	if !ok || !next.Equal(at(2026, 5, 2, 3, 0)) {
		t.Fatalf("next = %v ok=%v", next, ok)
	}
	if _, ok := (&Trigger{Schedule: "0 3 * * *", Enabled: enabled(false)}).NextFire(now); ok {
		t.Fatal("a disabled trigger promises a run")
	}
	if _, ok := (&Trigger{Schedule: "0 3 * * *", RunCount: 1, MaxRuns: 1}).NextFire(now); ok {
		t.Fatal("an exhausted trigger promises a run")
	}
	// LastFire in the future (a clock jump backwards) must not produce a past
	// next-run time.
	advanced := &Trigger{ID: "adv", Schedule: "* * * * *", Job: Job{Instruction: "i"}, LastFire: now.Add(time.Hour)}
	if next, ok := advanced.NextFire(now); !ok || !next.After(now) {
		t.Fatalf("next fire went backwards: %v (now %v)", next, now)
	}
}

// New must refuse to build a scheduler that cannot do its job.
func TestNewRequiresCollaborators(t *testing.T) {
	sub := (&submitted{}).Submit
	if _, err := New(Options{Submit: sub}); err == nil {
		t.Fatal("built a scheduler with no store")
	}
	if _, err := New(Options{Store: newMemStore()}); err == nil {
		t.Fatal("built a scheduler with no submit")
	}
}

// Start/Stop drive the real loop with an injected sleeper so the test does not
// wait a minute.
func TestStartStopLoop(t *testing.T) {
	st := newMemStore()
	sub := &submitted{}
	if _, err := Upsert(st, &Trigger{ID: "loop", Schedule: "* * * * *", Job: Job{Instruction: "i"}}); err != nil {
		t.Fatal(err)
	}
	clock := at(2026, 5, 1, 12, 0)
	var mu sync.Mutex
	sleeps := 0
	s := newTestScheduler(t, st, sub.Submit,
		func(o *Options) {
			o.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
			o.Sleep = func(ctx context.Context, d time.Duration) error {
				mu.Lock()
				sleeps++
				// Each "wait" advances the injected clock to the boundary.
				clock = clock.Truncate(time.Minute).Add(time.Minute)
				mu.Unlock()
				return nil
			}
		})
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	// Stop after a bounded number of passes; the sleep never blocks, so the
	// loop would otherwise spin.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := sleeps
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	cancel()
	if sub.count() < 1 {
		t.Fatalf("loop fired %d times", sub.count())
	}
	// A second Stop is a no-op, not an error.
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

// Two overlapping passes must not start the same job twice.
func TestConcurrentTicksFireOnce(t *testing.T) {
	st := newMemStore()
	sub := &submitted{}
	if _, err := Upsert(st, &Trigger{ID: "race", Schedule: "* * * * *", Job: Job{Instruction: "i"}}); err != nil {
		t.Fatal(err)
	}
	s := newTestScheduler(t, st, sub.Submit)
	now := at(2026, 5, 1, 12, 0)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.tickAt(context.Background(), now)
		}()
	}
	wg.Wait()
	if got := sub.count(); got != 1 {
		t.Fatalf("concurrent ticks submitted %d jobs, want exactly 1", got)
	}
}

// A panic in Submit must be recorded as a failed run, not as a success with an
// empty task id — otherwise a broken schedule looks healthy in the status view.
func TestSubmitPanicIsRecordedAsFailure(t *testing.T) {
	st := newMemStore()
	panicSubmit := func(ctx context.Context, job Job) (string, error) {
		panic("adapter exploded")
	}
	if _, err := Upsert(st, &Trigger{ID: "boom", Schedule: "0 3 * * *", Job: Job{Instruction: "i"}}); err != nil {
		t.Fatal(err)
	}
	s := newTestScheduler(t, st, panicSubmit)
	got := s.tickAt(context.Background(), at(2026, 5, 1, 3, 0))
	if len(got) != 1 || got[0].Err == nil {
		t.Fatalf("panic swallowed: %+v", got)
	}
	list, _ := List(st)
	if list[0].LastError == "" || list[0].LastTaskID != "" {
		t.Fatalf("state after a panicked run: %+v", list[0])
	}
}

// An edited YAML line takes effect on reload without a restart, and a bad
// expression in it is refused as a whole rather than applied partially.
func TestSetConfigTriggersAppliesLive(t *testing.T) {
	st := newMemStore()
	sub := &submitted{}
	s := newTestScheduler(t, st, sub.Submit, func(o *Options) {
		o.Config = []*Trigger{{ID: "old", Schedule: "0 3 * * *", Job: Job{Instruction: "old"}}}
	})
	if got := s.tickAt(context.Background(), at(2026, 5, 1, 12, 0)); len(got) != 0 {
		t.Fatalf("03:00 schedule fired at noon: %+v", got)
	}
	if err := s.SetConfigTriggers([]*Trigger{
		{ID: "new", Schedule: "* * * * *", Job: Job{Instruction: "new"}},
	}); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := s.tickAt(context.Background(), at(2026, 5, 1, 12, 1)); len(got) != 1 {
		t.Fatalf("reloaded schedule did not take effect: %+v", got)
	}
	if sub.jobs[0].Instruction != "new" {
		t.Fatalf("ran the retired schedule: %+v", sub.jobs)
	}
	// A rejected reload must leave the previous set intact, not half-applied.
	before := len(sub.jobs)
	if err := s.SetConfigTriggers([]*Trigger{
		{ID: "ok", Schedule: "* * * * *", Job: Job{Instruction: "ok"}},
		{ID: "bad", Schedule: "70 * * * *", Job: Job{Instruction: "x"}},
	}); err == nil {
		t.Fatal("accepted an out-of-range schedule")
	}
	if got := s.tickAt(context.Background(), at(2026, 5, 1, 12, 2)); len(got) != 1 || got[0].ID != "new" {
		t.Fatalf("a failed reload changed the live schedules: %+v", got)
	}
	if len(sub.jobs) != before+1 {
		t.Fatalf("jobs submitted = %d, want %d", len(sub.jobs), before+1)
	}
}

// New must refuse to start with an invalid config schedule.
func TestNewRejectsInvalidConfigSchedule(t *testing.T) {
	_, err := New(Options{Store: newMemStore(), Submit: (&submitted{}).Submit,
		Config: []*Trigger{{ID: "x", Schedule: "nope", Job: Job{Instruction: "i"}}}})
	if err == nil {
		t.Fatal("scheduler built with an unparsable schedule")
	}
}

// A config trigger's bookkeeping lives in the scheduler, not in the store, so
// two passes inside one minute must still start the job once. Cloning the
// template on every merge (as an earlier revision did) reset LastFire each
// time and made an every-minute schedule fire twice a minute.
func TestConfigTriggerFiresOncePerMinuteAcrossTicks(t *testing.T) {
	st := newMemStore()
	sub := &submitted{}
	s := newTestScheduler(t, st, sub.Submit, func(o *Options) {
		o.Config = []*Trigger{{ID: "cfg", Schedule: "* * * * *", Job: Job{Instruction: "i"}}}
	})
	now := at(2026, 5, 1, 12, 0)
	if got := s.tickAt(context.Background(), now); len(got) != 1 {
		t.Fatalf("config trigger did not fire: %+v", got)
	}
	if got := s.tickAt(context.Background(), now.Add(30*time.Second)); len(got) != 0 {
		t.Fatalf("config trigger double-fired within one minute: %+v", got)
	}
	// Reloading the same definition must not consume a second run of the minute.
	if err := s.SetConfigTriggers([]*Trigger{
		{ID: "cfg", Schedule: "* * * * *", Job: Job{Instruction: "i"}},
	}); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := s.tickAt(context.Background(), now.Add(40*time.Second)); len(got) != 0 {
		t.Fatalf("reload reset the in-flight minute: %+v", got)
	}
	if sub.count() != 1 {
		t.Fatalf("submitted %d jobs in one minute, want 1", sub.count())
	}
	// The next minute runs again: the guard is per minute, not permanent.
	if got := s.tickAt(context.Background(), now.Add(time.Minute)); len(got) != 1 {
		t.Fatalf("the following minute did not fire: %+v", got)
	}
}

// A reload that changes the schedule must take the new definition live; keeping
// the unchanged object is an optimization, not a sticky cache.
func TestConfigTriggerEditTakesEffect(t *testing.T) {
	st := newMemStore()
	sub := &submitted{}
	s := newTestScheduler(t, st, sub.Submit, func(o *Options) {
		o.Config = []*Trigger{{ID: "cfg", Schedule: "0 3 * * *", Job: Job{Instruction: "old"}}}
	})
	if err := s.SetConfigTriggers([]*Trigger{
		{ID: "cfg", Schedule: "0 4 * * *", Job: Job{Instruction: "new"}},
	}); err != nil {
		t.Fatalf("edit: %v", err)
	}
	if got := s.tickAt(context.Background(), at(2026, 5, 1, 4, 0)); len(got) != 1 {
		t.Fatalf("edited schedule did not fire: %+v", got)
	}
	if sub.jobs[0].Instruction != "new" {
		t.Fatalf("ran the old job after the edit: %+v", sub.jobs)
	}
}

// The same id cannot be declared twice: which definition wins would depend on
// list order, so the whole set is refused.
func TestConfigTriggerDuplicateIDRejected(t *testing.T) {
	s := newTestScheduler(t, newMemStore(), (&submitted{}).Submit)
	err := s.SetConfigTriggers([]*Trigger{
		{ID: "dup", Schedule: "* * * * *", Job: Job{Instruction: "a"}},
		{ID: "dup", Schedule: "0 3 * * *", Job: Job{Instruction: "b"}},
	})
	if err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("duplicate id accepted: %v", err)
	}
}

// untilBoundary must never return a non-positive wait (an infinite hot loop).
func TestUntilBoundaryIsAlwaysPositive(t *testing.T) {
	for _, now := range []time.Time{
		at(2026, 5, 1, 12, 0),
		at(2026, 5, 1, 12, 0).Add(30 * time.Second),
		at(2026, 5, 1, 12, 0).Add(59*time.Second + 999*time.Millisecond),
	} {
		if d := untilBoundary(now, time.Minute); d <= 0 {
			t.Fatalf("untilBoundary(%v) = %v, want positive", now, d)
		}
	}
	if d := untilBoundary(at(2026, 5, 1, 12, 0), 0); d <= 0 {
		t.Fatalf("untilBoundary with a zero interval = %v", d)
	}
}

// A fired job carries the id of its schedule, so a task in the journal reads as
// scheduled work. The trigger's own Job must not accumulate that label over
// repeated runs.
func TestFiredJobIsLabelledWithItsTrigger(t *testing.T) {
	st := newMemStore()
	sub := &submitted{}
	s := newTestScheduler(t, st, sub.Submit, func(o *Options) {
		o.Now = func() time.Time { return at(2026, 5, 1, 3, 0) }
	})
	if _, err := Upsert(st, &Trigger{ID: "nightly", Schedule: "0 3 * * *",
		Job: Job{Instruction: "vacuum"}}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	for i := 0; i < 2; i++ {
		s.tickAt(context.Background(), at(2026, 5, 1, 3, 0))
	}
	s.tickAt(context.Background(), at(2026, 5, 2, 3, 0))
	if sub.count() != 2 {
		t.Fatalf("fired %d times, want 2: %+v", sub.count(), sub.jobs)
	}
	for _, j := range sub.jobs {
		if j.Labels["trigger"] != "nightly" {
			t.Fatalf("job labels = %v, want trigger=nightly", j.Labels)
		}
	}
	// The stored definition keeps the operator's labels only: the per-run
	// attribution must not leak into the persisted job.
	back, err := List(st)
	if err != nil || len(back) != 1 {
		t.Fatalf("List: %v %+v", err, back)
	}
	if _, ok := back[0].Job.Labels["trigger"]; ok {
		t.Fatalf("stored job grew a trigger label: %+v", back[0].Job.Labels)
	}
}

// Views reports stored and config triggers together, with next-fire times; Add
// refuses to shadow a config id; Delete removes only stored ones.
func TestSchedulerFacadeForTheAPI(t *testing.T) {
	st := newMemStore()
	s := newTestScheduler(t, st, (&submitted{}).Submit, func(o *Options) {
		o.Config = []*Trigger{{ID: "pinned", Schedule: "0 3 * * *", Job: Job{Instruction: "pinned job"}}}
	})
	if _, err := Upsert(st, &Trigger{ID: "stored", Schedule: "*/30 * * * *",
		Job: Job{Instruction: "stored job"}}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	views, err := s.Views(at(2026, 5, 1, 12, 0))
	if err != nil {
		t.Fatalf("Views: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("Views = %+v, want pinned+stored", views)
	}
	for _, v := range views {
		if v.NextFire.IsZero() {
			t.Fatalf("view %q has no next fire: %+v", v.ID, v)
		}
	}

	// The config id is shadowed: writing it through the API would look applied
	// but never run, so it is refused.
	if _, err := s.Add(&Trigger{ID: "pinned", Schedule: "0 4 * * *", Job: Job{Instruction: "x"}}); err == nil {
		t.Fatal("Add accepted a trigger that shadows a config id")
	}
	if _, err := s.Add(&Trigger{ID: "late", Schedule: "not a cron", Job: Job{Instruction: "x"}}); err == nil {
		t.Fatal("Add accepted a malformed schedule")
	}
	if _, err := s.Add(&Trigger{ID: "late", Schedule: "0 4 * * *", Job: Job{Instruction: "late job"}}); err != nil {
		t.Fatalf("Add stored: %v", err)
	}

	removed, err := s.Delete("late")
	if err != nil || !removed {
		t.Fatalf("Delete stored: %v %v", removed, err)
	}
	if removed, err := s.Delete("pinned"); err != nil || removed {
		t.Fatalf("Delete of a config id must be a no-op, got %v %v", removed, err)
	}
}
