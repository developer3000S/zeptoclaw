package triggers

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ErrNotFound reports an unknown trigger id.
var ErrNotFound = errors.New("triggers: not found")

// ErrNoRecord means the backing store holds no trigger list yet — the first run
// of a fresh node. Saver.GetMeta must report absence this way, which keeps the
// package independent of whichever key-value store the node happens to use.
var ErrNoRecord = errors.New("triggers: no stored list")

// Job is the task a trigger injects. It is deliberately its own type rather
// than tasks.SubmitRequest: the trigger package must not depend on the task
// pipeline, so the node adapts this shape where the two meet.
type Job struct {
	Instruction    string            `json:"instruction"`
	RequiredSkills []string          `json:"required_skills,omitempty"`
	TTL            int32             `json:"ttl,omitempty"`
	Priority       int32             `json:"priority,omitempty"`
	TimeoutSeconds int32             `json:"timeout_seconds,omitempty"`
	AllowShell     bool              `json:"allow_shell,omitempty"`
	AllowNetwork   bool              `json:"allow_network_tools,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

// Trigger is a schedule plus the job it runs.
type Trigger struct {
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	Schedule string `json:"schedule"`
	Job      Job    `json:"job"`
	Enabled  *bool  `json:"enabled,omitempty"`
	MaxRuns  int    `json:"max_runs,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	// LastFire is the schedule minute that was already run. It is persisted:
	// without it a restart would either skip the interval entirely or replay
	// every minute since the node went down.
	LastFire time.Time `json:"last_fire,omitempty"`
	RunCount int       `json:"run_count"`
	// LastTaskID/LastError record the outcome of the most recent run so that an
	// operator can see a silently failing schedule from the status endpoint.
	LastTaskID string    `json:"last_task_id,omitempty"`
	LastError  string    `json:"last_error,omitempty"`
	LastRunAt  time.Time `json:"last_run_at,omitempty"`

	sched      *Schedule
	fromConfig bool // declared in node.yaml, therefore not persisted via the API
}

// EnabledOrDefault reports the effective switch: a trigger is on unless told
// otherwise, so config entries need not spell it out.
func (t *Trigger) EnabledOrDefault() bool {
	return t.Enabled == nil || *t.Enabled
}

// Parsed returns the compiled schedule, parsing it on first use.
func (t *Trigger) Parsed() (*Schedule, error) {
	if t.sched != nil {
		return t.sched, nil
	}
	s, err := ParseSchedule(t.Schedule)
	if err != nil {
		return nil, err
	}
	t.sched = s
	return s, nil
}

// NextFire reports the next run after the given instant, or the zero time when
// the trigger is switched off, exhausted, or its schedule can never fire.
func (t *Trigger) NextFire(now time.Time) (time.Time, bool) {
	if !t.EnabledOrDefault() || (t.MaxRuns > 0 && t.RunCount >= t.MaxRuns) {
		return time.Time{}, false
	}
	s, err := t.Parsed()
	if err != nil {
		return time.Time{}, false
	}
	from := now
	if !t.LastFire.IsZero() && t.LastFire.After(from) {
		from = t.LastFire
	}
	return s.Next(from)
}

// Validate checks the trigger without mutating it.
func (t *Trigger) Validate() error {
	var problems []string
	if strings.TrimSpace(t.Job.Instruction) == "" {
		problems = append(problems, "job.instruction is required")
	}
	if _, err := ParseSchedule(t.Schedule); err != nil {
		problems = append(problems, err.Error())
	}
	if t.MaxRuns < 0 {
		problems = append(problems, "max_runs must not be negative")
	}
	if t.TTLisNegative() {
		problems = append(problems, "job.ttl must not be negative")
	}
	if len(problems) > 1 {
		return fmt.Errorf("triggers: invalid trigger %q: %s", t.ID, strings.Join(problems, "; "))
	}
	if len(problems) == 1 {
		return fmt.Errorf("triggers: invalid trigger %q: %s", t.ID, problems[0])
	}
	return nil
}

func (t *Trigger) TTLisNegative() bool { return t.Job.TTL < 0 || t.Job.TimeoutSeconds < 0 }

// View is the wire shape of a trigger for the admin API: the compiled schedule
// is reported as its next run, which is what an operator actually asks.
type View struct {
	ID          string    `json:"id"`
	Name        string    `json:"name,omitempty"`
	Schedule    string    `json:"schedule"`
	Job         Job       `json:"job"`
	Enabled     bool      `json:"enabled"`
	MaxRuns     int       `json:"max_runs,omitempty"`
	RunCount    int       `json:"run_count"`
	LastFire    time.Time `json:"last_fire,omitempty"`
	LastRunAt   time.Time `json:"last_run_at,omitempty"`
	LastTaskID  string    `json:"last_task_id,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
	NextFire    time.Time `json:"next_fire,omitempty"`
	BadSchedule string    `json:"bad_schedule,omitempty"`
}

// View renders the trigger for status output at the given instant.
func (t *Trigger) View(now time.Time) View {
	v := View{
		ID: t.ID, Name: t.Name, Schedule: t.Schedule, Job: t.Job,
		Enabled: t.EnabledOrDefault(), MaxRuns: t.MaxRuns, RunCount: t.RunCount,
		LastFire: t.LastFire, LastRunAt: t.LastRunAt, LastTaskID: t.LastTaskID,
		LastError: t.LastError,
	}
	if next, ok := t.NextFire(now); ok {
		v.NextFire = next
	} else if _, err := ParseSchedule(t.Schedule); err != nil {
		v.BadSchedule = err.Error()
	}
	return v
}

// metaKey is the single store key holding the whole list. One value keeps the
// persistence atomic — a crash cannot leave a half-written set of schedules —
// and trigger lists are configuration-sized, not workload-sized.
const metaKey = "triggers"

// Saver is the slice of the node's store this package needs.
type Saver interface {
	PutMeta(key string, val []byte) error
	GetMeta(key string) ([]byte, error)
}

// List reads and decodes the stored triggers, sorted by id for a stable order.
func List(st Saver) ([]*Trigger, error) {
	raw, err := st.GetMeta(metaKey)
	if errors.Is(err, ErrNoRecord) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("triggers: read: %w", err)
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var out []*Trigger
	if err := json.Unmarshal(raw, &out); err != nil {
		// A corrupt list must not take the node's scheduler down: report it so
		// the operator can see why nothing fires.
		return nil, fmt.Errorf("triggers: stored list is unreadable: %w", err)
	}
	for _, t := range out {
		_, _ = t.Parsed() // best-effort; bad schedules surface in View
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Save writes the list atomically.
func Save(st Saver, list []*Trigger) error {
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	raw, err := json.Marshal(list)
	if err != nil {
		return fmt.Errorf("triggers: encode: %w", err)
	}
	if err := st.PutMeta(metaKey, raw); err != nil {
		return fmt.Errorf("triggers: write: %w", err)
	}
	return nil
}

// Upsert inserts or replaces a trigger by id, validating it first.
func Upsert(st Saver, t *Trigger) ([]*Trigger, error) {
	if t == nil {
		return nil, errors.New("triggers: nil trigger")
	}
	if strings.TrimSpace(t.ID) == "" {
		return nil, errors.New("triggers: id is required")
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if _, err := t.Parsed(); err != nil {
		return nil, err
	}
	list, err := List(st)
	if err != nil {
		return nil, err
	}
	replaced := false
	for i, existing := range list {
		if existing.ID != t.ID {
			continue
		}
		// Editing a schedule restarts the accounting from now: keeping LastFire
		// from the old expression could fire the new one retroactively.
		if existing.Schedule != t.Schedule {
			t.LastFire = time.Time{}
			t.RunCount = 0
		} else {
			t.LastFire, t.RunCount = existing.LastFire, existing.RunCount
			if t.CreatedAt.IsZero() {
				t.CreatedAt = existing.CreatedAt
			}
		}
		list[i] = t
		replaced = true
		break
	}
	if !replaced {
		if t.CreatedAt.IsZero() {
			t.CreatedAt = time.Now().UTC()
		}
		list = append(list, t)
	}
	if err := Save(st, list); err != nil {
		return nil, err
	}
	return list, nil
}

// Remove deletes a trigger by id, reporting whether anything was removed.
func Remove(st Saver, id string) (bool, error) {
	list, err := List(st)
	if err != nil {
		return false, err
	}
	out := list[:0]
	found := false
	for _, t := range list {
		if t.ID == id {
			found = true
			continue
		}
		out = append(out, t)
	}
	if !found {
		return false, nil
	}
	if err := Save(st, out); err != nil {
		return false, err
	}
	return true, nil
}

// Clone returns a deep-enough copy for safe mutation by callers.
func (t *Trigger) Clone() *Trigger {
	cp := *t
	cp.Job.RequiredSkills = append([]string(nil), t.Job.RequiredSkills...)
	if t.Job.Labels != nil {
		cp.Job.Labels = make(map[string]string, len(t.Job.Labels))
		for k, v := range t.Job.Labels {
			cp.Job.Labels[k] = v
		}
	}
	if t.Enabled != nil {
		enabled := *t.Enabled
		cp.Enabled = &enabled
	}
	cp.sched = t.sched
	return &cp
}
