package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/node"
	"github.com/developer3000S/zeptoclaw/internal/triggers"
)

// metaStore is the Saver the trigger scheduler needs, kept in memory: these
// tests exercise the HTTP surface, not the key-value engine.
type metaStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMetaStore() *metaStore { return &metaStore{data: map[string][]byte{}} }

func (m *metaStore) PutMeta(key string, val []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), val...)
	return nil
}

func (m *metaStore) GetMeta(key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return nil, triggers.ErrNoRecord
	}
	return append([]byte(nil), v...), nil
}

// triggerServer builds a Server whose only live collaborator is the trigger
// scheduler: the handlers under test touch nothing else of the node.
func triggerServer(t *testing.T, cfgTriggers ...*triggers.Trigger) (*Server, *triggers.Scheduler, *metaStore) {
	t.Helper()
	st := newMetaStore()
	sched, err := triggers.New(triggers.Options{
		Store:  st,
		Submit: func(context.Context, triggers.Job) (string, error) { return "task-test", nil },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config: cfgTriggers,
	})
	if err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	n := &node.Node{Scheduler: sched}
	return New(n, config.Default(), nil, slog.New(slog.NewTextHandler(io.Discard, nil))), sched, st
}

func do(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

func decodeList(t *testing.T, rec *httptest.ResponseRecorder) triggersList {
	t.Helper()
	var out triggersList
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body)
	}
	return out
}

type triggersList struct {
	Count    int             `json:"count"`
	Triggers []triggers.View `json:"triggers"`
}

// The listing is the operator's single view of both families of schedules:
// the ones pinned in node.yaml and the ones created through this API.
func TestTriggersAPIListsConfigSchedules(t *testing.T) {
	s, _, _ := triggerServer(t, &triggers.Trigger{
		ID: "pinned", Schedule: "0 3 * * *", Job: triggers.Job{Instruction: "from yaml"}})

	rec := do(t, s, http.MethodGet, "/api/v1/triggers", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET code = %d: %s", rec.Code, rec.Body)
	}
	list := decodeList(t, rec)
	if list.Count != 1 || list.Triggers[0].ID != "pinned" || !list.Triggers[0].Enabled {
		t.Fatalf("list = %+v", list)
	}
	if list.Triggers[0].NextFire.IsZero() {
		t.Fatalf("a config schedule must report its next run: %+v", list.Triggers[0])
	}
}

func TestTriggersAPICreateValidateAndDelete(t *testing.T) {
	s, _, st := triggerServer(t, &triggers.Trigger{
		ID: "pinned", Schedule: "0 3 * * *", Job: triggers.Job{Instruction: "from yaml"}})

	rec := do(t, s, http.MethodPost, "/api/v1/triggers",
		`{"id":"nightly","schedule":"30 2 * * *","job":{"instruction":"sweep","ttl":5}}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST code = %d: %s", rec.Code, rec.Body)
	}
	if stored, err := triggers.List(st); err != nil || len(stored) != 1 || stored[0].ID != "nightly" {
		t.Fatalf("stored after create = %+v (%v)", stored, err)
	}

	// A config-declared id cannot be shadowed: the write would look applied and
	// never run, since the config definition wins the merge.
	rec = do(t, s, http.MethodPost, "/api/v1/triggers",
		`{"id":"pinned","schedule":"0 4 * * *","job":{"instruction":"x"}}`)
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), "config") {
		t.Fatalf("shadow POST code = %d: %s", rec.Code, rec.Body)
	}

	// A malformed schedule is refused before anything is persisted.
	rec = do(t, s, http.MethodPost, "/api/v1/triggers",
		`{"id":"bad","schedule":"70 * * * *","job":{"instruction":"x"}}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("bad schedule code = %d: %s", rec.Code, rec.Body)
	}
	if stored, err := triggers.List(st); err != nil || len(stored) != 1 {
		t.Fatalf("rejected trigger reached the store: %+v (%v)", stored, err)
	}
	rec = do(t, s, http.MethodPost, "/api/v1/triggers", `not json`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad body code = %d: %s", rec.Code, rec.Body)
	}

	rec = do(t, s, http.MethodDelete, "/api/v1/triggers/nightly", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE code = %d: %s", rec.Code, rec.Body)
	}
	if stored, err := triggers.List(st); err != nil || len(stored) != 0 {
		t.Fatalf("store after delete = %+v (%v)", stored, err)
	}
	// Deleting again reports "not stored here", which is also the answer for a
	// config schedule: removing those means editing node.yaml.
	rec = do(t, s, http.MethodDelete, "/api/v1/triggers/nightly", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("second DELETE code = %d: %s", rec.Code, rec.Body)
	}
	rec = do(t, s, http.MethodDelete, "/api/v1/triggers/pinned", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("DELETE of a config id code = %d: %s", rec.Code, rec.Body)
	}

	list := decodeList(t, do(t, s, http.MethodGet, "/api/v1/triggers", ""))
	if list.Count != 1 || list.Triggers[0].ID != "pinned" {
		t.Fatalf("after deletes: %+v", list)
	}
}

// A stored trigger survives the scheduler's restart, so a node restart does
// not silently stop work an operator scheduled days ago.
func TestTriggersAPIStoredTriggerSurvivesRestart(t *testing.T) {
	_, sched, st := triggerServer(t)
	if _, err := sched.Add(&triggers.Trigger{ID: "kept", Schedule: "*/5 * * * *",
		Job: triggers.Job{Instruction: "heartbeat"}}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	reopened, err := triggers.New(triggers.Options{
		Store:  st,
		Submit: func(context.Context, triggers.Job) (string, error) { return "task-2", nil },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	views, err := reopened.Views(time.Now().UTC())
	if err != nil {
		t.Fatalf("Views: %v", err)
	}
	if len(views) != 1 || views[0].ID != "kept" {
		t.Fatalf("views after reopen = %+v", views)
	}
}

// The POST body describes a schedule, not its history: counters are the
// scheduler's alone, so a crafted last_fire cannot silently disarm the job.
func TestTriggersAPIIgnoresClientSuppliedBookkeeping(t *testing.T) {
	s, _, st := triggerServer(t)
	future := time.Now().UTC().Add(72 * time.Hour)
	body := `{"id":"sneaky","schedule":"* * * * *","run_count":99,"last_fire":"` +
		future.Format(time.RFC3339) + `","last_error":"injected","job":{"instruction":"x"}}`
	if rec := do(t, s, http.MethodPost, "/api/v1/triggers", body); rec.Code != http.StatusAccepted {
		t.Fatalf("POST code = %d: %s", rec.Code, rec.Body)
	}
	stored, err := triggers.List(st)
	if err != nil || len(stored) != 1 {
		t.Fatalf("stored = %+v (%v)", stored, err)
	}
	got := stored[0]
	if !got.LastFire.IsZero() || !got.LastRunAt.IsZero() || got.RunCount != 0 ||
		got.LastError != "" || got.LastTaskID != "" {
		t.Fatalf("client-supplied bookkeeping was accepted: %+v", got)
	}
	views, err := s.node.Scheduler.Views(time.Now().UTC())
	if err != nil || len(views) != 1 || views[0].NextFire.IsZero() {
		t.Fatalf("the schedule reports as disarmable: %+v (%v)", views, err)
	}
}
