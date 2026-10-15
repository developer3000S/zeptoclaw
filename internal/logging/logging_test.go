package logging

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// capture builds a logger over a writer with the same field bindings New uses, so
// the policy can be read back as data rather than eyeballed.
func capture(w io.Writer) *slog.Logger {
	lvl := new(slog.LevelVar)
	lvl.Set(slog.LevelDebug)
	return slog.New(&boundAttrs{
		Handler: newHandler("json", lvl, w),
		attrs:   []slog.Attr{slog.String("component", defaultComponent)},
	})
}

// decode reads one record and refuses a key that appears twice: encoding/json lets
// the last copy win silently, which is exactly how a duplicated field would survive
// a test that only decodes.
func decode(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	line := bytes.TrimSpace(raw)
	if i := bytes.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	var out map[string]any
	if err := json.Unmarshal(line, &out); err != nil {
		t.Fatalf("record is not JSON (%q): %v", string(raw), err)
	}
	for _, key := range []string{"component", "peer_id", "task_id", "node_name", "event", "ts", "message"} {
		if got := strings.Count(string(line), `"`+key+`":`); got > 1 {
			t.Fatalf("%q appears %d times in one record, want once: %s", key, got, string(line))
		}
	}
	return out
}

// ТЗ 14.1 names the mandatory fields; the record text has always been the event
// name here, so the assertion is about the emitted keys, not about call sites.
func TestMandatoryFieldsAreNamedPerTZ(t *testing.T) {
	var buf bytes.Buffer
	l := capture(&buf)
	l.Info("task_delegated", "task_id", "t-1", "to", "12D3")

	rec := decode(t, buf.Bytes())
	for _, key := range []string{"ts", "level", "event", "message", "component", "task_id"} {
		if _, ok := rec[key]; !ok {
			t.Errorf("record has no %q field: %s", key, buf.String())
		}
	}
	if rec["event"] != "task_delegated" {
		t.Errorf("event = %v, want task_delegated", rec["event"])
	}
	// The two text fields carry the same name by default: a viewer that renders a
	// log line and a dashboard that filters by event must both find the record.
	if rec["message"] != "task_delegated" {
		t.Errorf("message = %v, want the event name mirrored", rec["message"])
	}
	if rec["component"] != defaultComponent {
		t.Errorf("component = %v, want the %q default an unlabelled logger reports",
			rec["component"], defaultComponent)
	}
	// slog's own names must not survive: a consumer written against the ТЗ
	// schema reads `ts`/`event`, and a duplicate `time` is noise at best.
	for _, banned := range []string{"time", "msg"} {
		if _, ok := rec[banned]; ok {
			t.Errorf("record still carries slog's default key %q: %s", banned, buf.String())
		}
	}
	// The payload a call site adds is untouched by the renaming.
	if rec["task_id"] != "t-1" || rec["to"] != "12D3" {
		t.Errorf("attributes were rewritten: %v", rec)
	}
}

// A node binds peer_id, and a call site may name a peer it is talking about. Both
// are legitimate; exactly one value may reach the record, and it is the call site's,
// because that code knows which of the two it means.
func TestBoundKeysAreNotEmittedTwice(t *testing.T) {
	var buf bytes.Buffer
	lvl := new(slog.LevelVar)
	lvl.Set(slog.LevelDebug)
	l := slog.New(&boundAttrs{Handler: newHandler("json", lvl, &buf), attrs: []slog.Attr{
		slog.String("component", defaultComponent),
		slog.String("peer_id", "12D3self"),
	}})
	l = l.With("node_name", "probe0")
	l.Info("host_started", "peer_id", "12D3peer", "node_name", "renamed")

	rec := decode(t, buf.Bytes())
	if rec["peer_id"] != "12D3peer" {
		t.Errorf("peer_id = %v, want the call site's value", rec["peer_id"])
	}
	if rec["node_name"] != "renamed" {
		t.Errorf("node_name = %v, want the call site's value", rec["node_name"])
	}
	// Field order follows the first appearance, so a log read by eye stays stable
	// when a call site happens to repeat a bound key.
	first := strings.Index(buf.String(), `"peer_id"`)
	last := strings.LastIndex(buf.String(), `"node_name"`)
	if first > last {
		t.Errorf("field order changed under merging: %s", buf.String())
	}
}

// A call site with prose to add uses `message`; the event name stays where the
// dashboard looks for it.
func TestCallSiteMessageWins(t *testing.T) {
	var buf bytes.Buffer
	l := capture(&buf)
	l.Warn("task_rejected", "message", "peer refused: rate limit exceeded")

	rec := decode(t, buf.Bytes())
	if rec["event"] != "task_rejected" {
		t.Errorf("event = %v, want task_rejected", rec["event"])
	}
	if rec["message"] != "peer refused: rate limit exceeded" {
		t.Errorf("message = %v, want the call site's prose", rec["message"])
	}
}

// The narrowest logger must win: the node's records say "node", a subsystem's say
// which subsystem, and neither leaves two copies of the key behind.
func TestComponentIsOverriddenByTheNarrowestLogger(t *testing.T) {
	var buf bytes.Buffer
	root := capture(&buf)
	peer := root.With("peer_id", "12D3")
	peer = Component(peer, "api")
	peer.Info("admin_request", "path", "/api/v1/status")

	rec := decode(t, buf.Bytes())
	if rec["component"] != "api" {
		t.Fatalf("component = %v, want api", rec["component"])
	}
	if rec["peer_id"] != "12D3" {
		t.Errorf("peer_id was lost while resolving the component: %s", buf.String())
	}
}

// stdlib traffic (libp2p, badger) arrives as prose, not as an event name, and must
// still be a record the same tools can parse.
func TestBridgedStdlibLineKeepsTheSchema(t *testing.T) {
	var buf bytes.Buffer
	l := capture(&buf)
	BridgeStdlib(l)
	t.Cleanup(func() { BridgeStdlib(slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))) })

	l.Warn("noise handshake failed")
	rec := decode(t, buf.Bytes())
	if rec["event"] != "noise handshake failed" {
		t.Errorf("bridged text lost its message: %v", rec["event"])
	}
	if rec["component"] != defaultComponent {
		t.Errorf("bridged record has no component: %v", rec)
	}
}

// The level filter belongs to the sink and must survive the wrapper.
func TestLevelFilterStillApplies(t *testing.T) {
	var buf bytes.Buffer
	lvl := new(slog.LevelVar)
	lvl.Set(slog.LevelInfo)
	l := slog.New(&boundAttrs{Handler: newHandler("json", lvl, &buf)})
	l.Debug("not_emitted")
	if buf.Len() != 0 {
		t.Fatalf("debug record passed an info filter: %s", buf.String())
	}
	l.Info("emitted")
	if buf.Len() == 0 {
		t.Fatal("info record was dropped")
	}
}

// A logger is used from many goroutines at once (one per stream, per peer). The
// bound attributes are shared by all of them and must not be edited by any.
func TestConcurrentLoggingKeepsOneComponent(t *testing.T) {
	var buf bytes.Buffer
	l := Component(capture(&lockedWriter{w: &buf}), "tasks")
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Info("concurrent_event", "task_id", i)
		}(i)
	}
	wg.Wait()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 32 {
		t.Fatalf("%d records written, want 32", len(lines))
	}
	for _, line := range lines {
		if got := strings.Count(line, `"component"`); got != 1 {
			t.Fatalf("component appears %d times in %s", got, line)
		}
	}
}

// lockedWriter serialises writes so the test above exercises the handler's
// concurrency, not bytes.Buffer's (which is not safe for it).
type lockedWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
