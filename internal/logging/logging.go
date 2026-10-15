// Package logging configures the shared slog logger and the libp2p log bridge.
package logging

import (
	"context"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"
)

// Options controls logger construction.
type Options struct {
	Level     string
	Format    string // "json" | "text"
	PeerID    string
	Component string
}

// New builds a slog.Logger writing structured records.
//
// Field names follow ТЗ 14.1: `ts` in place of slog's `time`, and `event` for the
// record text, which in this codebase has always been the machine-readable name of
// what happened (task_delegated, caps_bad_signature, …) — so the mandatory `event`
// field costs nothing to obtain. The same text is mirrored into `message` for a
// consumer that renders one searchable log line, unless a call site brings prose of
// its own, which then wins. `component` is present on every record (see
// boundAttrs), `peer_id` is added by the node that owns an identity, `task_id` by
// the code paths working on a task.
func New(opts Options) (*slog.Logger, *slog.LevelVar) {
	lvl := new(slog.LevelVar)
	lvl.Set(parseLevel(opts.Level))

	bound := []slog.Attr{slog.String("component", orDefault(opts.Component, defaultComponent))}
	if opts.PeerID != "" {
		bound = append(bound, slog.String("peer_id", opts.PeerID))
	}
	return slog.New(&boundAttrs{
		Handler: newHandler(opts.Format, lvl, os.Stderr),
		attrs:   bound,
	}), lvl
}

// newHandler builds the sink. The stream is a parameter so the field policy can be
// asserted on a buffer instead of the process's stderr.
func newHandler(format string, lvl *slog.LevelVar, w io.Writer) slog.Handler {
	h := slog.HandlerOptions{Level: lvl, ReplaceAttr: renameBuiltins}
	if format == "text" {
		return slog.NewTextHandler(w, &h)
	}
	return slog.NewJSONHandler(w, &h)
}

// renameBuiltins maps slog's own record keys onto the names ТЗ 14.1 fixes.
func renameBuiltins(groups []string, a slog.Attr) slog.Attr {
	if len(groups) != 0 {
		return a
	}
	switch a.Key {
	case slog.TimeKey:
		a.Key = "ts"
	case slog.MessageKey:
		a.Key = "event"
	}
	return a
}

// defaultComponent is what an unlabelled logger reports. The field is mandatory,
// so silence would be a gap rather than an answer: "node" describes everything the
// node assembles, and each subsystem relabels itself with Component.
const defaultComponent = "node"

// boundAttrs holds the attributes a logger carries (as opposed to the ones a call
// site passes) and lets the call site win on a name collision.
//
// slog's own With appends: a node that binds `peer_id` and a host that also names
// the peer it started as produced one record with the key twice — not valid JSON
// for a consumer that then keeps whichever copy it happened to read last, and
// unparseable for a strict one. Collecting the bound attributes here instead of
// forwarding them to the wrapped handler gives one value per key, chosen by the
// code that knows more, which is the same rule that lets Component override the
// default without leaving two `component` fields behind.
type boundAttrs struct {
	slog.Handler
	attrs []slog.Attr
}

func (b *boundAttrs) Enabled(ctx context.Context, lvl slog.Level) bool {
	return b.Handler.Enabled(ctx, lvl)
}

func (b *boundAttrs) Handle(ctx context.Context, rec slog.Record) error {
	merged := make([]slog.Attr, len(b.attrs))
	copy(merged, b.attrs)
	callSite := make([]slog.Attr, 0, rec.NumAttrs())
	rec.Attrs(func(a slog.Attr) bool {
		if !a.Equal(slog.Attr{}) {
			callSite = append(callSite, a)
		}
		return true
	})
	// ТЗ 14.1 asks for `event` and `message` as separate fields. The record text is
	// the event name here (see New), so `event` is already covered; `message` gets
	// the same text unless the call site brings prose of its own, which merge() then
	// puts in its place. The default duplication is the point: a viewer that renders
	// one searchable log line reads `message`, a dashboard that filters reads `event`.
	merged = append(merged, slog.String("message", rec.Message))
	// A Record shares its attribute state with every copy of itself, so the line
	// is rebuilt rather than edited in place.
	out := slog.NewRecord(rec.Time, rec.Level, rec.Message, rec.PC)
	out.AddAttrs(merge(merged, callSite)...)
	return b.Handler.Handle(ctx, out)
}

func (b *boundAttrs) WithAttrs(attrs []slog.Attr) slog.Handler {
	kept := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if !a.Equal(slog.Attr{}) {
			kept = append(kept, a)
		}
	}
	clone := *b
	clone.attrs = merge(clone.attrs, kept)
	return &clone
}

// WithGroup qualifies the record the way slog's handlers do. Nothing to do for
// the bound attributes here: they are placed into the record before the wrapped
// handler sees it, so that handler prefixes them along with the rest.
func (b *boundAttrs) WithGroup(name string) slog.Handler {
	clone := *b
	clone.Handler = b.Handler.WithGroup(name)
	return &clone
}

// merge applies over on top of base: a repeated key keeps base's position and
// over's value, so field order stays readable while the newer answer wins.
func merge(base, over []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, len(base))
	copy(out, base)
	for _, a := range over {
		placed := false
		for i := range out {
			if out[i].Key == a.Key {
				out[i] = a
				placed = true
				break
			}
		}
		if !placed {
			out = append(out, a)
		}
	}
	return out
}

// Component returns a logger whose records name the subsystem that writes them.
func Component(l *slog.Logger, name string) *slog.Logger {
	return l.With("component", name)
}

func orDefault(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// BridgeStdlib routes the standard library logger (used by libp2p and badger)
// into slog so that a single stream carries every record.
func BridgeStdlib(l *slog.Logger) {
	log.SetFlags(0)
	log.SetOutput(slogWriter{logger: l, level: slog.LevelDebug})
}

type slogWriter struct {
	logger *slog.Logger
	level  slog.Level
}

func (w slogWriter) Write(p []byte) (int, error) {
	msg := strings.TrimSpace(string(p))
	if msg != "" {
		w.logger.LogAttrs(context.Background(), w.level, msg)
	}
	return len(p), nil
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Level maps a configuration string to a slog level, exposed so the node can
// retune the live handler on a config reload.
func Level(s string) slog.Level { return parseLevel(s) }
