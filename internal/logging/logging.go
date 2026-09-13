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
func New(opts Options) (*slog.Logger, *slog.LevelVar) {
	lvl := new(slog.LevelVar)
	lvl.Set(parseLevel(opts.Level))

	h := slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	w := io.Writer(os.Stderr)
	if opts.Format == "text" {
		handler = slog.NewTextHandler(w, &h)
	} else {
		handler = slog.NewJSONHandler(w, &h)
	}

	attrs := make([]any, 0, 4)
	if opts.Component != "" {
		attrs = append(attrs, "component", opts.Component)
	}
	if opts.PeerID != "" {
		attrs = append(attrs, "peer_id", opts.PeerID)
	}
	return slog.New(handler).With(attrs...), lvl
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
