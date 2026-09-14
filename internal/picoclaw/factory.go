package picoclaw

import (
	"fmt"
	"log/slog"

	"github.com/developer3000S/zeptoclaw/internal/config"
)

// New builds the adapter selected by picoclaw.mode:
//
//	stub   — deterministic offline executor (default; no credentials needed)
//	binary — one `picoclaw agent -m` child process per task
//	http   — Pico Protocol WebSocket against a running `picoclaw gateway`
//
// The returned adapter is always non-nil on success; callers own Close.
func New(cfg *config.Config, logger *slog.Logger) (Adapter, error) {
	if cfg == nil {
		return nil, fmt.Errorf("picoclaw: nil config")
	}
	if logger == nil {
		logger = slog.Default()
	}
	switch cfg.PicoClaw.Mode {
	case "binary":
		a, err := NewCLI(cfg.PicoClaw, logger)
		if err != nil {
			return nil, err
		}
		logger.Info("picoclaw_adapter", "mode", "binary", "binary", a.binary, "model", a.model)
		return a, nil
	case "http":
		a, err := NewWS(cfg.PicoClaw, logger)
		if err != nil {
			return nil, err
		}
		logger.Info("picoclaw_adapter", "mode", "http", "url", a.wsURL)
		return a, nil
	case "stub", "":
		a := NewStub(cfg.PicoClaw.Stub, cfg.EffectiveSkills())
		logger.Info("picoclaw_adapter", "mode", "stub", "note", "no real agent is invoked")
		return a, nil
	default:
		return nil, fmt.Errorf("picoclaw: unknown mode %q", cfg.PicoClaw.Mode)
	}
}
