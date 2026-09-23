// Command zeptomesh-ui runs the ZeptoClaw mesh dashboard backend: it polls
// the admin APIs of the configured agent nodes, merges their view of the mesh
// into one network graph, serves the embedded web UI and proxies operator
// actions back to a chosen node.
//
//	ZETOMESH_UI_NODES=http://zepto-0:8081 ./bin/zeptomesh-ui
//
// It is deliberately a separate binary and image from the agent: installing it
// touches no agent configuration (see install-ui.sh).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/developer3000S/zeptoclaw/internal/logging"
	"github.com/developer3000S/zeptoclaw/internal/ui"
	"github.com/developer3000S/zeptoclaw/internal/version"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help") {
		fmt.Print(usage)
		return
	}
	cfg, err := ui.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "zeptomesh-ui:", err)
		os.Exit(1)
	}
	logger, _ := logging.New(logging.Options{
		Level:  cfg.LogLevel.String(),
		Format: "text",
	})
	logger = logger.With("component", "zeptomesh-ui")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("starting",
		"listen", cfg.Listen,
		"nodes", len(cfg.Nodes),
		"poll_interval", cfg.PollInterval,
		"auth", cfg.Token != "")

	agg := ui.NewAggregator(cfg, logger)
	go func() { agg.Run(ctx) }()

	srv := ui.New(&cfg, agg, logger)
	err = srv.Serve(ctx)
	if err != nil {
		logger.Error("serve_failed", "err", err.Error())
		os.Exit(1)
	}
	logger.Info("stopped")
}

const usage = `zeptomesh-ui — ZeptoClaw mesh dashboard backend

Configuration is entirely environmental (like the agent):

  ZETOMESH_UI_LISTEN        listen address        (default 127.0.0.1:8090)
  ZETOMESH_UI_NODES         comma-separated agent admin API URLs (seed list)
  ZETOMESH_UI_NODES_FILE    persistent node list  (default <data>/nodes.json)
  ZETOMESH_UI_TOKEN         access token for this UI's own API (default: none)
  ZETOMESH_UI_API_TOKEN     bearer used for nodes without their own token
  ZETOMESH_UI_POLL_INTERVAL node poll interval    (default 5s)
  ZETOMESH_UI_DATA          data directory        (default ./zeptomesh-ui-data)
  ZETOMESH_UI_LOG_LEVEL     trace|debug|info|warn|error (default info)

See docs/UI.md for the full specification and install-ui.sh for the Docker path.
`

// version is wired by the Makefile/Dockerfile like the agent's stamp.
var _ = version.String

// guard keeps an unused-import error from surprising a future edit.
var _ = errors.New
