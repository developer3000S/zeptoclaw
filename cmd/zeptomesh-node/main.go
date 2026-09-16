// Command zeptomesh-node runs one ZeptoClaw Agent Mesh node and doubles as an
// admin client for a running local node.
//
//	zeptomesh-node run     -config configs/node.yaml     # start a node
//	zeptomesh-node version                               # build stamp
//	zeptomesh-node genkey  --out FILE [--print-id]       # create identity
//	zeptomesh-node psk                                   # private-network key
//	zeptomesh-node submit  -i "instruction" [-w]         # inject a task
//	zeptomesh-node status|peers|tasks|get|cancel|resubmit
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/developer3000S/zeptoclaw/internal/api"
	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/logging"
	"github.com/developer3000S/zeptoclaw/internal/node"
	"github.com/developer3000S/zeptoclaw/internal/p2p"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/telemetry"
	"github.com/developer3000S/zeptoclaw/internal/version"
)

// exRestart is the systemd sd_notify convention for "exit for a restart":
// units pair it with Restart=always (or on-failure) and RestartIsolation.
const exRestart = 75

const usage = `zeptomesh-node — ZeptoClaw Agent Mesh node daemon

Usage:
  zeptomesh-node <command> [flags]

Commands:
  run         start a mesh node (default when no command given)
  status      local node status            (alias: get-status)
  peers       list known neighbours
  capabilities      show this node's signed capabilities
  submit      inject a task                (alias: task)
  tasks       list recent tasks
  get         show one task (status/result)
  cancel      cancel a task originated here
  resubmit    re-inject a failed/timed-out task
  reload      re-read the node config file     (POST /admin/reload-config)
  leave       announce departure and restart   (POST /admin/leave)
  skills      show advertised skills and the versioned peer skill view
  skill-set   document one advertised skill    (POST /api/v1/skills)
  skill-rm    retire a skill's documentation   (DELETE /api/v1/skills/<name>)
  skills-sync reconcile peer skill descriptors now (POST /api/v1/skills/sync)
  triggers    list scheduled triggers          (GET /api/v1/triggers)
  trigger-add create or replace a trigger      (POST /api/v1/triggers)
  trigger-rm  delete a stored trigger          (DELETE /api/v1/triggers/<id>)
  rebinds     show retired and rotated peer identities (GET /api/v1/rebinds)
  genkey      create an Ed25519 identity key file
  rotate      replace the node key, announcing the handover to the mesh
  revoke      retire a peer id with a self-signed revocation
  psk         print a private-network PSK
  version     print build information

Run "zeptomesh-node <command> -h" for command flags.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "run":
		err = runNode(args)
	case "version", "-v", "--version":
		fmt.Println("zeptomesh-node", version.String())
	case "genkey":
		err = genKey(args)
	case "psk":
		var psk string
		psk, err = p2p.GeneratePSK()
		if err == nil {
			fmt.Println(psk)
		}
	case "status", "get-status":
		err = clientStatus(args)
	case "peers":
		err = clientPeers(args)
	case "capabilities":
		err = clientCapabilities(args)
	case "submit", "task":
		err = clientSubmit(args)
	case "tasks":
		err = clientTasks(args)
	case "get":
		err = clientGet(args)
	case "cancel":
		err = clientCancel(args)
	case "resubmit":
		err = clientResubmit(args)
	case "reload":
		err = clientReload(args)
	case "leave":
		err = clientLeave(args)
	case "rotate":
		err = clientRotate(args)
	case "revoke":
		err = clientRevoke(args)
	case "skills":
		err = clientSkills(args)
	case "skill-set":
		err = clientSkillSet(args)
	case "skill-rm":
		err = clientSkillRm(args)
	case "skills-sync":
		err = clientSkillsSync(args)
	case "triggers":
		err = clientTriggers(args)
	case "trigger-add":
		err = clientTriggerAdd(args)
	case "trigger-rm":
		err = clientTriggerRm(args)
	case "rebinds":
		err = clientRebinds(args)
	case "-h", "--help", "help":
		fmt.Print(usage)
	default:
		// Treat a leading flag as the default `run` invocation:
		// `zeptomesh-node -config x.yaml` should just work.
		if len(cmd) > 0 && cmd[0] == '-' {
			err = runNode(append([]string{cmd}, args...))
			break
		}
		fmt.Fprintf(os.Stderr, "zeptomesh-node: unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "zeptomesh-node:", err)
		os.Exit(1)
	}
}

// ---------- run ----------

func runNode(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	cfgPath := fs.String("config", envOr("ZETOMESH_CONFIG", ""), "path to YAML configuration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *cfgPath == "" {
		return fmt.Errorf("-config is required (or set ZETOMESH_CONFIG)")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}

	logger, levelVar := logging.New(logging.Options{
		Level:  cfg.Telemetry.LogLevel,
		Format: map[bool]string{true: "text", false: "json"}[!cfg.Telemetry.StructuredLogs],
	})
	logger = logger.With("node_name", cfg.Node.Name)
	logging.BridgeStdlib(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Tracing is installed before the node exists so the very first task this
	// process submits is already covered. When disabled, Setup returns a no-op
	// shutdown and links nothing (ТЗ 14.3).
	tracingShutdown, err := telemetry.Setup(ctx, telemetry.Config{
		Enabled:     cfg.Telemetry.Tracing.Enabled,
		Endpoint:    cfg.Telemetry.Tracing.Endpoint,
		Insecure:    cfg.Telemetry.Tracing.Insecure,
		SampleRatio: cfg.Telemetry.Tracing.SampleRatio,
		ServiceName: cfg.Telemetry.Tracing.ServiceName,
		Environment: cfg.Telemetry.Tracing.Environment,
	}, logger)
	if err != nil {
		logger.Warn("tracing_disabled", "err", err.Error())
	}

	n, err := node.New(node.Options{
		Config: cfg, Logger: logger, ConfigPath: *cfgPath, LevelVar: levelVar,
	})
	if err != nil {
		return err
	}
	if err := n.Start(ctx); err != nil {
		return err
	}

	// SIGHUP reloads the same file the admin endpoint does; operators
	// scripting outside the API get one more path to the same behaviour.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			d, err := n.ReloadConfig()
			if err != nil {
				logger.Error("config_reload_failed", "err", err.Error())
				continue
			}
			logger.Info("config_reloaded", "hot", len(d.Hot), "requires_restart", len(d.RequiresRestart))
		}
	}()

	stopNode := func() {
		sctx, scancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer scancel()
		if err := n.Stop(sctx); err != nil {
			logger.Error("node_stop", "err", err.Error())
		}
		// After the node: the manager's last spans are produced during shutdown,
		// and flushing them before it stops would drop them.
		if err := tracingShutdown(sctx); err != nil {
			logger.Error("tracing_shutdown", "err", err.Error())
		}
	}

	if cfg.API.Enabled {
		srv := api.New(n, cfg, n.Metrics, logging.Component(logger, "api"))
		go func() {
			if err := srv.Serve(ctx); err != nil {
				logger.Error("admin_api", "err", err.Error())
				stop()
			}
		}()
	}
	if cfg.Telemetry.PrometheusListen != "" {
		metricsAddr := cfg.Telemetry.PrometheusListen
		go func() {
			if err := n.Metrics.ServeMetrics(ctx, metricsAddr); err != nil {
				logger.Error("metrics_server", "addr", metricsAddr, "err", err.Error())
			}
		}()
		logger.Info("prometheus_listening", "addr", metricsAddr)
	}

	logger.Info("zeptomesh_node_running", "config", *cfgPath)
	restart := false
	select {
	case <-ctx.Done():
		logger.Info("shutdown_signal_received")
	case <-n.Halted():
		// The identity was revoked. Exiting non-zero is a request for a restart
		// under most supervisors, which is precisely what must not happen here,
		// so this path returns normally; node.RevokeSelf also moved the key file
		// aside, so even `restart: always` cannot resurrect the retired id.
		logger.Error("identity_revoked_not_restarting")
	case <-n.Quit():
		// /admin/leave: a supervisor-managed restart (systemd ExitCode=75,
		// docker restart policy) applies the on-disk configuration cleanly.
		logger.Info("leave_requested_restarting_process")
		restart = true
	}
	signal.Stop(hup)
	stop()
	stopNode()
	if restart {
		os.Exit(exRestart)
	}
	return nil
}

// ---------- genkey ----------

func genKey(args []string) error {
	fs := flag.NewFlagSet("genkey", flag.ContinueOnError)
	out := fs.String("out", "", "key file to create (required)")
	force := fs.Bool("force", false, "overwrite an existing key")
	printID := fs.Bool("print-id", true, "print the resulting peer id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return fmt.Errorf("-out is required")
	}
	if _, err := os.Stat(*out); err == nil && !*force {
		return fmt.Errorf("%s exists (use -force to replace)", *out)
	}
	id, err := security.LoadOrGenerate(*out)
	if err != nil {
		return err
	}
	if mode, derr := os.Stat(*out); derr == nil {
		_ = mode
	}
	if err := os.Chmod(*out, 0o600); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Dir(*out), 0o700); err != nil {
		return err
	}
	if *printID {
		fmt.Println(id.PeerID().String())
	}
	return nil
}

// ---------- client commands ----------

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
