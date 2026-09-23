package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/developer3000S/zeptoclaw/internal/discovery"
	"github.com/developer3000S/zeptoclaw/internal/logging"
	"github.com/developer3000S/zeptoclaw/internal/metrics"
	"github.com/developer3000S/zeptoclaw/internal/p2p"
	"github.com/developer3000S/zeptoclaw/internal/picoclaw"
	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/skills"
	"github.com/developer3000S/zeptoclaw/internal/storage"
	"github.com/developer3000S/zeptoclaw/internal/tasks"
	"github.com/developer3000S/zeptoclaw/internal/triggers"
	"github.com/developer3000S/zeptoclaw/internal/version"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// New builds every component, wired but not started.
func New(opts Options) (*Node, error) {
	cfg := opts.Config
	if cfg == nil {
		return nil, errors.New("node: config required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	dirs := []string{
		cfg.Node.DataDir, cfg.Storage.Dir, cfg.ArtifactsDir(), cfg.TasksDir(),
		cfg.AuditDir(), cfg.Discovery.LocalSocketDir, cfg.PicoClaw.WorkspaceRoot,
		filepath.Dir(cfg.Identity.KeyFile),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("node: mkdir %s: %w", d, err)
		}
	}

	identity, err := security.LoadOrGenerate(cfg.Identity.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("node: identity: %w", err)
	}
	logger = logger.With("peer_id", identity.PeerID().String())

	minTrust, err := security.ParseTrust(cfg.Security.MinTrustForTasks)
	if err != nil {
		return nil, fmt.Errorf("node: %w", err)
	}
	mode, err := security.ParseMode(cfg.Security.TrustMode)
	if err != nil {
		return nil, fmt.Errorf("node: %w", err)
	}
	policy := security.NewPolicy(mode, minTrust)
	if n, err := policy.LoadPeerFile(cfg.Security.AllowedPeersFile, true); err != nil {
		return nil, fmt.Errorf("node: allow list: %w", err)
	} else if n > 0 {
		logger.Info("allow_list_loaded", "peers", n)
	}
	if n, err := policy.LoadPeerFile(cfg.Security.BlockedPeersFile, false); err != nil {
		return nil, fmt.Errorf("node: block list: %w", err)
	} else if n > 0 {
		logger.Info("block_list_loaded", "peers", n)
	}

	rebindsPath := filepath.Join(cfg.Node.DataDir, "rebinds.json")
	rebinds, err := security.NewRebindStore(rebindsPath,
		logging.Component(logger, "rebinds"))
	if err != nil {
		return nil, fmt.Errorf("node: rebind ledger: %w", err)
	}
	policy.SetRebindStore(rebinds)

	audit, err := security.OpenAudit(filepath.Join(cfg.AuditDir(), "security.jsonl"), logger)
	if err != nil {
		return nil, fmt.Errorf("node: audit: %w", err)
	}
	closePartial := func() {
		_ = audit.Close()
	}

	store, err := storage.Open(storage.Options{Dir: cfg.Storage.Dir, ArtifactsDir: cfg.ArtifactsDir()})
	if err != nil {
		closePartial()
		return nil, fmt.Errorf("node: storage: %w", err)
	}
	adapter := opts.Adapter
	if adapter == nil {
		adapter, err = picoclaw.New(cfg, logging.Component(logger, "picoclaw"))
		if err != nil {
			_ = store.Close()
			closePartial()
			return nil, fmt.Errorf("node: picoclaw adapter: %w", err)
		}
	}

	mets := metrics.New("")
	mets.BuildInfo.Set(1)

	skillDocs := make([]skills.Descriptor, 0, len(cfg.Capabilities.SkillDocs))
	for _, d := range cfg.Capabilities.SkillDocs {
		skillDocs = append(skillDocs, skills.Descriptor{
			Name: d.Name, Version: d.Version, Description: d.Description,
			Models: d.Models, Attributes: d.Attributes,
		})
	}
	skillsPath := ""
	if cfg.Capabilities.SkillExchange.Persist {
		skillsPath = filepath.Join(cfg.Node.DataDir, "skills.json")
	}
	skillReg, err := skills.New(skills.Options{
		Path: skillsPath, Skills: cfg.EffectiveSkills(), Docs: skillDocs,
		ImportLimit: cfg.Capabilities.SkillExchange.ImportLimit,
		Logger:      logging.Component(logger, "skills"),
	})
	if err != nil {
		_ = store.Close()
		closePartial()
		return nil, fmt.Errorf("node: skill registry: %w", err)
	}
	mets.SkillsVersion.Set(float64(skillReg.Epoch()))

	table := routing.NewTable(cfg.Neighbors, store, logging.Component(logger, "routing"), policy)
	if n, err := table.Restore(); err != nil {
		logger.Warn("neighbor_table_restore_failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("neighbor_table_restored", "peers", n)
	}

	n := &Node{
		Cfg: cfg, Identity: identity, Store: store, Policy: policy,
		Limiter: security.NewLimiter(cfg.Security.RateLimit.RequestsPerSecond, cfg.Security.RateLimit.Burst),
		Audit:   audit, Rebinds: rebinds, Metrics: mets, Table: table, Adapter: adapter, Skills: skillReg, log: logger,
		configPath: opts.ConfigPath, levelVar: opts.LevelVar, quit: make(chan struct{}),
		halted:      make(chan struct{}),
		introducing: make(map[peer.ID]bool), handshaked: make(map[peer.ID]bool),
		skipDiscovery: opts.SkipDiscovery,
	}

	if n.Rebinds.Revoked(n.ID()) {
		_ = store.Close()
		closePartial()
		return nil, fmt.Errorf("node: identity %s is revoked; install a new key (zeptomesh-node genkey) or remove this statement from %s to rejoin",
			n.ID().String(), rebindsPath)
	}

	ph, err := p2p.New(context.Background(), p2p.Options{
		Config:  cfg,
		Key:     identity.PrivKey(),
		Gater:   p2p.NewGater(policy, audit),
		Logger:  logging.Component(logger, "transport"),
		Metrics: mets,
	})
	if err != nil {
		_ = adapter.Close()
		_ = store.Close()
		closePartial()
		return nil, fmt.Errorf("node: p2p host: %w", err)
	}
	n.Host = ph
	n.Service = p2p.NewService(ph, p2p.Handlers{
		Task:   n.dispatchTask,
		Result: n.dispatchResult,
		RPC:    n.dispatchRPC,
	}, logging.Component(logger, "transport"))

	if err := n.buildDiscovery(); err != nil {
		return nil, err
	}

	mgr, err := tasks.NewManager(tasks.Options{
		Config: cfg, Identity: identity, Policy: policy, Limiter: n.Limiter,
		Audit: audit, Store: store, Table: table, Adapter: adapter,
		Service: n.Service, Known: n.skillsSource(), SkillView: skillReg,
		OnRebind: n.acceptRebind,
		Metrics:  mets, Logger: logging.Component(logger, "tasks"),
	})
	if err != nil {
		return nil, fmt.Errorf("node: task manager: %w", err)
	}
	n.Manager = mgr
	mgr.SetCapabilitiesFunc(n.Capabilities)

	sched, err := triggers.New(triggers.Options{
		Store:  &triggerStore{store: store},
		Submit: n.submitTrigger,
		Logger: logging.Component(logger, "triggers"),
		Config: cfg.ConfigTriggers(),
	})
	if err != nil {
		return nil, fmt.Errorf("node: triggers: %w", err)
	}
	n.Scheduler = sched

	n.installConnNotifier()
	return n, nil
}

// triggerStore adapts the node's key-value store to the narrow Saver the
// trigger package needs.
type triggerStore struct{ store *storage.Store }

func (t *triggerStore) PutMeta(key string, val []byte) error { return t.store.PutMeta(key, val) }

func (t *triggerStore) GetMeta(key string) ([]byte, error) {
	raw, err := t.store.GetMeta(key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, triggers.ErrNoRecord
	}
	return raw, err
}

// submitTrigger turns a scheduled job into a task authored by this node.
func (n *Node) submitTrigger(ctx context.Context, job triggers.Job) (string, error) {
	if n.Manager == nil {
		return "", errors.New("node: task manager not ready")
	}
	labels := job.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	labels["source"] = "trigger"
	return n.Manager.Submit(ctx, tasks.SubmitRequest{
		Instruction:    job.Instruction,
		RequiredSkills: job.RequiredSkills,
		TTL:            job.TTL,
		Priority:       job.Priority,
		TimeoutSeconds: job.TimeoutSeconds,
		AllowShell:     job.AllowShell,
		AllowNetwork:   job.AllowNetwork,
		Labels:         labels,
	})
}

// dispatch* forward to the manager; they exist so the service can be built
// before the manager (the manager needs the service).
func (n *Node) dispatchTask(ctx context.Context, remote peer.ID, env *pb.TaskEnvelope) (*pb.TaskAck, error) {
	if n.Manager == nil {
		return nil, errors.New("node: manager not ready")
	}
	return n.Manager.OnTask(ctx, remote, env)
}

func (n *Node) dispatchResult(ctx context.Context, remote peer.ID, res *pb.TaskResult) (*pb.ResultAck, error) {
	if n.Manager == nil {
		return nil, errors.New("node: manager not ready")
	}
	return n.Manager.OnResult(ctx, remote, res)
}

func (n *Node) dispatchRPC(ctx context.Context, remote peer.ID, req *pb.RpcRequest) (*pb.RpcResponse, error) {
	if n.Manager == nil {
		return nil, errors.New("node: manager not ready")
	}
	return n.Manager.OnRPC(ctx, remote, req)
}

// Start runs every component. ctx governs the whole node lifetime.
func (n *Node) Start(ctx context.Context) error {
	n.mu.Lock()
	if n.running {
		n.mu.Unlock()
		return errors.New("node: already started")
	}
	n.running = true
	n.started = time.Now().UTC()
	runCtx, cancel := context.WithCancel(ctx)
	n.cancel = cancel
	n.mu.Unlock()

	n.Manager.Start(runCtx)
	n.Scheduler.Start(runCtx)

	if n.Registry != nil {
		if err := n.Registry.Start(runCtx); err != nil {
			n.log.Warn("local_registry_start_failed", "err", err.Error())
			_ = n.Registry.Stop()
			n.Registry = nil
		}
	}
	if n.Probe != nil {
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.Probe.Serve() }()
	}

	if n.Cfg.Discovery.MDNS && !n.skipDiscovery {
		m, err := discovery.NewMDNS(n.Host.Underlying(), n.Cfg.Discovery.MDNSServiceName, n.log)
		switch {
		case err != nil:
			n.log.Warn("mdns_init_failed", "err", err.Error())
		case m.Start(func(ai peer.AddrInfo) { n.onDiscovered(ai, routing.CatLAN, "mdns") }) != nil:
			n.log.Warn("mdns_start_failed", "err", m.Start(nil).Error())
			_ = m.Close()
		default:
			n.MDNS = m
		}
	}

	boot, err := n.bootstrapAddrInfos()
	if err != nil {
		return err
	}
	if n.DHT != nil {
		if err := n.DHT.Start(runCtx, boot); err != nil {
			n.log.Warn("dht_start_failed", "err", err.Error())
		}
	}
	if n.Membership != nil {
		if err := n.Membership.Start(runCtx); err != nil {
			return fmt.Errorf("node: membership start: %w", err)
		}
	}
	if n.SearchTopic != nil {
		if err := n.SearchTopic.Start(runCtx); err != nil {
			return fmt.Errorf("node: search topic start: %w", err)
		}
	}

	if n.Bootstrap != nil {
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.Bootstrap.Run(runCtx, n.needMorePeers) }()
	}
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.maintenance(runCtx) }()

	n.log.Info("node_started",
		"node_name", n.Cfg.Node.Name,
		"addrs", n.DiscoverableAddrs(),
		"skills", n.Cfg.EffectiveSkills(),
		"adapter", n.Adapter.Name(),
		"version", version.Version,
	)
	return nil
}

// Stop announces departure while the transport still works, then tears every
// component down in reverse order.
func (n *Node) Stop(ctx context.Context) error {
	n.mu.Lock()
	if !n.running {
		n.mu.Unlock()
		return nil
	}
	n.running = false
	cancel := n.cancel
	n.cancel = nil
	n.mu.Unlock()

	var errs []error
	if n.Membership != nil {
		if st := n.peerState(); st != nil {
			st.Status = "left"
			pctx, pcancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := n.Membership.PublishState(pctx, st); err != nil {
				errs = append(errs, fmt.Errorf("departure announcement: %w", err))
			}
			pcancel()
		}
	}

	if n.Probe != nil {
		errs = append(errs, n.Probe.Close())
	}
	if n.Registry != nil {
		errs = append(errs, n.Registry.Stop())
	}
	if n.MDNS != nil {
		errs = append(errs, n.MDNS.Close())
	}
	if n.Membership != nil {
		errs = append(errs, n.Membership.Close())
	}
	if n.SearchTopic != nil {
		errs = append(errs, n.SearchTopic.Close())
	}
	if n.DHT != nil {
		errs = append(errs, n.DHT.Close())
	}
	if cancel != nil {
		cancel()
	}
	done := make(chan struct{})
	go func() { n.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		errs = append(errs, errors.New("node: background work still running"))
	}
	if n.Scheduler != nil {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := n.Scheduler.Stop(sctx); err != nil {
			errs = append(errs, err)
		}
		scancel()
	}
	n.Manager.Stop()
	errs = append(errs, n.Adapter.Close(), n.Host.Close(), n.Store.Close(), n.Audit.Close())

	n.log.Info("node_stopped", "uptime", time.Since(n.started).Round(time.Second).String())
	return errors.Join(errs...)
}

// ID exposes the peer id.
func (n *Node) ID() peer.ID { return n.Identity.PeerID() }
