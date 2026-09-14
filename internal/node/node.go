// Package node assembles every mesh component into one running node and owns
// its lifecycle: start, maintenance, graceful stop.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/developer3000S/zeptoclaw/internal/config"
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
	"github.com/developer3000S/zeptoclaw/internal/version"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// SubmitRequest is re-exported so callers outside the tasks package (the admin
// API, the CLI) do not need to import it.
type SubmitRequest = tasks.SubmitRequest

// Node is one ZeptoClaw mesh participant.
type Node struct {
	Cfg *config.Config

	Identity *security.Identity
	Store    *storage.Store
	Policy   *security.Policy
	Limiter  *security.Limiter
	Audit    *security.Audit
	// Rebinds is the ledger of identity handovers (rotation/revocation) that
	// this node has verified; the trust policy consults it on every decision.
	Rebinds *security.RebindStore
	Metrics *metrics.Collector
	Host    *p2p.Host
	Service *p2p.Service
	Table   *routing.Table
	Adapter picoclaw.Adapter
	Manager *tasks.Manager
	// Skills is the local skill registry: this node's documented, versioned
	// skill set plus the descriptors learned from peers (skill exchange).
	Skills     *skills.Registry
	Registry   *discovery.LocalRegistry
	Probe      *discovery.Probe
	MDNS       *discovery.MDNS
	Bootstrap  *discovery.Bootstrap
	DHT        *discovery.DHT
	Membership *discovery.Membership

	log     *slog.Logger
	started time.Time

	// skipDiscovery makes this node reachable only through explicitly dialed
	// peers. It exists for air-gapped single-node setups and for integration
	// tests that need a deterministic topology.
	skipDiscovery bool

	// configPath and levelVar back the admin reload (ТЗ 13.1.1): the running
	// process re-reads the same file it started with and retunes the live
	// logger without a restart.
	configPath string
	levelVar   *slog.LevelVar
	// quit is closed by Leave: the daemon's main loop treats it as "exit now,
	// systemd/docker will relaunch a fresh process" — a graceful, operator-
	// driven restart that keeps the mesh membership announcements honest.
	quit     chan struct{}
	quitOnce sync.Once

	// halted is closed by a self-revocation, which is the opposite request:
	// the identity is retired for good, so a supervisor must not bring the
	// process back under the same (now untrustworthy) key.
	halted     chan struct{}
	haltedOnce sync.Once

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	notifee *network.NotifyBundle

	// introducing guards the capabilities handshake we run for a peer whose
	// connection we passively accepted, so repeated multi-transport links to
	// the same peer do not stampede the RPC.
	introduceMu sync.Mutex
	introducing map[peer.ID]bool
	// handshaked records the peers this *process* has completed a capabilities
	// handshake with. It must not be derived from the neighbour table: that table
	// is persisted, while the trust earned by a handshake (Policy.Observe) lives
	// only in memory. Reusing the restored skill list as proof of a handshake
	// made every node skip the handshake after a restart — and so never re-earn
	// trust — which reads as "peer not trusted for tasks" from every neighbour.
	handshaked map[peer.ID]bool
}

// Options parameterise New.
type Options struct {
	Config *config.Config
	Logger *slog.Logger
	// ConfigPath is the file the node was loaded from; when set, an admin
	// POST /api/v1/admin/reload-config re-reads it.
	ConfigPath string
	// LevelVar makes telemetry.log_level hot-reloadable.
	LevelVar *slog.LevelVar
	// Adapter overrides the configured PicoClaw adapter (integration tests).
	Adapter picoclaw.Adapter
	// SkipDiscovery disables network-facing discovery (unit tests, air-gapped
	// bootstrap of a local-only cluster).
	SkipDiscovery bool
}

// Quit returns a channel closed when an operator asks the node to leave the
// mesh via the admin API; the daemon exits and the supervisor starts it fresh.
func (n *Node) Quit() <-chan struct{} { return n.quit }

// Halted returns a channel closed when the node's identity has been retired for
// good. The daemon must then exit *successfully*: a supervisor that restarted a
// revoked node would bring back the very identity the mesh was told to refuse,
// and the operator's act would be undone by the unit file.
func (n *Node) Halted() <-chan struct{} { return n.halted }

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

	// The identity-handover ledger is consulted by the trust policy itself, so it
	// must exist before any peer is judged: a node that rotated its key should be
	// recognised as the peer it already was, not as a stranger (ТЗ 11.2 п.3–4).
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

	// The skill registry seeds itself from the operator config: names plus any
	// documentation, and a persisted version clock when configured. Learning a
	// peer's descriptors never adds a skill here — own is what this node serves.
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

	// The policy is passed to the table rather than quoted into it: peer standing
	// is decided in one place, so a neighbour view can never disagree with the
	// admission checks that run on the task path (ТЗ 11.4).
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

	// A revoked identity must not come back, whatever the supervisor does with
	// exit codes. The ledger is local and survives restarts, so it is the one
	// place this can be enforced: if this node's own id retired itself, refuse
	// to start rather than rejoin the mesh holding a statement every peer uses
	// to refuse it.
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
	// Handlers resolve n.Manager lazily: the manager is constructed after the
	// service that dispatches to it.
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

	n.installConnNotifier()
	return n, nil
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

// buildDiscovery prepares the discovery layer. skipDiscovery keeps the node
// fully manual: only peers dialed by the operator (or the tests) are ever
// reachable, which is what a deterministic topology needs. A disabled gossip
// section alone still permits bootstrap/local-registry/DHT discovery.
func (n *Node) buildDiscovery() error {
	cfg := n.Cfg
	// Every constructor below gets this logger: the discovery mechanisms are six
	// separate objects, and an operator reading "peer found" wants to know it came
	// from the registry rather than gossip (ТЗ 14.1).
	dlog := logging.Component(n.log, "discovery")
	if n.skipDiscovery {
		n.log.Info("discovery_disabled_manual_peers_only")
		return nil
	}
	if cfg.Discovery.LocalRegistry {
		rec := discovery.LocalRecord{
			NodeName: cfg.Node.Name,
			Addrs:    n.DiscoverableAddrs(),
			Skills:   cfg.EffectiveSkills(),
		}
		reg, err := discovery.NewLocalRegistry(cfg.Discovery.LocalSocketDir, n.Identity.PeerID(), rec,
			cfg.Discovery.Gossip.FailureTimeout.D()*2, dlog)
		if err != nil {
			n.log.Warn("local_registry_disabled", "err", err.Error())
		} else {
			n.Registry = reg
			probe, perr := discovery.NewProbe(cfg.Discovery.LocalSocketDir, n.Identity.PeerID().String(), dlog)
			if perr != nil {
				n.log.Warn("probe_socket_disabled", "err", perr.Error())
			} else {
				n.Probe = probe
				probe.SetStatus(func() any { return n.Status() })
				reg.SetSocket(probe.Path())
			}
		}
	}

	b, err := discovery.NewBootstrap(n.Host.Underlying(), cfg.Discovery.Bootstrap,
		cfg.Discovery.BootstrapInterval.D(), 10*time.Second, dlog)
	if err != nil {
		return fmt.Errorf("node: bootstrap: %w", err)
	}
	n.Bootstrap = b

	if cfg.Discovery.DHT && n.Host.DHT() != nil {
		n.DHT = discovery.NewDHT(n.Host.DHT(), cfg.EffectiveSkills(), dlog)
	}

	if cfg.Discovery.Gossip.Enabled {
		m, err := discovery.NewMembership(context.Background(), n.Host.Underlying(),
			cfg.Discovery.Gossip, n.Policy, n.Audit, dlog)
		if err != nil {
			return fmt.Errorf("node: membership: %w", err)
		}
		n.Membership = m
		m.SetSelfStateFunc(n.peerState)
		m.SetOnPeer(n.onGossipPeer)
		m.SetOnExpire(n.onPeerExpire)
		m.SetRebindSource(n.rebindsToRepublish, func(k *pb.KeyRebind) { n.acceptRebind(k) })
	} else {
		// Gossip is the only mechanism that learns peers it was not told about,
		// so disabling it makes the mesh strictly explicit: reachability comes
		// from bootstrap entries and manual dials. Everything else — signed
		// capabilities, task routing, result relay — keeps working.
		n.log.Info("gossip_disabled", "hint", "peers are reachable only via discovery.bootstrap or manual dialing")
	}
	return nil
}

// Start runs every component. ctx governs the whole node lifetime: cancelling
// it stops background work, and Stop then tears the node down.
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
	n.Manager.Stop()
	errs = append(errs, n.Adapter.Close(), n.Host.Close(), n.Store.Close(), n.Audit.Close())

	n.log.Info("node_stopped", "uptime", time.Since(n.started).Round(time.Second).String())
	return errors.Join(errs...)
}

// ID exposes the peer id.
func (n *Node) ID() peer.ID { return n.Identity.PeerID() }

// ---------- status ----------

// Status is the JSON shape served by the admin API and the probe socket.
type Status struct {
	NodeName    string    `json:"node_name"`
	PeerID      string    `json:"peer_id"`
	Version     string    `json:"version"`
	ProtocolVer string    `json:"protocol_version"`
	UptimeSec   int64     `json:"uptime_sec"`
	StartedAt   time.Time `json:"started_at"`
	Addrs       []string  `json:"addrs"`
	Skills      []string  `json:"skills"`
	ResourceCls string    `json:"resource_class"`
	Load        float64   `json:"load"`
	Running     int       `json:"running_tasks"`
	Tracked     int       `json:"tracked_tasks"`
	Neighbors   int       `json:"neighbors_total"`
	Connected   int       `json:"neighbors_connected"`
	Membership  int       `json:"membership_known"`
	Bootstrap   struct {
		Configured int `json:"configured"`
		Reachable  int `json:"reachable"`
	} `json:"bootstrap"`
	Discovery struct {
		LocalRegistry bool `json:"local_registry"`
		MDNS          bool `json:"mdns"`
		MDNSFound     int  `json:"mdns_peers_found"`
		DHT           bool `json:"dht"`
		DHTLookups    int  `json:"dht_lookups"`
		Gossip        bool `json:"gossip"`
	} `json:"discovery"`
	Adapter struct {
		Name    string `json:"name"`
		Healthy bool   `json:"healthy"`
		Detail  string `json:"detail,omitempty"`
		// Model is picoclaw.model as configured on this node (ТЗ 10.3): what the
		// node asks its local agent to use. It is not a routing attribute and
		// peers never see it — model selection is not part of the mesh protocol.
		Model string `json:"model,omitempty"`
	} `json:"adapter"`
	Security struct {
		TrustMode        string `json:"trust_mode"`
		RequireTaskSig   bool   `json:"require_task_signature"`
		MinTrustForTasks string `json:"min_trust_for_tasks"`
		RateLimitedPeers int    `json:"rate_limited_peers"`
		AuditLog         string `json:"audit_log,omitempty"`
	} `json:"security"`
	Storage struct {
		Tasks     int64 `json:"tasks"`
		Peers     int64 `json:"peers"`
		DedupKeys int64 `json:"dedup_keys"`
	} `json:"storage"`
}

// Status renders the current node state. It is cheap enough for a 2s poll.
func (n *Node) Status() Status {
	hc, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	st := Status{
		NodeName:    n.Cfg.Node.Name,
		PeerID:      n.Identity.PeerID().String(),
		Version:     version.Version,
		ProtocolVer: version.ProtocolVersion,
		UptimeSec:   int64(time.Since(n.started).Seconds()),
		StartedAt:   n.started,
		Addrs:       n.DiscoverableAddrs(),
		Skills:      n.Cfg.EffectiveSkills(),
		ResourceCls: n.Cfg.Capabilities.ResourceClass,
		Load:        n.Manager.Load(),
		Running:     n.Manager.RunningCount(),
		Tracked:     n.Manager.TrackedCount(),
		Neighbors:   n.Table.Len(),
		Connected:   n.Table.ConnectedCount(),
	}
	if n.Membership != nil {
		st.Membership = n.Membership.Len()
	}
	if n.Bootstrap != nil {
		st.Bootstrap.Configured = n.Bootstrap.Count()
		st.Bootstrap.Reachable = len(n.Bootstrap.Healthy(n.Cfg.Discovery.Gossip.FailureTimeout.D() * 2))
	}
	st.Discovery.LocalRegistry = n.Registry != nil
	st.Discovery.MDNS = n.MDNS != nil
	st.Discovery.DHT = n.DHT != nil && n.DHT.Enabled()
	st.Discovery.Gossip = n.Cfg.Discovery.Gossip.Enabled
	if n.MDNS != nil {
		st.Discovery.MDNSFound = n.MDNS.FoundCount()
	}
	if n.DHT != nil {
		lookups, _, _ := n.DHT.Stats()
		st.Discovery.DHTLookups = int(lookups)
	}
	inf := picoclaw.Describe(hc, n.Adapter)
	st.Adapter.Name = inf.Name
	st.Adapter.Healthy = inf.Healthy
	st.Adapter.Detail = inf.Detail
	st.Adapter.Model = inf.Model
	// Read the live posture, not the config snapshot: a reload changes the
	// policy before (or instead of) anything the process can restart with.
	st.Security.TrustMode = n.Policy.Mode().String()
	st.Security.RequireTaskSig = n.Cfg.Security.RequireTaskSignature
	st.Security.MinTrustForTasks = n.Policy.MinTrustForTasks().String()
	st.Security.RateLimitedPeers = n.Limiter.Size()
	st.Security.AuditLog = n.Audit.Path()
	if s, err := n.Store.Stats(); err == nil {
		st.Storage.Tasks = s.Tasks
		st.Storage.Peers = s.Peers
		st.Storage.DedupKeys = s.DedupKeys
	}
	return st
}

// Capabilities is the signed self-description served over the mesh protocol.
func (n *Node) Capabilities() *pb.Capabilities {
	caps := &pb.Capabilities{
		PeerId:              n.Identity.PeerID().String(),
		NodeName:            n.Cfg.Node.Name,
		Version:             version.Version,
		Skills:              n.Cfg.EffectiveSkills(),
		Models:              append([]string(nil), n.Cfg.Capabilities.Models...),
		MaxParallelTasks:    int32(n.Cfg.Tasks.MaxParallelTasks),
		RunningTasks:        int32(n.Manager.RunningCount()),
		Load:                n.Manager.Load(),
		AcceptExternalTasks: n.Cfg.Capabilities.AcceptExternalTasks,
		AllowShell:          n.Cfg.Capabilities.AllowShell,
		RelayCapable:        true,
		ResourceClass:       n.Cfg.Capabilities.ResourceClass,
		ListenAddrs:         n.DiscoverableAddrs(),
		Timestamp:           time.Now().UTC().Unix(),
		SkillsVersion:       n.Skills.Epoch(),
	}
	if n.Cfg.Capabilities.SkillExchange.Enabled {
		caps.SkillDocs = n.Skills.Descriptors()
	}
	if err := security.NewSigner(n.Identity).SignCaps(caps); err != nil {
		n.log.Error("caps_sign_failed", "err", err.Error())
	}
	return caps
}

func (n *Node) peerState() *pb.PeerState {
	return &pb.PeerState{
		PeerId:           n.Identity.PeerID().String(),
		Timestamp:        time.Now().UTC().Unix(),
		Skills:           n.Cfg.EffectiveSkills(),
		Load:             n.Manager.Load(),
		MaxParallelTasks: int32(n.Cfg.Tasks.MaxParallelTasks),
		Version:          version.Version,
		Status:           "active",
		Addrs:            n.DiscoverableAddrs(),
		SkillsVersion:    n.Skills.Epoch(),
	}
}

// ---------- addresses ----------

// DiscoverableAddrs renders dialable /p2p-terminated multiaddrs: the announce
// list when configured, otherwise the real per-interface listen addresses
// (0.0.0.0 expanded by libp2p itself via InterfaceListenAddresses).
func (n *Node) DiscoverableAddrs() []string {
	ai := peer.AddrInfo{ID: n.Identity.PeerID()}
	if len(n.Cfg.Node.Announce) > 0 {
		for _, a := range n.Cfg.Node.Announce {
			if m, err := ma.NewMultiaddr(a); err == nil {
				ai.Addrs = append(ai.Addrs, m)
			}
		}
	} else {
		iface, err := n.Host.Underlying().Network().InterfaceListenAddresses()
		if err != nil {
			iface = n.Host.Underlying().Addrs()
		}
		ai.Addrs = iface
	}
	p2pAddrs, err := peer.AddrInfoToP2pAddrs(&ai)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(p2pAddrs))
	for _, a := range p2pAddrs {
		out = append(out, a.String())
	}
	return out
}

// LocalAddr is the loopback form of the first listen address, used by the
// local registry so co-located nodes do not route through an external NIC.
func (n *Node) LocalAddr() string {
	iface, err := n.Host.Underlying().Network().InterfaceListenAddresses()
	if err != nil || len(iface) == 0 {
		return ""
	}
	addrs := iface
	if len(n.Cfg.Node.Announce) > 0 {
		if parsed, perr := ma.NewMultiaddr(n.Cfg.Node.Announce[0]); perr == nil {
			addrs = append([]ma.Multiaddr{parsed}, iface...)
		}
	}
	for _, a := range addrs {
		comp := map[string]string{}
		var proto string
		ma.ForEach(a, func(c ma.Component) bool {
			switch c.Protocol().Code {
			case ma.P_IP4, ma.P_IP6:
				proto = "ip"
				comp["ip"] = c.Value()
			case ma.P_TCP:
				comp["tcp"] = c.Value()
			case ma.P_QUIC_V1:
				comp["quic"] = "1"
			case ma.P_UDP:
				comp["udp"] = c.Value()
			}
			return true
		})
		if proto == "" {
			continue
		}
		// Rewrite wildcards and public IPs to loopback for same-host peers.
		host := "127.0.0.1"
		if comp["ip"] != "0.0.0.0" && comp["ip"] != "::" {
			host = comp["ip"]
		}
		if comp["tcp"] != "" {
			return fmt.Sprintf("/ip4/%s/tcp/%s/p2p/%s", host, comp["tcp"], n.Identity.PeerID())
		}
		if comp["udp"] != "" && comp["quic"] == "1" {
			return fmt.Sprintf("/ip4/%s/udp/%s/quic-v1/p2p/%s", host, comp["udp"], n.Identity.PeerID())
		}
	}
	return ""
}

// ---------- discovery glue ----------

func (n *Node) bootstrapAddrInfos() ([]peer.AddrInfo, error) {
	var out []peer.AddrInfo
	for _, raw := range n.Cfg.Discovery.Bootstrap {
		ai, err := discovery.ParseAddrInfo(raw)
		if err != nil {
			return nil, err
		}
		if ai.ID != "" {
			out = append(out, *ai)
		}
	}
	return out, nil
}

func (n *Node) needMorePeers() bool {
	return n.Table.ConnectedCount() < n.Cfg.Neighbors.Target
}

// onDiscovered dials a freshly found peer and records it.
func (n *Node) onDiscovered(ai peer.AddrInfo, category, source string) {
	if ai.ID == "" || ai.ID == n.Identity.PeerID() {
		return
	}
	if !n.Policy.AllowConnection(ai.ID) {
		n.Audit.Log(security.AuditEvent{Event: "discovery_blocked", PeerID: ai.ID.String(), Reason: "blocked", Detail: source})
		return
	}
	existing, known := n.Table.Get(ai.ID)
	if known && existing.Connected {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Host.Connect(ctx, ai); err != nil {
		n.log.Debug("dial_discovered_failed", "peer", ai.ID.String(), "source", source, "err", err.Error())
		return
	}
	n.Table.Upsert(&routing.Neighbor{
		PeerID: ai.ID, Addrs: addrsOf(ai), Category: category, Connected: true,
	})
	switch source {
	case "mdns":
		n.Metrics.DiscoveryMDNS.Inc()
	case "local":
		n.Metrics.DiscoveryLocal.Inc()
	}
	n.refreshCapabilities(ctx, ai.ID)
	n.log.Info("peer_joined", "peer", ai.ID.String(), "via", source, "category", category)
}

func (n *Node) onGossipPeer(ai peer.AddrInfo, st *pb.PeerState) {
	if ai.ID == "" || ai.ID == n.Identity.PeerID() {
		return
	}
	if !n.Policy.AllowConnection(ai.ID) {
		return
	}
	n.Table.Upsert(&routing.Neighbor{
		PeerID: ai.ID, Addrs: addrsOf(ai), Skills: append([]string(nil), st.GetSkills()...),
		Category: routing.CatWAN, Version: st.GetVersion(), Load: st.GetLoad(),
		MaxPar: st.GetMaxParallelTasks(), Connected: n.Host.IsConnected(ai.ID),
		// Gossip is the cheap channel for "my skills changed": the epoch rides
		// on every PeerState, so a documented-skill update is noticed without a
		// capabilities round trip per peer.
		SkillsVersion: st.GetSkillsVersion(),
	})
	if n.Host.IsConnected(ai.ID) || n.Table.ConnectedCount() >= n.Cfg.Neighbors.Max {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := n.Host.Connect(ctx, ai); err == nil {
		n.Table.SetConnected(ai.ID, true)
		n.refreshCapabilities(ctx, ai.ID)
	}
}

func (n *Node) onPeerExpire(pid peer.ID) {
	n.Table.SetConnected(pid, false)
	// A peer we can no longer see cannot be the source of a live skill claim;
	// dropping its descriptors frees the import budget and stops a departed
	// node from lingering in the routing view as an executor.
	n.Skills.DropPeer(pid.String())
	n.log.Debug("peer_expired", "peer", pid.String())
}

// refreshCapabilities fetches and verifies a neighbour's signed capabilities.
func (n *Node) refreshCapabilities(ctx context.Context, pid peer.ID) {
	resp, err := n.Service.RPC(ctx, pid, &pb.RpcRequest{
		Kind: &pb.RpcRequest_Capabilities{Capabilities: &pb.CapabilitiesRequest{}},
	})
	if err != nil {
		n.log.Debug("caps_fetch_failed", "peer", pid.String(), "err", err.Error())
		return
	}
	caps := resp.GetCapabilities().GetCapabilities()
	if caps == nil || caps.GetPeerId() != pid.String() {
		n.Table.RecordProtocolError(pid)
		return
	}
	if err := security.VerifyCaps(caps, n.keyLookup()); err != nil {
		n.Audit.Log(security.AuditEvent{Event: "caps_bad_signature", PeerID: pid.String(), Reason: err.Error()})
		n.Metrics.Security("caps_bad_signature")
		n.Table.RecordProtocolError(pid)
		return
	}
	n.Policy.Observe(pid, security.TrustKnown)
	// Only a verified answer counts as a completed handshake: a fetch that
	// failed or did not verify leaves the peer to be introduced again.
	n.introduceMu.Lock()
	if n.handshaked == nil {
		n.handshaked = make(map[peer.ID]bool)
	}
	n.handshaked[pid] = true
	n.introduceMu.Unlock()
	n.Table.Upsert(&routing.Neighbor{
		PeerID: pid, Skills: append([]string(nil), caps.GetSkills()...),
		MaxPar: caps.GetMaxParallelTasks(), Running: caps.GetRunningTasks(),
		Load: caps.GetLoad(), Version: caps.GetVersion(),
		Connected:     n.Host.IsConnected(pid),
		SkillsVersion: caps.GetSkillsVersion(),
	})
	// The capabilities signature now covers the epoch and the descriptors
	// (wire.CapsBody), so a verified answer is authoritative on its own: import
	// it directly instead of asking again.
	if len(caps.GetSkillDocs()) > 0 {
		if updated, names := n.Skills.ImportPeer(pid.String(), caps.GetSkillDocs()); updated > 0 {
			n.Metrics.SkillsImported.Add(float64(updated))
			n.Table.Upsert(&routing.Neighbor{PeerID: pid, Skills: names, Connected: n.Host.IsConnected(pid)})
			n.log.Debug("skills_imported", "peer", pid.String(), "descriptors", updated)
		}
		return
	}
	// Documented skills were not in the advertisement: fetch just the delta we
	// do not have, if the peer claims to be newer than our view.
	if n.Cfg.Capabilities.SkillExchange.Enabled &&
		n.Skills.PeerEpoch(pid.String(), caps.GetSkillsVersion()) {
		n.syncSkills(pid)
	}
}

// syncSkills asks one peer for the skill descriptors we do not hold (or hold
// older) and folds the signed answer into the local view. An empty or refused
// answer is not an error: disclosure is the peer's policy decision.
func (n *Node) syncSkills(pid peer.ID) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := n.Service.RPC(ctx, pid, &pb.RpcRequest{Kind: &pb.RpcRequest_SkillsSync{
		SkillsSync: &pb.SkillsSyncRequest{Known: n.Skills.KnownVersions(pid.String())},
	}})
	if err != nil {
		n.log.Debug("skills_sync_failed", "peer", pid.String(), "err", err.Error())
		return
	}
	got := resp.GetSkillsSync()
	if got == nil {
		return
	}
	if err := security.VerifySkillsSync(got, pid, n.keyLookup()); err != nil {
		n.Audit.Log(security.AuditEvent{Event: "skills_sync_bad_signature", PeerID: pid.String(), Reason: err.Error()})
		n.Metrics.Security("skills_sync_bad_signature")
		n.Table.RecordProtocolError(pid)
		return
	}
	if updated, names := n.Skills.ImportPeer(pid.String(), got.GetSkills()); updated > 0 {
		n.Metrics.SkillsSynced.Inc()
		n.Metrics.SkillsImported.Add(float64(updated))
		n.Table.Upsert(&routing.Neighbor{PeerID: pid, Skills: names, Connected: n.Host.IsConnected(pid)})
		n.log.Info("skills_refreshed", "peer", pid.String(), "descriptors", updated, "epoch", got.GetSkillsVersion())
	}
}

// reconcileSkills walks the neighbours whose advertised epoch outruns our view
// of them and refreshes those, so a node that changes its documented skills is
// followed without waiting for a task to expose the gap.
func (n *Node) reconcileSkills(ctx context.Context) {
	if !n.Cfg.Capabilities.SkillExchange.Enabled {
		return
	}
	for _, nb := range n.Table.List() {
		if nb.PeerID == n.ID() || !nb.Connected || nb.Left || nb.SkillsVersion <= 0 {
			continue
		}
		if !n.Skills.PeerEpoch(nb.PeerID.String(), nb.SkillsVersion) {
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		n.syncSkills(nb.PeerID)
	}
}

// ---------- skill administration (ТЗ 6.3, 13.1.1) ----------

// SetSkillDoc adds or updates the documentation of one skill this node
// advertises and announces the new epoch, so neighbours pull the revision
// instead of waiting for a task to expose the gap.
//
// It deliberately does not change what this node can execute: skills come from
// the operator config plus the adapter's real capability, and an API call that
// made the mesh believe a new skill appeared here would route work to a node
// that cannot do it. Documenting a skill is metadata, and metadata is bounded by
// the advertised set.
func (n *Node) SetSkillDoc(d skills.Descriptor) (skills.Descriptor, error) {
	if !slices.ContainsFunc(n.Cfg.EffectiveSkills(), func(s string) bool {
		return strings.EqualFold(strings.TrimSpace(s), strings.TrimSpace(d.Name))
	}) {
		return skills.Descriptor{}, fmt.Errorf("node: skill %q is not advertised by this node", d.Name)
	}
	stored, changed := n.Skills.Set(d)
	if !changed {
		return stored, nil
	}
	n.Metrics.SkillsVersion.Set(float64(n.Skills.Epoch()))
	n.announceSkills()
	n.log.Info("skill_documented", "skill", stored.Name, "version", stored.Version, "epoch", n.Skills.Epoch())
	return stored, nil
}

// RemoveSkillDoc drops documentation (not the capability): the skill stays
// advertised by name, only its descriptor is retired and the epoch bumped.
func (n *Node) RemoveSkillDoc(name string) bool {
	n.Skills.DropDoc(name)
	n.Metrics.SkillsVersion.Set(float64(n.Skills.Epoch()))
	n.announceSkills()
	return true
}

// announceSkills republishes this node's state on every channel a skill change
// is visible on: gossip membership and the co-located registry.
func (n *Node) announceSkills() {
	if n.Membership != nil {
		if st := n.peerState(); st != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := n.Membership.PublishState(ctx, st); err != nil {
				n.log.Debug("skill_announce_failed", "err", err.Error())
			}
		}
	}
	if n.Registry != nil {
		_ = n.Registry.UpdateSkills(n.Cfg.EffectiveSkills(), nil)
	}
}

// SyncSkillsNow forces descriptor reconciliation against connected neighbours,
// ignoring the periodic schedule, and returns how many were queried.
func (n *Node) SyncSkillsNow(ctx context.Context) int {
	queried := 0
	for _, nb := range n.Table.List() {
		if nb.PeerID == n.ID() || !nb.Connected || nb.Left {
			continue
		}
		select {
		case <-ctx.Done():
			return queried
		default:
		}
		n.syncSkills(nb.PeerID)
		queried++
	}
	return queried
}

// PeerSkillView is the operator-facing answer to "what do I believe my peers
// can do, and how fresh is that belief" — the two questions a mis-routed task
// usually comes down to.
type PeerSkillView struct {
	PeerID      string              `json:"peer_id"`
	Connected   bool                `json:"connected"`
	Left        bool                `json:"left"`
	Advertised  []string            `json:"advertised_skills,omitempty"`
	Epoch       int64               `json:"skills_version"`
	Descriptors []skills.Descriptor `json:"descriptors,omitempty"`
}

// PeerSkillViews reports the learned skill view of every known peer.
func (n *Node) PeerSkillViews() []PeerSkillView {
	out := make([]PeerSkillView, 0, n.Table.Len())
	for _, nb := range n.Table.List() {
		out = append(out, PeerSkillView{
			PeerID: nb.PeerID.String(), Connected: nb.Connected, Left: nb.Left,
			Advertised: append([]string(nil), nb.Skills...), Epoch: nb.SkillsVersion,
			Descriptors: n.Skills.PeerSkills(nb.PeerID.String()),
		})
	}
	slices.SortFunc(out, func(a, b PeerSkillView) int { return strings.Compare(a.PeerID, b.PeerID) })
	return out
}

// rebindMaxHeld bounds the local handover ledger so a hostile gossip source
// cannot grow our state by flooding statements — each must verify, but the set
// of distinct old ids is unbounded in principle.
const rebindMaxHeld = 4096

// rebindsToRepublish is the gossip source for handover statements. Only recent
// statements are republished: an older one is still true and still resolved
// locally, but pushing it forever would have every recipient log it as too old
// to adopt, and the propagation window that matters is the live one.
func (n *Node) rebindsToRepublish() []*pb.KeyRebind {
	return n.Rebinds.StatementsFresh(rebindRepublishWindow)
}

// rebindRepublishWindow bounds how long a statement keeps being piggybacked on
// heartbeats. It is a transport horizon, not a validity rule.
const rebindRepublishWindow = 12 * time.Hour

// acceptRebind applies one verified identity-handover statement and reports
// whether it took effect, plus why not. It returns a status rather than just
// logging because the same entry point serves gossip (fire-and-forget) and the
// control RPC, whose peer legitimately asks "did you accept this?".
//
// A rotation carries the retiring id's standing over to the successor: the
// neighbour-table entry (metrics, skills, category) moves so a planned key
// change does not reset the relationship the node earned, and the trust policy
// resolves the old id through the ledger on every decision. A revocation
// removes the peer from the view and drops its learned skills — an id retired
// by its own key must stop being trusted everywhere without anyone editing
// files (ТЗ 11.2 п.4).
func (n *Node) acceptRebind(k *pb.KeyRebind) (bool, string) {
	old, err := peer.Decode(k.GetOldPeerId())
	if err != nil {
		return false, "unparsable old peer id"
	}
	if old == n.ID() {
		// Our own handover statement echoed back from a peer: nothing to adopt.
		// The exception worth acting on is a revocation of this very id that we
		// did not issue ourselves — a retired key being reused (the stolen-key
		// case). The mesh already gates that identity everywhere, so the honest
		// outcome is to stop this process rather than let it run as a zombie
		// whose local API still looks healthy. It cannot be forged: only this
		// node's own key signs its revocation, and the key in hand just did.
		if k.GetNewPeerId() == "" {
			// Recorded locally so the restart guard covers this identity even if
			// the key file is put back in place by hand.
			if _, err := n.Rebinds.Apply(k); err != nil {
				n.log.Warn("self_revocation_ledger", "err", err.Error())
			}
			n.log.Error("own_identity_revoked_stopping", "peer", old.String(), "reason", k.GetReason())
			n.Audit.Log(security.AuditEvent{Event: "self_revoked_learned", PeerID: old.String(),
				Reason: k.GetReason()})
			if err := n.retireKeyFile(); err != nil {
				n.log.Error("revocation_key_retirement_failed", "err", err.Error())
			}
			n.haltedOnce.Do(func() { close(n.halted) })
		}
		return true, ""
	}
	if n.Rebinds.Len() >= rebindMaxHeld && !n.Rebinds.HoldsStatement(old) {
		n.log.Warn("rebind_ledger_full", "old", old.String())
		return false, "ledger full"
	}
	applied, err := n.Rebinds.Apply(k)
	if err != nil {
		n.Audit.Log(security.AuditEvent{Event: "rebind_rejected", PeerID: old.String(),
			Reason: err.Error()})
		n.Metrics.Security("rebind_rejected")
		return false, err.Error()
	}
	if !applied {
		return false, "older sequence already held" // stale replay
	}
	n.Metrics.RebindsApplied.Inc()
	if k.GetNewPeerId() == "" {
		n.revokePeer(old)
		n.log.Info("peer_revoked", "peer", old.String(), "reason", k.GetReason())
		return true, ""
	}
	n.rotatePeer(old, k)
	return true, ""
}

// rotatePeer migrates state from the retired id to its successor and announces
// the successor under this node's own observation, so the handover spreads.
func (n *Node) rotatePeer(old peer.ID, k *pb.KeyRebind) {
	nid, err := peer.Decode(k.GetNewPeerId())
	if err != nil {
		return
	}
	// Inherit the neighbour record wholesale — scores, skills, capacity — then
	// keep the connection truth: the new id has not been dialled yet.
	if prev, ok := n.Table.Get(old); ok {
		prev.PeerID = nid
		prev.Connected = n.Host.IsConnected(nid)
		prev.LastSeen = time.Now().UTC()
		n.Table.Upsert(&prev)
		// The retired identity no longer exists as far as routing is concerned;
		// leaving it behind would let Select keep preferring a dead id.
		n.Table.Remove(old)
	}
	// Carry the learned skill view across the rotation and credit the successor
	// with the predecessor's trust: the same operator, a new key.
	if docs := n.Skills.PeerSkills(old.String()); len(docs) > 0 {
		if updated, names := n.Skills.ImportPeer(nid.String(), descriptorsToProto(docs)); updated > 0 {
			if nb, ok := n.Table.Get(nid); ok {
				n.Table.Upsert(&routing.Neighbor{PeerID: nid, Skills: names, Connected: nb.Connected})
			}
		}
		n.Skills.DropPeer(old.String())
	}
	n.Policy.Observe(nid, n.Policy.TrustOf(old))
	// A bootstrap entry is addressed by identity ("/…/p2p/<old>"), so after the
	// handover it would dial an address whose peer fails the Noise handshake.
	// The address is still correct — only the id part went stale.
	if n.Bootstrap != nil && n.Bootstrap.Rename(old, nid) {
		n.log.Info("bootstrap_entry_renamed", "old", old.String(), "new", nid.String(),
			"hint", "update discovery.bootstrap in the config file when convenient")
	}
	n.Audit.Log(security.AuditEvent{Event: "peer_rebound", PeerID: old.String(),
		Reason: fmt.Sprintf("-> %s (%s)", nid.String(), k.GetReason())})
	n.Metrics.Security("peer_rebound")
	n.log.Info("peer_rebound", "old", old.String(), "new", nid.String(), "reason", k.GetReason())
}

// revokePeer removes a retired identity from every local view.
func (n *Node) revokePeer(old peer.ID) {
	// Tasks that were waiting on this peer must not linger until their timeout:
	// the identity is gone by declaration, which is stronger information than a
	// dropped connection.
	n.Manager.OnPeerDisconnected(old)
	n.Table.Remove(old)
	n.Skills.DropPeer(old.String())
	n.Policy.SetListed(old, false) // operator-grade block, survives restarts in-memory
	n.Audit.Log(security.AuditEvent{Event: "peer_revoked", PeerID: old.String(), Reason: "revoked by own key"})
	n.Metrics.Security("peer_revoked")
}

// descriptorsToProto re-renders learned descriptors for an import under a new
// peer key (import re-verifies digests, so this is a format conversion only).
func descriptorsToProto(in []skills.Descriptor) []*pb.SkillDescriptor {
	out := make([]*pb.SkillDescriptor, 0, len(in))
	for _, d := range in {
		out = append(out, d.ToProto())
	}
	return out
}

// PublishRebind records a locally produced handover statement and announces it
// immediately on every channel, so the mesh learns of a rotation before the
// next heartbeat would have carried it.
func (n *Node) PublishRebind(k *pb.KeyRebind) error {
	applied, err := n.Rebinds.Apply(k)
	if err != nil {
		return err
	}
	if applied {
		n.Metrics.RebindsApplied.Inc()
	}
	if n.Membership != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Publishing our own state piggybacks the whole held ledger (the
		// rebindSource callback), which is also how the statement propagates.
		if st := n.peerState(); st != nil {
			if err := n.Membership.PublishState(ctx, st); err != nil {
				n.log.Debug("rebind_announce_failed", "err", err.Error())
			}
		}
	}
	return nil
}

// RotateKeyResult describes a completed identity rotation.
type RotateKeyResult struct {
	OldPeerID peer.ID       `json:"old_peer_id"`
	NewPeerID peer.ID       `json:"new_peer_id"`
	Rebind    *pb.KeyRebind `json:"-"`
	// AnnouncedTo lists the peers the statement was pushed to directly (the
	// gossip carrier is best-effort, so the operator wants to know who heard it
	// synchronously).
	AnnouncedTo []string `json:"announced_to,omitempty"`
	Restart     bool     `json:"restart_required"`
}

// RotateKey replaces this node's identity key with a freshly generated one and
// announces the handover to the mesh (ТЗ 11.2 п.3).
//
// The rotation is self-certifying: the statement is signed by the old key and
// the new key over the same canonical body, so a peer that never met the new
// key can still accept it, and no operator has to edit an allow list on every
// node. Ordering matters for exactly that reason — the handover must be spread
// while the old key is still the live one. If the process died after writing
// the new key but before the announcement, peers would see a stranger rather
// than the successor of a node they trusted. So: build and verify the
// statement, publish it (ledger + gossip + direct RPC), and only then install
// the new key file. The caller restarts the process; the config file still
// points at the same path, so no YAML edit is needed.
//
// In-flight work is the operator's decision, not this function's: tasks this
// node executes finish under the old identity (the signed result is already
// bound to it), and originators see the new id on subsequent hops.
func (n *Node) RotateKey(ctx context.Context, reason string) (*RotateKeyResult, error) {
	if reason == "" {
		reason = "rotation"
	}
	old := n.Identity
	fresh, err := security.NewEphemeral()
	if err != nil {
		return nil, fmt.Errorf("node: generate new identity: %w", err)
	}
	k, err := security.IssueRebind(old, fresh, reason, n.Rebinds.NextSequence(old.PeerID()))
	if err != nil {
		return nil, fmt.Errorf("node: build rebind statement: %w", err)
	}

	// Announce first, from the old identity — see the ordering note above.
	res := &RotateKeyResult{OldPeerID: old.PeerID(), NewPeerID: fresh.PeerID(), Rebind: k, Restart: true}
	if err := n.PublishRebind(k); err != nil {
		return nil, fmt.Errorf("node: record rebind statement: %w", err)
	}
	res.AnnouncedTo = n.pushRebind(ctx, k)

	// Now move the identity on disk. Our own ledger already holds the
	// statement, so after the restart the node recognises its predecessor and
	// keeps the trust it had earned rather than relearning from zero.
	if err := fresh.Save(n.Cfg.Identity.KeyFile); err != nil {
		return nil, fmt.Errorf("node: install new key: %w", err)
	}
	n.log.Info("identity_rotated", "old", res.OldPeerID.String(), "new", res.NewPeerID.String(),
		"reason", reason, "announced", len(res.AnnouncedTo))
	return res, nil
}

// pushRebind sends the statement directly to every connected peer. Gossip
// carries it epidemically, but a direct push guarantees the neighbours that
// already route work here — the ones whose in-flight delegation would otherwise
// break — hear about the handover immediately, and it is the only channel in a
// mesh running with gossip disabled.
func (n *Node) pushRebind(ctx context.Context, k *pb.KeyRebind) []string {
	var sent []string
	for _, nb := range n.Table.List() {
		if nb.PeerID == n.ID() || !nb.Connected || nb.Left {
			continue
		}
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := n.Service.RPC(dctx, nb.PeerID, &pb.RpcRequest{Kind: &pb.RpcRequest_Rebind{
			Rebind: &pb.RebindRequest{Rebind: k},
		}})
		cancel()
		if err != nil {
			n.log.Debug("rebind_push_failed", "peer", nb.PeerID.String(), "err", err.Error())
			continue
		}
		if r := resp.GetRebind(); r != nil && r.GetAccepted() {
			sent = append(sent, nb.PeerID.String())
		}
	}
	return sent
}

// RevokeSelf retires this node's identity for good (ТЗ 11.2 п.4): it signs the
// revocation with the key being retired — the only authority that has — spreads
// it, and then asks the process to stop and stay stopped.
//
// Unlike a rotation this is not a handover, so there is no successor to keep
// routing to: the point is that every other node starts refusing this
// identifier, which is what an operator needs after a key leak. The ledger on
// this node keeps the statement too, so the retired id cannot be revived by a
// restart with the same key file.
func (n *Node) RevokeSelf(ctx context.Context, reason string) error {
	if reason == "" {
		reason = "retire"
	}
	k, err := security.IssueRevocation(n.Identity, reason, n.Rebinds.NextSequence(n.ID()))
	if err != nil {
		return fmt.Errorf("node: build revocation: %w", err)
	}
	if err := n.PublishRebind(k); err != nil {
		return fmt.Errorf("node: record revocation: %w", err)
	}
	pushed := n.pushRebind(ctx, k)
	n.log.Warn("identity_revoked", "peer", n.ID().String(), "reason", reason, "announced", len(pushed))
	// A revocation that the supervisor can undo is not a revocation. Restart
	// policies ignore exit codes (`restart: unless-stopped` in particular), so
	// signalling "stay down" is not enough — the key material has to leave the
	// path the config points at. It is moved aside rather than destroyed: after
	// a leak the private key is still evidence, and the operator may legitimately
	// un-revoke a node they retired by mistake. The suffix is random because the
	// destination sits in a directory the node user can write, and a predictable
	// name there is a file-clobber primitive.
	if err := n.retireKeyFile(); err != nil {
		n.log.Error("revocation_key_retirement_failed", "err", err.Error(),
			"hint", "remove "+n.Cfg.Identity.KeyFile+" manually before the next start")
	}
	n.haltedOnce.Do(func() { close(n.halted) })
	return nil
}

// retireKeyFile moves the retired node's key file out of the way and reports the
// new location.
func (n *Node) retireKeyFile() error {
	path := n.Cfg.Identity.KeyFile
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("node: stat key file: %w", err)
	}
	// CreateTemp picks a unique name (O_EXCL, unpredictable), so the destination
	// cannot be raced or pre-planted by another writer in the key directory.
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, filepath.Base(path)+".revoked-*")
	if err != nil {
		return fmt.Errorf("node: name key archive: %w", err)
	}
	dest := f.Name()
	_ = f.Close()
	if err := os.Rename(path, dest); err != nil {
		_ = os.Remove(dest)
		return fmt.Errorf("node: retire key file: %w", err)
	}
	n.log.Warn("key_file_retired", "from", path, "to", dest,
		"hint", "the node cannot start again until identity.key_file points at a live key")
	return nil
}

// ShareRebinds pushes held handover statements directly to a peer over the
// control RPC. Gossip is the primary carrier, but a mesh that runs with
// discovery.gossip.enabled=false still needs a way to spread rotations, and a
// freshly joined node should be told what the network already retired.
func (n *Node) ShareRebinds(ctx context.Context, pid peer.ID) int {
	sent := 0
	for _, k := range n.Rebinds.Statements() {
		resp, err := n.Service.RPC(ctx, pid, &pb.RpcRequest{Kind: &pb.RpcRequest_Rebind{
			Rebind: &pb.RebindRequest{Rebind: k},
		}})
		if err != nil {
			n.log.Debug("rebind_share_failed", "peer", pid.String(), "err", err.Error())
			return sent
		}
		if r := resp.GetRebind(); r != nil && r.GetAccepted() {
			sent++
		}
	}
	return sent
}

func (n *Node) keyLookup() security.KeyLookup {
	h := n.Host.Underlying()
	return func(p peer.ID) (ic.PubKey, error) {
		pk := h.Peerstore().PubKey(p)
		if pk == nil {
			return nil, fmt.Errorf("node: no pubkey cached for %s", p)
		}
		return pk, nil
	}
}

// ---------- connection bookkeeping ----------

// installConnNotifier keeps Connected flags honest and absorbs addresses
// learned during connection setup.
func (n *Node) installConnNotifier() {
	bundle := &network.NotifyBundle{
		ConnectedF: func(_ network.Network, conn network.Conn) {
			pid := conn.RemotePeer()
			// A socket opening is not a judgement: the table holds no trust, and the
			// peer's standing comes from the policy when something asks for it.
			n.Table.Upsert(&routing.Neighbor{
				PeerID: pid, Addrs: []string{conn.RemoteMultiaddr().String()},
				Connected: true, Category: n.classify(conn),
			})
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
			n.Metrics.PeersTotal.Set(float64(n.Table.Len()))
			go n.introduce(pid)
		},
		DisconnectedF: func(_ network.Network, conn network.Conn) {
			n.Table.SetConnected(conn.RemotePeer(), false)
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
			if n.Manager != nil {
				n.Manager.OnPeerDisconnected(conn.RemotePeer())
			}
		},
	}
	n.notifee = bundle
	n.Host.Underlying().Network().Notify(bundle)
}

// introduce completes the capability handshake for a peer whose connection we
// did not dial ourselves. Without it a passively-connected node stays blind:
// it has no signed skill list and, under the limited posture, no trust
// elevation for the peer — so it can never delegate work back the way the
// task arrived. One fetch per peer is enough; gossip keeps it refreshed.
//
// "Enough" is measured per process, not per neighbour table entry: the table is
// persisted and the trust a handshake earns is not. Skipping the handshake
// because a *restored* record already lists skills left the policy with nothing
// observed for that peer, so every neighbour's tasks were refused as
// "peer not trusted for tasks" after a restart while the peer list still read
// "trusted" — the two views disagreed and only the wrong one was visible.
func (n *Node) introduce(pid peer.ID) {
	if pid == "" || pid == n.Identity.PeerID() {
		return
	}
	n.introduceMu.Lock()
	if n.handshaked[pid] || n.introducing[pid] {
		n.introduceMu.Unlock()
		return
	}
	if n.introducing == nil {
		n.introducing = make(map[peer.ID]bool)
	}
	n.introducing[pid] = true
	n.introduceMu.Unlock()
	defer func() {
		n.introduceMu.Lock()
		delete(n.introducing, pid)
		n.introduceMu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	n.refreshCapabilities(ctx, pid)

	// Handover statements ride along once the peer is known. Gossip carries them
	// too, but a mesh with discovery.gossip.enabled=false has no epidemic
	// channel at all: without this a node that joins after a rotation would keep
	// treating the successor as a stranger (and, worse, would happily accept a
	// key the rest of the network retired more than a republication window ago).
	if n.completedHandshake(pid) {
		sctx, scancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer scancel()
		if sent := n.ShareRebinds(sctx, pid); sent > 0 {
			n.log.Debug("rebinds_shared", "peer", pid.String(), "statements", sent)
		}
	}
}

// completedHandshake reports whether this process has verified a peer's
// capabilities (the condition that also earns it observed trust).
func (n *Node) completedHandshake(pid peer.ID) bool {
	n.introduceMu.Lock()
	defer n.introduceMu.Unlock()
	return n.handshaked[pid]
}

// classify labels a connection's locality: same host, same LAN, or WAN.
func (n *Node) classify(conn network.Conn) string {
	r4 := firstStringComponent(conn.RemoteMultiaddr(), ma.P_IP4)
	if r4 == "" {
		return routing.CatWAN
	}
	if r4 == "127.0.0.1" {
		return routing.CatLocal
	}
	var l4 string
	for _, a := range n.Host.Underlying().Addrs() {
		if v := firstStringComponent(a, ma.P_IP4); v != "" && v != "0.0.0.0" {
			l4 = v
			break
		}
	}
	if l4 != "" && samePrivateSubnet(l4, r4) {
		return routing.CatLAN
	}
	return routing.CatWAN
}

func firstStringComponent(m ma.Multiaddr, code int) string {
	if m == nil {
		return ""
	}
	var out string
	ma.ForEach(m, func(c ma.Component) bool {
		if c.Protocol().Code == code {
			out = c.Value()
			return false
		}
		return true
	})
	return out
}

// samePrivateSubnet compares /24 prefixes for the common RFC1918 case. It is
// a heuristic for the routing category label, not an access-control decision.
func samePrivateSubnet(a, b string) bool {
	if a == b {
		return true
	}
	if !privateIPv4(a) || !privateIPv4(b) {
		return false
	}
	trim := func(s string) string {
		for i, seen := 0, 0; i < len(s); i++ {
			if s[i] == '.' {
				seen++
				if seen == 3 {
					return s[:i]
				}
			}
		}
		return s
	}
	return trim(a) == trim(b)
}

func privateIPv4(ip string) bool {
	switch {
	case strings.HasPrefix(ip, "10."), strings.HasPrefix(ip, "192.168."):
		return true
	case strings.HasPrefix(ip, "172.16."), strings.HasPrefix(ip, "172.17."),
		strings.HasPrefix(ip, "172.18."), strings.HasPrefix(ip, "172.19."),
		strings.HasPrefix(ip, "172.2"), strings.HasPrefix(ip, "172.30."),
		strings.HasPrefix(ip, "172.31."):
		return true
	default:
		return false
	}
}

// ---------- maintenance ----------

// maintenance is the node's slow loop: local-registry sync, full gossip sync,
// peer exchange when thin, stale pruning and metric refresh.
func (n *Node) maintenance(ctx context.Context) {
	beat := n.Cfg.Discovery.Gossip.Heartbeat.D()
	t := time.NewTicker(beat)
	full := time.NewTicker(n.Cfg.Discovery.Gossip.FullSync.D())
	local := time.NewTicker(5 * time.Second)
	rtt := time.NewTicker(15 * time.Second)
	skillEvery := n.Cfg.Capabilities.SkillExchange.Interval.D()
	if skillEvery <= 0 {
		skillEvery = 30 * time.Second
	}
	skillTick := time.NewTicker(skillEvery)
	defer t.Stop()
	defer full.Stop()
	defer local.Stop()
	defer rtt.Stop()
	defer skillTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-local.C:
			n.syncLocalRegistry(ctx)
			n.syncDHT(ctx)
		case <-rtt.C:
			n.measureRTT(ctx)
		case <-skillTick.C:
			n.reconcileSkills(ctx)
			n.Metrics.SkillsVersion.Set(float64(n.Skills.Epoch()))
		case <-t.C:
			if n.Table.ConnectedCount() < n.Cfg.Neighbors.Min && n.Cfg.Discovery.PeerExchange {
				n.requestPeerExchange(ctx)
			}
			for _, gone := range n.Table.PruneStale(n.Cfg.Discovery.Gossip.FailureTimeout.D() * 3) {
				if n.Membership != nil {
					n.Membership.MarkLeft(gone)
				}
				n.Skills.DropPeer(gone.String())
				n.log.Debug("neighbor_dropped", "peer", gone.String())
			}
			n.Metrics.PeersTotal.Set(float64(n.Table.Len()))
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
		case <-full.C:
			if n.Membership == nil {
				continue
			}
			if err := n.Membership.PublishFull(ctx); err != nil {
				n.log.Debug("gossip_full_sync", "err", err.Error())
			}
		}
	}
}

// syncLocalRegistry dials co-located nodes announced in the registry.
func (n *Node) syncLocalRegistry(ctx context.Context) {
	if n.Registry == nil {
		return
	}
	recs, err := n.Registry.List()
	if err != nil {
		n.log.Debug("local_registry_list_failed", "err", err.Error())
		return
	}
	for _, r := range recs {
		if n.Table.ConnectedCount() >= n.Cfg.Neighbors.Max {
			return
		}
		pid, err := peer.Decode(r.PeerID)
		if err != nil || pid == n.Identity.PeerID() {
			continue
		}
		if nb, ok := n.Table.Get(pid); ok && nb.Connected {
			continue
		}
		ai := peer.AddrInfo{ID: pid}
		for _, a := range r.Addrs {
			if parsed, perr := discovery.ParseAddrInfo(a); perr == nil {
				ai.Addrs = append(ai.Addrs, parsed.Addrs...)
			}
		}
		if len(ai.Addrs) == 0 {
			continue
		}
		n.Table.Upsert(&routing.Neighbor{PeerID: pid, Addrs: addrsOf(ai), Category: routing.CatLocal, Skills: r.Skills})
		n.onDiscovered(ai, routing.CatLocal, "local")
		_ = ctx
	}
}

// syncDHT looks up this node's skills in the DHT and dials unseen providers.
func (n *Node) syncDHT(ctx context.Context) {
	if n.DHT == nil || !n.DHT.Enabled() || !n.Cfg.Discovery.DHT {
		return
	}
	if n.Table.ConnectedCount() >= n.Cfg.Neighbors.Target {
		return
	}
	peers, err := n.DHT.FindPeersBySkill(ctx, n.Cfg.EffectiveSkills()[:min(2, len(n.Cfg.EffectiveSkills()))], 20)
	if err != nil {
		n.log.Debug("dht_lookup_failed", "err", err.Error())
		return
	}
	for _, ai := range peers {
		if nb, ok := n.Table.Get(ai.ID); ok && nb.Connected {
			continue
		}
		n.onDiscovered(ai, routing.CatWAN, "dht")
	}
}

// requestPeerExchange asks up to two neighbours for more peers.
func (n *Node) requestPeerExchange(ctx context.Context) {
	for _, nb := range n.Table.Sample(2) {
		if !nb.Connected {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 8*time.Second)
		resp, err := n.Service.RPC(pctx, nb.PeerID, &pb.RpcRequest{
			Kind: &pb.RpcRequest_PeerExchange{PeerExchange: &pb.PeerExchangeRequest{Count: 16}},
		})
		cancel()
		if err != nil || resp == nil {
			continue
		}
		for _, rec := range resp.GetPeerExchange().GetPeers() {
			pid, err := peer.Decode(rec.GetPeerId())
			if err != nil || pid == n.Identity.PeerID() || pid == nb.PeerID {
				continue
			}
			if !n.Policy.AllowConnection(pid) {
				continue
			}
			if existing, ok := n.Table.Get(pid); ok && existing.Connected {
				continue
			}
			ai := peer.AddrInfo{ID: pid}
			for _, a := range rec.GetAddrs() {
				if parsed, perr := discovery.ParseAddrInfo(a); perr == nil {
					ai.Addrs = append(ai.Addrs, parsed.Addrs...)
				}
			}
			if len(ai.Addrs) == 0 {
				continue
			}
			n.Table.Upsert(&routing.Neighbor{PeerID: pid, Addrs: addrsOf(ai), Skills: rec.GetSkills(), Category: routing.CatWAN})
			n.log.Debug("pex_candidate", "peer", pid.String(), "from", nb.PeerID.String())
		}
	}
}

// measureRTT pings a bounded sample of connected neighbours so the routing
// score's latency term (ТЗ 6.9.2) reflects reality: an unmeasured peer keeps
// the neutral 0.5 and a stale average decays only as new samples arrive.
func (n *Node) measureRTT(ctx context.Context) {
	for _, nb := range n.Table.Sample(8) {
		if !nb.Connected {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		start := time.Now()
		resp, err := n.Service.RPC(pctx, nb.PeerID, &pb.RpcRequest{
			Kind: &pb.RpcRequest_Ping{Ping: &pb.PingRequest{Nonce: time.Now().UnixNano()}},
		})
		elapsed := time.Since(start)
		cancel()
		// A failed ping is not a task-execution failure: it must not drain the
		// peer's success-rate term, it just means we keep the old estimate.
		if err != nil || resp == nil || resp.GetPing() == nil {
			continue
		}
		n.Table.RecordRTT(nb.PeerID, elapsed)
	}
}

// ReloadConfig re-reads the configuration file the node started with and
// applies the section that live objects own (ТЗ 13.1.1 reload). The config
// struct itself is left untouched — components that read it without
// synchronisation keep a consistent startup snapshot — while the trust
// policy, the rate limiter and the log level are swapped through their own
// locks. The diff says honestly which sections need a restart.
func (n *Node) ReloadConfig() (config.ReloadDiff, error) {
	if n.configPath == "" {
		return config.ReloadDiff{}, errors.New("node: no config file to reload (started without -config)")
	}
	next, err := config.Load(n.configPath)
	if err != nil {
		return config.ReloadDiff{}, err
	}
	d := n.Cfg.Diff(next)

	mode, err := security.ParseMode(next.Security.TrustMode)
	if err != nil {
		return d, err
	}
	minTrust, err := security.ParseTrust(next.Security.MinTrustForTasks)
	if err != nil {
		return d, err
	}
	n.Policy.SetPosture(mode, minTrust)
	// List files merge in: entries added since startup (or edited in place)
	// take effect; existing entries are never silently dropped.
	if _, err := n.Policy.LoadPeerFile(next.Security.AllowedPeersFile, true); err != nil {
		n.log.Warn("reload_allowed_peers", "err", err.Error())
	}
	if _, err := n.Policy.LoadPeerFile(next.Security.BlockedPeersFile, false); err != nil {
		n.log.Warn("reload_blocked_peers", "err", err.Error())
	}
	if rps := next.Security.RateLimit.RequestsPerSecond; rps > 0 {
		n.Limiter.SetRate(rps, next.Security.RateLimit.Burst)
	}
	if n.levelVar != nil {
		n.levelVar.Set(logging.Level(next.Telemetry.LogLevel))
	}
	n.log.Info("config_reloaded", "hot", len(d.Hot), "requires_restart", len(d.RequiresRestart))
	if n.Audit != nil {
		n.Audit.Log(security.AuditEvent{Event: "config_reload", Detail: fmt.Sprintf("hot=%v restart=%v", d.Hot, d.RequiresRestart)})
	}
	return d, nil
}

// Leave announces departure and asks the daemon to exit (ТЗ 13.1.1
// /admin/leave). The supervisor (systemd Restart=on-failure + ExitCode=75,
// docker restart policy) starts a fresh process; the mesh learns of the
// departure from the announcement instead of a timeout.
func (n *Node) Leave(ctx context.Context) {
	if n.Membership != nil {
		if st := n.peerState(); st != nil {
			st.Status = "left"
			pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
			if err := n.Membership.PublishState(pctx, st); err != nil {
				n.log.Warn("leave_announce_failed", "err", err.Error())
			}
			pcancel()
		}
	}
	n.quitOnce.Do(func() { close(n.quit) })
}

// ---------- helpers ----------

func addrsOf(ai peer.AddrInfo) []string {
	out := make([]string, 0, len(ai.Addrs))
	for _, a := range ai.Addrs {
		out = append(out, a.String())
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------- composite skill source ----------

// skillSource is the tasks.SkillSource the manager queries: it folds gossip
// membership, DHT provider records and the neighbour table into one view and
// can reload all of them on demand (a "full refresh").
//
// Why a composite instead of the raw *discovery.Membership: Select() requires an
// exact skill match, so a peer whose capabilities were never fetched scores
// zero and is invisible. When a task needs an executor nobody nearby has, the
// only way out is to widen the view — which means pulling in DHT providers and
// peers learned from peer-exchange, not just gossip.
type skillSource struct {
	n *Node
}

func (n *Node) skillsSource() tasks.SkillSource { return &skillSource{n: n} }

// PeersBySkill returns every known peer that covers want, from any source.
func (s *skillSource) PeersBySkill(want []string) []peer.ID {
	seen := make(map[peer.ID]bool)
	out := make([]peer.ID, 0, 8)
	add := func(pid peer.ID) {
		if pid == "" || pid == s.n.Identity.PeerID() || seen[pid] {
			return
		}
		seen[pid] = true
		out = append(out, pid)
	}
	if s.n.Membership != nil {
		for _, pid := range s.n.Membership.PeersBySkill(want) {
			add(pid)
		}
	}
	// The table also holds peers gossip never mentioned (bootstraps, DHT dials,
	// peer-exchange records) whose skills were confirmed by a capabilities RPC.
	for _, c := range s.n.Table.Select(want, nil, s.n.Identity.PeerID(), 0, nil) {
		add(c.Neighbor.PeerID)
	}
	return out
}

// State resolves a peer's last advertised state across sources.
func (s *skillSource) State(pid peer.ID) (*pb.PeerState, bool) {
	if s.n.Membership != nil {
		if st, ok := s.n.Membership.State(pid); ok {
			return st, true
		}
	}
	if nb, ok := s.n.Table.Get(pid); ok && len(nb.Skills) > 0 {
		return &pb.PeerState{
			PeerId:           pid.String(),
			Timestamp:        nb.LastSeen.Unix(),
			Skills:           append([]string(nil), nb.Skills...),
			Load:             nb.Load,
			MaxParallelTasks: nb.MaxPar,
			Version:          nb.Version,
			Addrs:            append([]string(nil), nb.Addrs...),
		}, true
	}
	return nil, false
}

// Refresh reloads the whole skill view and folds the result into the neighbour
// table, so a subsequent Table.Select can actually pick the newly learned peers.
//
// Order matters: the DHT lookup is the only source that can be re-queried
// synchronously, so it runs first and its completeness decides the return value.
// Gossip is refreshed afterwards (it can only re-broadcast and wait), and peers
// learned from either source are dialed and have their signed capabilities
// fetched — that is what turns "someone claims X has this skill" into "X
// actually advertises this skill", which is the precondition for Select.
func (s *skillSource) Refresh(ctx context.Context, want []string) (int, bool) {
	if len(want) == 0 {
		return 0, false
	}
	rctx, cancel := context.WithTimeout(ctx, s.refreshBudget())
	defer cancel()

	dhtComplete := false
	if s.n.DHT != nil && s.n.DHT.Enabled() {
		providers, ok := s.n.DHT.Providers(rctx, want)
		dhtComplete = ok
		for _, ai := range providers {
			s.n.onDiscovered(ai, routing.CatWAN, "skill_relay")
		}
	}

	gossipWait := time.Duration(0)
	if s.n.Membership != nil {
		gossipWait = s.refreshBudget() / 2
		if dhtComplete {
			gossipWait = s.n.Cfg.Discovery.Gossip.Heartbeat.D()
		}
		s.n.Membership.Refresh(rctx, want, gossipWait)
	}

	// Every candidate the widened view produced must be reachable and must have
	// confirmed its skills, otherwise Select still cannot see it.
	matched := s.PeersBySkill(want)
	for _, pid := range matched {
		if !s.n.Host.IsConnected(pid) {
			if st, ok := s.State(pid); ok {
				ai := peer.AddrInfo{ID: pid}
				for _, a := range st.GetAddrs() {
					if m, err := ma.NewMultiaddr(a); err == nil {
						ai.Addrs = append(ai.Addrs, m)
					}
				}
				if len(ai.Addrs) == 0 {
					continue
				}
				if err := s.n.Host.Connect(rctx, ai); err != nil {
					continue
				}
				s.n.Table.SetConnected(pid, true)
			}
		}
		if nb, ok := s.n.Table.Get(pid); !ok || len(nb.Skills) == 0 {
			s.n.refreshCapabilities(rctx, pid)
		}
	}

	found := len(s.PeersBySkill(want))
	// Never claim completeness on a partial network view: that would let the
	// caller conclude "no executor exists" from a lookup that simply did not
	// reach far enough.
	return found, dhtComplete && s.n.Table.ConnectedCount() >= s.n.Cfg.Neighbors.Min
}

// Adopt folds peer records returned by a search relay into the neighbour table.
//
// A relay answer is a *claim* made by whoever answered the lookup, so it is not
// trusted: each record is dialed and, once connected, its signed capabilities
// are fetched (refreshCapabilities verifies the signature and only then writes
// the skills). A peer we cannot reach contributes nothing. Returns how many
// peers ended up connected with confirmed skills.
func (s *skillSource) Adopt(ctx context.Context, recs []*pb.PeerRecord) int {
	adopted := 0
	for _, r := range recs {
		if r.GetPeerId() == "" || r.GetPeerId() == s.n.Identity.PeerID().String() {
			continue
		}
		pid, err := peer.Decode(r.GetPeerId())
		if err != nil {
			continue
		}
		if !s.n.Policy.AllowConnection(pid) {
			s.n.Audit.Log(security.AuditEvent{
				Event: "search_relay_blocked", PeerID: pid.String(), Reason: "blocked", Detail: "search_relay",
			})
			continue
		}
		ai := peer.AddrInfo{ID: pid}
		for _, a := range r.GetAddrs() {
			if m, err := ma.NewMultiaddr(a); err == nil {
				ai.Addrs = append(ai.Addrs, m)
			}
		}
		// Seed the table with the advertised skills as a *hint* (so we have an
		// address to dial), then confirm them with a signed capabilities RPC.
		if len(ai.Addrs) > 0 {
			s.n.Table.Upsert(&routing.Neighbor{
				PeerID: pid, Addrs: addrsOf(ai), Category: routing.CatWAN,
				Connected: s.n.Host.IsConnected(pid),
			})
		}
		if !s.n.Host.IsConnected(pid) {
			if len(ai.Addrs) == 0 {
				continue
			}
			dctx, cancel := context.WithTimeout(ctx, 6*time.Second)
			derr := s.n.Host.Connect(dctx, ai)
			cancel()
			if derr != nil {
				continue
			}
			s.n.Table.SetConnected(pid, true)
		}
		before := s.tableSkills(pid)
		s.n.refreshCapabilities(ctx, pid)
		after := s.tableSkills(pid)
		if len(after) > 0 && (len(before) == 0 || !sameSkills(before, after)) {
			adopted++
		}
	}
	return adopted
}

// tableSkills reads the confirmed skill set for a peer from the neighbour table.
func (s *skillSource) tableSkills(pid peer.ID) []string {
	if nb, ok := s.n.Table.Get(pid); ok {
		return nb.Skills
	}
	return nil
}

func sameSkills(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, x := range a {
		set[x] = true
	}
	for _, y := range b {
		if !set[y] {
			return false
		}
	}
	return true
}

// refreshBudget bounds one Refresh: it must fit inside the task deadline that
// triggered it and stay below the RPC stream timeout of the peers we fan out to.
func (s *skillSource) refreshBudget() time.Duration {
	budget := s.n.Cfg.Tasks.Forwarding.SearchRelay.RequestTimeout.D()
	if budget <= 0 {
		budget = 8 * time.Second
	}
	if stream := s.n.Service.Timeout(); stream > 0 && budget >= stream {
		budget = stream / 2
	}
	return budget
}
