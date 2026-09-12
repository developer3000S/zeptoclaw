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
	"strings"
	"sync"
	"time"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/zeptoclaw/zeptomesh/internal/config"
	"github.com/zeptoclaw/zeptomesh/internal/discovery"
	"github.com/zeptoclaw/zeptomesh/internal/metrics"
	"github.com/zeptoclaw/zeptomesh/internal/p2p"
	"github.com/zeptoclaw/zeptomesh/internal/picoclaw"
	"github.com/zeptoclaw/zeptomesh/internal/routing"
	"github.com/zeptoclaw/zeptomesh/internal/security"
	"github.com/zeptoclaw/zeptomesh/internal/storage"
	"github.com/zeptoclaw/zeptomesh/internal/tasks"
	"github.com/zeptoclaw/zeptomesh/internal/version"

	pb "github.com/zeptoclaw/zeptomesh/gen/zeptomesh/v1"
)

// SubmitRequest is re-exported so callers outside the tasks package (the admin
// API, the CLI) do not need to import it.
type SubmitRequest = tasks.SubmitRequest

// Node is one ZeptoClaw mesh participant.
type Node struct {
	Cfg *config.Config

	Identity   *security.Identity
	Store      *storage.Store
	Policy     *security.Policy
	Limiter    *security.Limiter
	Audit      *security.Audit
	Metrics    *metrics.Collector
	Host       *p2p.Host
	Service    *p2p.Service
	Table      *routing.Table
	Adapter    picoclaw.Adapter
	Manager    *tasks.Manager
	Registry   *discovery.LocalRegistry
	Probe      *discovery.Probe
	MDNS       *discovery.MDNS
	Bootstrap  *discovery.Bootstrap
	DHT        *discovery.DHT
	Membership *discovery.Membership

	log     *slog.Logger
	started time.Time

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	notifee *network.NotifyBundle
}

// Options parameterise New.
type Options struct {
	Config *config.Config
	Logger *slog.Logger
	// Adapter overrides the configured PicoClaw adapter (integration tests).
	Adapter picoclaw.Adapter
	// SkipDiscovery disables network-facing discovery (unit tests, air-gapped
	// bootstrap of a local-only cluster).
	SkipDiscovery bool
}

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
		adapter, err = picoclaw.New(cfg, logger)
		if err != nil {
			_ = store.Close()
			closePartial()
			return nil, fmt.Errorf("node: picoclaw adapter: %w", err)
		}
	}

	mets := metrics.New("")
	mets.BuildInfo.Set(1)

	table := routing.NewTable(cfg.Neighbors, store, logger)
	if n, err := table.Restore(); err != nil {
		logger.Warn("neighbor_table_restore_failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("neighbor_table_restored", "peers", n)
	}

	n := &Node{
		Cfg: cfg, Identity: identity, Store: store, Policy: policy,
		Limiter: security.NewLimiter(cfg.Security.RateLimit.RequestsPerSecond, cfg.Security.RateLimit.Burst),
		Audit:   audit, Metrics: mets, Table: table, Adapter: adapter, log: logger,
	}

	ph, err := p2p.New(context.Background(), p2p.Options{
		Config:  cfg,
		Key:     identity.PrivKey(),
		Gater:   p2p.NewGater(policy, audit),
		Logger:  logger,
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
	}, logger)

	if err := n.buildDiscovery(); err != nil {
		return nil, err
	}

	mgr, err := tasks.NewManager(tasks.Options{
		Config: cfg, Identity: identity, Policy: policy, Limiter: n.Limiter,
		Audit: audit, Store: store, Table: table, Adapter: adapter,
		Service: n.Service, Known: n.skillsSource(), Metrics: mets, Logger: logger,
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

// buildDiscovery prepares the discovery layer.
func (n *Node) buildDiscovery() error {
	cfg := n.Cfg
	if cfg.Discovery.LocalRegistry {
		rec := discovery.LocalRecord{
			NodeName: cfg.Node.Name,
			Addrs:    n.DiscoverableAddrs(),
			Skills:   cfg.EffectiveSkills(),
		}
		reg, err := discovery.NewLocalRegistry(cfg.Discovery.LocalSocketDir, n.Identity.PeerID(), rec,
			cfg.Discovery.Gossip.FailureTimeout.D()*2, n.log)
		if err != nil {
			n.log.Warn("local_registry_disabled", "err", err.Error())
		} else {
			n.Registry = reg
			probe, perr := discovery.NewProbe(cfg.Discovery.LocalSocketDir, n.Identity.PeerID().String(), n.log)
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
		cfg.Discovery.BootstrapInterval.D(), 10*time.Second, n.log)
	if err != nil {
		return fmt.Errorf("node: bootstrap: %w", err)
	}
	n.Bootstrap = b

	if cfg.Discovery.DHT && n.Host.DHT() != nil {
		n.DHT = discovery.NewDHT(n.Host.DHT(), cfg.EffectiveSkills(), n.log)
	}

	m, err := discovery.NewMembership(context.Background(), n.Host.Underlying(),
		cfg.Discovery.Gossip, n.Policy, n.Audit, n.log)
	if err != nil {
		return fmt.Errorf("node: membership: %w", err)
	}
	n.Membership = m
	m.SetSelfStateFunc(n.peerState)
	m.SetOnPeer(n.onGossipPeer)
	m.SetOnExpire(n.onPeerExpire)
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

	if n.Cfg.Discovery.MDNS {
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
	if err := n.Membership.Start(runCtx); err != nil {
		return fmt.Errorf("node: membership start: %w", err)
	}

	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.Bootstrap.Run(runCtx, n.needMorePeers) }()
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
		Membership:  n.Membership.Len(),
	}
	st.Bootstrap.Configured = n.Bootstrap.Count()
	st.Bootstrap.Reachable = len(n.Bootstrap.Healthy(n.Cfg.Discovery.Gossip.FailureTimeout.D() * 2))
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
	st.Security.TrustMode = n.Cfg.Security.TrustMode
	st.Security.RequireTaskSig = n.Cfg.Security.RequireTaskSignature
	st.Security.MinTrustForTasks = n.Cfg.Security.MinTrustForTasks
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
	n.Table.Upsert(&routing.Neighbor{
		PeerID: pid, Skills: append([]string(nil), caps.GetSkills()...),
		MaxPar: caps.GetMaxParallelTasks(), Running: caps.GetRunningTasks(),
		Load: caps.GetLoad(), Version: caps.GetVersion(),
		Trust: n.Policy.TrustOf(pid), Connected: n.Host.IsConnected(pid),
	})
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
			n.Table.Upsert(&routing.Neighbor{
				PeerID: pid, Addrs: []string{conn.RemoteMultiaddr().String()},
				Connected: true, Category: n.classify(conn), Trust: n.Policy.TrustOf(pid),
			})
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
			n.Metrics.PeersTotal.Set(float64(n.Table.Len()))
		},
		DisconnectedF: func(_ network.Network, conn network.Conn) {
			n.Table.SetConnected(conn.RemotePeer(), false)
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
		},
	}
	n.notifee = bundle
	n.Host.Underlying().Network().Notify(bundle)
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
	defer t.Stop()
	defer full.Stop()
	defer local.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-local.C:
			n.syncLocalRegistry(ctx)
			n.syncDHT(ctx)
		case <-t.C:
			if n.Table.ConnectedCount() < n.Cfg.Neighbors.Min && n.Cfg.Discovery.PeerExchange {
				n.requestPeerExchange(ctx)
			}
			for _, gone := range n.Table.PruneStale(n.Cfg.Discovery.Gossip.FailureTimeout.D() * 3) {
				n.Membership.MarkLeft(gone)
				n.log.Debug("neighbor_dropped", "peer", gone.String())
			}
			n.Metrics.PeersTotal.Set(float64(n.Table.Len()))
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
		case <-full.C:
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
