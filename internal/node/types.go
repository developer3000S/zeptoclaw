package node

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/discovery"
	"github.com/developer3000S/zeptoclaw/internal/metrics"
	"github.com/developer3000S/zeptoclaw/internal/p2p"
	"github.com/developer3000S/zeptoclaw/internal/picoclaw"
	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/skills"
	"github.com/developer3000S/zeptoclaw/internal/storage"
	"github.com/developer3000S/zeptoclaw/internal/tasks"
	"github.com/developer3000S/zeptoclaw/internal/triggers"
)

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
	// SearchTopic is the epidemic skill-search plane (ТЗ 6.9.5 п.5): nil unless
	// tasks.forwarding.search_relay.topic.enabled joined it at startup. It rides
	// the same pubsub router as Membership — one host, one router.
	SearchTopic *discovery.SearchTopic
	// Scheduler runs the cron-declared triggers (ТЗ 6.6.1 п.4), injecting their
	// jobs as tasks authored by this node.
	Scheduler *triggers.Scheduler

	log     *slog.Logger
	started time.Time

	// lastScan keeps the most recent environment-sweep report (manual or
	// background) for the admin API's GET /api/v1/admin/scan.
	scanMu   sync.Mutex
	lastScan *ScanResult

	// pubsub is the single gossip router this host speaks. Membership and the
	// search topic are built over it (a second router would steal the first's
	// stream handler); the router is closed with the host.
	pubsub *pubsub.PubSub

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
