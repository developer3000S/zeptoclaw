package simulator

// Everything the model needs about protocol timing and limits is read either
// from config.Default() or from go-libp2p-pubsub's own defaults. A number that
// lives only in this package is a number that can drift away from the node, so
// the unit tests pin the handful of literals that have no exported source
// (TestPublishFullBatchSizeMatchesGossipCode, TestMaintenanceTickersMatchNode).

import (
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/security"
)

// Params configures one run. Cfg and GS carry the protocol truth; the rest
// describes the workload, which the specification deliberately leaves open.
type Params struct {
	// Cfg is the node configuration the simulated nodes would boot with. The
	// model reads neighbors min/target/max, discovery.gossip heartbeat/
	// full_sync/failure_timeout, security.max_message_bytes,
	// capabilities.skill_exchange and tasks.forwarding from it.
	Cfg *config.Config
	// GS is gossipsub's parameter set. internal/discovery builds the router
	// with pubsub.NewGossipSub plus a peer filter and no parameter overrides,
	// so the library defaults are exactly what a real node runs with.
	GS pubsub.GossipSubParams

	// SkillCatalog is the advertised-skill vocabulary of a heterogeneous mesh.
	// config.Default() advertises only "general", and routing.skillsMatch lets a
	// peer advertising "general" satisfy any request — so a homogeneous default
	// mesh makes every task one hop and every search trivial, and its routing
	// numbers would be artefacts rather than measurements.
	SkillCatalog []string
	// GeneralShare is the fraction of nodes advertising only "general".
	GeneralShare float64
	// SkillShare is the probability that a specialised node advertises any
	// given catalog skill. A task's required skill is drawn uniformly from the
	// catalog, so the expected number of executors is SkillShare × SkillCount ×
	// nodes.
	SkillShare float64

	// TaskPayloadBytes is the length of the instruction text of an injected task.
	TaskPayloadBytes int
	// ActiveTaskEveryBeats: during the active phase every node injects one task
	// this often. This is the workload ТЗ 16.4 calls "активная маршрутизация".
	ActiveTaskEveryBeats int
	// ExecTicks is the executor's modelled runtime in gossipsub heartbeats. It
	// is a workload assumption, not a protocol fact: the offline stub adapter in
	// config.Default() answers in 50 ms, a duration no beat-quantised model can
	// resolve. The report states the value used instead of implying that
	// PicoClaw was measured.
	ExecTicks int
	// HopTicks is the modelled round-trip cost of one delegation hop. The model
	// has no channel latency, so this is its floor rather than a measurement;
	// it is reported as part of the boundaries.
	HopTicks int
	// WarmRounds is how many independent joins the warm-up phase measures, so
	// the reported discovery speed is an average rather than one draw.
	WarmRounds int
	// WarmBeats bounds one warm-up round in node heartbeats.
	WarmBeats int
}

// DefaultParams reads the shipped defaults.
func DefaultParams() Params {
	cfg := config.Default()
	return Params{
		Cfg:                  cfg,
		GS:                   pubsub.DefaultGossipSubParams(),
		SkillCatalog:         []string{"coding", "summarize", "web-search", "translate", "data-analysis", "sql"},
		GeneralShare:         0.05,
		SkillShare:           0.05,
		TaskPayloadBytes:     320,
		ActiveTaskEveryBeats: 3,
		ExecTicks:            2,
		HopTicks:             1,
		WarmRounds:           3,
		WarmBeats:            12,
	}
}

// Heartbeat is the membership publish interval (discovery.Membership.publishLoop).
func (p Params) Heartbeat() time.Duration { return p.Cfg.Discovery.Gossip.Heartbeat.D() }

// FullSync is the period of the node-initiated full-view sync
// (Node.maintenance's full ticker calling Membership.PublishFull).
func (p Params) FullSync() time.Duration { return p.Cfg.Discovery.Gossip.FullSync.D() }

// FailureTimeout is how long a peer may stay silent before Membership.expireLoop
// drops it from the view.
func (p Params) FailureTimeout() time.Duration { return p.Cfg.Discovery.Gossip.FailureTimeout.D() }

// StalePrune is the neighbour-table prune horizon of Node.maintenance, three
// times the membership expiry.
func (p Params) StalePrune() time.Duration { return 3 * p.FailureTimeout() }

// SkillInterval is the skill-exchange reconcile period.
func (p Params) SkillInterval() time.Duration { return p.Cfg.Capabilities.SkillExchange.Interval.D() }

// GossipTick is one gossipsub heartbeat: the clock the mesh is maintained, IHAVE
// emitted and IDONTWANT aged on.
func (p Params) GossipTick() time.Duration { return p.GS.HeartbeatInterval }

// TicksPerBeat is how many gossipsub heartbeats fit into one node heartbeat.
func (p Params) TicksPerBeat() int {
	n := int(p.Heartbeat() / p.GS.HeartbeatInterval)
	if n < 1 {
		return 1
	}
	return n
}

// FullSyncEveryBeats is the PublishFull period in node heartbeats.
func (p Params) FullSyncEveryBeats() int {
	n := int(p.FullSync() / p.Heartbeat())
	if n < 1 {
		return 1
	}
	return n
}

// FullSyncBatchPauseTicks is PublishFull's inter-batch pause, heartbeat/8,
// rounded to at least one gossipsub heartbeat.
func (p Params) FullSyncBatchPauseTicks() int {
	n := int(p.Heartbeat() / 8 / p.GS.HeartbeatInterval)
	if n < 1 {
		return 1
	}
	return n
}

// IDontWantTTL is how long an IDONTWANT suppresses a message id, in gossipsub
// heartbeats.
func (p Params) IDontWantTTL() int { return p.GS.IDontWantMessageTTL }

// PruneBackoffTicks is gossipsub's backoff after a PRUNE, in heartbeats.
func (p Params) PruneBackoffTicks() int {
	n := int(p.GS.PruneBackoff / p.GS.HeartbeatInterval)
	if n < 1 {
		return 1
	}
	return n
}

// SkillSyncEveryTicks is the reconcile ticker in gossipsub heartbeats.
func (p Params) SkillSyncEveryTicks() int {
	n := int(p.SkillInterval() / p.GS.HeartbeatInterval)
	if n < 1 {
		return 1
	}
	return n
}

// TrustMode is the configured posture.
func (p Params) TrustMode() security.Mode {
	m, err := security.ParseMode(p.Cfg.Security.TrustMode)
	if err != nil {
		return security.ModeLimited
	}
	return m
}

// MinTrustForTasks is the configured admission threshold.
func (p Params) MinTrustForTasks() security.Trust {
	t, err := security.ParseTrust(p.Cfg.Security.MinTrustForTasks)
	if err != nil {
		return security.TrustLimited
	}
	return t
}

// skillSeed derives the per-node skill draw from the catalog, so the same
// catalog always produces the same mesh composition.
func (p Params) skillSeed() int64 {
	var h int64 = 1469598103934665603
	for _, s := range p.SkillCatalog {
		for i := 0; i < len(s); i++ {
			h ^= int64(s[i])
			h *= 1099511628211
		}
	}
	return h ^ int64(p.GeneralShare*1e9) ^ int64(p.SkillShare*1e9)
}

// Kind enumerates what a counted byte was spent on, so a report that says "the
// ceiling is exceeded" can also say by what.
type Kind int

// Traffic kinds, in report order.
const (
	KindOwnState     Kind = iota // this node's own PeerState, published once per heartbeat
	KindRelay                    // forwarding somebody else's publication (gossipsub mesh relay)
	KindFullSync                 // this node's full-view sync batches (PublishFull)
	KindControl                  // gossipsub IHAVE/IWANT/GRAFT/PRUNE/IDONTWANT frames
	KindHandshake                // capabilities RPC turning a dialled peer into a routable one
	KindPeerExchange             // peer-exchange RPC, asked while the table is thinner than min
	KindTask                     // task envelopes, acks and results
	KindSkillSync                // skill-descriptor sync and skill-lookup RPCs
	KindCount
)

// String implements fmt.Stringer.
func (k Kind) String() string {
	switch k {
	case KindOwnState:
		return "own state"
	case KindRelay:
		return "mesh relay"
	case KindFullSync:
		return "full sync"
	case KindControl:
		return "gossipsub control"
	case KindHandshake:
		return "capabilities RPC"
	case KindPeerExchange:
		return "peer exchange"
	case KindTask:
		return "task traffic"
	case KindSkillSync:
		return "skill lookup/sync"
	default:
		return "unknown"
	}
}

// ServiceKinds are the kinds ТЗ 16.4 bounds in idle mode. Task traffic is
// deliberately absent: it is payload, not overhead, and the specification's
// second ceiling covers it while routing is active.
func ServiceKinds() []Kind {
	return []Kind{KindOwnState, KindRelay, KindFullSync, KindControl,
		KindHandshake, KindPeerExchange, KindSkillSync}
}

// AllKinds lists every kind in report order.
func AllKinds() []Kind {
	out := make([]Kind, 0, KindCount)
	for k := Kind(0); k < KindCount; k++ {
		out = append(out, k)
	}
	return out
}

// CeilKbitIdle and CeilKbitActive are ТЗ 16.4's per-node limits. They are
// requirements rather than configuration, which is why they are constants here
// and not read from a file.
const (
	CeilingKbitIdle   = 100.0
	CeilingKbitActive = 1000.0
)
