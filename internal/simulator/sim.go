package simulator

// The mesh runs on two clocks, because the node does:
//
//   - the membership heartbeat (discovery.gossip.heartbeat), on which each node
//     publishes exactly one PeerState (discovery.Membership.publishLoop) and
//     Node.maintenance handshakes, re-syncs and asks for peer exchange; and
//   - gossipsub's own heartbeat (GossipSubParams.HeartbeatInterval), on which the
//     topic mesh is maintained and IHAVE is emitted.
//
// The model's quantum is the gossipsub tick; with the shipped defaults a node
// heartbeat is three ticks. Every rate in a report is derived from simulated seconds
// and never from wall-clock time, so a 1000-node run costs CPU but not real time.

import (
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/version"
)

// fullSyncBatchStates is the number of states one PublishFull batch carries. The
// literal lives inside discovery.Membership.PublishFull and is not exported, so
// TestPublishFullBatchSizeMatchesGossipCode pins this constant against that source
// text instead of trusting the two to keep agreeing.
const fullSyncBatchStates = 32

// peerExchangeAnswerLimit is how many records a peer-exchange answer carries.
const peerExchangeAnswerLimit = 10

// distributionCap bounds the per-node series kept for medians and percentiles. A p95
// does not need a million samples to be stable, and keeping every one of them would
// cost memory that buys nothing.
const distributionCap = 50000

// Phases, and the index each occupies in the per-phase counters.
const (
	PhaseIdle    = "idle"
	PhaseActive  = "active"
	PhaseFailure = "failure"

	phaseIdleIdx   = 0
	phaseActiveIdx = 1
	phaseFailIdx   = 2
	phaseCount     = 3
)

func phaseIndex(name string) int {
	switch name {
	case PhaseActive:
		return phaseActiveIdx
	case PhaseFailure:
		return phaseFailIdx
	default:
		return phaseIdleIdx
	}
}

// msgKind distinguishes what a publication is, because the node ingests the two
// kinds differently and the report charges them to different lines.
type msgKind uint8

const (
	// msgOwnState is Membership.Publish: one PeerState about the publisher.
	msgOwnState msgKind = iota
	// msgFullSync is Membership.PublishFull: up to fullSyncBatchStates states about
	// other peers, relayed by the publisher.
	msgFullSync
)

func (k msgKind) String() string {
	if k == msgFullSync {
		return "full sync"
	}
	return "own state"
}

// Msg is one gossipsub publication. Only its size and the (peer, timestamp) pairs it
// asserts are kept: a message's byte cost is fully determined by its kind, its
// publisher and those pairs, and those are all ingestion needs.
type Msg struct {
	stamp int64
	kind  msgKind
	src   int32
	size  int
	peers []int32
	tss   []int64
	// reached is this publication's own delivery bitmap, taken from the pool in
	// newMsg and returned when the flood settles. It has to be per message rather
	// than per node: a thousand publications are in flight at once, and a single
	// "last message that reached you" slot gets overwritten by the next flood,
	// which would let every concurrent publication queue you again.
	reached []uint64
	// settled marks the flood as complete and repaired, so it is never charged
	// twice and its bitmap goes back to the pool once.
	settled bool
	// registered keeps a publication on the repair list exactly once: the author
	// publishes a paced batch beat-heartbeat later, and that is when its flood starts.
	registered bool
	// firstHop is the tick the first copy actually left the author.
	firstHop int32
	// billed marks an analytic full-sync batch whose relay has been charged, so the
	// charge happens exactly once and exactly when the flood is over.
	billed bool
	// reachedCount is how many nodes this flood has taken delivery of. It is the
	// observable coverage of one publication, which is both the repair trigger and
	// what the analytic full-sync relay subtracts from the population.
	reachedCount int32
	sent         int32 // tick the author published it: the start of the propagation clock
	hops         int
	dups         int
	queued       int32 // outstanding copies in the delivery schedule, not a total
}

// qitem is one node on the delivery frontier: a peer that must be sent m by from on
// the next gossipsub heartbeat.
type qitem struct {
	n    int32
	from int32
}

// suppression is one IDONTWANT: "peer p, stop sending me message s", valid for
// IDontWantMessageTTL gossipsub heartbeats.
type suppression struct {
	p     int32
	stamp int64
	ttl   int32
}

// ventry is one membership-view record.
type ventry struct {
	// ts is the timestamp the node believes for this peer — the author's clock
	// reading it accepted. Membership.expireLoop compares exactly this value against
	// the failure_timeout cutoff, and a newer timestamp is the only thing that
	// overwrites it.
	ts int64
	// at is the simulated tick the entry arrived on. Propagation lag and freshness
	// are measured from this and not from ts, because a relayed state always carries
	// its author's clock value: comparing that against the receiver's clock would
	// report a clock artefact as a membership fault.
	at int32
}

// node is one simulated mesh member.
type node struct {
	idx    int32
	id     peer.ID
	addrs  []string
	skills []string
	// idStr is id.String() computed once at construction. peer.ID.String runs
	// base58 over the multihash, and PublishFull's tie-break comparator calls it
	// per swap — Θ(N² log N) encodings per full-sync beat at mesh scale, which a
	// CPU profile measured as 28 % of a run. A peer id is immutable, so the memo
	// cannot go stale; it changes timing only, never a result.
	idStr string
	// state is what this node publishes: a real pb.PeerState re-rendered once per
	// heartbeat the way Node.peerState renders it.
	state *pb.PeerState

	// view mirrors discovery.Membership.states.
	view map[int32]*ventry
	// known are the peers whose signed capabilities this node verified. Under the
	// shipped `limited` posture those — and only those — are the peers
	// security.Policy lets it delegate to, so a node that has not handshaked a peer
	// cannot route work to it even when gossip says the peer exists.
	known map[int32]struct{}
	// pendingCaps are peers dialled but not yet handshaked, so the model charges the
	// capabilities round trip Node.refreshCapabilities performs.
	pendingCaps map[int32]struct{}
	// links is the neighbour set. Dials stop at neighbors.max, as they do in
	// Node.onGossipPeer. It is also gossipsub's view of the topic's other peers —
	// gs.p.topics is built from connected subscribers — so mesh candidates and IHAVE
	// targets are bounded by links, not by the mesh size.
	links map[int32]struct{}
	// mesh is gossipsub's mesh for the membership topic: a subset of links, of size
	// D once the overlay has converged.
	mesh []int32
	// unwanted holds the IDONTWANT entries this node has sent.
	unwanted []suppression
	// backoff is the graft backoff of peers this node pruned, in ticks.
	backoff map[int32]int32
	// pxAsked throttles peer exchange to one attempt per peer per backoff window.
	pxAsked map[int32]int32
	// heldEpoch is the highest skill epoch imported per peer; a strictly larger
	// advertised epoch is what triggers a sync (skills.Registry.PeerEpoch).
	heldEpoch map[int32]int64

	policy *security.Policy
	table  *routing.Table

	// acc counts bytes by kind and direction (0 sent, 1 received) for the current
	// tick; sample() folds it into phaseKind and clears it.
	acc [KindCount][2]int64
	// phaseKind accumulates the whole run per phase, which is what the per-node rate
	// distribution and the per-kind attribution are computed from.
	phaseKind [phaseCount][KindCount][2]int64
	// phasePeak is this node's largest single-tick byte total per phase: a window
	// average hides a burst, and a ceiling is about the burst.
	phasePeak [phaseCount]int64

	// preCrashView and rebuiltAt measure how long a node's view takes to recover
	// after the failure injection.
	preCrashView int
	rebuiltAt    int32
	rebuildBeats float64

	// pendingDead is how many crashed peers a live node still holds in its view;
	// the beat at which it reaches zero is the expiry recovery time.
	pendingDead int32
	// lagged counts the propagation samples already recorded for this node.
	lagged bool

	// inUse is how many tasks this node is executing, bounded by
	// tasks.max_parallel_tasks the way the real manager's slot count is.
	inUse int32

	tasksInjected int
	tasksLocal    int
	tasksRouted   int
	tasksNoWorker int
	tasksLost     int

	dead bool
}

// Sim is one mesh of a fixed size driven through the phases. It is single-threaded
// by design: a load report that could race could not be reproduced either.
type Sim struct {
	p    Params
	ph   Phases
	n    int
	seed int64
	rng  *rng

	nodes []*node
	// live indexes the nodes that have not crashed.
	live []int32
	// byPeer resolves a peer id back to its index, because routing.Table keys its
	// rows and its answers by peer.ID.
	byPeer map[peer.ID]int32

	// clock
	tick         int32
	secs         int64
	ticksPerBeat int32
	beat         int32
	phase        string
	phaseIdx     int

	// counters
	seqNo        int64
	deliveries   int64
	grafts       int64
	prunes       int64
	ihaveFrames  int64
	iwantFrames  int64
	idontFrames  int64
	pxAttempts   int64
	repairCopies int64
	unrepaired   int64
	// dupSuppressions counts the copies a node did not forward because it had already
	// accepted the message. The real router delivers such a copy and then declines it, so
	// this is the model's known undercount of frames: boundary note 2 quotes it.
	dupSuppressions int64
	handshakes      int64
	skillSyncs      int64
	lookups         int64
	lostEntries     int64
	rejoinEntries   int64
	rejectedState   int64
	dialCapped      int64
	edges           int64
	syncMsgs        int64
	syncStates      int64
	maxQueued       int64
	// maxBatches is the largest number of PublishFull batches one node had to send in
	// one beat, which is what bounds the repair list's memory growth.
	maxBatches int
	// inflight is the number of copies currently sitting in the delivery schedule, and
	// inflightBytes their size. Both are what a run ends owing: a copy billed to its
	// sender on the last tick has no tick left in which to arrive.
	inflight      int64
	inflightBytes int64
	// lostInflight is the traffic the model drops: a copy that left a sender and whose
	// recipient crashed before it arrived. It is the loss term the traffic-symmetry
	// invariant is stated against.
	lostInflight int64
	// deadNodes lists the crashed nodes so their byte counters keep being folded into
	// the phase window after they leave the live set. Without this, a late charge to a
	// victim (a capabilities answer on a link that was torn down in the same beat)
	// would land in an accumulator nobody reads and break Σsend == Σrecv + loss.
	deadNodes []int32
	// wireKind[k] accumulates every byte put on the wire for kind k over the whole run.
	// The analytic-versus-exact full-sync cross-check compares these.
	wireKind [KindCount]int64

	// task outcomes, aggregated across the mesh
	taskSeq       int64
	tasksInjected int
	tasksLocal    int
	tasksRouted   int
	tasksNoWorker int
	tasksLost     int
	completions   int64
	lostReasons   map[string]int
	routeSecs     []float64
	routeHops     []float64

	// discovery and recovery series
	lag         []float64
	expireBeats []float64
	expireMax   int32
	rebuildB    []float64
	crashed     int
	crashTick   int32

	// Stabilisation, in node heartbeats: the earliest tick on which any node's view was
	// complete, and the earliest on which every entry of it was inside the expiry
	// horizon — measured from the start of the run in the idle phase and from the crash
	// in the failure phase. -1 means no node got there inside the window.
	stabFull, stabFresh       int32
	recoverFull, recoverFresh float64
	// lagMax is the widest gap seen between an author publishing a state and some node
	// holding it, in ticks — a genuine worst case, not a maximum of per-tick means.
	lagMax int32
	// horizonTicks is failure_timeout in gossipsub ticks, the freshness test's cutoff.
	horizonTicks int32
	// Scratch buffers for connectivity(), reused every tick.
	scratchParent []int32
	scratchLive   []bool
	scratchSizes  map[int32]int64

	// series
	samp          []Sample
	tickPeak      [phaseCount]float64
	freshMin      float64
	connectIdle   float64
	connectMin    float64
	connectEnd    float64
	connectBefore float64

	// historyWindow[i][slot] is how many distinct messages node i first accepted in
	// that gossipsub heartbeat. The last HistoryGossip slots are what IHAVE
	// advertises, so the window is also what bounds the advertisement's size.
	historyWindow [][]int32
	cacheCur      int32
	cacheGoss     int32

	// buckets is the delivery schedule, a ring indexed by tick.
	buckets [][]*pkt

	// nWords is the width of a reachability bitmap for this mesh.
	nWords int
	// exactRelay walks a full sync's delivery tree node by node instead of charging
	// it analytically. The fast unit tests turn it on at a size where walking is
	// affordable and compare the two answers; the load run leaves it off.
	exactRelay bool

	// inflight are the publications whose flood has not yet drained, and the pools
	// their storage comes from. Without the pools a 1000-node full sync would
	// allocate a bitmap per batch per node per beat, which is memory the answer does
	// not need: the flood is over within a few heartbeats and the bitmap with it.
	msgFree    []*Msg
	bitmapFree [][]uint64
	// openMsgs are this beat's publications whose flood still has to be repaired:
	// gossipsub's mesh does not cover the topic on its own, and the IHAVE/IWANT
	// exchange that completes delivery is a real cost the report has to include.
	openMsgs []*Msg

	due   []*task
	tasks []*task

	res *Result

	// measured once, at construction
	sizeOwnGossip int
	sizeSyncBatch int
	sizeCapsReq   int
	sizeCapsRT    int
	sizePxReq     int
	sizePxRT      int
	sizeSyncReq   int
	sizeSyncRT    int
	sizeLookupRT  int
	sizeEnvelope  int
	sizeAck       int
	sizeResult    int
	topic         string
	idSize        int
	pxRecordBytes int
	descBytes     int

	execSecs int64
	hopSecs  int64
}

// pkt is one scheduled delivery of one message to one node from one peer.
type pkt struct {
	at   int32
	to   int32
	from int32
	msg  *Msg
}

// New builds a mesh of n nodes whose identities come from seed.
func New(n int, seed int64, p Params, ph Phases) (*Sim, error) {
	if p.Cfg == nil {
		return nil, errors.New("simulator: params carry no node configuration")
	}
	if n < 3 {
		return nil, fmt.Errorf("simulator: a mesh needs at least 3 nodes, got %d", n)
	}
	if len(p.SkillCatalog) == 0 {
		return nil, errors.New("simulator: an empty skill catalog would make routing unmeasurable")
	}
	if p.GS.HeartbeatInterval <= 0 {
		return nil, errors.New("simulator: gossipsub heartbeat interval must be positive")
	}
	if p.GS.D <= 0 {
		return nil, errors.New("simulator: gossipsub mesh degree must be positive")
	}
	if p.GS.HistoryGossip > p.GS.HistoryLength {
		return nil, fmt.Errorf("simulator: gossipsub HistoryGossip (%d) exceeds HistoryLength (%d)",
			p.GS.HistoryGossip, p.GS.HistoryLength)
	}
	ids, addrs, err := identitySet(n, seed, p.Cfg.Node.Listen)
	if err != nil {
		return nil, err
	}
	s := &Sim{
		p:            p,
		ph:           ph,
		n:            n,
		seed:         seed,
		rng:          newRNG(seed),
		ticksPerBeat: int32(max(p.TicksPerBeat(), 1)),
		topic:        p.Cfg.Discovery.Gossip.Topic,
		idSize:       msgIDSize(ids[0]),
		phase:        PhaseIdle,
		lostReasons:  map[string]int{},
	}
	tickSecs := s.tickSeconds()
	s.secs = tickSecs
	// The schedule is in node heartbeats, so no phase may be shorter than one: a
	// shorter window would be measured over zero samples and reported as a zero rate.
	ph.IdleBeats = max(ph.IdleBeats, int(s.ticksPerBeat))
	ph.ActiveBeats = max(ph.ActiveBeats, int(s.ticksPerBeat))
	ph.FailureBeats = max(ph.FailureBeats, 2*int(s.ticksPerBeat))
	s.execSecs = int64(max(p.ExecTicks, 1)) * tickSecs
	s.hopSecs = int64(max(p.HopTicks, 1)) * tickSecs
	s.cacheGoss = int32(max(p.GS.HistoryGossip, 1))
	s.horizonTicks = int32(max(s.p.FailureTimeout()/time.Duration(s.tickSeconds())/time.Second, 1))
	s.scratchParent = make([]int32, n)
	s.scratchLive = make([]bool, n)
	s.scratchSizes = make(map[int32]int64, n)
	// The scheduler window has to cover the largest offset ever requested. A node's
	// PublishFull batches are paced FullSyncBatchPauseTicks apart and its widest view is
	// the whole mesh, so the last batch of a beat goes out pause×(n/batch) ticks later;
	// a ring shorter than that would deliver a late batch into a bucket that comes round
	// sooner, i.e. out of order.
	pause := int32(max(s.p.FullSyncBatchPauseTicks(), 1))
	wide := int32(n/fullSyncBatchStates + 2)
	s.buckets = make([][]*pkt, int(s.ticksPerBeat)*(int(wide)*int(pause)+2)+8)
	s.historyWindow = make([][]int32, n)
	for i := range s.historyWindow {
		s.historyWindow[i] = make([]int32, s.cacheGoss)
	}
	s.nWords = (n + 63) / 64
	s.byPeer = make(map[peer.ID]int32, n)

	maxPar := int32(p.Cfg.Tasks.MaxParallelTasks)
	for i := 0; i < n; i++ {
		skills := s.drawSkills(i)
		nd := &node{
			idx:         int32(i),
			id:          ids[i],
			idStr:       ids[i].String(),
			addrs:       addrs[i],
			skills:      skills,
			view:        make(map[int32]*ventry, 16),
			known:       make(map[int32]struct{}, 16),
			pendingCaps: make(map[int32]struct{}, 8),
			links:       make(map[int32]struct{}, 16),
			backoff:     map[int32]int32{},
			pxAsked:     map[int32]int32{},
			heldEpoch:   map[int32]int64{},
			policy:      newPolicy(p),
			table:       routing.NewTable(p.Cfg.Neighbors, nil, nil, newPolicy(p)),
		}
		nd.state = &pb.PeerState{
			PeerId:           ids[i].String(),
			Timestamp:        s.secs,
			Skills:           skills,
			MaxParallelTasks: maxPar,
			Version:          version.Version,
			Status:           "active",
			Addrs:            addrs[i],
			SkillsVersion:    1,
		}
		s.nodes = append(s.nodes, nd)
		s.live = append(s.live, int32(i))
		s.byPeer[ids[i]] = int32(i)
	}
	s.freshMin = 1
	s.stabFull, s.stabFresh = -1, -1
	s.recoverFull, s.recoverFresh = -1, -1
	s.measureSizes()
	s.bootstrapSeeds()
	return s, nil
}

// drawSkills renders one node's advertised skills with a per-node stream, so the
// mesh composition never depends on the order phases ran in.
func (s *Sim) drawSkills(idx int) []string {
	r := newRNG(s.p.skillSeed() + int64(idx)*7919)
	if r.float() < s.p.GeneralShare {
		return []string{"general"}
	}
	out := make([]string, 0, 4)
	for _, sk := range s.p.SkillCatalog {
		if r.float() < s.p.SkillShare {
			out = append(out, sk)
		}
	}
	if len(out) == 0 {
		out = append(out, s.p.SkillCatalog[r.intn(len(s.p.SkillCatalog))])
	}
	return out
}

// newPolicy mirrors the trust posture the configuration asks for.
func newPolicy(p Params) *security.Policy {
	return security.NewPolicy(p.TrustMode(), p.MinTrustForTasks())
}

// mustMarshal marshals or panics: a failure here would be a bug in the model, not a
// runtime condition, and no caller can do anything with the error.
func (s *Sim) mustMarshal(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		panic("simulator: marshal: " + err.Error())
	}
	return b
}

// tickSeconds is one gossipsub heartbeat in simulated seconds.
func (s *Sim) tickSeconds() int64 {
	n := int64(s.p.GS.HeartbeatInterval / time.Second)
	if n < 1 {
		return 1
	}
	return n
}

// beatSeconds is one membership heartbeat in simulated seconds.
func (s *Sim) beatSeconds() int64 { return int64(s.p.Heartbeat() / time.Second) }

// meshD is gossipsub's mesh degree for the membership topic.
func (s *Sim) meshD() int { return max(s.p.GS.D, 1) }

// onBeat is true on a tick that opens a node heartbeat. Tick zero opens the first
// one, so the mesh publishes at once instead of waiting a full heartbeat before
// anyone learns anything — which is also what a ticker armed at boot does.
func (s *Sim) onBeat() bool { return s.tick%s.ticksPerBeat == 0 }

// onFullSync is true on a heartbeat on which Node.maintenance's full ticker calls
// Membership.PublishFull.
func (s *Sim) onFullSync() bool {
	return s.beat%int32(max(s.p.FullSyncEveryBeats(), 1)) == 0
}

// ---------- scheduler ----------

// schedule queues one delivery.
func (s *Sim) schedule(at int32, from, to int32, m *Msg) {
	i := modInt(int(at), len(s.buckets))
	s.buckets[i] = append(s.buckets[i], &pkt{at: at, to: to, from: from, msg: m})
	m.queued++
	s.inflight++
	s.inflightBytes += int64(m.size)
	if s.inflight > s.maxQueued {
		s.maxQueued = s.inflight
	}
}

// drainBucket returns and clears the deliveries queued in the ring slot for tick t.
func (s *Sim) drainBucket(t int32) []*pkt {
	i := modInt(int(t), len(s.buckets))
	out := s.buckets[i]
	s.buckets[i] = out[:0]
	return out
}

func modInt(v, m int) int {
	if m <= 0 {
		return 0
	}
	r := v % m
	if r < 0 {
		r += m
	}
	return r
}

// ---------- node bookkeeping ----------

// charge accounts for one framed RPC: dir 0 is this node sending, 1 receiving.
func (nd *node) charge(k Kind, dir, size int) {
	nd.acc[k][dir] += int64(size)
}

// totalIn and totalOut are this node's received and sent bytes for the tick.
func (nd *node) totalIn() int64 {
	var t int64
	for k := Kind(0); k < KindCount; k++ {
		t += nd.acc[k][1]
	}
	return t
}

func (nd *node) totalOut() int64 {
	var t int64
	for k := Kind(0); k < KindCount; k++ {
		t += nd.acc[k][0]
	}
	return t
}

// addUnwanted records an IDONTWANT sent to one peer for one publication.
func (nd *node) addUnwanted(p int32, stamp int64, ttl int32) {
	nd.unwanted = append(nd.unwanted, suppression{p: p, stamp: stamp, ttl: ttl})
}

// unwantedBy lists the peers this node asked to stop sending it a given publication.
func (nd *node) unwantedBy(stamp int64) []int32 {
	if len(nd.unwanted) == 0 {
		return nil
	}
	var out []int32
	for _, w := range nd.unwanted {
		if w.stamp == stamp && w.ttl > 0 {
			out = append(out, w.p)
		}
	}
	return out
}

// ageUnwanted decrements every IDONTWANT timer by one gossipsub heartbeat.
func (nd *node) ageUnwanted() {
	kept := nd.unwanted[:0]
	for _, w := range nd.unwanted {
		w.ttl--
		if w.ttl > 0 {
			kept = append(kept, w)
		}
	}
	nd.unwanted = kept
}

func remove32(a []int32, v int32) []int32 {
	out := a[:0]
	for _, x := range a {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}

func contains32(a []int32, v int32) bool {
	for _, x := range a {
		if x == v {
			return true
		}
	}
	return false
}

// peerID resolves a simulated index to its real peer id, so every trust and routing
// decision runs on the same type the node uses.
func (s *Sim) peerID(idx int32) peer.ID { return s.nodes[idx].id }
