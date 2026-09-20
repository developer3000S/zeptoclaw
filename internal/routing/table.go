// Package routing keeps the neighbour table, scores candidates and decides
// where a task should go next.
package routing

import (
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/storage"
)

// Category labels where a neighbour sits relative to this node.
const (
	CatLocal     = "local"
	CatLAN       = "lan"
	CatWAN       = "wan"
	CatBootstrap = "bootstrap"
)

// Neighbor is the in-memory view of one peer.
type Neighbor struct {
	PeerID    peer.ID
	Addrs     []string
	Skills    []string
	Category  string
	Version   string
	Load      float64
	MaxPar    int32
	Running   int32
	Successes uint64
	Failures  uint64
	ProtoErrs uint64
	RTT       time.Duration
	LastSeen  time.Time
	Connected bool
	// SkillsVersion mirrors the peer's advertised skill epoch: a strictly
	// larger value means the peer changed its skills or their documentation and
	// descriptors must be re-fetched (skill exchange, ТЗ 6.3).
	SkillsVersion int64
	// Left marks a peer that announced a graceful departure.
	Left bool
}

// Neighbor deliberately has no trust field. Trust is a judgement this node
// makes about a peer, and security.Policy is the only thing that computes it;
// a copy here was a standing invitation to two views of the same fact
// disagreeing. Before it was removed, an observation-shaped Upsert ("these are
// the peer's skills") left the zero value in place, and the zero value of
// security.Trust is TrustTrusted — the most privileged level. Since peer ids
// are self-certifying Ed25519 keys, whichever bookkeeping write touched a
// gossiped stranger last was promoting that stranger to trusted, and the
// promotion was persisted, so the Restore rule "trust is re-earned per
// process" was undone by the first partial upsert after a restart. Use
// Table.TrustOf.

// availableCapacity is the fraction of the peer's slots still free.
func (n *Neighbor) availableCapacity() float64 {
	if n.MaxPar <= 0 {
		return 0.5 // unknown capacity: neutral
	}
	free := float64(n.MaxPar - n.Running)
	if free < 0 {
		return 0
	}
	return free / float64(n.MaxPar)
}

// successRate is 0.5 when nothing has been tried yet.
func (n *Neighbor) successRate() float64 {
	total := n.Successes + n.Failures
	if total == 0 {
		return 0.5
	}
	return float64(n.Successes) / float64(total)
}

// trustScore maps a trust level onto [0,1].
func trustScore(t security.Trust) float64 {
	switch t {
	case security.TrustTrusted:
		return 1
	case security.TrustKnown:
		return 0.75
	case security.TrustLimited:
		return 0.5
	case security.TrustUntrusted:
		return 0.2
	default:
		return 0
	}
}

// latencyScore maps an RTT onto [0,1] with 50ms ≈ perfect, 2s ≈ useless.
func latencyScore(rtt time.Duration) float64 {
	if rtt <= 0 {
		return 0.5
	}
	const good = 50e6 // 50ms in ns
	const bad = 2e9   // 2s in ns
	if rtt.Nanoseconds() <= good {
		return 1
	}
	if rtt.Nanoseconds() >= bad {
		return 0
	}
	return 1 - float64(rtt.Nanoseconds()-good)/float64(bad-good)
}

// Table is the concurrency-safe neighbour set with an optional persistence hook.
type Table struct {
	mu   sync.RWMutex
	neis map[peer.ID]*Neighbor
	cfg  config.NeighborsConfig
	log  *slog.Logger
	st   *storage.Store
	rng  *rand.Rand
	// policy is the authority on peer trust. The table reads it and never
	// derives trust itself, because an observation such as "this peer's skills
	// are [coding]" says nothing about the relationship.
	policy *security.Policy
}

// NewTable builds an empty table. policy may be nil (unit tests, read-only
// views), in which case every peer scores as untrusted.
func NewTable(cfg config.NeighborsConfig, st *storage.Store, logger *slog.Logger, policy *security.Policy) *Table {
	return &Table{
		neis:   make(map[peer.ID]*Neighbor),
		cfg:    cfg,
		log:    logger,
		st:     st,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
		policy: policy,
	}
}

// TrustOf reports what this node currently believes about a peer. It is a pass-
// through to the security policy rather than a lookup, so a view of a peer can
// never go stale relative to an operator's allow/block list or a handshake that
// has not touched this table.
//
// The lock order in this package is always table → policy, which is the only
// order that can arise: security.Policy never calls back into the table, so no
// path exists that would hold the policy lock and then want this one.
func (t *Table) TrustOf(p peer.ID) security.Trust {
	if t.policy == nil {
		return security.TrustUntrusted
	}
	return t.policy.TrustOf(p)
}

// Upsert merges an observation into the table, preserving accumulated metrics.
// The neighbour has no trust to merge: see the note on Neighbor.
func (t *Table) Upsert(n *Neighbor) *Neighbor {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur, ok := t.neis[n.PeerID]
	if !ok {
		cp := *n
		cp.LastSeen = time.Now().UTC()
		t.neis[n.PeerID] = &cp
		t.persistLocked(&cp)
		return &cp
	}
	if len(n.Addrs) > 0 {
		cur.Addrs = append([]string(nil), n.Addrs...)
	}
	if len(n.Skills) > 0 {
		cur.Skills = append([]string(nil), n.Skills...)
	}
	if n.Category != "" {
		cur.Category = n.Category
	}
	if n.Version != "" {
		cur.Version = n.Version
	}
	if n.MaxPar > 0 {
		cur.MaxPar = n.MaxPar
	}
	// The skill epoch is monotonic: a peer that restarted with an older number
	// is lying or misconfigured, and rewinding our view would silently stop us
	// from re-syncing descriptors we already have.
	if n.SkillsVersion > cur.SkillsVersion {
		cur.SkillsVersion = n.SkillsVersion
	}
	cur.Running = n.Running
	cur.Load = n.Load
	cur.Connected = n.Connected
	cur.Left = n.Left
	cur.LastSeen = time.Now().UTC()
	t.persistLocked(cur)
	return cur
}

// SetConnected records connection state.
func (t *Table) SetConnected(p peer.ID, connected bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n, ok := t.neis[p]
	if !ok {
		n = &Neighbor{PeerID: p}
		t.neis[p] = n
	}
	n.Connected = connected
	n.LastSeen = time.Now().UTC()
	if connected {
		n.Left = false
	}
}

// RecordRTT folds a new round-trip measurement into the moving average.
func (t *Table) RecordRTT(p peer.ID, rtt time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n, ok := t.neis[p]
	if !ok {
		return
	}
	if n.RTT <= 0 {
		n.RTT = rtt
		return
	}
	n.RTT = time.Duration(float64(n.RTT)*0.7 + float64(rtt)*0.3)
}

// RecordSuccess credits a peer with a completed task and decays local suspicion.
func (t *Table) RecordSuccess(p peer.ID) { t.record(p, true) }

// RecordFailure debits a peer and increases local suspicion.
func (t *Table) RecordFailure(p peer.ID) { t.record(p, false) }

func (t *Table) record(p peer.ID, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n, exists := t.neis[p]
	if !exists {
		return
	}
	if ok {
		n.Successes++
	} else {
		n.Failures++
	}
	t.persistLocked(n)

	// Active defense is local policy: successful interactions decay suspicion,
	// failures escalate it. This does not change the remote peer's behavior.
	if t.policy != nil {
		if defense := t.policy.DefenseOf(p); defense != nil {
			if ok {
				defense.ReduceSuspicion(20)
			} else {
				defense.AddSuspicion(20)
			}
		}
	}
}

// RecordProtocolError counts a malformed or policy-violating message.
func (t *Table) RecordProtocolError(p peer.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n, exists := t.neis[p]
	if !exists {
		return
	}
	n.ProtoErrs++
	t.persistLocked(n)
}

// Get returns a copy of a neighbour.
func (t *Table) Get(p peer.ID) (Neighbor, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.neis[p]
	if !ok {
		return Neighbor{}, false
	}
	return *n, true
}

// GetOrNew returns the live entry for a peer, creating an empty one when the
// peer is unknown. The returned pointer is owned by the table.
func (t *Table) GetOrNew(p peer.ID) *Neighbor {
	t.mu.Lock()
	defer t.mu.Unlock()
	n, ok := t.neis[p]
	if !ok {
		n = &Neighbor{PeerID: p}
		t.neis[p] = n
	}
	return n
}

// Len reports the table size.
func (t *Table) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.neis)
}

// List returns a snapshot of every neighbour.
func (t *Table) List() []Neighbor {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]Neighbor, 0, len(t.neis))
	for _, n := range t.neis {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Connected != out[j].Connected {
			return out[i].Connected
		}
		return out[i].LastSeen.After(out[j].LastSeen)
	})
	return out
}

// ConnectedCount is the number of live connections in the table.
func (t *Table) ConnectedCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := 0
	for _, e := range t.neis {
		if e.Connected && !e.Left {
			n++
		}
	}
	return n
}

// Remove drops a neighbour entirely.
func (t *Table) Remove(p peer.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.neis, p)
}

// Suspects returns the neighbours silent for longer than timeout, without
// removing them. Eviction based on silence alone punishes a peer that is alive
// but quiet (its gossip is delayed, our inbound queue is not), so the caller
// confirms each suspect with a direct probe and prunes only those that answer
// nothing (ТЗ 6.5.3).
func (t *Table) Suspects(timeout time.Duration) []Neighbor {
	cutoff := time.Now().UTC().Add(-timeout)
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []Neighbor
	for _, n := range t.neis {
		if n.LastSeen.Before(cutoff) {
			out = append(out, *n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastSeen.Before(out[j].LastSeen) })
	return out
}

// Touch records a live confirmation from a peer: it keeps the neighbour in the
// table with a fresh LastSeen and the connectivity the caller just observed.
func (t *Table) Touch(p peer.ID, connected bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n, ok := t.neis[p]
	if !ok {
		return
	}
	n.LastSeen = time.Now().UTC()
	n.Connected = connected
	if connected {
		n.Left = false
	}
}

// PruneStale drops neighbours silent for longer than timeout and returns the
// peers that were evicted.
func (t *Table) PruneStale(timeout time.Duration) []peer.ID {
	cutoff := time.Now().UTC().Add(-timeout)
	t.mu.Lock()
	defer t.mu.Unlock()
	var gone []peer.ID
	for id, n := range t.neis {
		if n.LastSeen.Before(cutoff) {
			gone = append(gone, id)
			delete(t.neis, id)
		}
	}
	return gone
}

// Sample returns up to k random neighbours, used by gossip and peer exchange.
func (t *Table) Sample(k int) []Neighbor {
	all := t.List()
	if k >= len(all) {
		return all
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	perm := t.rng.Perm(len(all))
	out := make([]Neighbor, 0, k)
	for _, i := range perm[:k] {
		out = append(out, all[i])
	}
	return out
}

func (t *Table) persistLocked(n *Neighbor) {
	if t.st == nil {
		return
	}
	rec := &storage.PeerRecord{
		PeerID: n.PeerID.String(),
		Addrs:  append([]string(nil), n.Addrs...),
		Skills: append([]string(nil), n.Skills...),
		// Diagnostic only: the peer's standing is never read back from here (see
		// Restore), so this records what the policy believed at write time.
		Trust:          t.TrustOf(n.PeerID).String(),
		Category:       n.Category,
		Version:        n.Version,
		Load:           n.Load,
		MaxParallel:    n.MaxPar,
		Running:        n.Running,
		Successes:      n.Successes,
		Failures:       n.Failures,
		ProtocolErrors: n.ProtoErrs,
		AvgRTTMillis:   n.RTT.Milliseconds(),
		Connected:      n.Connected,
		LastStatus:     statusLabel(n),
		SkillsVersion:  n.SkillsVersion,
	}
	if err := t.st.PutPeer(rec); err != nil && t.log != nil {
		t.log.Debug("peer_persist_failed", "peer", n.PeerID.String(), "err", err.Error())
	}
}

func statusLabel(n *Neighbor) string {
	switch {
	case n.Left:
		return "left"
	case n.Connected:
		return "active"
	default:
		return "idle"
	}
}

// Restore loads persisted peers into the table at startup.
func (t *Table) Restore() (int, error) {
	if t.st == nil {
		return 0, nil
	}
	recs, err := t.st.ListPeers()
	if err != nil {
		return 0, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range recs {
		pid, err := peer.Decode(r.PeerID)
		if err != nil {
			continue
		}
		t.neis[pid] = &Neighbor{
			PeerID:    pid,
			Addrs:     append([]string(nil), r.Addrs...),
			Skills:    append([]string(nil), r.Skills...),
			Category:  r.Category,
			Version:   r.Version,
			Load:      r.Load,
			MaxPar:    r.MaxParallel,
			Running:   r.Running,
			Successes: r.Successes,
			Failures:  r.Failures,
			ProtoErrs: r.ProtocolErrors,
			RTT:       time.Duration(r.AvgRTTMillis) * time.Millisecond,
			LastSeen:  time.Unix(r.SeenAt, 0).UTC(),
			// The epoch survives a restart: the node must not re-announce a
			// "stale" view of a peer whose descriptors it already imported.
			SkillsVersion: r.SkillsVersion,
			Connected:     false, // connections never survive a restart
			// Trust is not restored either, for the same reason: it is re-earned per
			// process by the security policy through a verified capabilities
			// handshake. Restoring the old label made the peer list read "trusted"
			// while the policy still treated the peer as a stranger, so after a
			// restart the mesh refused every task from a healthy neighbour and the
			// two views disagreed — only the wrong one was visible to an operator.
			// With the field gone from Neighbor there is nothing left to disagree:
			// Table.TrustOf answers from the policy, where only an explicit operator
			// allow-list entry can come back already trusted.
		}
	}
	return len(t.neis), nil
}

// Candidate is a scored neighbour for a specific task.
type Candidate struct {
	Neighbor   Neighbor
	Score      float64
	SkillMatch float64
	Reasons    []string
}

// Scoring weights from the specification.
const (
	wSkill    = 0.40
	wTrust    = 0.25
	wCapacity = 0.20
	wLatency  = 0.10
	wSuccess  = 0.05
)

// Select scores every eligible neighbour for a task and returns the best
// fan-out candidates. exclude prevents retrying a peer that already refused.
func (t *Table) Select(
	wantSkills []string,
	policy *security.Policy,
	self peer.ID,
	fanout int,
	exclude map[peer.ID]bool,
) []Candidate {
	// Values, not pointers: the entries are mutated by Upsert, so a snapshot of
	// pointers would leave every field read below racing with a concurrent write.
	t.mu.RLock()
	snap := make([]Neighbor, 0, len(t.neis))
	for _, n := range t.neis {
		snap = append(snap, *n)
	}
	t.mu.RUnlock()

	cands := make([]Candidate, 0, len(snap))
	for i := range snap {
		n := &snap[i]
		if n.PeerID == self || n.Left {
			continue
		}
		if exclude != nil && exclude[n.PeerID] {
			continue
		}
		if policy != nil && !policy.AllowDelegationTo(n.PeerID) {
			continue
		}
		matched, ok := skillsMatch(n.Skills, wantSkills)
		if !ok {
			continue
		}
		skillScore := 0.0
		if len(wantSkills) > 0 {
			skillScore = float64(matched) / float64(len(wantSkills))
		} else {
			skillScore = 1
		}
		// Score with the policy's current verdict, not a cached copy: an operator
		// may have changed the peer's standing since the last write, and this is
		// the place where that decides where work goes.
		trust := t.TrustOf(n.PeerID)
		if policy != nil {
			trust = policy.TrustOf(n.PeerID)
		}
		score := skillScore*wSkill +
			trustScore(trust)*wTrust +
			n.availableCapacity()*wCapacity +
			latencyScore(n.RTT)*wLatency +
			n.successRate()*wSuccess
		if !n.Connected {
			score *= 0.85 // dialing costs a round trip
		}
		cands = append(cands, Candidate{
			Neighbor:   *n,
			Score:      math.Round(score*10000) / 10000,
			SkillMatch: skillScore,
			Reasons: []string{
				fmt.Sprintf("skill=%.2f", skillScore),
				fmt.Sprintf("trust=%s", trust),
				fmt.Sprintf("cap=%.2f", n.availableCapacity()),
				fmt.Sprintf("rtt=%s", n.RTT.Round(time.Millisecond)),
			},
		})
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].Score == cands[j].Score {
			return cands[i].Neighbor.PeerID.String() < cands[j].Neighbor.PeerID.String()
		}
		return cands[i].Score > cands[j].Score
	})
	if fanout > 0 && len(cands) > fanout {
		cands = cands[:fanout]
	}
	return cands
}

// AnyWithSkills reports whether a connected neighbour advertises the skills.
func (t *Table) AnyWithSkills(want []string) bool {
	return len(t.Select(want, nil, "", 1, nil)) > 0
}

func skillsMatch(have, want []string) (int, bool) {
	set := make(map[string]bool, len(have))
	for _, s := range have {
		set[strings.ToLower(strings.TrimSpace(s))] = true
	}
	if set["general"] || set["any"] {
		return len(want), true
	}
	matched := 0
	for _, w := range want {
		if set[strings.ToLower(strings.TrimSpace(w))] {
			matched++
		}
	}
	if len(want) == 0 {
		return 0, true
	}
	return matched, matched == len(want)
}
