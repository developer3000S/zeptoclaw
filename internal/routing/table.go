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

	"github.com/zeptoclaw/zeptomesh/internal/config"
	"github.com/zeptoclaw/zeptomesh/internal/security"
	"github.com/zeptoclaw/zeptomesh/internal/storage"
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
	Trust     security.Trust
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
	// Left marks a peer that announced a graceful departure.
	Left bool
}

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
}

// NewTable builds an empty table.
func NewTable(cfg config.NeighborsConfig, st *storage.Store, logger *slog.Logger) *Table {
	return &Table{
		neis: make(map[peer.ID]*Neighbor),
		cfg:  cfg,
		log:  logger,
		st:   st,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Upsert merges an observation into the table, preserving accumulated metrics.
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
	cur.Running = n.Running
	cur.Load = n.Load
	cur.Connected = n.Connected
	cur.Left = n.Left
	cur.LastSeen = time.Now().UTC()
	// Trust only moves up through observation; operator lists win in TrustOf.
	if n.Trust < cur.Trust {
		cur.Trust = n.Trust
	}
	t.persistLocked(cur)
	return cur
}

// SetConnected records connection state.
func (t *Table) SetConnected(p peer.ID, connected bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n, ok := t.neis[p]
	if !ok {
		n = &Neighbor{PeerID: p, Trust: security.TrustUntrusted}
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

// RecordSuccess credits a peer with a completed task.
func (t *Table) RecordSuccess(p peer.ID) { t.record(p, true) }

// RecordFailure debits a peer.
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
		n = &Neighbor{PeerID: p, Trust: security.TrustUntrusted}
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
		PeerID:         n.PeerID.String(),
		Addrs:          append([]string(nil), n.Addrs...),
		Skills:         append([]string(nil), n.Skills...),
		Trust:          n.Trust.String(),
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
		trust, err := security.ParseTrust(r.Trust)
		if err != nil {
			trust = security.TrustUntrusted
		}
		t.neis[pid] = &Neighbor{
			PeerID:    pid,
			Addrs:     append([]string(nil), r.Addrs...),
			Skills:    append([]string(nil), r.Skills...),
			Category:  r.Category,
			Trust:     trust,
			Version:   r.Version,
			Load:      r.Load,
			MaxPar:    r.MaxParallel,
			Running:   r.Running,
			Successes: r.Successes,
			Failures:  r.Failures,
			ProtoErrs: r.ProtocolErrors,
			RTT:       time.Duration(r.AvgRTTMillis) * time.Millisecond,
			LastSeen:  time.Unix(r.SeenAt, 0).UTC(),
			Connected: false, // connections never survive a restart
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
	t.mu.RLock()
	snap := make([]*Neighbor, 0, len(t.neis))
	for _, n := range t.neis {
		snap = append(snap, n)
	}
	t.mu.RUnlock()

	cands := make([]Candidate, 0, len(snap))
	for _, n := range snap {
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
		score := skillScore*wSkill +
			trustScore(n.Trust)*wTrust +
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
				fmt.Sprintf("trust=%s", n.Trust),
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
