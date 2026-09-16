package simulator

// The tick loop and the series it samples.
//
// Everything a report claims about stability, discovery speed or traffic comes from
// here, so the order of the steps is part of the model: publications first (what a
// node does on its beat), then the deliveries that came due, then gossipsub's own
// heartbeat work, then the node's beat work, then tasks, then the sample. A sample
// taken before the beat's work would report the mesh as it was rather than as it is.

// Sample is one gossipsub heartbeat of mesh-wide measurements.
type Sample struct {
	Tick  int32
	Secs  int64
	Phase string

	// Mesh and view shape, averaged over the live nodes.
	MeshPeers  float64 // gossipsub mesh size
	Links      float64 // neighbours held
	Routable   float64 // peers with a verified capabilities handshake
	ViewSize   float64 // states in the membership view
	ViewTarget float64 // live nodes minus one: the complete view

	// Fresh is the share of a node's view entries that arrived within one
	// failure_timeout. A view can be complete and still stale, which is the
	// distinction heartbeat and failure_timeout exist to draw.
	Fresh float64
	// Lag is the median number of ticks a state took to reach a node that accepted
	// one on this tick.
	Lag float64

	KbitIn    float64
	KbitOut   float64
	KbitTotal float64

	Connectivity float64

	// Viewed is how many live nodes held at least one view entry on this tick. A tick
	// before the first heartbeat has been gossiped has none, and averaging it into
	// Fresh would report the harness's start-up as instability, so meanSeries skips
	// such ticks.
	Viewed float64
}

// step advances the model by one gossipsub heartbeat.
func (s *Sim) step() {
	if s.onBeat() {
		s.publishOwnStates()
		if s.onFullSync() {
			s.publishFullSyncs()
		}
	}
	s.drainDue()
	s.serviceMesh()
	s.emitGossip()
	s.ageUnwanted()
	s.advanceCache()
	// IHAVE/IWANT is gossipsub's last-resort delivery: it runs on the router's own
	// heartbeat, after the advertisements of this tick have gone out.
	s.repairUndelivered()
	if s.onBeat() {
		s.completeHandshakes()
		s.maintainNeighbors()
		s.reconcileSkills()
		s.expireAndPrune()
		s.trackRecovery()
	}
	s.tickTasks()
	s.sample()
	s.tick++
	s.secs = int64(s.tick) * s.tickSeconds()
	s.beat = s.tick / s.ticksPerBeat
}

// sample records this tick's mesh-wide series and folds the tick's byte counters
// into the phase window.
func (s *Sim) sample() {
	pi := s.phaseIdx
	var (
		mesh, links, routable, view, freshSum, lagSum, lagN float64
		freshD                                              float64
		in, out, peak                                       int64
		fullOK, freshOK                                     int
		oldestSeen                                          int32
	)
	// The mesh width is needed before the loop: a view counts as complete against the
	// number of live peers, and that has to be read once per tick, not per node.
	liveN := int32(len(s.live))
	for _, i := range s.live {
		nd := s.nodes[i]
		ni, no := nd.totalIn(), nd.totalOut()
		in += ni
		out += no
		if t := ni + no; t > peak {
			peak = t
		}
		if t := ni + no; t > nd.phasePeak[pi] {
			nd.phasePeak[pi] = t
		}
		for k := Kind(0); k < KindCount; k++ {
			nd.phaseKind[pi][k][0] += nd.acc[k][0]
			nd.phaseKind[pi][k][1] += nd.acc[k][1]
			s.wireKind[k] += nd.acc[k][0]
		}
		mesh += float64(len(nd.mesh))
		links += float64(len(nd.links))
		routable += float64(len(nd.known))
		view += float64(len(nd.view))
		// Membership.expireLoop compares a state's own timestamp against
		// failure_timeout; this is the same horizon, applied to the tick the entry
		// arrived on, because a relayed state always carries its author's clock value and
		// comparing that would measure a clock offset instead of a membership fault.
		//
		// Θ(view width) per node per tick, i.e. Θ(N²) per tick: a thousand nodes make
		// that a million cheap comparisons, which is not what dominates a run — the
		// deliveries are — so it is computed exactly every tick rather than sampled.
		horizon := s.horizonTicks
		var fresh, newest int32
		var ages int64
		for _, e := range nd.view {
			age := s.tick - e.at
			if age < horizon {
				fresh++
			}
			ages += int64(age)
			if age > newest {
				newest = age
			}
		}
		if len(nd.view) > 0 {
			ratio := float64(fresh) / float64(len(nd.view))
			freshSum += ratio
			freshD++
			lagSum += float64(ages) / float64(len(nd.view))
			lagN++
			if newest > oldestSeen {
				oldestSeen = newest
			}
			if ratio >= 0.995 {
				freshOK++
			}
		}
		if liveN > 1 && int32(len(nd.view)) >= liveN-1 {
			fullOK++
		}
		if nd.pendingDead > 0 && nd.rebuiltAt == 0 {
			dead := 0
			for pid := range nd.view {
				if s.nodes[pid].dead {
					dead++
				}
			}
			if dead == 0 {
				// One observation per node: clearing the last dead entry is the recovery
				// ТЗ 22 п.6 asks the time of, and recording it again on every later tick
				// would turn a series into a constant.
				nd.rebuiltAt = s.tick
				s.rebuildB = append(s.rebuildB,
					float64(s.tick-s.crashTick)/float64(max(int(s.ticksPerBeat), 1)))
			}
			nd.pendingDead = int32(dead)
		}
		if s.phase == PhaseIdle && len(nd.view) > nd.preCrashView {
			nd.preCrashView = len(nd.view)
		}
		for k := Kind(0); k < KindCount; k++ {
			nd.acc[k] = [2]int64{}
		}
	}
	// Crashed nodes are no longer sampled, but their accumulators are still folded so
	// every byte the model billed is attributed to a phase.
	for _, i := range s.deadNodes {
		s.foldDead(s.nodes[i])
	}
	d := float64(max(len(s.live), 1))
	// A node with no view yet is not a node with a stale view: averaging the ticks before
	// the first heartbeat has been gossiped at all would report the harness's start-up as
	// instability.
	fresh := 0.0
	if freshD > 0 {
		fresh = freshSum / freshD
	}
	// Stabilisation is a mesh-wide property, so it is measured mesh-wide rather than as
	// the luckiest node's first tick: fullOK and freshOK count the live nodes whose view
	// is complete and whose view is entirely inside the expiry horizon.
	s.sampleBeat(d, float64(fullOK), float64(freshOK))
	if oldestSeen > s.lagMax {
		s.lagMax = oldestSeen
	}
	secs := s.tickSeconds()
	conn := s.connectivity()
	switch s.phase {
	case PhaseIdle:
		s.connectIdle = conn
		s.connectBefore = conn
	case PhaseFailure:
		if s.connectMin == 0 || conn < s.connectMin {
			s.connectMin = conn
		}
		s.connectEnd = conn
	}
	// A tick on which no live node had a view at all carries no membership information:
	// recording its "freshness" as zero would report the harness's start-up as instability.
	if freshD > 0 && fresh < s.freshMin {
		s.freshMin = fresh
	}
	lag := lagSum / max(lagN, 1)
	samp := Sample{
		Tick: s.tick, Secs: s.secs, Phase: s.phase,
		MeshPeers: mesh / d, Links: links / d, Routable: routable / d,
		ViewSize: view / d, ViewTarget: d - 1,
		Fresh: fresh, Lag: lag,
		KbitIn: kbit(in, len(s.live), secs), KbitOut: kbit(out, len(s.live), secs),
		KbitTotal:    kbit(in+out, len(s.live), secs),
		Connectivity: conn,
		Viewed:       freshD,
	}
	s.samp = append(s.samp, samp)
	// phasePeak is one node's worst single tick, so it is billed for one node: scaling it
	// by the mesh width would report an average of a peak.
	s.tickPeak[pi] = max(s.tickPeak[pi], kbit(peak, 1, secs))
}

// connectivity is the share of live node pairs still joined by a path of neighbour
// links. The union-find runs over scratch buffers held on the Sim, because this is
// called once per tick and three O(N) allocations a tick would be fifteen million of
// them over a thousand-node run for no information the report uses.
func (s *Sim) connectivity() float64 {
	n := len(s.live)
	if n < 2 {
		return 1
	}
	parent := s.scratchParent
	live := s.scratchLive
	for i := range parent {
		parent[i] = int32(i)
	}
	for i := range live {
		live[i] = false
	}
	var find func(int32) int32
	find = func(x int32) int32 {
		for parent[x] != x {
			parent[x] = parent[parent[x]]
			x = parent[x]
		}
		return x
	}
	for _, i := range s.live {
		live[i] = true
	}
	for _, i := range s.live {
		for p := range s.nodes[i].links {
			if !live[p] {
				continue
			}
			a, b := find(i), find(p)
			if a != b {
				parent[a] = b
			}
		}
	}
	sizes := s.scratchSizes
	for k := range sizes {
		delete(sizes, k)
	}
	for _, i := range s.live {
		sizes[find(i)]++
	}
	var pairs int64
	for _, sz := range sizes {
		pairs += sz * (sz - 1) / 2
	}
	total := int64(n) * int64(n-1) / 2
	if total == 0 {
		return 1
	}
	return float64(pairs) / float64(total)
}

// trackRecovery arms the post-crash measurement. A node whose view still holds a
// crashed peer is being lied to by failure_timeout's slack; the tick at which the
// last such entry expires is the mesh's honest recovery time, and sample() records
// it per node.
func (s *Sim) trackRecovery() {
	if s.crashTick == 0 {
		return
	}
	for _, i := range s.live {
		nd := s.nodes[i]
		if nd.rebuiltAt != 0 {
			continue
		}
		nd.pendingDead = 1 // arm: sample() will recount and record when it clears
	}
}

// sampleBeat folds this heartbeat's mesh-wide completeness and freshness into the
// convergence times the report quotes: the first heartbeat from the start of the run at
// which the mesh's views were complete (and, separately, fresh), and the same pair
// measured from the crash. A share of nodes, not a fastest node — one lucky peer
// converging early must not read as a converged mesh.
func (s *Sim) sampleBeat(liveN, fullOK, freshOK float64) {
	if liveN <= 0 {
		return
	}
	// One node short of the whole mesh still counts: a view can lag by a beat for
	// reasons of ordering, and demanding 100 % would report the harness's quantisation.
	tol := 1.0 / liveN
	complete := fullOK >= liveN-tol-0.5
	freshening := freshOK >= liveN-tol-0.5
	if s.phase == PhaseIdle {
		if s.stabFull < 0 && complete {
			s.stabFull = s.beat
		}
		if s.stabFresh < 0 && freshening {
			s.stabFresh = s.beat
		}
		return
	}
	if s.phase != PhaseFailure || s.crashTick <= 0 {
		return
	}
	since := float64(s.beat - s.crashTick/int32(max(int(s.ticksPerBeat), 1)))
	if s.recoverFull < 0 && complete {
		s.recoverFull = since
	}
	if s.recoverFresh < 0 && freshening {
		s.recoverFresh = since
	}
}

// meanSeries averages a phase's samples.
func meanSeries(in []Sample, from, to int32) Sample {
	var acc Sample
	var k int
	// Fresh is averaged over the ticks that had a view at all, so the ramp-up of the
	// first heartbeat does not read as instability.
	var freshAcc, freshK float64
	for _, x := range in {
		if x.Tick < from || x.Tick >= to {
			continue
		}
		// Freshness is only a membership measurement where there is a view to be
		// fresh about; everything else in the sample is reported as-is.
		if x.Viewed > 0 {
			freshK++
			freshAcc += x.Fresh
		}
		k++
		acc.MeshPeers += x.MeshPeers
		acc.Links += x.Links
		acc.Routable += x.Routable
		acc.ViewSize += x.ViewSize
		acc.ViewTarget += x.ViewTarget
		acc.Fresh += x.Fresh
		acc.Lag += x.Lag
		acc.KbitIn += x.KbitIn
		acc.KbitOut += x.KbitOut
		acc.KbitTotal += x.KbitTotal
		acc.Connectivity += x.Connectivity
	}
	if k == 0 {
		return acc
	}
	f := float64(k)
	acc.MeshPeers /= f
	acc.Links /= f
	acc.Routable /= f
	acc.ViewSize /= f
	acc.ViewTarget /= f
	acc.Fresh /= f
	acc.Lag /= f
	acc.KbitIn /= f
	acc.KbitOut /= f
	acc.KbitTotal /= f
	acc.Connectivity /= f
	if freshK > 0 {
		acc.Fresh = freshAcc / freshK
	}
	return acc
}
