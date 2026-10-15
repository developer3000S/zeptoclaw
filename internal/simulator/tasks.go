package simulator

// Task routing runs on the node's own decision function, not on an approximation of
// it: candidates come from routing.Table.Select, so the peer a task is handed to is
// chosen by the shipped weights (0.40 skill, 0.25 trust, 0.20 capacity, 0.10
// latency, 0.05 success), the not-connected penalty and the trust filter. The loop
// around it mirrors tasks.Manager.routeOrigin: execute locally if this node can,
// otherwise forward to the top candidates, exclude whoever refused, retry up to
// tasks.forwarding.max_retries after the retry interval, widen the view with a
// bounded skill-lookup relay before giving up, and fail the task when the ttl runs
// out.
//
// The model has no LLM: execution takes a stated number of gossipsub heartbeats
// (Params.ExecTicks), because the offline stub adapter in config.Default() answers
// in 50 ms, a duration no beat-quantised model can resolve. The value used is
// reported rather than implied.

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/developer3000S/zeptoclaw/internal/routing"
)

// Task lifecycle states.
const (
	taskPending   = iota // waiting to be routed, or waiting for a retry
	taskExecuting        // accepted by a worker, running
	taskDone
	taskLost
)

// task is one unit of work in the model.
type task struct {
	id       int64
	origin   int32
	want     []string
	at       int32 // tick the origin injects it
	state    int
	ttl      int32
	route    []int32
	worker   int32
	started  int32
	accepted int32
	retries  int
	waiting  int32
	exclude  map[peer.ID]bool
}

// newTask builds one task at the origin. The required skill is drawn with a
// per-(node, sequence) stream, so the workload does not depend on event order, and
// the ttl is the configured default.
func (s *Sim) newTask(origin int32, seq int32) *task {
	r := newRNG(s.p.skillSeed() + int64(origin)*104729 + int64(seq)*15485863)
	want := s.p.SkillCatalog[r.intn(len(s.p.SkillCatalog))]
	return &task{
		id:      s.nextTaskID(),
		origin:  origin,
		want:    []string{want},
		at:      s.phaseTick(seq * int32(max(s.ph.InjectEveryBeats, 1))),
		state:   taskPending,
		ttl:     int32(s.p.Cfg.Tasks.DefaultTTL),
		worker:  -1,
		exclude: map[peer.ID]bool{},
	}
}

func (s *Sim) nextTaskID() int64 { s.taskSeq++; return s.taskSeq }

// phaseTick converts a beat offset inside the active phase into an absolute tick.
func (s *Sim) phaseTick(beatOffset int32) int32 {
	idleEnd := int32(max(s.ph.IdleBeats, 0))
	return (idleEnd + beatOffset) * s.ticksPerBeat
}

// tickTasks advances the routing loop by one gossipsub heartbeat.
func (s *Sim) tickTasks() {
	s.startDueTasks()
	for _, t := range s.tasks {
		switch t.state {
		case taskExecuting:
			s.runningTask(t)
		case taskPending:
			if t.waiting <= s.tick {
				s.routeTask(t)
			}
		}
	}
	s.tasks = s.compactTasks(s.tasks)
}

// startDueTasks admits the tasks whose injection tick has arrived.
func (s *Sim) startDueTasks() {
	kept := s.due[:0]
	for _, t := range s.due {
		if t.at > s.tick {
			kept = append(kept, t)
			continue
		}
		if s.nodes[t.origin].dead {
			continue
		}
		s.nodes[t.origin].tasksInjected++
		s.tasksInjected++
		s.tasks = append(s.tasks, t)
	}
	s.due = kept
}

// runningTask waits for the worker to finish and books the result on the way back.
//
// A worker that dies mid-flight is one of the failure modes ТЗ 22 п.6 asks about:
// the envelope was accepted, the real manager would notice on its attempt timeout,
// and the task is lost.
func (s *Sim) runningTask(t *task) {
	w := s.nodes[t.worker]
	if w.dead {
		s.loseTask(t, "worker crashed")
		return
	}
	// The real manager admits a task it can execute regardless of how busy it is —
	// canExecute is a skill and constraint test, not a capacity test — and the task
	// then waits for one of tasks.max_parallel_tasks slots. The queueing delay is
	// part of the routing time the report gives, which is why it is measured from
	// injection rather than from acceptance.
	if t.started < 0 {
		if w.inUse >= int32(s.p.Cfg.Tasks.MaxParallelTasks) {
			return
		}
		w.inUse++
		t.started = s.tick
	}
	if s.tick < t.started+int32(max(s.p.ExecTicks, 1)) {
		return
	}
	s.releaseSlot(t.worker)
	nd := s.nodes[t.origin]
	if t.worker == t.origin {
		nd.tasksLocal++
		s.tasksLocal++
	} else {
		nd.tasksRouted++
		s.tasksRouted++
	}
	// The result travels back along the reversed route stack, one copy per hop —
	// TaskResult's own comment says exactly that — so a relay pays relay-sized
	// traffic for somebody else's answer.
	for k := len(t.route) - 1; k >= 1; k-- {
		from, to := s.nodes[t.route[k]], s.nodes[t.route[k-1]]
		if from.dead {
			// The return path is broken at this hop: the copy the deeper hop sent is
			// already accounted for, and nothing further goes on the wire.
			break
		}
		from.charge(KindTask, 0, s.sizeResult)
		if to.dead {
			// Sent and never received — the answer was on its way to a node that
			// crashed. That is loss, and it is billed as loss so that the sender's
			// bytes are not left unpaired.
			s.lostInflight += int64(s.sizeResult)
			break
		}
		to.charge(KindTask, 1, s.sizeResult)
	}
	t.state = taskDone
	s.completions++
	s.routeHops = append(s.routeHops, float64(len(t.route)-1))
	s.routeSecs = append(s.routeSecs, float64(s.tick-t.at)*float64(s.tickSeconds()))
}

// routeTask is one attempt of routeOrigin's decision loop.
func (s *Sim) routeTask(t *task) {
	nd := s.nodes[t.origin]
	if nd.dead {
		s.loseTask(t, "origin crashed")
		return
	}
	// canExecute: the origin's own advertised skills satisfy the request.
	// canExecute: the origin's own advertised skills satisfy the request. Capacity
	// does not exclude it — the manager queues the task — so the local path is
	// modelled the same way the remote one is, with started < 0 meaning "queued".
	if skillsMatch(nd.skills, t.want) {
		t.worker = t.origin
		t.route = []int32{t.origin}
		t.started, t.accepted = -1, s.tick
		t.state = taskExecuting
		return
	}
	if t.ttl <= 1 {
		s.loseTask(t, "ttl exhausted")
		return
	}
	fw := s.p.Cfg.Tasks.Forwarding
	cands := nd.table.Select(t.want, nd.policy, nd.id, fw.MaxFanout, t.exclude)
	if len(cands) == 0 {
		if s.searchRelay(t, nd) {
			cands = nd.table.Select(t.want, nd.policy, nd.id, fw.MaxFanout, t.exclude)
		}
		if len(cands) == 0 {
			nd.tasksNoWorker++
			s.tasksNoWorker++
			s.loseTask(t, "no eligible peers reachable")
			return
		}
	}
	// The real manager asks up to max_parallel_candidates at once and the first
	// acceptance wins. Select is deterministic, so the top-ranked candidate is the
	// same peer a live node would hand the task to; the model charges the envelope
	// to everyone it would have been offered to and accepts the best available one.
	parallel := min(fw.MaxParallelCandidates, len(cands))
	for _, c := range cands[:parallel] {
		w := s.byID(c.Neighbor.PeerID)
		if w < 0 {
			t.exclude[c.Neighbor.PeerID] = true
			continue
		}
		nd.charge(KindTask, 0, s.sizeEnvelope)
		worker := s.nodes[w]
		worker.charge(KindTask, 1, s.sizeEnvelope)
		if worker.dead {
			// A refusal costs the rejection ack, which is a real round trip.
			worker.charge(KindTask, 0, s.sizeAck)
			nd.charge(KindTask, 1, s.sizeAck)
			t.exclude[c.Neighbor.PeerID] = true
			continue
		}
		t.worker = w
		t.route = []int32{t.origin, w}
		t.ttl--
		// The envelope crosses one stream hop per modelled hop interval; the worker
		// acknowledges immediately, which is the TaskAck the origin waits for.
		t.started = s.tick + int32(max(s.p.HopTicks, 1)) - 1
		t.accepted = t.started + 1
		t.state = taskExecuting
		worker.charge(KindTask, 0, s.sizeAck)
		nd.charge(KindTask, 1, s.sizeAck)
		return
	}
	s.retryOrFail(t)
}

// retryOrFail is routeOrigin's retry ladder: exclude whoever refused, wait the
// configured interval, and give up after max_retries.
func (s *Sim) retryOrFail(t *task) {
	fw := s.p.Cfg.Tasks.Forwarding
	if t.retries >= fw.MaxRetries {
		s.loseTask(t, "no eligible peers reachable")
		return
	}
	t.retries++
	t.ttl--
	if t.ttl <= 1 {
		s.loseTask(t, "ttl exhausted")
		return
	}
	interval := int32(fw.RetryInterval.D() / time.Duration(s.tickSeconds()) / time.Second)
	t.waiting = s.tick + max(interval, 1)
}

// searchRelay widens a thin view the way tasks.Manager.searchRelay does: a bounded
// fan-out of skill-lookup RPCs through peers this node can route to, to the
// configured depth. It reports whether the view now holds somebody with the skill.
func (s *Sim) searchRelay(t *task, nd *node) bool {
	sr := s.p.Cfg.Tasks.Forwarding.SearchRelay
	if !sr.Enabled {
		return false
	}
	peers := make([]int32, 0, len(nd.known))
	for p := range nd.known {
		if !s.nodes[p].dead {
			peers = append(peers, p)
		}
	}
	if len(peers) == 0 {
		return false
	}
	// The real manager samples its fan-out targets randomly; peers was built from a
	// map, so a shuffle over it is a draw from an order that changes every run.
	peers = pickRound(peers, nd.idx, s.beat, len(peers))
	found := false
	for depth := 0; depth < sr.MaxDepth && !found; depth++ {
		target := peers[:min(sr.Fanout, len(peers))]
		for _, p := range target {
			w := s.nodes[p]
			nd.charge(KindSkillSync, 0, s.sizePxReq)
			w.charge(KindSkillSync, 1, s.sizePxReq)
			w.charge(KindSkillSync, 0, s.sizeLookupRT)
			nd.charge(KindSkillSync, 1, s.sizeLookupRT)
			s.lookups++
			for _, o := range s.widened(w, nd, t.want) {
				// Adoption gives this node a view entry and a table row, but not
				// yet a delegation target: under the shipped posture the peer must
				// be dialled and handshaked first, which the next beat's
				// completeHandshakes pays for.
				s.tryDial(nd, o)
				nd.noteCaps(o)
				s.upsertRow(nd, o)
				if skillsMatch(s.nodes[o].skills, t.want) {
					found = true
				}
			}
		}
		if len(peers) <= len(target) {
			break
		}
		peers = peers[len(target):]
	}
	return found
}

// widened lists the peers an answer introduces that could serve the request.
func (s *Sim) widened(from *node, ask *node, want []string) []int32 {
	cands := make([]int32, 0, 8)
	for p := range from.view {
		if p == ask.idx || s.nodes[p].dead || ask.knows(p) {
			continue
		}
		if !skillsMatch(s.nodes[p].skills, want) {
			continue
		}
		cands = append(cands, p)
	}
	// The answer is capped, and from.view is a map: without the sort the cap would keep an
	// arbitrary ten candidates, so which worker a search found would depend on map order.
	// Same normalisation as introduced.
	return pickRound(cands, from.idx, s.beat, peerExchangeAnswerLimit)
}

// loseTask ends a task without an answer.
func (s *Sim) loseTask(t *task, reason string) {
	if t.state == taskLost || t.state == taskDone {
		return
	}
	if t.state == taskExecuting && t.worker >= 0 && t.started >= 0 && !s.nodes[t.worker].dead {
		s.releaseSlot(t.worker)
	}
	t.state = taskLost
	if !s.nodes[t.origin].dead {
		s.nodes[t.origin].tasksLost++
	}
	s.tasksLost++
	s.lostReasons[reason]++
}

// releaseSlot frees one of a worker's execution slots.
func (s *Sim) releaseSlot(idx int32) {
	if nd := s.nodes[idx]; nd.inUse > 0 {
		nd.inUse--
	}
}

func (s *Sim) compactTasks(a []*task) []*task {
	out := a[:0]
	for _, t := range a {
		if t.state != taskDone {
			out = append(out, t)
		}
	}
	for k := len(out); k < len(a); k++ {
		a[k] = nil
	}
	return out
}

// byID resolves a peer id back to its simulated index, or -1.
func (s *Sim) byID(p peer.ID) int32 {
	if idx, ok := s.byPeer[p]; ok {
		return idx
	}
	return -1
}

// knows reports whether this node already holds a peer as routable.
func (nd *node) knows(pid int32) bool {
	_, ok := nd.known[pid]
	return ok
}

// skillsMatch is routing's own rule, restated only because the package keeps it
// unexported: a peer advertising "general" satisfies any request, otherwise every
// required skill must be advertised. TestSkillsMatchMatchesRouting pins it against
// the real function's observable behaviour through Table.Select.
func skillsMatch(have, want []string) bool {
	set := make(map[string]bool, len(have))
	for _, s := range have {
		set[s] = true
	}
	if set["general"] || set["any"] {
		return true
	}
	if len(want) == 0 {
		return false
	}
	for _, w := range want {
		if !set[w] {
			return false
		}
	}
	return true
}

// upsertRow is the table write a verified capabilities answer performs; the search
// relay needs it because an adopted peer becomes selectable before it is dialled.
func (s *Sim) upsertRow(nd *node, pid int32) {
	o := s.nodes[pid]
	nd.table.Upsert(&routing.Neighbor{
		PeerID:        o.id,
		Addrs:         o.addrs,
		Skills:        append([]string(nil), o.skills...),
		Category:      routing.CatWAN,
		Version:       o.state.Version,
		MaxPar:        o.state.MaxParallelTasks,
		Connected:     true,
		SkillsVersion: o.state.SkillsVersion,
	})
}
