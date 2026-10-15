package simulator

// A run is three phases, because ТЗ 17.3 and 22 п.6 ask for three different
// questions: what the mesh costs when nothing happens, what it costs while work is
// being routed, and what happens when a tenth of it disappears.
//
// Phases are a schedule, not a mode switch: the same step function runs throughout,
// and the only thing a phase changes is whether tasks are injected and which nodes
// are alive. That matters for the report — a discontinuity in the traffic series
// then means the model did something, not that the harness did.

import (
	"fmt"
	"sort"
)

// Phases is one run's schedule, in node heartbeats.
type Phases struct {
	// IdleBeats is how long the mesh runs with no tasks at all. ТЗ 16.4's first
	// ceiling is measured here.
	IdleBeats int
	// ActiveBeats is how long every node injects work at the configured cadence.
	ActiveBeats int
	// FailureBeats is how long the mesh runs after the crash, which has to be
	// several failure timeouts or the recovery cannot be observed at all.
	FailureBeats int
	// CrashFraction is the share of nodes that disappear at the start of the
	// failure phase.
	CrashFraction float64
	// InjectEveryBeats is the per-node task cadence during the active phase. A
	// value of 3 means every node submits one task every third heartbeat.
	InjectEveryBeats int
}

// DefaultPhases is the schedule the load test runs with. The lengths are chosen so
// that each phase outlasts the mechanism it is measuring: the idle window is longer
// than a failure_timeout so expiry has settled, and the failure window is three
// failure timeouts so a view has time to be rebuilt through peer exchange.
func DefaultPhases() Phases {
	return Phases{
		IdleBeats:        40,
		ActiveBeats:      40,
		FailureBeats:     60,
		CrashFraction:    0.10,
		InjectEveryBeats: 3,
	}
}

// beats is the whole run's length in node heartbeats.
func (ph Phases) beats() int { return ph.IdleBeats + ph.ActiveBeats + ph.FailureBeats }

// Validate rejects a schedule that could not answer the question it is for.
func (ph Phases) Validate() error {
	// New() raises a schedule to the shortest length it can measure (one node
	// heartbeat per phase), so this rejects only a phase that is not a number at all.
	if ph.IdleBeats <= 0 || ph.ActiveBeats <= 0 || ph.FailureBeats <= 0 {
		return fmt.Errorf("simulator: every phase needs at least one heartbeat, got %+v", ph)
	}
	if ph.CrashFraction < 0 || ph.CrashFraction >= 1 {
		return fmt.Errorf("simulator: crash fraction must be in [0,1), got %g", ph.CrashFraction)
	}
	if ph.InjectEveryBeats <= 0 {
		return fmt.Errorf("simulator: task cadence must be positive, got %d", ph.InjectEveryBeats)
	}
	return nil
}

// Run drives the mesh through its schedule and returns the measured result.
func (s *Sim) Run() (*Result, error) {
	if err := s.ph.Validate(); err != nil {
		return nil, err
	}
	// The schedule is in node heartbeats; the loop below runs one gossipsub tick at
	// a time, so the tick count is the beat count times ticks per beat.
	total := s.ph.beats() * int(s.ticksPerBeat)
	idleEnd := s.ph.IdleBeats * int(s.ticksPerBeat)
	activeEnd := idleEnd + s.ph.ActiveBeats*int(s.ticksPerBeat)
	for t := 0; t < total; t++ {
		switch {
		case t < idleEnd:
			s.setPhase(PhaseIdle)
		case t < activeEnd:
			s.setPhase(PhaseActive)
			if t == idleEnd {
				s.injectTasks()
			}
		default:
			if t == activeEnd {
				s.crashNodes()
			}
			s.setPhase(PhaseFailure)
		}
		s.step()
	}
	r := s.summarize()
	s.res = r
	return r, nil
}

// setPhase records the phase on the tick counter so sample() can bucket by it.
func (s *Sim) setPhase(name string) {
	s.phase = name
	s.phaseIdx = phaseIndex(name)
}

// injectTasks is the active phase's workload: every node submits work at the
// configured cadence. The tasks are injected once, at the start of the phase, and
// then advance tick by tick, because the cadence is expressed in heartbeats and the
// routing loop in ticks.
func (s *Sim) injectTasks() {
	every := int32(max(s.ph.InjectEveryBeats, 1))
	for _, i := range s.live {
		for b := int32(0); b*every < int32(s.ph.ActiveBeats); b++ {
			s.due = append(s.due, s.newTask(i, b))
		}
	}
	sort.SliceStable(s.due, func(a, b int) bool {
		if s.due[a].at != s.due[b].at {
			return s.due[a].at < s.due[b].at
		}
		return s.due[a].origin < s.due[b].origin
	})
}

// crashNodes is the failure injection: a fraction of the mesh stops answering, in
// deterministic index order so the survivors and the victim set are reproducible.
//
// A crash is silent, not announced: the node sends no "I am leaving" state, so the
// only way its peers learn is that heartbeats stop and failure_timeout expires them.
// That is the mechanism ТЗ 22 п.6 asks the recovery time to be measured against.
func (s *Sim) crashNodes() {
	n := int(float64(len(s.live)) * s.ph.CrashFraction)
	if n == 0 {
		return
	}
	victims := make([]int32, len(s.live))
	copy(victims, s.live)
	s.rng.permShuffle(victims)
	// The recovery series is measured from the beat the crash happened on, so the stamp
	// goes down before anything is killed: kill() drops the victim out of the live set and
	// its peers start aging the entry immediately.
	s.crashTick = s.tick
	for _, v := range victims[:n] {
		s.kill(v)
	}
	s.crashed = n
}

// kill removes a node from the live set and from every view and link set. Its own
// publications stop, which is the only difference a silent crash has from a
// graceful one in this model — a graceful one would send a "left" state first.
func (s *Sim) kill(idx int32) {
	nd := s.nodes[idx]
	if nd.dead {
		return
	}
	nd.dead = true
	s.deadNodes = append(s.deadNodes, idx)
	s.foldDead(nd)
	// Every peer that held an edge to the victim loses it, exactly as it loses it
	// when the connection closes: the link goes, the routing-table row goes, and
	// the view entry stays behind until failure_timeout expires it. That leftover
	// is the whole question the recovery series asks.
	for p := range nd.links {
		if o := s.nodes[p]; !o.dead {
			delete(o.links, idx)
			delete(o.known, idx)
			delete(o.pendingCaps, idx)
			delete(o.pxAsked, idx)
			o.mesh = remove32(o.mesh, idx)
			o.table.Remove(nd.id)
		}
	}
	live := s.live[:0]
	for _, i := range s.live {
		if i != idx {
			live = append(live, i)
		}
	}
	s.live = live
}
