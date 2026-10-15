package simulator

// Membership traffic, modelled the way internal/discovery and go-libp2p-pubsub
// produce it.
//
// Three properties decide the numbers, and all three are reproduced rather than
// approximated:
//
//   - a publication goes to the author's mesh (D peers), not to everybody:
//     gossipsub's rpcs() fans out to the topic mesh, and flood publish is off in
//     the node's router, so peers outside the mesh are not sent the message;
//   - a node that has already accepted a message does not forward it again
//     (pubsub's markSeen gate in pushMsg), so a publication costs on the order of
//     one delivery per subscriber rather than one per mesh edge. A model that
//     charged every mesh peer of every receiver would report a storm the protocol
//     does not produce; a model that charged every publication to every node would
//     report a broadcast the protocol explicitly avoids;
//   - a message at or above IDontWantMessageThreshold bytes triggers IDONTWANT,
//     which stops the copies still queued for the receiver's other mesh peers. A
//     one-state heartbeat is below the threshold; a 32-state full-sync batch is
//     well above it, which is exactly why the protocol has the mechanism.
//
// Each publication is therefore walked over the real mesh graph delivery by
// delivery, and every frame is charged to the two endpoints that pay for it. The
// walk is scheduled rather than recursed: a message crosses one overlay edge per
// gossipsub heartbeat, because the router writes into a peer's outbound queue and
// the peer handles the RPC on its own beat. That is the model's only stand-in for
// channel latency, and the report says so.

import (
	"sort"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// ---------- publications ----------

// publishOwnStates is discovery.Membership.Publish for every live node: its own
// PeerState, re-rendered with a fresh timestamp, as one publication.
func (s *Sim) publishOwnStates() {
	for _, i := range s.live {
		nd := s.nodes[i]
		nd.state.Timestamp = s.secs
		payload := s.mustMarshal(&pb.MembershipGossip{
			States:     []*pb.PeerState{nd.state},
			FromPeerId: nd.idStr,
		})
		m := s.newMsg(msgOwnState, i, payload)
		m.peers = append(m.peers, i)
		m.tss = append(m.tss, nd.state.Timestamp)
		s.publish(nd, m)
	}
}

// publishFullSyncs is Membership.PublishFull for every live node: the whole view,
// newest state first, in batches of fullSyncBatchStates published heartbeat/8 apart.
// The pacing is what keeps a full sync from being a spike, and in the model it also
// decides which gossipsub heartbeat each batch's IHAVE lands in.
//
// The author's first hop is walked exactly like any other publication. What is
// charged analytically instead of walked is the batches' onward relay, and that is a
// deliberate boundary of the model, not an oversight:
//
//   - a full-sync batch carries states every node already holds from the
//     per-heartbeat own-state path, which IS walked exactly. The batch's job is to
//     refresh timestamps for a node that has been cut off from an author, not to
//     teach anything new, so ingesting it node by node cannot change the membership
//     or routing numbers the report gives;
//   - its bytes, however, are the largest single line of the traffic budget, so they
//     are charged in full: a publication reaches every subscriber exactly once, and
//     IDONTWANT is the mechanism that makes "exactly once" true for a message this
//     big (a 32-state batch is far above the 1024-byte threshold, a one-state
//     heartbeat is below it). TestFullSyncAnalyticVersusExactFlood measures how far
//     that assumption is from the walked truth at a size where walking is possible,
//     and the report quotes the number.
func (s *Sim) publishFullSyncs() {
	pause := int32(max(s.p.FullSyncBatchPauseTicks(), 1))
	for _, i := range s.live {
		nd := s.nodes[i]
		batches := s.syncBatches(nd)
		if len(batches) > s.maxBatches {
			s.maxBatches = len(batches)
		}
		for b, bt := range batches {
			m := s.newMsg(msgFullSync, i, s.syncPayload(nd, bt))
			for _, sp := range bt {
				m.peers = append(m.peers, sp.idx)
				m.tss = append(m.tss, sp.ts)
			}
			s.syncMsgs++
			s.syncStates += int64(len(bt))
			if b == 0 {
				s.publish(nd, m)
				// publish may already have left nothing in flight (every mesh peer
				// reached or IDONTWANTed), in which case the batch is done here.
				s.settleMsg(m)
				continue
			}
			// The author holds the later batches back until their pause elapses, and
			// their first hop goes out on that beat.
			s.schedule(s.tick+pause*int32(b), nd.idx, nd.idx, m)
		}
	}
}

// chargeSyncRelay books the relayed copies of one full-sync batch.
//
// The delivery tree of a publication that reaches everybody is, by definition, a
// spanning structure: L−1 receives and exactly L−1 sends, so charging one copy per
// subscriber and distributing those same copies over the nodes the first hop already
// reached — weighted by each one's measured mesh out-degree — is exact in total and
// symmetric per byte. Every copy has one sender and one receiver, which is what
// TestTrafficIsSymmetric asserts.
func (s *Sim) chargeSyncRelay(author *node, m *Msg) {
	var receivers, weight int64
	for _, i := range s.live {
		if i != author.idx && !reached(m, i) {
			receivers++
		}
	}
	if receivers == 0 {
		return
	}
	senders := make([]int32, 0, len(s.live))
	for _, i := range s.live {
		if i == author.idx || !reached(m, i) {
			continue
		}
		w := int64(max(len(s.nodes[i].mesh), 1))
		weight += w
		senders = append(senders, i)
	}
	if weight == 0 {
		return
	}
	total := receivers * int64(m.size)
	var charged int64
	for _, i := range senders {
		share := total * int64(max(len(s.nodes[i].mesh), 1)) / weight
		if share == 0 {
			continue
		}
		s.nodes[i].charge(KindFullSync, 0, int(share))
		charged += share
	}
	// Integer division can leave a few bytes unsent; the remainder goes to the first
	// sender so that the two totals stay equal rather than drifting per batch.
	if charged < total {
		s.nodes[senders[0]].charge(KindFullSync, 0, int(total-charged))
	}
	for _, i := range s.live {
		if i != author.idx && !reached(m, i) {
			s.nodes[i].charge(KindFullSync, 1, m.size)
		}
	}
}

type syncSnap struct {
	idx int32
	ts  int64
}

// syncBatches sorts and chunks a node's view exactly as PublishFull does: newest
// timestamp first, peer id breaking ties, in blocks of fullSyncBatchStates.
func (s *Sim) syncBatches(nd *node) [][]syncSnap {
	if len(nd.view) == 0 {
		return nil
	}
	idxs := make([]int32, 0, len(nd.view))
	for j := range nd.view {
		if j != nd.idx && !s.nodes[j].dead {
			idxs = append(idxs, j)
		}
	}
	sort.Slice(idxs, func(a, b int) bool {
		ta, tb := nd.view[idxs[a]].ts, nd.view[idxs[b]].ts
		if ta == tb {
			return s.nodes[idxs[a]].idStr < s.nodes[idxs[b]].idStr
		}
		return ta > tb
	})
	out := make([][]syncSnap, 0, len(idxs)/fullSyncBatchStates+1)
	for off := 0; off*fullSyncBatchStates < len(idxs); off++ {
		lo := off * fullSyncBatchStates
		hi := min(lo+fullSyncBatchStates, len(idxs))
		bt := make([]syncSnap, 0, hi-lo)
		for _, j := range idxs[lo:hi] {
			bt = append(bt, syncSnap{idx: j, ts: nd.view[j].ts})
		}
		out = append(out, bt)
	}
	return out
}

// syncPayload marshals one batch, so its cost is measured rather than assumed.
func (s *Sim) syncPayload(nd *node, bt []syncSnap) []byte {
	states := make([]*pb.PeerState, 0, len(bt))
	for _, sp := range bt {
		states = append(states, s.stateOf(s.nodes[sp.idx], sp.ts))
	}
	return s.mustMarshal(&pb.MembershipGossip{States: states, FromPeerId: nd.idStr})
}

// stateOf renders a peer's state as its holder believes it: the peer's own
// advertised content carrying the timestamp the holder accepted, which is what
// PublishFull re-sends. The message is rebuilt field by field rather than copied,
// because a protobuf message embeds a mutex and copying it is a vet error.
func (s *Sim) stateOf(o *node, ts int64) *pb.PeerState {
	return &pb.PeerState{
		PeerId:           o.state.PeerId,
		Timestamp:        ts,
		Skills:           o.skills,
		Load:             o.state.Load,
		MaxParallelTasks: o.state.MaxParallelTasks,
		Version:          o.state.Version,
		Status:           o.state.Status,
		Addrs:            o.addrs,
		SkillsVersion:    o.state.SkillsVersion,
	}
}

// newMsg builds a publication and measures its wire cost from the real generated
// message: the payload marshalled the way discovery marshals it, wrapped in the
// signed pubsub envelope, prefixed with the varint frame internal/p2p writes.
//
// The delivery bitmap comes from a pool. Every node publishes every heartbeat, so
// without pooling a 1000-node beat would allocate a thousand 128-byte bitmaps and a
// full-sync beat another thirty-one thousand of them, for storage that is dead as
// soon as the flood settles.
func (s *Sim) newMsg(kind msgKind, src int32, payload []byte) *Msg {
	s.seqNo++
	m := s.allocMsg()
	m.stamp = s.seqNo
	m.kind = kind
	m.src = src
	m.size = gossipFrameBytes(s.topic, s.nodes[src].id, payload)
	m.sent = s.tick
	m.hops = 0
	m.dups = 0
	m.queued = 0
	m.settled = false
	m.registered = false
	m.billed = false
	m.firstHop = 0
	m.reachedCount = 0
	m.peers = m.peers[:0]
	m.tss = m.tss[:0]
	return m
}

func (s *Sim) allocMsg() *Msg {
	var m *Msg
	if k := len(s.msgFree); k > 0 {
		m = s.msgFree[k-1]
		s.msgFree[k-1] = nil
		s.msgFree = s.msgFree[:k-1]
	} else {
		m = new(Msg)
	}
	// releaseMsg detaches the bitmap, so a recycled Msg always arrives without one:
	// a flood's reachability has to start zeroed, and a pooled bitmap is zeroed on the
	// way out.
	if cap(m.reached) < s.nWords {
		m.reached = s.allocBitmap()
	} else {
		m.reached = m.reached[:s.nWords]
		for i := range m.reached {
			m.reached[i] = 0
		}
	}
	return m
}

// allocBitmap hands out a zeroed reachability bitmap of the mesh's width.
func (s *Sim) allocBitmap() []uint64 {
	var b []uint64
	if k := len(s.bitmapFree); k > 0 {
		b = s.bitmapFree[k-1]
		s.bitmapFree[k-1] = nil
		s.bitmapFree = s.bitmapFree[:k-1]
	} else {
		b = make([]uint64, s.nWords)
	}
	for i := range b {
		b[i] = 0
	}
	return b
}

// releaseMsg returns a settled publication's storage to the pool. The flood is over
// within a few heartbeats, so retaining its bitmap any longer would be memory the
// answer does not use.
func (s *Sim) releaseMsg(m *Msg) {
	if m.reached == nil {
		return
	}
	s.bitmapFree = append(s.bitmapFree, m.reached)
	m.reached = nil
	s.msgFree = append(s.msgFree, m)
}

// ---------- delivery ----------

// publish is the author's side of a publication. gossipsub's rpcs() sends it to the
// author's mesh and excludes both the peer it came from and the author; for a
// locally published message those are the same node, so the fanout is the mesh.
func (s *Sim) publish(nd *node, m *Msg) {
	markReached(m, nd.idx)
	m.reachedCount = 1
	s.trackMsg(m)
	s.fanout(nd, nd.idx, m)
	if m.kind == msgFullSync && !s.exactRelay {
		s.settleMsg(m)
	}
}

// trackMsg puts a publication on the repair list so its flood is followed to the end
// and its reachability bitmap goes back to the pool. Every node publishes every
// heartbeat and a full sync adds up to thirty-one thousand batches per beat at a
// thousand nodes, so an untracked publication would either leak its bitmap or have it
// yanked out from under the copies still in flight.
func (s *Sim) trackMsg(m *Msg) {
	if m.registered {
		return
	}
	m.registered = true
	s.openMsgs = append(s.openMsgs, m)
}

// fanout books and schedules the copies nd must send of m, marking each recipient as
// reached so no later sender queues it a second copy.
//
// That marking is the model's counterpart to pubsub's markSeen gate. It suppresses
// the *forwarded* copy rather than the arriving one, which is one copy per cycle of
// the mesh graph cheaper than reality: gossipsub does deliver a duplicate to a node
// two mesh peers point at, and then declines to relay it. The difference is bounded
// by the number of triangles in a degree-D mesh, it only ever undercounts, and the
// report states it as a boundary rather than hiding it.
func (s *Sim) fanout(nd *node, from int32, m *Msg) {
	suppressed := nd.unwantedBy(m.stamp)
	for _, p := range nd.mesh {
		if p == from || p == m.src || s.nodes[p].dead {
			continue
		}
		if reached(m, p) {
			m.dups++
			s.dupSuppressions++
			continue
		}
		if len(suppressed) > 0 && contains32(suppressed, p) {
			continue
		}
		markReached(m, p)
		m.reachedCount++
		if m.firstHop == 0 {
			m.firstHop = s.tick
		}
		nd.charge(s.kindFor(nd, m), 0, m.size)
		s.schedule(s.tick+1, nd.idx, p, m)
		m.hops++
	}
}

// kindFor is the report line this node's outgoing copies of m are booked to. The
// author of a heartbeat state pays on the membership line; a node relaying somebody
// else's pays on the mesh line, which is the overhead ТЗ 16.4 bounds. A full sync is
// the author's own traffic by construction — every state in it is a peer the author
// holds — so it stays on its own line in both directions.
func (s *Sim) kindFor(nd *node, m *Msg) Kind {
	if m.kind == msgFullSync {
		return KindFullSync
	}
	if nd.idx == m.src {
		return KindOwnState
	}
	return KindRelay
}

// relayKind is the inbound counterpart of kindFor.
func relayKind(m *Msg) Kind {
	if m.kind == msgFullSync {
		return KindFullSync
	}
	return KindRelay
}

// receive is one arrival: the frame is paid for, ingested, and forwarded onwards.
// reached and markReached are the publication's own delivery bitmap. The cost is
// one bit per (message, node) — 128 bytes for a thousand-node mesh — which is what
// lets the model state, and charge, exactly one copy per subscriber.
func reached(m *Msg, i int32) bool { return m.reached[i>>6]&(1<<(uint(i)&63)) != 0 }

func markReached(m *Msg, i int32) { m.reached[i>>6] |= 1 << (uint(i) & 63) }

func (s *Sim) receive(from, to int32, m *Msg) {
	nd := s.nodes[to]
	nd.charge(relayKind(m), 1, m.size)
	s.deliveries++
	s.ingest(to, from, m)
	s.historyWindow[to][s.cacheCur]++
	if m.size >= s.p.GS.IDontWantMessageThreshold {
		s.sendIdontwant(nd, from, m)
	}
	if m.kind == msgFullSync && !s.exactRelay {
		// The relay of this batch is charged analytically by chargeSyncRelay, so
		// forwarding it here would bill the same bytes twice.
		return
	}
	s.fanout(nd, from, m)
}

// drainDue delivers everything scheduled for this tick.
func (s *Sim) drainDue() {
	for _, p := range s.drainBucket(s.tick) {
		// One outstanding copy of the publication less, whatever happens below.
		p.msg.queued--
		s.inflight--
		s.inflightBytes -= int64(p.msg.size)
		if p.to == p.from {
			// A paced PublishFull batch becoming due: the author is not sending it
			// twice nor receiving it, this is its first hop and the relay it makes
			// necessary.
			nd := s.nodes[p.to]
			s.trackMsg(p.msg)
			if nd.dead {
				// The author crashed before this batch's pause elapsed, so nothing went
				// on the wire: neither its first hop nor its relay is billed, and there
				// is no flood to repair. billed is set to keep settleMsg from charging a
				// batch nobody ever sent.
				p.msg.billed = true
				s.settleMsg(p.msg)
				continue
			}
			s.fanout(nd, p.from, p.msg)
			s.settleMsg(p.msg)
			continue
		}
		if s.nodes[p.to].dead {
			// A silent crash mid-flight: the copy left the sender (it was billed when
			// it was queued) and never arrived. That is the model's dropped traffic,
			// counted rather than assumed away — TestTrafficIsSymmetric asserts
			// Σreceived == Σsent − Σlost, where Σlost also holds what was still in
			// flight when the window closed.
			s.lostInflight += int64(p.msg.size)
			s.settleMsg(p.msg)
			continue
		}
		s.receive(p.from, p.to, p.msg)
		// An analytic batch is billed in full by chargeSyncRelay and is never
		// forwarded here, so once its last first-hop copy has been drained there is
		// nothing left to do with it and its bitmap goes back to the pool. Waiting for
		// the repair pass to notice would hold thousands of bitmaps per beat at a
		// thousand nodes.
		if p.msg.kind == msgFullSync && !s.exactRelay {
			s.settleMsg(p.msg)
		}
	}
}

// settleMsg ends a publication and returns its bitmap to the pool. It does nothing
// while copies of the publication are still in the delivery schedule, because a
// recycled Msg handed to a new flood under an old flood's still-queued pointers would
// deliver the wrong payload under a real message id.
func (s *Sim) settleMsg(m *Msg) {
	if m.settled || m.queued > 0 {
		return
	}
	m.settled = true
	// The relay of an analytic batch is billed here, once the first hop has finished,
	// rather than when the batch was built: the reached set is only complete after the
	// last first-hop copy has arrived, and billing it a beat early credits copies to
	// nodes the author never reached and misses the ones it reached late.
	if m.kind == msgFullSync && !s.exactRelay && !m.billed {
		m.billed = true
		s.chargeSyncRelay(s.nodes[m.src], m)
	}
	s.releaseMsg(m)
}

// sendIdontwant is gossipsub v1.2's suppression control: the receiver of a large
// message tells its other mesh peers to stop sending it this one.
func (s *Sim) sendIdontwant(nd *node, from int32, m *Msg) {
	cost := idontwantBytes(s.idSize, 1)
	for _, p := range nd.mesh {
		if p == from || p == m.src || s.nodes[p].dead {
			continue
		}
		nd.charge(KindControl, 0, cost)
		s.nodes[p].charge(KindControl, 1, cost)
		nd.addUnwanted(p, m.stamp, int32(s.p.IDontWantTTL()))
		s.idontFrames++
	}
}

// ageUnwanted lets every IDONTWANT timer run down by one gossipsub heartbeat.
func (s *Sim) ageUnwanted() {
	for _, nd := range s.nodes {
		nd.ageUnwanted()
	}
}

// ---------- delivery repair ----------

// repairLag is how many gossipsub heartbeats the model lets a flood run before it
// calls the message undeliverable by mesh alone. Beyond it any node that has not
// seen the message will only get it through IHAVE/IWANT, so the pull is charged
// rather than assumed away.
const repairLag = 4

// repairUndelivered is gossipsub's last-resort path. The mesh covers a fraction of
// the topic's subscribers at any instant — measured, not assumed — and the peers the
// flood missed learn about the message from an IHAVE advertisement and pull it with
// an IWANT. That costs one extra copy of the message on the wire per missed node
// plus the two control frames, and skipping it would understate the traffic of
// exactly the mechanism the report is about.
func (s *Sim) repairUndelivered() {
	kept := s.openMsgs[:0]
	for _, m := range s.openMsgs {
		if m.settled {
			continue
		}
		// A paced batch is only published when its pause elapses, so its propagation
		// clock starts then, not when the author built it.
		age := s.tick - max(m.sent, m.firstHop)
		if age < repairLag || m.queued > 0 {
			kept = append(kept, m)
			continue
		}
		// A full sync's missed subscribers are already accounted for: chargeSyncRelay
		// billed a copy to every node the first hop did not reach, which is the same
		// population this repair would pull.
		if m.kind == msgOwnState || s.exactRelay {
			s.repairOne(m)
		}
		s.settleMsg(m)
	}
	s.openMsgs = kept
}

func (s *Sim) repairOne(m *Msg) {
	iwant := iwantBytes(s.idSize, 1)
	for _, i := range s.live {
		if reached(m, i) {
			continue
		}
		nd := s.nodes[i]
		// The pull goes to a peer it holds a link to; if it has none, nothing can
		// repair this view and the message is genuinely lost for this node — a
		// partition, counted in the connectivity number rather than papered over.
		src := s.repairSource(nd, m)
		if src < 0 {
			s.unrepaired++
			continue
		}
		nd.charge(KindControl, 0, iwant)
		s.nodes[src].charge(KindControl, 1, iwant)
		s.iwantFrames++
		s.nodes[src].charge(relayKind(m), 0, m.size)
		nd.charge(relayKind(m), 1, m.size)
		markReached(m, i)
		m.reachedCount++
		s.deliveries++
		s.ingest(i, src, m)
		s.repairCopies++
	}
}

// repairSource picks a peer the node can ask: the real router answers an IWANT for a
// message it holds from its own cache, so any linked peer that holds it will do.
func (s *Sim) repairSource(nd *node, m *Msg) int32 {
	cands := make([]int32, 0, len(nd.links))
	for p := range nd.links {
		// A dead peer cannot answer an IWANT, and charging one would bill a copy nobody
		// receives: its accumulator is folded, but the pull's reply would never arrive.
		if !s.nodes[p].dead {
			cands = append(cands, p)
		}
	}
	ordered := pickRound(cands, nd.idx, s.beat, len(cands))
	for _, p := range ordered {
		if reached(m, p) {
			return p
		}
	}
	if len(ordered) > 0 {
		return ordered[0]
	}
	return -1
}

// foldDead credits a crashed node's uncounted bytes to the phase window and clears
// them, so a peer that dies between two samples does not take its inbound total with
// it. Without this, Σsend and Σrecv differ by exactly the last tick of every victim.
func (s *Sim) foldDead(nd *node) {
	pi := s.phaseIdx
	for k := Kind(0); k < KindCount; k++ {
		nd.phaseKind[pi][k][0] += nd.acc[k][0]
		nd.phaseKind[pi][k][1] += nd.acc[k][1]
		s.wireKind[k] += nd.acc[k][0]
		nd.acc[k] = [2]int64{}
	}
}

// ---------- ingestion ----------

// ingest applies a publication's assertions to a node's membership view.
//
// Two rules from discovery/gossip.go shape every membership number in the report,
// and both are honoured rather than waved away:
//
//   - a state relayed about a third party is accepted only from a peer this node
//     already trusts to route (policy.AllowDelegationTo), because otherwise the
//     table is trivially poisonable; and
//   - a state is applied only if it is no older than the one held, so a stale relay
//     can never roll a view back.
//
// A fresh state also triggers the dial attempt Node.onGossipPeer makes, bounded by
// neighbors.max, and the capabilities round trip that follows a successful one.
func (s *Sim) ingest(to, from int32, m *Msg) {
	if from == to {
		return
	}
	nd := s.nodes[to]
	if m.kind == msgOwnState {
		if m.src != to {
			s.acceptState(nd, m.src, m.tss[0], m)
			s.tryDial(nd, m.src)
			// Node.onGossipPeer handshakes the peer the state is about, once the
			// dial to it succeeded — not the peer that carried the message.
			nd.noteCaps(m.src)
		}
		return
	}
	// A full sync relays other peers' states, so the relay must be routable to us.
	// The relay's own state is self-asserted and always admissible.
	trusted := nd.policy.AllowDelegationTo(s.peerID(from))
	for k, pid := range m.peers {
		if pid == to {
			continue
		}
		if pid != m.src && !trusted {
			s.rejectedState++
			continue
		}
		s.acceptState(nd, pid, m.tss[k], m)
		s.tryDial(nd, pid)
		nd.noteCaps(pid)
	}
}

// acceptState writes one accepted state, newest wins, and records the propagation
// delay the arrival cost: the ticks between the author publishing the state and this
// node holding it. That is ТЗ 22 п.6's "скорость обнаружения", measured rather than
// asserted, and it is measured per observer because it genuinely differs per node.
func (s *Sim) acceptState(nd *node, pid int32, ts int64, m *Msg) {
	e, had := nd.view[pid]
	if !had {
		nd.view[pid] = &ventry{ts: ts, at: s.tick}
		s.rejoinEntries++
		s.recordLag(m)
		return
	}
	if e.ts > ts {
		return // monotonically apply newer state only
	}
	e.ts = ts
	e.at = s.tick
}

// recordLag keeps a bounded, deterministic sample of propagation delays.
func (s *Sim) recordLag(m *Msg) {
	if len(s.lag) >= distributionCap {
		return
	}
	s.lag = append(s.lag, float64(s.tick-m.sent))
}

// noteCaps records a linked-but-unverified peer, which is what makes the model pay
// for the capabilities RPC rather than assume it is free.
func (nd *node) noteCaps(from int32) {
	if from == nd.idx {
		return
	}
	if _, known := nd.known[from]; known {
		return
	}
	if _, linked := nd.links[from]; !linked {
		return
	}
	nd.pendingCaps[from] = struct{}{}
}

// tryDial opens an edge towards a peer this node just learned about, bounded by
// neighbors.max exactly as Node.onGossipPeer is. Both ends record the edge, because a
// real connection is held by both peers.
func (s *Sim) tryDial(nd *node, pid int32) {
	if pid == nd.idx || s.nodes[pid].dead {
		return
	}
	if _, have := nd.links[pid]; have {
		return
	}
	if len(nd.links) >= s.neighborCap() {
		s.dialCapped++
		return
	}
	nd.links[pid] = struct{}{}
	s.nodes[pid].links[nd.idx] = struct{}{}
	s.edges++
}

// neighborCap is neighbors.max, the ceiling Node.onGossipPeer enforces through
// ConnectedCount.
func (s *Sim) neighborCap() int { return int(s.p.Cfg.Neighbors.Max) }

// ---------- view expiry ----------

// forgetPeer drops a peer from the view, the trust set and the graph, on both ends
// of the edge, and from the routing table. This is what a state that stopped being
// refreshed costs: not just a view entry, but a delegation target.
func (s *Sim) forgetPeer(nd *node, pid int32) {
	delete(nd.view, pid)
	delete(nd.links, pid)
	delete(nd.known, pid)
	delete(nd.pendingCaps, pid)
	delete(nd.heldEpoch, pid)
	delete(nd.backoff, pid)
	delete(nd.pxAsked, pid)
	nd.mesh = remove32(nd.mesh, pid)
	nd.table.Remove(s.peerID(pid))
	if o := s.nodes[pid]; !o.dead {
		delete(o.links, nd.idx)
		delete(o.known, nd.idx)
		delete(o.pendingCaps, nd.idx)
		delete(o.heldEpoch, nd.idx)
		o.mesh = remove32(o.mesh, nd.idx)
		o.table.Remove(nd.id)
	}
}

// expireAndPrune is Membership.expireLoop plus Node.maintenance's PruneStale,
// sampled once per node heartbeat because that is how often the real tickers run.
//
// A peer whose believed timestamp is older than failure_timeout leaves the view, and
// with it the routing table and the link set. That is what turns a crash into a
// rebuild, and the beat on which it happens is what the recovery series measures.
func (s *Sim) expireAndPrune() {
	cutoff := s.secs - int64(s.p.FailureTimeout()/1e9)
	for _, i := range s.live {
		nd := s.nodes[i]
		for pid, e := range nd.view {
			if e.ts >= cutoff {
				continue
			}
			s.forgetPeer(nd, pid)
			s.lostEntries++
		}
	}
}

// ---------- measurement plumbing ----------

// advanceCache rotates the IHAVE window by one gossipsub heartbeat.
func (s *Sim) advanceCache() {
	s.cacheCur = (s.cacheCur + 1) % s.cacheGoss
	for _, nd := range s.nodes {
		s.historyWindow[nd.idx][s.cacheCur] = 0
	}
}

// windowIDs is how many message ids a node's IHAVE advertises: the distinct
// messages it accepted in each of the last HistoryGossip slots, which is what
// MessageCache.GetGossipIDs returns.
func (s *Sim) windowIDs(idx int32) int {
	total := 0
	for _, n := range s.historyWindow[idx] {
		total += int(n)
	}
	return min(total, s.p.GS.HistoryLength)
}
