package simulator

// Mesh maintenance and the control plane, mirroring gossipsub.heartbeat and
// Node.maintenance.
//
// gossipsub keeps the topic mesh at D peers: below Dlo it grafts up to D from the
// topic's other peers, above Dhi it prunes back to D and puts the pruned peer in
// graft backoff. Every heartbeat it also advertises its message cache to
// max(Dlazy, GossipFactor of the non-mesh topic peers) peers. Node.maintenance on
// the slower membership heartbeat completes the capabilities handshakes a fresh
// edge needs, asks for peer exchange while the table is thinner than
// neighbors.min, and reconciles skill documents.
//
// Every one of those actions costs bytes, and the model charges each of them with a
// frame built from the real generated messages.

import (
	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
)

// ---------- gossipsub heartbeat ----------

// serviceMesh keeps every live node's mesh at D, the way gossipsub's heartbeat does.
func (s *Sim) serviceMesh() {
	dlo, dhi := max(s.p.GS.Dlo, 1), max(s.p.GS.Dhi, s.meshD())
	for _, i := range s.live {
		nd := s.nodes[i]
		s.dropGonePeers(nd)
		s.clearBackoff(nd)
		for len(nd.mesh) > dhi {
			s.pruneOne(nd)
		}
		if len(nd.mesh) >= dlo {
			continue
		}
		s.graftTo(nd, s.meshD())
	}
}

// dropGonePeers removes mesh slots that no longer correspond to a live neighbour.
// gossipsub does the same on its next heartbeat after the connection closes.
func (s *Sim) dropGonePeers(nd *node) {
	kept := nd.mesh[:0]
	for _, p := range nd.mesh {
		if _, linked := nd.links[p]; linked && !s.nodes[p].dead {
			kept = append(kept, p)
		}
	}
	nd.mesh = kept
}

func (s *Sim) clearBackoff(nd *node) {
	for p, until := range nd.backoff {
		if s.tick >= until {
			delete(nd.backoff, p)
		}
	}
}

// graftTo fills the mesh from the topic's other peers, randomly, which is what
// gossipsub's graftMesh does with a shuffled candidate list. getPeers draws from the
// topic's peer set, which in the model is the node's link set: a node can only be in
// somebody's mesh if it is connected to them.
func (s *Sim) graftTo(nd *node, d int) {
	need := d - len(nd.mesh)
	if need <= 0 {
		return
	}
	cands := make([]int32, 0, len(nd.links))
	for p := range nd.links {
		if contains32(nd.mesh, p) {
			continue
		}
		if _, blocked := nd.backoff[p]; blocked {
			continue
		}
		if s.nodes[p].dead {
			continue
		}
		cands = append(cands, p)
	}
	if len(cands) == 0 {
		return
	}
	// The candidate list is built from a map, so its order differs every run; picking a
	// rotation of the sorted set is what makes the mesh reproducible, and rotating by the
	// beat keeps a node from grafting the same lowest-indexed peers for ever.
	cands = pickRound(cands, nd.idx, s.beat, need)
	for _, p := range cands {
		nd.mesh = append(nd.mesh, p)
		cost := graftBytes(s.topic)
		nd.charge(KindControl, 0, cost)
		s.nodes[p].charge(KindControl, 1, cost)
		s.grafts++
	}
}

// pruneOne sheds one mesh member above Dhi, charging a PRUNE with the peer-exchange
// records gossipsub attaches to it.
func (s *Sim) pruneOne(nd *node) {
	last := len(nd.mesh) - 1
	p := nd.mesh[last]
	nd.mesh = nd.mesh[:last]
	nd.backoff[p] = s.tick + int32(s.p.PruneBackoffTicks())
	px := min(s.p.GS.PrunePeers, max(len(nd.links)-1, 0))
	cost := pruneBytes(s.topic, px, s.idSize, s.signedRecordBytes())
	// A peer that is already gone cannot receive the PRUNE, so the frame does not go
	// on the wire in either direction.
	if !s.nodes[p].dead {
		nd.charge(KindControl, 0, cost)
		s.nodes[p].charge(KindControl, 1, cost)
	}
	s.prunes++
}

// emitGossip is gossipsub's IHAVE pass. The router advertises its message-cache
// window to max(Dlazy, GossipFactor × non-mesh topic peers) peers per heartbeat.
// Because every node publishes every heartbeat, the window is what makes the
// advertisement the dominant control-plane cost of a wide mesh — and its size is
// measured, not assumed.
func (s *Sim) emitGossip() {
	dlazy := max(s.p.GS.Dlazy, 1)
	for _, i := range s.live {
		nd := s.nodes[i]
		ids := s.windowIDs(i)
		if ids == 0 {
			continue
		}
		nonMesh := max(len(nd.links)-len(nd.mesh), 0)
		target := dlazy
		if f := int(s.p.GS.GossipFactor * float64(nonMesh)); f > target {
			target = f
		}
		target = min(target, nonMesh)
		if target <= 0 {
			continue
		}
		if ids > s.p.GS.MaxIHaveLength {
			ids = s.p.GS.MaxIHaveLength
		}
		cost := ihaveBytes(s.topic, s.idSize, 1, ids)
		// The advertisement goes to `target` of the topic peers outside the mesh,
		// drawn at random, which is literally what emitGossip does after
		// shufflePeers. The receivers are picked rather than counted in aggregate so
		// that every frame is charged to both endpoints: a report whose inbound and
		// outbound totals disagree is not a report.
		// The sender is billed for the advertisements it actually has a recipient
		// for: if fewer non-mesh topic peers are alive than the target, the router
		// sends fewer IHAVEs, and billing the difference would put bytes on the wire
		// that nobody receives.
		targets := s.gossipTargets(nd, target)
		nd.charge(KindControl, 0, cost*len(targets))
		for _, p := range targets {
			s.nodes[p].charge(KindControl, 1, cost)
		}
		s.ihaveFrames += int64(len(targets))
	}
}

// gossipTargets picks the IHAVE recipients: topic peers this node holds that are
// not in its mesh.
func (s *Sim) gossipTargets(nd *node, target int) []int32 {
	cands := make([]int32, 0, len(nd.links))
	for p := range nd.links {
		if contains32(nd.mesh, p) || s.nodes[p].dead {
			continue
		}
		cands = append(cands, p)
	}
	if target >= len(cands) {
		return cands
	}
	// The real router shuffles its topic peers before picking, so which peers are
	// advertised to is genuinely arbitrary. Drawing that from the run's shared stream
	// would make the mesh depend on how many draws happened earlier in the beat — a
	// property of the harness, not of the protocol — so the window is taken from a
	// rotation of the sorted candidate set, which advances with the beat and needs no
	// global state. The sort is load-bearing: candidates come out of a map, and without
	// it the chosen window moves with map order, which leaves every mesh total identical
	// (each node still advertises to exactly `target` peers) while making a single node's
	// worst heartbeat — the reported peak — irreproducible from run to run.
	return pickRound(cands, nd.idx, s.beat, target)
}

// ---------- node heartbeat ----------

// completeHandshakes is refreshCapabilities for every peer dialled since the last
// beat: one RPC round trip, then Policy.Observe(TrustKnown) — the only thing that
// makes a peer a delegation target under the shipped posture — and the routing-table
// row the verified answer carries.
func (s *Sim) completeHandshakes() {
	for _, i := range s.live {
		nd := s.nodes[i]
		for pid := range nd.pendingCaps {
			if s.nodes[pid].dead {
				delete(nd.pendingCaps, pid)
				continue
			}
			o := s.nodes[pid]
			nd.charge(KindHandshake, 0, s.sizeCapsReq)
			o.charge(KindHandshake, 1, s.sizeCapsReq)
			o.charge(KindHandshake, 0, s.sizeCapsRT)
			nd.charge(KindHandshake, 1, s.sizeCapsRT)
			delete(nd.pendingCaps, pid)
			if _, dup := nd.known[pid]; dup {
				continue
			}
			nd.known[pid] = struct{}{}
			s.handshakes++
			// Observe is keyed by the responder's id, as node.refreshCapabilities
			// does after VerifyCaps accepts the answer.
			nd.policy.Observe(o.id, security.TrustKnown)
			nd.table.Upsert(&routing.Neighbor{
				PeerID:        o.id,
				Addrs:         o.addrs,
				Skills:        append([]string(nil), o.skills...),
				Category:      routing.CatWAN,
				Version:       o.state.Version,
				Load:          o.state.Load,
				MaxPar:        o.state.MaxParallelTasks,
				Connected:     true,
				SkillsVersion: o.state.SkillsVersion,
			})
			nd.heldEpoch[pid] = o.state.SkillsVersion
		}
	}
}

// maintainNeighbors is Node.maintenance's beat work: peer exchange while the table is
// thinner than neighbors.min. The stale prune at 3x failure_timeout is the same
// horizon expireAndPrune applies to the membership view, so a peer that stops being
// heard leaves the routing table with the view entry that named it.
func (s *Sim) maintainNeighbors() {
	if s.p.Cfg.Discovery.PeerExchange {
		s.askPeerExchange()
	}
}

// askPeerExchange is the request a node makes when it has fewer connected neighbours
// than neighbors.min. Only discovery.PeerExchange being on produces it, and the
// answer is capped at peerExchangeAnswerLimit records.
func (s *Sim) askPeerExchange() {
	for _, i := range s.live {
		nd := s.nodes[i]
		if len(nd.links) >= int(s.p.Cfg.Neighbors.Min) {
			continue
		}
		// A real node asks a peer it has a table row for, so the model picks from
		// the handshaked set: an unverified neighbour would refuse the request.
		peers := make([]int32, 0, len(nd.known))
		for p := range nd.known {
			if !s.nodes[p].dead {
				peers = append(peers, p)
			}
		}
		if len(peers) == 0 {
			continue
		}
		// graftTo's reason: peers comes out of a map, so a shuffle over it is a draw
		// from an order that changes every run.
		to := pickRound(peers, nd.idx, s.beat, 1)[0]
		if until, asked := nd.pxAsked[to]; asked && s.tick-until < int32(s.p.PruneBackoffTicks()) {
			continue
		}
		nd.pxAsked[to] = s.tick
		o := s.nodes[to]
		nd.charge(KindPeerExchange, 0, s.sizePxReq)
		o.charge(KindPeerExchange, 1, s.sizePxReq)
		o.charge(KindPeerExchange, 0, s.sizePxRT)
		nd.charge(KindPeerExchange, 1, s.sizePxRT)
		s.pxAttempts++
		for _, c := range s.introduced(o, nd) {
			s.tryDial(nd, c)
			nd.noteCaps(c)
		}
	}
}

// introduced is the record list a peer-exchange answer carries: the responder's own
// handshaked peers, capped, excluding the asker.
func (s *Sim) introduced(from *node, ask *node) []int32 {
	cands := make([]int32, 0, len(from.known))
	for p := range from.known {
		if p == ask.idx || s.nodes[p].dead {
			continue
		}
		cands = append(cands, p)
	}
	// The answer is capped, and from.known is a map: taking the first ten of an
	// arbitrarily ordered slice would make the introduced set — and therefore the graph —
	// differ between runs. pickRound sorts first and rotates by (caller, beat), which is
	// also how the asker's own candidate draws are normalised.
	return pickRound(cands, from.idx, s.beat, peerExchangeAnswerLimit)
}

// reconcileSkills is node.maintenance's skill loop: a peer advertising a newer epoch
// gets a SkillsSync request for the delta. The node pays for it only when the epoch
// moved, so in a static mesh this is a periodic no-op — which the report shows as a
// zero rather than as an estimate.
func (s *Sim) reconcileSkills() {
	if !s.p.Cfg.Capabilities.SkillExchange.Enabled {
		return
	}
	perBeat := float64(s.p.Heartbeat()) / float64(s.p.GS.HeartbeatInterval)
	period := int32(max(float64(s.p.SkillSyncEveryTicks())/max(perBeat, 1), 1))
	if s.beat%period != 0 {
		return
	}
	for _, i := range s.live {
		nd := s.nodes[i]
		for pid := range nd.known {
			o := s.nodes[pid]
			if o.dead || o.state.SkillsVersion <= nd.heldEpoch[pid] {
				continue
			}
			nd.charge(KindSkillSync, 0, s.sizeSyncReq)
			o.charge(KindSkillSync, 1, s.sizeSyncReq)
			o.charge(KindSkillSync, 0, s.sizeSyncRT)
			nd.charge(KindSkillSync, 1, s.sizeSyncRT)
			nd.heldEpoch[pid] = o.state.SkillsVersion
			s.skillSyncs++
		}
	}
}

// ---------- wire sizes ----------

// signedRecordBytes is the size of the signed peer record gossipsub puts in a PRUNE's
// peer-exchange list. The router fills it from the peer's stored record, so the model
// measures the record the simulated node would actually hold: the addresses it
// announces, the id, a 64-byte signature and the public key.
func (s *Sim) signedRecordBytes() int {
	return s.pxRecordBytes
}

// bootstrapSeeds are the peers a node can be pointed at before it has any gossip
// view. config.Default() ships an empty discovery.bootstrap list, so the model boots
// a node by connecting it to a deterministic slice of the mesh rather than to an
// operator-managed peer: with no seeds at all, a 1000-node simulation would measure
// 1000 isolated nodes, which is a statement about the fixture, not about the mesh.
func (s *Sim) bootstrapSeeds() {
	seeds := int(s.p.Cfg.Neighbors.Min)
	if seeds < 2 {
		seeds = 2
	}
	for _, i := range s.live {
		nd := s.nodes[i]
		for k := 1; k <= seeds; k++ {
			p := (int(i) + k) % s.n
			if int32(p) == nd.idx || s.nodes[p].dead {
				continue
			}
			nd.links[int32(p)] = struct{}{}
			s.nodes[p].links[nd.idx] = struct{}{}
			s.edges++
		}
	}
}

// capsOf renders the signed self-description a node returns for itself.
func (s *Sim) capsOf(nd *node) *pb.Capabilities {
	return &pb.Capabilities{
		PeerId:              nd.id.String(),
		NodeName:            s.p.Cfg.Node.Name,
		Version:             nd.state.Version,
		Skills:              nd.skills,
		MaxParallelTasks:    nd.state.MaxParallelTasks,
		Load:                nd.state.Load,
		AcceptExternalTasks: s.p.Cfg.Capabilities.AcceptExternalTasks,
		AllowShell:          s.p.Cfg.Capabilities.AllowShell,
		RelayCapable:        false,
		ResourceClass:       s.p.Cfg.Capabilities.ResourceClass,
		ListenAddrs:         nd.addrs,
		Timestamp:           nd.state.Timestamp,
		Signature:           make([]byte, 64),
		SkillsVersion:       nd.state.SkillsVersion,
	}
}

// peerRecords renders the records a peer-exchange or skill-lookup answer carries.
func (s *Sim) peerRecords(n int) []*pb.PeerRecord {
	out := make([]*pb.PeerRecord, 0, n)
	for i := 0; i < n && i < s.n; i++ {
		o := s.nodes[i]
		out = append(out, &pb.PeerRecord{
			PeerId: o.id.String(),
			Addrs:  o.addrs,
			Skills: o.skills,
			SeenAt: s.secs,
		})
	}
	return out
}

// descriptors renders the skill documents a SkillsSync answer discloses.
func (s *Sim) descriptors(nd *node, docs int) []*pb.SkillDescriptor {
	out := make([]*pb.SkillDescriptor, 0, docs)
	for k := 0; k < docs && k < len(nd.skills); k++ {
		name := nd.skills[k]
		out = append(out, &pb.SkillDescriptor{
			Name:        name,
			Version:     nd.state.SkillsVersion,
			UpdatedAt:   nd.state.Timestamp,
			Digest:      "sha256:" + pad(name, 64),
			Description: pad(name, s.descBytes),
		})
	}
	return out
}

// pad is deterministic filler with the shape of real prose: a repeat of a seed
// string truncated to n bytes. Skill descriptions are what make a descriptor the
// size it is, so the model has to carry a length for them; the length is a workload
// assumption and the report says so.
func pad(seed string, n int) string {
	if n <= 0 {
		return ""
	}
	if seed == "" {
		seed = "x"
	}
	b := make([]byte, 0, n)
	for len(b) < n {
		b = append(b, seed...)
	}
	return string(b[:n])
}
