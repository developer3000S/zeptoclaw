package node

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/discovery"
	"github.com/developer3000S/zeptoclaw/internal/logging"
	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/tasks"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// rebindMaxHeld bounds the local handover ledger so a hostile gossip source
// cannot grow our state by flooding statements — each must verify, but the set
// of distinct old ids is unbounded in principle.
const rebindMaxHeld = 4096

// rebindRepublishWindow bounds how long a statement keeps being piggybacked on
// heartbeats. It is a transport horizon, not a validity rule.
const rebindRepublishWindow = 12 * time.Hour

// rebindsToRepublish is the gossip source for handover statements. Only recent
// statements are republished: an older one is still true and still resolved
// locally, but pushing it forever would have every recipient log it as too old
// to adopt, and the propagation window that matters is the live one.
func (n *Node) rebindsToRepublish() []*pb.KeyRebind {
	return n.Rebinds.StatementsFresh(rebindRepublishWindow)
}

// keyLookup returns a function that looks up a peer's public key from the host's
// peerstore. It is used for verifying signed capabilities and skill syncs.
func (n *Node) keyLookup() security.KeyLookup {
	h := n.Host.Underlying()
	return func(p peer.ID) (ic.PubKey, error) {
		pk := h.Peerstore().PubKey(p)
		if pk == nil {
			return nil, fmt.Errorf("node: no pubkey cached for %s", p)
		}
		return pk, nil
	}
}

// searchTopicView renders what this node is willing to sign an answer with:
// every peer its combined view says covers want, plus itself when it does. The
// records carry addresses so the requester can dial them; a peer known only by
// id is still worth disclosing (the requester resolves routes via the table).
func (n *Node) searchTopicView(want []string) []*pb.PeerRecord {
	if len(want) == 0 {
		return nil
	}
	covers := func(skills []string) bool {
		set := make(map[string]bool, len(skills))
		for _, s := range skills {
			set[s] = true
		}
		for _, w := range want {
			if !set[w] {
				return false
			}
		}
		return true
	}
	self := n.Identity.PeerID().String()
	out := make([]*pb.PeerRecord, 0, 8)
	if covers(n.Cfg.EffectiveSkills()) {
		out = append(out, &pb.PeerRecord{
			PeerId: self, Addrs: n.DiscoverableAddrs(),
			Skills: n.Cfg.EffectiveSkills(), SeenAt: time.Now().UTC().Unix(),
		})
	}
	seen := map[string]bool{self: true}
	if n.Membership != nil {
		for _, st := range n.Membership.Snapshot() {
			if st.GetPeerId() == "" || seen[st.GetPeerId()] {
				continue
			}
			if !covers(st.GetSkills()) {
				continue
			}
			seen[st.GetPeerId()] = true
			out = append(out, &pb.PeerRecord{
				PeerId: st.GetPeerId(), Addrs: st.GetAddrs(),
				Skills: st.GetSkills(), SeenAt: st.GetTimestamp(),
			})
		}
	}
	for _, nb := range n.Table.List() {
		if nb.PeerID.String() == "" || seen[nb.PeerID.String()] || nb.Left {
			continue
		}
		if !covers(nb.Skills) {
			continue
		}
		seen[nb.PeerID.String()] = true
		out = append(out, &pb.PeerRecord{
			PeerId: nb.PeerID.String(), Addrs: nb.Addrs,
			Skills: nb.Skills, SeenAt: nb.LastSeen.Unix(),
		})
	}
	return out
}

// observeSearchTopic maps the topic plane's events onto metrics counters.
func (n *Node) observeSearchTopic(event string) {
	if n.Metrics == nil {
		return
	}
	switch event {
	case "requested":
		n.Metrics.SearchTopicRequests.Inc()
	case "answered":
		n.Metrics.SearchTopicAnswers.Inc()
	case "rejected":
		n.Metrics.SearchTopicRejected.Inc()
	}
}

// ---------- connection bookkeeping ----------

// installConnNotifier keeps Connected flags honest and absorbs addresses
// learned during connection setup.
func (n *Node) installConnNotifier() {
	bundle := &network.NotifyBundle{
		ConnectedF: func(_ network.Network, conn network.Conn) {
			pid := conn.RemotePeer()
			// A socket opening is not a judgement: the table holds no trust, and the
			// peer's standing comes from the policy when something asks for it.
			n.Table.Upsert(&routing.Neighbor{
				PeerID: pid, Addrs: []string{conn.RemoteMultiaddr().String()},
				Connected: true, Category: n.classify(conn),
			})
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
			n.Metrics.PeersTotal.Set(float64(n.Table.Len()))
			go n.introduce(pid)
		},
		DisconnectedF: func(_ network.Network, conn network.Conn) {
			n.Table.SetConnected(conn.RemotePeer(), false)
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
			if n.Manager != nil {
				n.Manager.OnPeerDisconnected(conn.RemotePeer())
			}
		},
	}
	n.notifee = bundle
	n.Host.Underlying().Network().Notify(bundle)
}

// introduce completes the capability handshake for a peer whose connection we
// did not dial ourselves. Without it a passively-connected node stays blind:
// it has no signed skill list and, under the limited posture, no trust
// elevation for the peer — so it can never delegate work back the way the
// task arrived. One fetch per peer is enough; gossip keeps it refreshed.
//
// "Enough" is measured per process, not per neighbour table entry: the table is
// persisted and the trust a handshake earns is not. Skipping the handshake
// because a *restored* record already lists skills left the policy with nothing
// observed for that peer, so every neighbour's tasks were refused as
// "peer not trusted for tasks" after a restart while the peer list still read
// "trusted" — the two views disagreed and only the wrong one was visible.
func (n *Node) introduce(pid peer.ID) {
	if pid == "" || pid == n.Identity.PeerID() {
		return
	}
	n.introduceMu.Lock()
	if n.handshaked[pid] || n.introducing[pid] {
		n.introduceMu.Unlock()
		return
	}
	if n.introducing == nil {
		n.introducing = make(map[peer.ID]bool)
	}
	n.introducing[pid] = true
	n.introduceMu.Unlock()
	defer func() {
		n.introduceMu.Lock()
		delete(n.introducing, pid)
		n.introduceMu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	n.refreshCapabilities(ctx, pid)

	// Handover statements ride along once the peer is known. Gossip carries them
	// too, but a mesh with discovery.gossip.enabled=false has no epidemic
	// channel at all: without this a node that joins after a rotation would keep
	// treating the successor as a stranger (and, worse, would happily accept a
	// key the rest of the network retired more than a republication window ago).
	if n.completedHandshake(pid) {
		sctx, scancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer scancel()
		if sent := n.ShareRebinds(sctx, pid); sent > 0 {
			n.log.Debug("rebinds_shared", "peer", pid.String(), "statements", sent)
		}
	}
}

// completedHandshake reports whether this process has verified a peer's
// capabilities (the condition that also earns it observed trust).
func (n *Node) completedHandshake(pid peer.ID) bool {
	n.introduceMu.Lock()
	defer n.introduceMu.Unlock()
	return n.handshaked[pid]
}

// classify labels a connection's locality: same host, same LAN, or WAN.
func (n *Node) classify(conn network.Conn) string {
	r4 := firstStringComponent(conn.RemoteMultiaddr(), ma.P_IP4)
	if r4 == "" {
		return routing.CatWAN
	}
	if r4 == "127.0.0.1" {
		return routing.CatLocal
	}
	var l4 string
	for _, a := range n.Host.Underlying().Addrs() {
		if v := firstStringComponent(a, ma.P_IP4); v != "" && v != "0.0.0.0" {
			l4 = v
			break
		}
	}
	if l4 != "" && samePrivateSubnet(l4, r4) {
		return routing.CatLAN
	}
	return routing.CatWAN
}

func firstStringComponent(m ma.Multiaddr, code int) string {
	if m == nil {
		return ""
	}
	var out string
	ma.ForEach(m, func(c ma.Component) bool {
		if c.Protocol().Code == code {
			out = c.Value()
			return false
		}
		return true
	})
	return out
}

// samePrivateSubnet compares /24 prefixes for the common RFC1918 case. It is
// a heuristic for the routing category label, not an access-control decision.
func samePrivateSubnet(a, b string) bool {
	if a == b {
		return true
	}
	if !privateIPv4(a) || !privateIPv4(b) {
		return false
	}
	trim := func(s string) string {
		for i, seen := 0, 0; i < len(s); i++ {
			if s[i] == '.' {
				seen++
				if seen == 3 {
					return s[:i]
				}
			}
		}
		return s
	}
	return trim(a) == trim(b)
}

func privateIPv4(ip string) bool {
	switch {
	case strings.HasPrefix(ip, "10."), strings.HasPrefix(ip, "192.168."):
		return true
	case strings.HasPrefix(ip, "172.16."), strings.HasPrefix(ip, "172.17."),
		strings.HasPrefix(ip, "172.18."), strings.HasPrefix(ip, "172.19."),
		strings.HasPrefix(ip, "172.2"), strings.HasPrefix(ip, "172.30."),
		strings.HasPrefix(ip, "172.31."):
		return true
	default:
		return false
	}
}

// ---------- maintenance ----------

// maintenance is the node's slow loop: local-registry sync, full gossip sync,
// peer exchange when thin, stale pruning and metric refresh.
func (n *Node) maintenance(ctx context.Context) {
	beat := n.Cfg.Discovery.Gossip.Heartbeat.D()
	t := time.NewTicker(beat)
	full := time.NewTicker(n.Cfg.Discovery.Gossip.FullSync.D())
	local := time.NewTicker(5 * time.Second)
	rtt := time.NewTicker(15 * time.Second)
	skillEvery := n.Cfg.Capabilities.SkillExchange.Interval.D()
	if skillEvery <= 0 {
		skillEvery = 30 * time.Second
	}
	skillTick := time.NewTicker(skillEvery)
	defer t.Stop()
	defer full.Stop()
	defer local.Stop()
	defer rtt.Stop()
	defer skillTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-local.C:
			n.syncLocalRegistry(ctx)
			n.syncDHT(ctx)
		case <-rtt.C:
			n.measureRTT(ctx)
		case <-skillTick.C:
			n.reconcileSkills(ctx)
			n.Metrics.SkillsVersion.Set(float64(n.Skills.Epoch()))
		case <-t.C:
			if n.Table.ConnectedCount() < n.Cfg.Neighbors.Min && n.Cfg.Discovery.PeerExchange {
				n.requestPeerExchange(ctx)
			}
			n.reapSuspects(ctx)
			n.Metrics.PeersTotal.Set(float64(n.Table.Len()))
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
		case <-full.C:
			if n.Membership == nil {
				continue
			}
			if err := n.Membership.PublishFull(ctx); err != nil {
				n.log.Debug("gossip_full_sync", "err", err.Error())
			}
		}
	}
}

// syncLocalRegistry dials co-located nodes announced in the registry.
func (n *Node) syncLocalRegistry(ctx context.Context) {
	if n.Registry == nil {
		return
	}
	recs, err := n.Registry.List()
	if err != nil {
		n.log.Debug("local_registry_list_failed", "err", err.Error())
		return
	}
	for _, r := range recs {
		if n.Table.ConnectedCount() >= n.Cfg.Neighbors.Max {
			return
		}
		pid, err := peer.Decode(r.PeerID)
		if err != nil || pid == n.Identity.PeerID() {
			continue
		}
		if nb, ok := n.Table.Get(pid); ok && nb.Connected {
			continue
		}
		ai := peer.AddrInfo{ID: pid}
		for _, a := range r.Addrs {
			if parsed, perr := discovery.ParseAddrInfo(a); perr == nil {
				ai.Addrs = append(ai.Addrs, parsed.Addrs...)
			}
		}
		if len(ai.Addrs) == 0 {
			continue
		}
		n.Table.Upsert(&routing.Neighbor{PeerID: pid, Addrs: addrsOf(ai), Category: routing.CatLocal, Skills: r.Skills})
		n.onDiscovered(ai, routing.CatLocal, "local")
		_ = ctx
	}
}

// syncDHT looks up this node's skills in the DHT and dials unseen providers.
func (n *Node) syncDHT(ctx context.Context) {
	if n.DHT == nil || !n.DHT.Enabled() || !n.Cfg.Discovery.DHT {
		return
	}
	if n.Table.ConnectedCount() >= n.Cfg.Neighbors.Target {
		return
	}
	peers, err := n.DHT.FindPeersBySkill(ctx, n.Cfg.EffectiveSkills()[:min(2, len(n.Cfg.EffectiveSkills()))], 20)
	if err != nil {
		n.log.Debug("dht_lookup_failed", "err", err.Error())
		return
	}
	for _, ai := range peers {
		if nb, ok := n.Table.Get(ai.ID); ok && nb.Connected {
			continue
		}
		n.onDiscovered(ai, routing.CatWAN, "dht")
	}
}

// requestPeerExchange asks up to two neighbours for more peers.
func (n *Node) requestPeerExchange(ctx context.Context) {
	for _, nb := range n.Table.Sample(2) {
		if !nb.Connected {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 8*time.Second)
		resp, err := n.Service.RPC(pctx, nb.PeerID, &pb.RpcRequest{
			Kind: &pb.RpcRequest_PeerExchange{PeerExchange: &pb.PeerExchangeRequest{Count: 16}},
		})
		cancel()
		if err != nil || resp == nil {
			continue
		}
		for _, rec := range resp.GetPeerExchange().GetPeers() {
			pid, err := peer.Decode(rec.GetPeerId())
			if err != nil || pid == n.Identity.PeerID() || pid == nb.PeerID {
				continue
			}
			if !n.Policy.AllowConnection(pid) {
				continue
			}
			if existing, ok := n.Table.Get(pid); ok && existing.Connected {
				continue
			}
			ai := peer.AddrInfo{ID: pid}
			for _, a := range rec.GetAddrs() {
				if parsed, perr := discovery.ParseAddrInfo(a); perr == nil {
					ai.Addrs = append(ai.Addrs, parsed.Addrs...)
				}
			}
			if len(ai.Addrs) == 0 {
				continue
			}
			n.Table.Upsert(&routing.Neighbor{PeerID: pid, Addrs: addrsOf(ai), Skills: rec.GetSkills(), Category: routing.CatWAN})
			n.log.Debug("pex_candidate", "peer", pid.String(), "from", nb.PeerID.String())
		}
	}
}

// measureRTT pings a bounded sample of connected neighbours so the routing
// score's latency term (ТЗ 6.9.2) reflects reality: an unmeasured peer keeps
// the neutral 0.5 and a stale average decays only as new samples arrive.
func (n *Node) measureRTT(ctx context.Context) {
	for _, nb := range n.Table.Sample(8) {
		if !nb.Connected {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		start := time.Now()
		resp, err := n.Service.RPC(pctx, nb.PeerID, &pb.RpcRequest{
			Kind: &pb.RpcRequest_Ping{Ping: &pb.PingRequest{Nonce: time.Now().UnixNano()}},
		})
		elapsed := time.Since(start)
		cancel()
		// A failed ping is not a task-execution failure: it must not drain the
		// peer's success-rate term, it just means we keep the old estimate.
		if err != nil || resp == nil || resp.GetPing() == nil {
			continue
		}
		n.Table.RecordRTT(nb.PeerID, elapsed)
	}
}

// suspectAfter is how long a neighbour may stay silent before this node asks it
// directly. Three heartbeats' worth of lost gossip used to be the point where
// the entry was deleted; now it is only the point where confirmation is asked.
func (n *Node) suspectAfter() time.Duration {
	return n.Cfg.Discovery.Gossip.FailureTimeout.D() * 3
}

// reapSuspects evicts silent neighbours, but only after asking them directly
// (ТЗ 6.5.3). Silence is weak evidence: gossip is best-effort, and a peer that
// is alive but quiet — a delayed pubsub batch, a dropped message, a busy
// inbound queue on our side — used to be dropped from the routing view, which
// reached the operator as "no eligible peers reachable" for tasks that had a
// perfectly good executor. A live libp2p connection makes the question cheap, so
// each suspect is pinged over it, and only those that answer nothing are removed.
//
// A probe is not proof of capacity: answering a ping costs the peer almost
// nothing. That is deliberate — the mesh protocol has no "are you free to work"
// message, and inventing one is out of scope here. The probe answers "is this
// identity reachable at all", which is the question eviction actually asks.
func (n *Node) reapSuspects(ctx context.Context) {
	suspects := n.Table.Suspects(n.suspectAfter())
	// Bound the pass: each probe may wait out its timeout, and the maintenance
	// loop also carries peer exchange and metric refresh. Oldest first, so the
	// remainder is confirmed on the next tick rather than starving the loop.
	if len(suspects) > maxProbesPerPass {
		suspects = suspects[:maxProbesPerPass]
	}
	for _, nb := range suspects {
		if nb.PeerID == n.ID() {
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		if n.probePeer(ctx, nb.PeerID) {
			// Alive: refresh the sighting so the next pass sees it as healthy.
			n.Table.Touch(nb.PeerID, n.Host != nil && n.Host.IsConnected(nb.PeerID))
			n.log.Debug("neighbor_reconfirmed", "peer", nb.PeerID.String())
			continue
		}
		if n.Membership != nil {
			n.Membership.MarkLeft(nb.PeerID)
		}
		n.Table.Remove(nb.PeerID)
		n.Skills.DropPeer(nb.PeerID.String())
		n.log.Debug("neighbor_dropped", "peer", nb.PeerID.String(), "reason", "unreachable on probe")
	}
}

// maxProbesPerPass bounds one eviction sweep so a partition cannot stall
// maintenance behind a long row of timeouts.
const maxProbesPerPass = 8

// probePeer asks one neighbour directly whether it is there. It is bounded so a
// partition cannot hang the loop, and silent on purpose: the answer decides
// eviction, and the log line belongs to the caller.
func (n *Node) probePeer(ctx context.Context, pid peer.ID) bool {
	if n.Service == nil {
		// No transport to ask over: keep the entry rather than evict on a guess.
		return true
	}
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := n.Service.RPC(pctx, pid, &pb.RpcRequest{
		Kind: &pb.RpcRequest_Ping{Ping: &pb.PingRequest{Nonce: time.Now().UnixNano()}},
	})
	return err == nil && resp != nil && resp.GetPing() != nil
}

// ---------- admin: ReloadConfig / Leave ----------

// ReloadConfig re-reads the configuration file the node started with and
// applies the section that live objects own (ТЗ 13.1.1 reload). The config
// struct itself is left untouched — components that read it without
// synchronisation keep a consistent startup snapshot — while the trust
// policy, the rate limiter and the log level are swapped through their own
// locks. The diff says honestly which sections need a restart.
func (n *Node) ReloadConfig() (config.ReloadDiff, error) {
	if n.configPath == "" {
		return config.ReloadDiff{}, errors.New("node: no config file to reload (started without -config)")
	}
	next, err := config.Load(n.configPath)
	if err != nil {
		return config.ReloadDiff{}, err
	}
	d := n.Cfg.Diff(next)

	mode, err := security.ParseMode(next.Security.TrustMode)
	if err != nil {
		return d, err
	}
	minTrust, err := security.ParseTrust(next.Security.MinTrustForTasks)
	if err != nil {
		return d, err
	}
	n.Policy.SetPosture(mode, minTrust)
	// List files merge in: entries added since startup (or edited in place)
	// take effect; existing entries are never silently dropped.
	if _, err := n.Policy.LoadPeerFile(next.Security.AllowedPeersFile, true); err != nil {
		n.log.Warn("reload_allowed_peers", "err", err.Error())
	}
	if _, err := n.Policy.LoadPeerFile(next.Security.BlockedPeersFile, false); err != nil {
		n.log.Warn("reload_blocked_peers", "err", err.Error())
	}
	if rps := next.Security.RateLimit.RequestsPerSecond; rps > 0 {
		n.Limiter.SetRate(rps, next.Security.RateLimit.Burst)
	}
	if n.levelVar != nil {
		n.levelVar.Set(logging.Level(next.Telemetry.LogLevel))
	}
	// Trigger schedules are hot: the scheduler owns a live copy of the
	// config-declared list. SetConfigTriggers validates the group before
	// swapping, so a rejected reload leaves the previous schedules running.
	if n.Scheduler != nil {
		if err := n.Scheduler.SetConfigTriggers(next.ConfigTriggers()); err != nil {
			return d, fmt.Errorf("triggers not reloaded (previous schedules stay active): %w", err)
		}
	}
	n.log.Info("config_reloaded", "hot", len(d.Hot), "requires_restart", len(d.RequiresRestart))
	if n.Audit != nil {
		n.Audit.Log(security.AuditEvent{Event: "config_reload", Detail: fmt.Sprintf("hot=%v restart=%v", d.Hot, d.RequiresRestart)})
	}
	return d, nil
}

// Leave announces departure and asks the daemon to exit (ТЗ 13.1.1
// /admin/leave). The supervisor (systemd Restart=on-failure + ExitCode=75,
// docker restart policy) starts a fresh process; the mesh learns of the
// departure from the announcement instead of a timeout.
func (n *Node) Leave(ctx context.Context) {
	if n.Membership != nil {
		if st := n.peerState(); st != nil {
			st.Status = "left"
			pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
			if err := n.Membership.PublishState(pctx, st); err != nil {
				n.log.Warn("leave_announce_failed", "err", err.Error())
			}
			pcancel()
		}
	}
	n.quitOnce.Do(func() { close(n.quit) })
}

// ---------- helpers ----------

func addrsOf(ai peer.AddrInfo) []string {
	out := make([]string, 0, len(ai.Addrs))
	for _, a := range ai.Addrs {
		out = append(out, a.String())
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------- composite skill source ----------

// skillSource is the tasks.SkillSource the manager queries: it folds gossip
// membership, DHT provider records and the neighbour table into one view and
// can reload all of them on demand (a "full refresh").
//
// Why a composite instead of the raw *discovery.Membership: Select() requires an
// exact skill match, so a peer whose capabilities were never fetched scores
// zero and is invisible. When a task needs an executor nobody nearby has, the
// only way out is to widen the view — which means pulling in DHT providers and
// peers learned from peer-exchange, not just gossip.
type skillSource struct {
	n *Node
}

func (n *Node) skillsSource() tasks.SkillSource { return &skillSource{n: n} }

// PeersBySkill returns every known peer that covers want, from any source.
func (s *skillSource) PeersBySkill(want []string) []peer.ID {
	seen := make(map[peer.ID]bool)
	out := make([]peer.ID, 0, 8)
	add := func(pid peer.ID) {
		if pid == "" || pid == s.n.Identity.PeerID() || seen[pid] {
			return
		}
		seen[pid] = true
		out = append(out, pid)
	}
	if s.n.Membership != nil {
		for _, pid := range s.n.Membership.PeersBySkill(want) {
			add(pid)
		}
	}
	// The table also holds peers gossip never mentioned (bootstraps, DHT dials,
	// peer-exchange records) whose skills were confirmed by a capabilities RPC.
	for _, c := range s.n.Table.Select(want, nil, s.n.Identity.PeerID(), 0, nil) {
		add(c.Neighbor.PeerID)
	}
	return out
}

// State resolves a peer's last advertised state across sources.
func (s *skillSource) State(pid peer.ID) (*pb.PeerState, bool) {
	if s.n.Membership != nil {
		if st, ok := s.n.Membership.State(pid); ok {
			return st, true
		}
	}
	if nb, ok := s.n.Table.Get(pid); ok && len(nb.Skills) > 0 {
		return &pb.PeerState{
			PeerId:           pid.String(),
			Timestamp:        nb.LastSeen.Unix(),
			Skills:           append([]string(nil), nb.Skills...),
			Load:             nb.Load,
			MaxParallelTasks: nb.MaxPar,
			Version:          nb.Version,
			Addrs:            append([]string(nil), nb.Addrs...),
		}, true
	}
	return nil, false
}

// Refresh reloads the whole skill view and folds the result into the neighbour
// table, so a subsequent Table.Select can actually pick the newly learned peers.
//
// Order matters: the DHT lookup is the only source that can be re-queried
// synchronously, so it runs first and its completeness decides the return value.
// Gossip is refreshed afterwards (it can only re-broadcast and wait), and peers
// learned from either source are dialed and have their signed capabilities
// fetched — that is what turns "someone claims X has this skill" into "X
// actually advertises this skill", which is the precondition for Select.
func (s *skillSource) Refresh(ctx context.Context, want []string) (int, bool) {
	if len(want) == 0 {
		return 0, false
	}
	rctx, cancel := context.WithTimeout(ctx, s.refreshBudget())
	defer cancel()

	dhtComplete := false
	if s.n.DHT != nil && s.n.DHT.Enabled() {
		providers, ok := s.n.DHT.Providers(rctx, want)
		dhtComplete = ok
		for _, ai := range providers {
			s.n.onDiscovered(ai, routing.CatWAN, "skill_relay")
		}
	}

	gossipWait := time.Duration(0)
	if s.n.Membership != nil {
		gossipWait = s.refreshBudget() / 2
		if dhtComplete {
			gossipWait = s.n.Cfg.Discovery.Gossip.Heartbeat.D()
		}
		s.n.Membership.Refresh(rctx, want, gossipWait)
	}

	// Every candidate the widened view produced must be reachable and must have
	// confirmed its skills, otherwise Select still cannot see it.
	matched := s.PeersBySkill(want)
	for _, pid := range matched {
		if !s.n.Host.IsConnected(pid) {
			if st, ok := s.State(pid); ok {
				ai := peer.AddrInfo{ID: pid}
				for _, a := range st.GetAddrs() {
					if m, err := ma.NewMultiaddr(a); err == nil {
						ai.Addrs = append(ai.Addrs, m)
					}
				}
				if len(ai.Addrs) == 0 {
					continue
				}
				if err := s.n.Host.Connect(rctx, ai); err != nil {
					continue
				}
				s.n.Table.SetConnected(pid, true)
			}
		}
		if nb, ok := s.n.Table.Get(pid); !ok || len(nb.Skills) == 0 {
			s.n.refreshCapabilities(rctx, pid)
		}
	}

	found := len(s.PeersBySkill(want))
	// Never claim completeness on a partial network view: that would let the
	// caller conclude "no executor exists" from a lookup that simply did not
	// reach far enough.
	return found, dhtComplete && s.n.Table.ConnectedCount() >= s.n.Cfg.Neighbors.Min
}

// Adopt folds peer records returned by a search relay into the neighbour table.
//
// A relay answer is a *claim* made by whoever answered the lookup, so it is not
// trusted: each record is dialed and, once connected, its signed capabilities
// are fetched (refreshCapabilities verifies the signature and only then writes
// the skills). A peer we cannot reach contributes nothing. Returns how many
// peers ended up connected with confirmed skills.
func (s *skillSource) Adopt(ctx context.Context, recs []*pb.PeerRecord) int {
	adopted := 0
	for _, r := range recs {
		if r.GetPeerId() == "" || r.GetPeerId() == s.n.Identity.PeerID().String() {
			continue
		}
		pid, err := peer.Decode(r.GetPeerId())
		if err != nil {
			continue
		}
		if !s.n.Policy.AllowConnection(pid) {
			s.n.Audit.Log(security.AuditEvent{
				Event: "search_relay_blocked", PeerID: pid.String(), Reason: "blocked", Detail: "search_relay",
			})
			continue
		}
		ai := peer.AddrInfo{ID: pid}
		for _, a := range r.GetAddrs() {
			if m, err := ma.NewMultiaddr(a); err == nil {
				ai.Addrs = append(ai.Addrs, m)
			}
		}
		// Seed the table with the advertised skills as a *hint* (so we have an
		// address to dial), then confirm them with a signed capabilities RPC.
		if len(ai.Addrs) > 0 {
			s.n.Table.Upsert(&routing.Neighbor{
				PeerID: pid, Addrs: addrsOf(ai), Category: routing.CatWAN,
				Connected: s.n.Host.IsConnected(pid),
			})
		}
		if !s.n.Host.IsConnected(pid) {
			if len(ai.Addrs) == 0 {
				continue
			}
			dctx, cancel := context.WithTimeout(ctx, 6*time.Second)
			derr := s.n.Host.Connect(dctx, ai)
			cancel()
			if derr != nil {
				continue
			}
			s.n.Table.SetConnected(pid, true)
		}
		before := s.tableSkills(pid)
		s.n.refreshCapabilities(ctx, pid)
		after := s.tableSkills(pid)
		if len(after) > 0 && (len(before) == 0 || !sameSkills(before, after)) {
			adopted++
		}
	}
	return adopted
}

// tableSkills reads the confirmed skill set for a peer from the neighbour table.
func (s *skillSource) tableSkills(pid peer.ID) []string {
	if nb, ok := s.n.Table.Get(pid); ok {
		return nb.Skills
	}
	return nil
}

func sameSkills(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, x := range a {
		set[x] = true
	}
	for _, y := range b {
		if !set[y] {
			return false
		}
	}
	return true
}

// refreshBudget bounds one Refresh: it must fit inside the task deadline that
// triggered it and stay below the RPC stream timeout of the peers we fan out to.
func (s *skillSource) refreshBudget() time.Duration {
	budget := s.n.Cfg.Tasks.Forwarding.SearchRelay.RequestTimeout.D()
	if budget <= 0 {
		budget = 8 * time.Second
	}
	if stream := s.n.Service.Timeout(); stream > 0 && budget >= stream {
		budget = stream / 2
	}
	return budget
}

// SearchTopic publishes the skills-only lookup on the epidemic search plane and
// collects candidate records until ctx expires. The records are claims — the
// caller adopts them through Adopt, which dials and verifies each peer's own
// signed capabilities before routing to it.
func (s *skillSource) SearchTopic(ctx context.Context, want []string) []*pb.PeerRecord {
	if s.n.SearchTopic == nil {
		return nil
	}
	return s.n.SearchTopic.Search(ctx, want)
}
