package node

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/developer3000S/zeptoclaw/internal/discovery"
	"github.com/developer3000S/zeptoclaw/internal/picoclaw"
	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/skills"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// ---------- discovery glue ----------

func (n *Node) bootstrapAddrInfos() ([]peer.AddrInfo, error) {
	var out []peer.AddrInfo
	for _, raw := range n.Cfg.Discovery.Bootstrap {
		ai, err := discovery.ParseAddrInfo(raw)
		if err != nil {
			return nil, err
		}
		if ai.ID != "" {
			out = append(out, *ai)
		}
	}
	return out, nil
}

func (n *Node) needMorePeers() bool {
	return n.Table.ConnectedCount() < n.Cfg.Neighbors.Target
}

// ---------- discovery construction ----------

func (n *Node) buildDiscovery() error {
	cfg := n.Cfg
	dlog := logging.Component(n.log, "discovery")
	wantTopic := cfg.Tasks.Forwarding.SearchRelay.Topic.Enabled
	if n.skipDiscovery {
		n.log.Info("discovery_disabled_manual_peers_only")
		if wantTopic {
			return n.buildSearchTopic()
		}
		return nil
	}
	if cfg.Discovery.LocalRegistry {
		rec := discovery.LocalRecord{
			NodeName: cfg.Node.Name,
			Addrs:    n.DiscoverableAddrs(),
			Skills:   cfg.EffectiveSkills(),
		}
		reg, err := discovery.NewLocalRegistry(cfg.Discovery.LocalSocketDir, n.Identity.PeerID(), rec,
			cfg.Discovery.Gossip.FailureTimeout.D()*2, dlog)
		if err != nil {
			n.log.Warn("local_registry_disabled", "err", err.Error())
		} else {
			n.Registry = reg
			probe, perr := discovery.NewProbe(cfg.Discovery.LocalSocketDir, n.Identity.PeerID().String(), dlog)
			if perr != nil {
				n.log.Warn("probe_socket_disabled", "err", perr.Error())
			} else {
				n.Probe = probe
				probe.SetStatus(func() any { return n.Status() })
				reg.SetSocket(probe.Path())
			}
		}
	}

	b, err := discovery.NewBootstrap(n.Host.Underlying(), cfg.Discovery.Bootstrap,
		cfg.Discovery.BootstrapInterval.D(), 10*time.Second, dlog)
	if err != nil {
		return fmt.Errorf("node: bootstrap: %w", err)
	}
	n.Bootstrap = b

	if cfg.Discovery.DHT && n.Host.DHT() != nil {
		n.DHT = discovery.NewDHT(n.Host.DHT(), cfg.EffectiveSkills(), dlog)
	}

	if cfg.Discovery.Gossip.Enabled || wantTopic {
		ps, err := discovery.NewPubSub(context.Background(), n.Host.Underlying(), n.Policy, n.Audit, dlog)
		if err != nil {
			return fmt.Errorf("node: pubsub: %w", err)
		}
		n.pubsub = ps
	}

	if cfg.Discovery.Gossip.Enabled {
		m, err := discovery.NewMembership(context.Background(), n.Host.Underlying(), n.pubsub,
			cfg.Discovery.Gossip, n.Policy, n.Audit, dlog)
		if err != nil {
			return fmt.Errorf("node: membership: %w", err)
		}
		n.Membership = m
		m.SetSelfStateFunc(n.peerState)
		m.SetOnPeer(n.onGossipPeer)
		m.SetOnExpire(n.onPeerExpire)
		m.SetRebindSource(n.rebindsToRepublish, func(k *pb.KeyRebind) { n.acceptRebind(k) })
	} else {
		n.log.Info("gossip_disabled", "hint", "peers are reachable only via discovery.bootstrap or manual dialing")
	}

	if wantTopic {
		return n.buildSearchTopic()
	}
	return nil
}

func (n *Node) buildSearchTopic() error {
	if n.pubsub == nil {
		ps, err := discovery.NewPubSub(context.Background(), n.Host.Underlying(), n.Policy, n.Audit,
			logging.Component(n.log, "discovery"))
		if err != nil {
			return fmt.Errorf("node: pubsub: %w", err)
		}
		n.pubsub = ps
	}
	st, err := discovery.NewSearchTopic(discovery.SearchTopicParams{
		Host:      n.Host.Underlying(),
		PubSub:    n.pubsub,
		Config:    n.Cfg.Tasks.Forwarding.SearchRelay.Topic,
		Signer:    security.NewSigner(n.Identity),
		KeyLookup: n.keyLookup(),
		Policy:    n.Policy,
		Audit:     n.Audit,
		View:      n.searchTopicView,
		Logger:    logging.Component(n.log, "search"),
		OnEvent:   n.observeSearchTopic,
	})
	if err != nil {
		return fmt.Errorf("node: search topic: %w", err)
	}
	n.SearchTopic = st
	return nil
}

// ---------- discovery callbacks ----------

func (n *Node) onDiscovered(ai peer.AddrInfo, category, source string) {
	if ai.ID == "" || ai.ID == n.Identity.PeerID() {
		return
	}
	if !n.Policy.AllowConnection(ai.ID) {
		n.Audit.Log(security.AuditEvent{Event: "discovery_blocked", PeerID: ai.ID.String(), Reason: "blocked", Detail: source})
		return
	}
	existing, known := n.Table.Get(ai.ID)
	if known && existing.Connected {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Host.Connect(ctx, ai); err != nil {
		n.log.Debug("dial_discovered_failed", "peer", ai.ID.String(), "source", source, "err", err.Error())
		return
	}
	n.Table.Upsert(&routing.Neighbor{
		PeerID: ai.ID, Addrs: addrsOf(ai), Category: category, Connected: true,
	})
	switch source {
	case "mdns":
		n.Metrics.DiscoveryMDNS.Inc()
	case "local":
		n.Metrics.DiscoveryLocal.Inc()
	}
	n.refreshCapabilities(ctx, ai.ID)
	n.log.Info("peer_joined", "peer", ai.ID.String(), "via", source, "category", category)
}

func (n *Node) onGossipPeer(ai peer.AddrInfo, st *pb.PeerState) {
	if ai.ID == "" || ai.ID == n.Identity.PeerID() {
		return
	}
	if !n.Policy.AllowConnection(ai.ID) {
		return
	}
	n.Table.Upsert(&routing.Neighbor{
		PeerID: ai.ID, Addrs: addrsOf(ai), Skills: append([]string(nil), st.GetSkills()...),
		Category: routing.CatWAN, Version: st.GetVersion(), Load: st.GetLoad(),
		MaxPar: st.GetMaxParallelTasks(), Connected: n.Host.IsConnected(ai.ID),
		SkillsVersion: st.GetSkillsVersion(),
	})
	if n.Host.IsConnected(ai.ID) || n.Table.ConnectedCount() >= n.Cfg.Neighbors.Max {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := n.Host.Connect(ctx, ai); err == nil {
		n.Table.SetConnected(ai.ID, true)
		n.refreshCapabilities(ctx, ai.ID)
	}
}

func (n *Node) onPeerExpire(pid peer.ID) {
	n.Table.SetConnected(pid, false)
	n.Skills.DropPeer(pid.String())
	n.log.Debug("peer_expired", "peer", pid.String())
}

// refreshCapabilities fetches and verifies a neighbour's signed capabilities.
func (n *Node) refreshCapabilities(ctx context.Context, pid peer.ID) {
	resp, err := n.Service.RPC(ctx, pid, &pb.RpcRequest{
		Kind: &pb.RpcRequest_Capabilities{Capabilities: &pb.CapabilitiesRequest{}},
	})
	if err != nil {
		n.log.Debug("caps_fetch_failed", "peer", pid.String(), "err", err.Error())
		return
	}
	caps := resp.GetCapabilities().GetCapabilities()
	if caps == nil || caps.GetPeerId() != pid.String() {
		n.Table.RecordProtocolError(pid)
		return
	}
	if err := security.VerifyCaps(caps, n.keyLookup()); err != nil {
		n.Audit.Log(security.AuditEvent{Event: "caps_bad_signature", PeerID: pid.String(), Reason: err.Error()})
		n.Metrics.Security("caps_bad_signature")
		n.Table.RecordProtocolError(pid)
		return
	}
	n.Policy.Observe(pid, security.TrustKnown)
	n.introduceMu.Lock()
	if n.handshaked == nil {
		n.handshaked = make(map[peer.ID]bool)
	}
	n.handshaked[pid] = true
	n.introduceMu.Unlock()
	n.Table.Upsert(&routing.Neighbor{
		PeerID: pid, Skills: append([]string(nil), caps.GetSkills()...),
		MaxPar: caps.GetMaxParallelTasks(), Running: caps.GetRunningTasks(),
		Load: caps.GetLoad(), Version: caps.GetVersion(),
		Connected:     n.Host.IsConnected(pid),
		SkillsVersion: caps.GetSkillsVersion(),
	})
	if len(caps.GetSkillDocs()) > 0 {
		if updated, names := n.Skills.ImportPeer(pid.String(), caps.GetSkillDocs()); updated > 0 {
			n.Metrics.SkillsImported.Add(float64(updated))
			n.Table.Upsert(&routing.Neighbor{PeerID: pid, Skills: names, Connected: n.Host.IsConnected(pid)})
			n.log.Debug("skills_imported", "peer", pid.String(), "descriptors", updated)
		}
		return
	}
	if n.Cfg.Capabilities.SkillExchange.Enabled &&
		n.Skills.PeerEpoch(pid.String(), caps.GetSkillsVersion()) {
		n.syncSkills(pid)
	}
}

// syncSkills asks one peer for the skill descriptors we do not hold.
func (n *Node) syncSkills(pid peer.ID) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := n.Service.RPC(ctx, pid, &pb.RpcRequest{Kind: &pb.RpcRequest_SkillsSync{
		SkillsSync: &pb.SkillsSyncRequest{Known: n.Skills.KnownVersions(pid.String())},
	}})
	if err != nil {
		n.log.Debug("skills_sync_failed", "peer", pid.String(), "err", err.Error())
		return
	}
	got := resp.GetSkillsSync()
	if got == nil {
		return
	}
	if err := security.VerifySkillsSync(got, pid, n.keyLookup()); err != nil {
		n.Audit.Log(security.AuditEvent{Event: "skills_sync_bad_signature", PeerID: pid.String(), Reason: err.Error()})
		n.Metrics.Security("skills_sync_bad_signature")
		n.Table.RecordProtocolError(pid)
		return
	}
	if updated, names := n.Skills.ImportPeer(pid.String(), got.GetSkills()); updated > 0 {
		n.Metrics.SkillsSynced.Inc()
		n.Metrics.SkillsImported.Add(float64(updated))
		n.Table.Upsert(&routing.Neighbor{PeerID: pid, Skills: names, Connected: n.Host.IsConnected(pid)})
		n.log.Info("skills_refreshed", "peer", pid.String(), "descriptors", updated, "epoch", got.GetSkillsVersion())
	}
}

// reconcileSkills walks neighbours whose advertised epoch outruns our view.
func (n *Node) reconcileSkills(ctx context.Context) {
	if !n.Cfg.Capabilities.SkillExchange.Enabled {
		return
	}
	for _, nb := range n.Table.List() {
		if nb.PeerID == n.ID() || !nb.Connected || nb.Left || nb.SkillsVersion <= 0 {
			continue
		}
		if !n.Skills.PeerEpoch(nb.PeerID.String(), nb.SkillsVersion) {
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		n.syncSkills(nb.PeerID)
	}
}

// ---------- skill administration (ТЗ 6.3, 13.1.1) ----------

func (n *Node) SetSkillDoc(d skills.Descriptor) (skills.Descriptor, error) {
	if !slices.ContainsFunc(n.Cfg.EffectiveSkills(), func(s string) bool {
		return strings.EqualFold(strings.TrimSpace(s), strings.TrimSpace(d.Name))
	}) {
		return skills.Descriptor{}, fmt.Errorf("node: skill %q is not advertised by this node", d.Name)
	}
	stored, changed := n.Skills.Set(d)
	if !changed {
		return stored, nil
	}
	n.Metrics.SkillsVersion.Set(float64(n.Skills.Epoch()))
	n.announceSkills()
	n.log.Info("skill_documented", "skill", stored.Name, "version", stored.Version, "epoch", n.Skills.Epoch())
	return stored, nil
}

func (n *Node) RemoveSkillDoc(name string) bool {
	n.Skills.DropDoc(name)
	n.Metrics.SkillsVersion.Set(float64(n.Skills.Epoch()))
	n.announceSkills()
	return true
}

func (n *Node) announceSkills() {
	if n.Membership != nil {
		if st := n.peerState(); st != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := n.Membership.PublishState(ctx, st); err != nil {
				n.log.Debug("skill_announce_failed", "err", err.Error())
			}
		}
	}
	if n.Registry != nil {
		_ = n.Registry.UpdateSkills(n.Cfg.EffectiveSkills(), nil)
	}
}

func (n *Node) SyncSkillsNow(ctx context.Context) int {
	queried := 0
	for _, nb := range n.Table.List() {
		if nb.PeerID == n.ID() || !nb.Connected || nb.Left {
			continue
		}
		select {
		case <-ctx.Done():
			return queried
		default:
		}
		n.syncSkills(nb.PeerID)
		queried++
	}
	return queried
}

// PeerSkillView is the operator-facing answer to "what do I believe my peers can do".
type PeerSkillView struct {
	PeerID      string              `json:"peer_id"`
	Connected   bool                `json:"connected"`
	Left        bool                `json:"left"`
	Advertised  []string            `json:"advertised_skills,omitempty"`
	Epoch       int64               `json:"skills_version"`
	Descriptors []skills.Descriptor `json:"descriptors,omitempty"`
}

// PeerSkillViews reports the learned skill view of every known peer.
func (n *Node) PeerSkillViews() []PeerSkillView {
	out := make([]PeerSkillView, 0, n.Table.Len())
	for _, nb := range n.Table.List() {
		out = append(out, PeerSkillView{
			PeerID: nb.PeerID.String(), Connected: nb.Connected, Left: nb.Left,
			Advertised: append([]string(nil), nb.Skills...), Epoch: nb.SkillsVersion,
			Descriptors: n.Skills.PeerSkills(nb.PeerID.String()),
		})
	}
	slices.SortFunc(out, func(a, b PeerSkillView) int { return strings.Compare(a.PeerID, b.PeerID) })
	return out
}

// ---------- addresses ----------

func (n *Node) DiscoverableAddrs() []string {
	ai := peer.AddrInfo{ID: n.Identity.PeerID()}
	if len(n.Cfg.Node.Announce) > 0 {
		for _, a := range n.Cfg.Node.Announce {
			if m, err := ma.NewMultiaddr(a); err == nil {
				ai.Addrs = append(ai.Addrs, m)
			}
		}
	} else {
		iface, err := n.Host.Underlying().Network().InterfaceListenAddresses()
		if err != nil {
			iface = n.Host.Underlying().Addrs()
		}
		ai.Addrs = iface
	}
	p2pAddrs, err := peer.AddrInfoToP2pAddrs(&ai)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(p2pAddrs))
	for _, a := range p2pAddrs {
		out = append(out, a.String())
	}
	return out
}

func (n *Node) LocalAddr() string {
	iface, err := n.Host.Underlying().Network().InterfaceListenAddresses()
	if err != nil || len(iface) == 0 {
		return ""
	}
	addrs := iface
	if len(n.Cfg.Node.Announce) > 0 {
		if parsed, perr := ma.NewMultiaddr(n.Cfg.Node.Announce[0]); perr == nil {
			addrs = append([]ma.Multiaddr{parsed}, iface...)
		}
	}
	for _, a := range addrs {
		comp := map[string]string{}
		var proto string
		ma.ForEach(a, func(c ma.Component) bool {
			switch c.Protocol().Code {
			case ma.P_IP4, ma.P_IP6:
				proto = "ip"
				comp["ip"] = c.Value()
			case ma.P_TCP:
				comp["tcp"] = c.Value()
			case ma.P_QUIC_V1:
				comp["quic"] = "1"
			case ma.P_UDP:
				comp["udp"] = c.Value()
			}
			return true
		})
		if proto == "" {
			continue
		}
		host := "127.0.0.1"
		if comp["ip"] != "0.0.0.0" && comp["ip"] != "::" {
			host = comp["ip"]
		}
		if comp["tcp"] != "" {
			return fmt.Sprintf("/ip4/%s/tcp/%s/p2p/%s", host, comp["tcp"], n.Identity.PeerID())
		}
		if comp["udp"] != "" && comp["quic"] == "1" {
			return fmt.Sprintf("/ip4/%s/udp/%s/quic-v1/p2p/%s", host, comp["udp"], n.Identity.PeerID())
		}
	}
	return ""
}