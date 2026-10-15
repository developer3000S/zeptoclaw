package node

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/developer3000S/zeptoclaw/internal/discovery"
	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
)

// ScanRequest parameterises one environment sweep. An empty Scopes probes
// every enabled source; otherwise "local", "subnet", "wan" select the layers.
type ScanRequest struct {
	// Scopes selects which sources to probe: "", "local", "subnet", "wan".
	// Empty (or containing "all") probes every enabled source.
	Scopes []string
	// MaxCandidates caps the peers dialed in one pass; 0 = config default.
	MaxCandidates int
	// Timeout bounds the whole pass; 0 = config default.
	Timeout time.Duration
}

// ScanAgent is one agent the sweep found on the wire.
type ScanAgent struct {
	PeerID    string   `json:"peer_id"`
	Addrs     []string `json:"addrs,omitempty"`
	Skills    []string `json:"skills,omitempty"`
	Source    string   `json:"source"`
	Connected bool     `json:"connected"`
	Verified  bool     `json:"verified"`
}

// ScanResult is the operator-facing report of a scan pass.
type ScanResult struct {
	RequestedAt time.Time      `json:"requested_at,omitempty"`
	Duration    string         `json:"duration,omitempty"`
	Probed      int            `json:"addresses_probed"`
	Candidates  int            `json:"candidates"`
	Connected   int            `json:"connected"`
	Verified    int            `json:"verified"`
	Agents      []ScanAgent    `json:"agents"`
	Errors      []string       `json:"errors,omitempty"`
	BySource    map[string]int `json:"by_source,omitempty"`
}

// ScanNetwork probes the environment for other agents and establishes contact
// with those it finds: each candidate is dialed, its signed capabilities are
// verified and the peer enters the neighbour table, exactly like every other
// discovery path — so a scanned agent becomes a working, task-routable
// neighbour. It is the manual (CLI/admin API) and the shared background path.
//
// The sweep is a superset of the passive maintenance loop: it forces the local
// registry and mDNS, optionally walks the LAN subnet, and re-queries the
// external finders (DHT, peer-exchange, bootstrap, search topic) instead of
// waiting for their next slow tick.
func (n *Node) ScanNetwork(ctx context.Context, req ScanRequest) ScanResult {
	start := time.Now()
	if n.Host == nil {
		return ScanResult{RequestedAt: start, Errors: []string{"node: transport not ready"},
			Duration: time.Since(start).Round(time.Millisecond).String()}
	}

	cfg := n.Cfg.Discovery.Scan
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = cfg.Timeout.D()
	}
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	dialTimeout := cfg.DialTimeout.D()
	if dialTimeout <= 0 {
		dialTimeout = 6 * time.Second
	}
	maxCand := req.MaxCandidates
	if maxCand <= 0 {
		maxCand = cfg.MaxCandidates
	}
	// Guarded so a zero config default still leaves the sweep sane.
	if maxCand < 0 {
		maxCand = cfg.MaxCandidates
	}
	if maxCand <= 0 {
		maxCand = 64
	}

	res := ScanResult{RequestedAt: start, BySource: map[string]int{}}
	sctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	want := n.Cfg.EffectiveSkills()
	scanAll := len(req.Scopes) == 0
	doLocal := scanAll || slices.Contains(req.Scopes, "local")
	doSubnet := cfg.SubnetScan && (scanAll || slices.Contains(req.Scopes, "subnet"))
	doWAN := scanAll || slices.Contains(req.Scopes, "wan")

	// Tally connected peers and handshaked ones at entry so the per-source
	// counts and the final connected/verified totals stay truthful (a peer we
	// already knew is still "connected", just not newly dialed by this pass).
	preConnected := n.Table.ConnectedCount()

	// Local host: co-located nodes in the registry, then mDNS sightings.
	if doLocal {
		if n.Registry != nil {
			recs, err := n.Registry.List()
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("local: %v", err))
			} else {
				for _, r := range recs {
					if ai, ok := n.scanRegistryAddrInfo(r.PeerID, r.Addrs); ok {
						n.scanAdopt(sctx, &res, ai, "local", dialTimeout)
					}
				}
			}
		}
		if n.MDNS != nil {
			for _, pid := range n.MDNS.Recent(time.Hour) {
				if ai := n.scanPeerAddrInfo(pid); addrInfoValid(ai) {
					n.scanAdopt(sctx, &res, ai, "mdns", dialTimeout)
				}
			}
		}
	}

	// LAN subnet sweep: the only truly active probe. Off by default.
	if doSubnet {
		subnets := discovery.PrivateSubnets()
		ports := discovery.MeshTCPPorts(n.Cfg.Node.Listen)
		found, probed := discovery.SubnetSweep(sctx, n.Host.Underlying(), subnets, ports,
			n.Identity.PeerID(), dialTimeout, maxCand)
		res.Probed += probed
		for _, ai := range found {
			// A subnet hit carries no peer id (libp2p ids are self-certifying,
			// so nothing can dial a bare address). Try to attribute the open
			// port to a peer already in the table; otherwise report the address
			// as an unresolved candidate the id-bearing sources must still name.
			if pid, ok := n.peerByAddress(ai.Addrs); ok {
				ai.ID = pid
				n.scanAdopt(sctx, &res, ai, "subnet", dialTimeout)
				continue
			}
			n.recordCandidate(&res, ai, "subnet")
		}
	}

	// External (Internet): DHT, peer-exchange, bootstrap, search topic.
	if doWAN {
		if n.DHT != nil && n.DHT.Enabled() && len(want) > 0 {
			count := 32
			if count > maxCand {
				count = maxCand
			}
			peers, err := n.DHT.FindPeersBySkill(sctx, want[:min(2, len(want))], count)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("dht: %v", err))
			} else {
				for _, ai := range peers {
					n.scanAdopt(sctx, &res, ai, "dht", dialTimeout)
				}
			}
		}
		if n.Membership != nil {
			before := n.Table.ConnectedCount()
			n.requestPeerExchange(sctx)
			if d := n.Table.ConnectedCount() - before; d > 0 {
				res.BySource["pex"] += d
			}
		}
		if n.Bootstrap != nil {
			before := n.Table.ConnectedCount()
			n.Bootstrap.DialAll(sctx)
			if d := n.Table.ConnectedCount() - before; d > 0 {
				res.BySource["bootstrap"] += d
			}
		}
		if n.SearchTopic != nil {
			recs := n.SearchTopic.Search(sctx, want)
			if adopted := n.skillsSource().Adopt(sctx, recs); adopted > 0 {
				res.BySource["search_topic"] += adopted
			}
		}
	}

	// Reconcile the report against the final table state.
	res.Candidates = len(res.Agents)
	res.Connected = n.Table.ConnectedCount()
	res.Verified = n.countVerified()
	_ = preConnected
	res.Duration = time.Since(start).Round(time.Millisecond).String()
	sort.Slice(res.Agents, func(i, j int) bool { return res.Agents[i].PeerID < res.Agents[j].PeerID })
	return res
}

// scanRegistryAddrInfo builds a dialable AddrInfo from a local registry record.
func (n *Node) scanRegistryAddrInfo(pidStr string, addrs []string) (peer.AddrInfo, bool) {
	pid, err := peer.Decode(pidStr)
	if err != nil || pid == n.Identity.PeerID() {
		return peer.AddrInfo{}, false
	}
	ai := peer.AddrInfo{ID: pid}
	for _, a := range addrs {
		if parsed, perr := discovery.ParseAddrInfo(a); perr == nil {
			ai.Addrs = append(ai.Addrs, parsed.Addrs...)
		}
	}
	return ai, len(ai.Addrs) > 0
}

// scanPeerAddrInfo builds an AddrInfo from the peerstore for a known peer id.
func (n *Node) scanPeerAddrInfo(pid peer.ID) peer.AddrInfo {
	return peer.AddrInfo{ID: pid, Addrs: n.Host.Underlying().Peerstore().Addrs(pid)}
}

func addrInfoValid(ai peer.AddrInfo) bool { return ai.ID != "" && len(ai.Addrs) > 0 }

// recordCandidate adds a found agent to the report without dialing it — used
// for candidates that carry no peer id, which nothing can dial yet. The report
// is still useful to the operator: it names the addresses where a mesh port is
// open, and the id-bearing sources can adopt the peer once it announces.
func (n *Node) recordCandidate(res *ScanResult, ai peer.AddrInfo, source string) {
	if len(ai.Addrs) == 0 {
		return
	}
	// De-duplicate by address: an address-only candidate has no peer id, so
	// the address is its identity for the report.
	key := ""
	for _, a := range ai.Addrs {
		key = a.String()
		break
	}
	for i := range res.Agents {
		for _, a := range res.Agents[i].Addrs {
			if a == key {
				return
			}
		}
	}
	agent := ScanAgent{PeerID: key, Source: source}
	for _, a := range ai.Addrs {
		agent.Addrs = append(agent.Addrs, a.String())
	}
	res.Agents = append(res.Agents, agent)
	res.BySource[source]++
}

// peerByAddress looks for a known neighbour whose recorded addresses include
// one of addrs. A subnet hit carries no identity, so this is how an open port
// gets attributed to a peer the node already met through another source.
func (n *Node) peerByAddress(addrs []ma.Multiaddr) (peer.ID, bool) {
	if len(addrs) == 0 {
		return "", false
	}
	set := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		set[a.String()] = true
	}
	for _, nb := range n.Table.List() {
		for _, a := range nb.Addrs {
			if set[a] {
				return nb.PeerID, true
			}
		}
	}
	return "", false
}

// scanAdopt records a candidate and dials/verifies/adopts it through the same
// path every discovery source uses (tryAdopt → Host.Connect + capabilities).
func (n *Node) scanAdopt(ctx context.Context, res *ScanResult, ai peer.AddrInfo, source string, dialTimeout time.Duration) {
	if ai.ID == "" {
		return
	}
	// De-duplicate against both kinds of candidate: one already recorded by
	// peer id (another id-bearing source), and one recorded by address only
	// (a subnet hit for this same peer).
	for i := range res.Agents {
		if res.Agents[i].PeerID == ai.ID.String() {
			return
		}
		for _, a := range ai.Addrs {
			for _, have := range res.Agents[i].Addrs {
				if have == a.String() {
					return
				}
			}
		}
	}
	agent := ScanAgent{PeerID: ai.ID.String(), Source: source}
	for _, a := range ai.Addrs {
		agent.Addrs = append(agent.Addrs, a.String())
	}
	if nb, ok := n.Table.Get(ai.ID); ok {
		agent.Skills = append([]string(nil), nb.Skills...)
		agent.Connected = n.Host.IsConnected(ai.ID)
		agent.Verified = len(nb.Skills) > 0 && n.completedHandshake(ai.ID)
	}
	res.Agents = append(res.Agents, agent)
	res.BySource[source]++
	n.tryAdopt(ctx, ai, dialTimeout)
}

// tryAdopt connects to a candidate agent and verifies its signed capabilities,
// folding it into the neighbour table — the same path every discovery source
// uses (see onDiscovered). A peer that is already connected and handshaked is
// left alone; a peer blocked by policy is ignored.
func (n *Node) tryAdopt(ctx context.Context, ai peer.AddrInfo, dialTimeout time.Duration) {
	if ai.ID == "" {
		return
	}
	if !n.Policy.AllowConnection(ai.ID) {
		n.Audit.Log(security.AuditEvent{Event: "scan_blocked", PeerID: ai.ID.String(), Reason: "blocked"})
		return
	}
	if nb, ok := n.Table.Get(ai.ID); ok && nb.Connected && n.completedHandshake(ai.ID) {
		return
	}
	// Seed the table with the advertised address as a dial hint, then connect.
	n.Table.Upsert(&routing.Neighbor{
		PeerID: ai.ID, Addrs: addrsOf(ai), Category: routing.CatWAN,
		Connected: n.Host.IsConnected(ai.ID),
	})
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	if err := n.Host.Connect(dctx, ai); err != nil {
		n.log.Debug("scan_dial_failed", "peer", ai.ID.String(), "err", err.Error())
		return
	}
	n.Table.SetConnected(ai.ID, true)
	n.refreshCapabilities(ctx, ai.ID)
}

// countVerified walks the table for connected peers whose skills were confirmed
// by a capabilities handshake this process.
func (n *Node) countVerified() int {
	count := 0
	for _, nb := range n.Table.List() {
		if nb.Connected && len(nb.Skills) > 0 && n.completedHandshake(nb.PeerID) {
			count++
		}
	}
	return count
}

// runBackgroundScan is the maintenance-loop tick: a bounded environment sweep
// followed by a short log summary. Errors never stop the node.
func (n *Node) runBackgroundScan(ctx context.Context) {
	res := n.ScanNetwork(ctx, ScanRequest{})
	n.SetLastScan(res)
	if len(res.Agents) > 0 || res.Probed > 0 || len(res.Errors) > 0 {
		n.log.Info("environment_scan",
			"probed", res.Probed, "candidates", res.Candidates,
			"connected", res.Connected, "sources", fmt.Sprintf("%v", res.BySource),
			"took", res.Duration)
	}
}

// SetLastScan stores the most recent sweep report for later reading.
func (n *Node) SetLastScan(res ScanResult) {
	n.scanMu.Lock()
	defer n.scanMu.Unlock()
	cp := res
	n.lastScan = &cp
}

// LastScan returns the most recent sweep report (manual or background). It is
// nil when no scan has run yet.
func (n *Node) LastScan() any {
	n.scanMu.Lock()
	defer n.scanMu.Unlock()
	if n.lastScan == nil {
		return map[string]any{"status": "no scan has run yet"}
	}
	return *n.lastScan
}
