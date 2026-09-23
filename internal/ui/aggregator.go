package ui

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Vertex is one node of the mesh graph. Managed vertices are configured
// endpoints the operator can act on; discovered vertices are peers the managed
// nodes reported. Trust/connected/left come from the reporting node's table and
// may differ between observers — the last one wins on conflict, with the worst
// trust kept aside for the UI badge via TrustOf queries at render time.
type Vertex struct {
	ID        string   `json:"id"`
	Label     string   `json:"label"`
	Kind      string   `json:"kind"` // managed | discovered
	Trust     string   `json:"trust"`
	Connected bool     `json:"connected"`
	Left      bool     `json:"left"`
	Skills    []string `json:"skills,omitempty"`
	Load      float64  `json:"load"`
	Source    string   `json:"source,omitempty"` // node that reported it
	Self      bool     `json:"self,omitempty"`   // the managed node itself
}

// Edge is one "A knows B" relation from a managed node's neighbour table.
// Connected marks a live link versus a remembered-but-currently-down peer.
type Edge struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Connected bool   `json:"connected"`
}

// Graph is the merged mesh topology.
type Graph struct {
	Vertices []Vertex `json:"vertices"`
	Edges    []Edge   `json:"edges"`
}

// MeshSnapshot is the whole dashboard state served by GET /api/v1/mesh.
type MeshSnapshot struct {
	UpdatedUnix    int64      `json:"updated_unix"`
	PollIntervalMS int        `json:"poll_interval_ms"`
	Nodes          []NodePoll `json:"nodes"`
	Graph          Graph      `json:"graph"`
	ManagedPeerIDs []string   `json:"managed_peer_ids"`
}

// Aggregator polls the configured nodes on a ticker and keeps the last
// snapshot in memory. Serving reads the cached copy, so a dashboard refresh
// costs the browser one request regardless of how many nodes are behind it.
type Aggregator struct {
	log      *slog.Logger
	clients  []*NodeClient
	interval time.Duration

	mu       sync.RWMutex
	snapshot MeshSnapshot
}

// NewAggregator wires clients for the configured nodes.
func NewAggregator(cfg Config, log *slog.Logger) *Aggregator {
	clients := make([]*NodeClient, 0, len(cfg.Nodes))
	for _, entry := range cfg.Nodes {
		if entry.Token == "" {
			entry.Token = cfg.APIToken
		}
		clients = append(clients, NewNodeClient(entry))
	}
	return &Aggregator{log: log, clients: clients, interval: cfg.PollInterval}
}

// Run polls until ctx ends. The first round runs immediately so /api/v1/mesh
// has data as soon as the server accepts requests.
func (a *Aggregator) Run(ctx context.Context) {
	a.pollOnce(ctx)
	t := time.NewTicker(a.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.pollOnce(ctx)
		}
	}
}

// pollOnce asks every node in parallel; each client carries its own timeout,
// and the round deadline bounds the stragglers.
func (a *Aggregator) pollOnce(ctx context.Context) {
	roundCtx, cancel := context.WithTimeout(ctx, a.interval)
	defer cancel()

	polls := make([]NodePoll, len(a.clients))
	var wg sync.WaitGroup
	for i, c := range a.clients {
		wg.Add(1)
		go func(i int, c *NodeClient) {
			defer wg.Done()
			polls[i] = c.Poll(roundCtx)
		}(i, c)
	}
	wg.Wait()

	snap := buildSnapshot(polls, int(a.interval.Milliseconds()), time.Now().Unix())
	a.mu.Lock()
	a.snapshot = snap
	a.mu.Unlock()
	if a.log.Enabled(ctx, slog.LevelDebug) {
		a.log.Debug("mesh_snapshot",
			"nodes", len(snap.Nodes), "vertices", len(snap.Graph.Vertices),
			"edges", len(snap.Graph.Edges))
	}
}

// Snapshot returns the last mesh state. It is always non-empty after the first
// poll round: unreachable nodes appear with an error instead of being dropped.
func (a *Aggregator) Snapshot() MeshSnapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.snapshot
}

// buildSnapshot merges per-node polls into one mesh view. Nodes that answered
// contribute themselves (managed) plus every peer they reported (discovered);
// a discovered peer whose id matches a managed node is upgraded to managed.
func buildSnapshot(polls []NodePoll, intervalMS int, now int64) MeshSnapshot {
	snap := MeshSnapshot{
		UpdatedUnix:    now,
		PollIntervalMS: intervalMS,
		Nodes:          polls,
		Graph:          Graph{Vertices: []Vertex{}, Edges: []Edge{}},
		ManagedPeerIDs: []string{},
	}

	managed := make(map[string]*Vertex, len(polls))
	for i := range polls {
		p := &polls[i]
		if !p.Reachable || p.PeerID == "" {
			continue
		}
		label := p.Name
		if p.Status != nil && p.Status.NodeName != "" {
			label = p.Status.NodeName
		}
		v := Vertex{
			ID: p.PeerID, Label: label, Kind: "managed", Self: true,
			Connected: true, Source: p.Name,
		}
		if p.Status != nil {
			v.Skills = p.Status.Skills
			v.Load = p.Status.Load
			v.Trust = "self"
		}
		snap.Graph.Vertices = append(snap.Graph.Vertices, v)
		snap.ManagedPeerIDs = append(snap.ManagedPeerIDs, p.PeerID)
		managed[p.PeerID] = &snap.Graph.Vertices[len(snap.Graph.Vertices)-1]
	}

	seenEdge := make(map[string]struct{})
	seenVertex := make(map[string]struct{})
	for _, pid := range snap.ManagedPeerIDs {
		seenVertex[pid] = struct{}{}
	}
	for i := range polls {
		p := &polls[i]
		if !p.Reachable || p.PeerID == "" {
			continue
		}
		from := p.PeerID
		for _, pr := range p.Peers {
			if pr.PeerID == "" || pr.PeerID == from {
				continue
			}
			if _, ok := seenVertex[pr.PeerID]; !ok {
				label := shortPeerID(pr.PeerID)
				if host := hostOfPeerAddrs(pr.Addrs); host != "" {
					label = host
				}
				snap.Graph.Vertices = append(snap.Graph.Vertices, Vertex{
					ID: pr.PeerID, Label: label, Kind: "discovered",
					Trust: pr.Trust, Connected: pr.Connected, Left: pr.Left,
					Skills: pr.Skills, Load: pr.Load, Source: p.Name,
				})
				seenVertex[pr.PeerID] = struct{}{}
			} else if m, ok := managed[pr.PeerID]; ok && m.Kind == "managed" {
				// A managed node seen as a neighbour by another managed node:
				// report the live observation without downgrading its kind.
				if !m.Connected {
					m.Connected = pr.Connected
				}
				if m.Trust == "self" && !pr.Connected {
					m.Trust = pr.Trust
				}
			}
			key := from + ">" + pr.PeerID
			if _, ok := seenEdge[key]; !ok {
				snap.Graph.Edges = append(snap.Graph.Edges, Edge{
					From: from, To: pr.PeerID, Connected: pr.Connected,
				})
				seenEdge[key] = struct{}{}
			}
		}
	}
	return snap
}

// shortPeerID keeps the graph readable: "12D3KooWJNXW5mq1…" beats the full id.
func shortPeerID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:10] + "…"
}

// hostOfPeerAddrs pulls a hostname or IP out of the peer's listen addresses
// ("/ip4/172.22.0.2/tcp/58583" → "172.22.0.2") to label discovered vertices.
func hostOfPeerAddrs(addrs []string) string {
	for _, a := range addrs {
		for _, proto := range []string{"/ip4/", "/dns4/", "/dnsaddr/", "/ip6/"} {
			if i := indexOf(a, proto); i >= 0 {
				rest := a[i+len(proto):]
				host, _, _ := cutFirst(rest, '/')
				if host != "" {
					return host
				}
			}
		}
	}
	return ""
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func cutFirst(s string, sep byte) (string, string, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}
