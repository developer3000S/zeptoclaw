package ui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// stubNode serves canned admin-API answers for one fake agent node.
type stubNode struct {
	peerID string
	status map[string]any
	peers  []map[string]any
	tasks  []map[string]any
}

func (s *stubNode) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/healthz":
			writeJSONStub(t, w, map[string]any{"status": "ok", "peer_id": s.peerID})
		case r.URL.Path == "/api/v1/status":
			writeJSONStub(t, w, s.status)
		case r.URL.Path == "/api/v1/peers":
			writeJSONStub(t, w, map[string]any{"count": len(s.peers), "peers": s.peers})
		case r.URL.Path == "/api/v1/tasks":
			writeJSONStub(t, w, map[string]any{"count": len(s.tasks), "tasks": s.tasks})
		default:
			http.NotFound(w, r)
		}
	})
}

func writeJSONStub(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode: %v", err)
	}
}

// twoNodeMesh returns stubs for a mesh where a knows b and both know the same
// random internet peer (the DHT noise seen on the live cluster).
func twoNodeMesh() (*stubNode, *stubNode) {
	a := &stubNode{
		peerID: "12D3KooWaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		status: map[string]any{
			"node_name": "zepto-0", "peer_id": "12D3KooWaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"version": "0.1.0", "skills": []string{"general"}, "load": 0.5, "running_tasks": 1,
		},
		peers: []map[string]any{
			{"peer_id": "12D3KooWbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"trust": "known", "connected": true, "skills": []string{"general"}, "addrs": []string{"/ip4/172.22.0.2/tcp/4001"}},
			{"peer_id": "12D3KooWnoiseeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
				"trust": "untrusted", "connected": false, "addrs": []string{"/ip4/69.5.169.161/tcp/9646"}},
		},
	}
	b := &stubNode{
		peerID: "12D3KooWbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		status: map[string]any{
			"node_name": "zepto-1", "peer_id": "12D3KooWbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			"version": "0.1.0", "skills": []string{"general"}, "load": 0.0,
		},
		peers: []map[string]any{
			{"peer_id": "12D3KooWaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"trust": "known", "connected": true},
			{"peer_id": "12D3KooWnoiseeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
				"trust": "untrusted", "connected": false},
		},
	}
	return a, b
}

func TestBuildSnapshotMergesPeersAndDedupsEdges(t *testing.T) {
	a, b := twoNodeMesh()
	polls := []NodePoll{
		pollFromStub(a, "zepto-0"),
		pollFromStub(b, "zepto-1"),
	}
	snap := buildSnapshot(polls, 5000, 1700000000)

	if len(snap.ManagedPeerIDs) != 2 {
		t.Fatalf("managed peer ids = %d, want 2", len(snap.ManagedPeerIDs))
	}
	kindOf := map[string]string{}
	for _, v := range snap.Graph.Vertices {
		kindOf[v.ID] = v.Kind
	}
	if kindOf[a.peerID] != "managed" || kindOf[b.peerID] != "managed" {
		t.Errorf("both stubs must be managed, got %q/%q", kindOf[a.peerID], kindOf[b.peerID])
	}
	if kindOf["12D3KooWnoiseeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"] != "discovered" {
		t.Errorf("internet peer must be discovered, got %q", kindOf["12D3KooWnoise…"])
	}
	seen := map[string]bool{}
	for _, e := range snap.Graph.Edges {
		key := e.From + ">" + e.To
		if seen[key] {
			t.Errorf("duplicate edge %s", key)
		}
		seen[key] = true
	}
	// a→b and b→a are distinct directed edges; each must appear exactly once.
	if !seen[a.peerID+">"+b.peerID] || !seen[b.peerID+">"+a.peerID] {
		t.Errorf("both directions of the a-b link must exist, got %v", seen)
	}
}

func TestBuildSnapshotVertexLabelsUseHost(t *testing.T) {
	a, _ := twoNodeMesh()
	polls := []NodePoll{pollFromStub(a, "zepto-0")}
	snap := buildSnapshot(polls, 5000, 0)
	var noise Vertex
	for _, v := range snap.Graph.Vertices {
		if v.ID == "12D3KooWnoiseeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" {
			noise = v
		}
	}
	if noise.Label != "69.5.169.161" {
		t.Errorf("discovered vertex label = %q, want the host from its addrs", noise.Label)
	}
	if noise.Trust != "untrusted" || noise.Connected {
		t.Errorf("discovered vertex flags wrong: trust=%q connected=%v", noise.Trust, noise.Connected)
	}
}

func TestBuildSnapshotUnreachableNodeIsKept(t *testing.T) {
	polls := []NodePoll{
		{Name: "zepto-0", URL: "http://gone:8081", Error: "connection refused"},
	}
	snap := buildSnapshot(polls, 5000, 0)
	if len(snap.Nodes) != 1 || snap.Nodes[0].Error == "" {
		t.Fatalf("unreachable node must be reported with its error, got %+v", snap.Nodes)
	}
	if len(snap.Graph.Vertices) != 0 {
		t.Errorf("an unreachable node contributes no vertices, got %d", len(snap.Graph.Vertices))
	}
}

// pollFromStub materialises a NodePoll as the client would produce it.
func pollFromStub(s *stubNode, name string) NodePoll {
	var st NodeStatus
	rec, _ := json.Marshal(s.status)
	_ = json.Unmarshal(rec, &st)
	var pr PeersResponse
	pb, _ := json.Marshal(map[string]any{"count": len(s.peers), "peers": s.peers})
	_ = json.Unmarshal(pb, &pr)
	return NodePoll{
		Name: name, URL: "http://" + name, PeerID: s.peerID,
		Reachable: true, LatencyMS: 3, Status: &st, Peers: pr.Peers,
	}
}

func TestParseNodesEnv(t *testing.T) {
	cases := []struct {
		in   string
		want []NodeEntry
	}{
		{"", nil},
		{"http://zepto-0:8081", []NodeEntry{{Name: "zepto-0", URL: "http://zepto-0:8081"}}},
		{"http://a:1,http://b:2", []NodeEntry{
			{Name: "a", URL: "http://a:1"}, {Name: "b", URL: "http://b:2"}}},
		{"zepto-0=http://localhost:8081", []NodeEntry{{Name: "zepto-0", URL: "http://localhost:8081"}}},
	}
	for _, c := range cases {
		got := parseNodesEnv(c.in)
		if len(got) != len(c.want) {
			t.Errorf("parseNodesEnv(%q) = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("parseNodesEnv(%q)[%d] = %+v, want %+v", c.in, i, got[i], c.want[i])
			}
		}
	}
}

func TestSaveLoadNodesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/nodes.json"
	want := []NodeEntry{{Name: "zepto-0", URL: "http://zepto-0:8081", Token: "sec"}}
	if err := saveNodes(path, want); err != nil {
		t.Fatalf("saveNodes: %v", err)
	}
	got, err := loadNodes(path)
	if err != nil {
		t.Fatalf("loadNodes: %v", err)
	}
	if len(got) != 1 || got[0] != want[0] {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
	// A missing file is a first start, not an error.
	if _, err := loadNodes(path + ".missing"); err != nil {
		t.Errorf("missing nodes file must not error, got %v", err)
	}
}

// TestPollAgainstStub drives the real client against an httptest node so the
// request paths and the decoding are exercised, not just the merge.
func TestPollAgainstStub(t *testing.T) {
	a, _ := twoNodeMesh()
	srv := httptest.NewServer(a.handler(t))
	defer srv.Close()
	c := NewNodeClient(NodeEntry{Name: "zepto-0", URL: srv.URL})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := c.Poll(ctx)
	if !p.Reachable || p.PeerID != a.peerID {
		t.Fatalf("poll = %+v", p)
	}
	if p.Status == nil || p.Status.NodeName != "zepto-0" {
		t.Errorf("status not decoded: %+v", p.Status)
	}
	if len(p.Peers) != 2 || len(p.Tasks) != 0 {
		t.Errorf("peers/tasks = %d/%d", len(p.Peers), len(p.Tasks))
	}
}
