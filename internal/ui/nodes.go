package ui

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// NodeClient talks to one agent node's admin API. It mirrors the CLI client's
// conventions (cmd/zeptomesh-node/client.go): bearer auth only when a token is
// configured, bounded reads, errors that name the node and the failing call.
type NodeClient struct {
	base    string
	token   string
	http    *http.Client
	name    string
	logHTTP bool
}

// NewNodeClient wraps one configured node. token may be empty.
func NewNodeClient(entry NodeEntry) *NodeClient {
	base := strings.TrimRight(entry.URL, "/")
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	return &NodeClient{
		base:  base,
		token: entry.Token,
		name:  entry.Name,
		http:  &http.Client{Timeout: pollTimeout},
	}
}

// Name is the configured label of this node.
func (c *NodeClient) Name() string { return c.name }

// do performs one request and decodes into out when it is non-nil. timeout
// overrides the client default for long actions (submit with wait, leave).
func (c *NodeClient) do(ctx context.Context, method, path string, body any, out any, timeout time.Duration) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, method, c.base+path, rd)
	if err != nil {
		return fmt.Errorf("%s: %s %s: %w", c.name, method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %s %s: %w (is the node running at %s?)", c.name, method, path, err, c.base)
	}
	defer res.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("%s: %s %s: read body: %w", c.name, method, path, err)
	}
	if res.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(payload, &e); err == nil && e.Error != "" {
			return fmt.Errorf("%s: %s %s: http %d: %s", c.name, method, path, res.StatusCode, e.Error)
		}
		return fmt.Errorf("%s: %s %s: http %d: %s", c.name, method, path, res.StatusCode, truncate(string(payload), 200))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return fmt.Errorf("%s: %s %s: decode: %w", c.name, method, path, err)
	}
	return nil
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// ---------- admin API response shapes ----------

// HealthZ is the unauthenticated liveness probe.
type HealthZ struct {
	Status string `json:"status"`
	PeerID string `json:"peer_id"`
}

// NodeStatus mirrors internal/node.Status served by GET /api/v1/status. Only
// the fields the dashboard renders are typed; the rest land in RawStatus.
type NodeStatus struct {
	NodeName    string   `json:"node_name"`
	PeerID      string   `json:"peer_id"`
	Version     string   `json:"version"`
	ProtocolVer string   `json:"protocol_version"`
	UptimeSec   int64    `json:"uptime_sec"`
	StartedAt   string   `json:"started_at"`
	Addrs       []string `json:"addrs"`
	Skills      []string `json:"skills"`
	ResourceCls string   `json:"resource_class"`
	Load        float64  `json:"load"`
	Running     int      `json:"running_tasks"`
	Tracked     int      `json:"tracked_tasks"`
	Neighbors   int      `json:"neighbors_total"`
	Connected   int      `json:"neighbors_connected"`
	Membership  int      `json:"membership_known"`
	Bootstrap   struct {
		Configured int `json:"configured"`
		Reachable  int `json:"reachable"`
	} `json:"bootstrap"`
	Discovery struct {
		LocalRegistry bool `json:"local_registry"`
		MDNS          bool `json:"mdns"`
		MDNSFound     int  `json:"mdns_peers_found"`
		DHT           bool `json:"dht"`
		DHTLookups    int  `json:"dht_lookups"`
		Gossip        bool `json:"gossip"`
	} `json:"discovery"`
	Adapter struct {
		Name    string `json:"name"`
		Healthy bool   `json:"healthy"`
		Detail  string `json:"detail,omitempty"`
		Model   string `json:"model,omitempty"`
	} `json:"adapter"`
	Security struct {
		TrustMode        string `json:"trust_mode"`
		RequireTaskSig   bool   `json:"require_task_signature"`
		MinTrustForTasks string `json:"min_trust_for_tasks"`
		RateLimitedPeers int    `json:"rate_limited_peers"`
		AuditLog         string `json:"audit_log,omitempty"`
	} `json:"security"`
	Storage struct {
		Tasks     int64 `json:"tasks"`
		Peers     int64 `json:"peers"`
		DedupKeys int64 `json:"dedup_keys"`
	} `json:"storage"`
	Triggers int `json:"triggers"`
}

// PeerView mirrors api.peerView served by GET /api/v1/peers.
type PeerView struct {
	PeerID       string   `json:"peer_id"`
	Addrs        []string `json:"addrs,omitempty"`
	Skills       []string `json:"skills,omitempty"`
	Category     string   `json:"category,omitempty"`
	Trust        string   `json:"trust"`
	Version      string   `json:"version,omitempty"`
	Load         float64  `json:"load"`
	Connected    bool     `json:"connected"`
	Left         bool     `json:"left,omitempty"`
	RTTMillis    int64    `json:"rtt_ms,omitempty"`
	Successes    uint64   `json:"successes"`
	Failures     uint64   `json:"failures"`
	LastSeenUnix int64    `json:"last_seen_unix"`
}

// PeersResponse wraps the peer list.
type PeersResponse struct {
	Count int        `json:"count"`
	Peers []PeerView `json:"peers"`
}

// TaskRecord mirrors storage.TaskRecord served by GET /api/v1/tasks.
type TaskRecord struct {
	TaskID         string   `json:"task_id"`
	ParentTaskID   string   `json:"parent_task_id,omitempty"`
	OriginPeerID   string   `json:"origin_peer_id"`
	SenderPeerID   string   `json:"sender_peer_id"`
	Status         string   `json:"status"`
	RequiredSkills []string `json:"required_skills,omitempty"`
	ReceivedAt     string   `json:"received_at"`
	FinishedAt     string   `json:"finished_at,omitempty"`
	WorkerPeerID   string   `json:"worker_peer_id,omitempty"`
	DelegatedTo    []string `json:"delegated_to,omitempty"`
	Error          string   `json:"error,omitempty"`
	ResultDigest   string   `json:"result_digest,omitempty"`
	Attempts       int      `json:"attempts"`
	TTL            int32    `json:"ttl"`
	Priority       int32    `json:"priority"`
	PayloadHash    string   `json:"payload_hash,omitempty"`
	UpdatedAt      int64    `json:"updated_at"`
}

// TasksResponse wraps the task journal slice.
type TasksResponse struct {
	Count int          `json:"count"`
	Tasks []TaskRecord `json:"tasks"`
}

// ---------- polling ----------

// Poll collects the data the dashboard needs from one node in one round:
// identity (healthz), self status, the neighbour table and recent tasks. Each
// part is reported independently — a node that answers /healthz but nothing
// else still shows up as reachable-with-error.
type NodePoll struct {
	Name      string       `json:"name"`
	URL       string       `json:"url"`
	PeerID    string       `json:"peer_id,omitempty"`
	Reachable bool         `json:"reachable"`
	LatencyMS int64        `json:"latency_ms"`
	Error     string       `json:"error,omitempty"`
	Status    *NodeStatus  `json:"status,omitempty"`
	Peers     []PeerView   `json:"peers,omitempty"`
	Tasks     []TaskRecord `json:"tasks,omitempty"`
}

// Poll queries the node with the polling timeout. ctx should carry a deadline
// covering the whole round so a stalled node cannot block the aggregator.
func (c *NodeClient) Poll(ctx context.Context) NodePoll {
	start := time.Now()
	p := NodePoll{Name: c.name, URL: c.base}
	var hz HealthZ
	if err := c.do(ctx, http.MethodGet, "/healthz", nil, &hz, pollTimeout); err != nil {
		p.Error = err.Error()
		return p
	}
	p.Reachable = true
	p.PeerID = hz.PeerID
	p.LatencyMS = time.Since(start).Milliseconds()

	var st NodeStatus
	if err := c.do(ctx, http.MethodGet, "/api/v1/status", nil, &st, pollTimeout); err != nil {
		p.Error = err.Error()
		return p
	}
	p.Status = &st

	var pr PeersResponse
	// The peer table drives the network graph; ask for the full view
	// (minimal=false) so skills, version and load are available in the UI.
	if err := c.do(ctx, http.MethodGet, "/api/v1/peers?limit=500", nil, &pr, pollTimeout); err != nil {
		p.Error = err.Error()
		return p
	}
	p.Peers = pr.Peers

	var tr TasksResponse
	if err := c.do(ctx, http.MethodGet, "/api/v1/tasks?limit=50", nil, &tr, pollTimeout); err != nil {
		// A node may legitimately refuse the journal (private trust mode), so
		// this degrades to an empty list rather than failing the whole poll.
		p.Tasks = nil
	} else {
		p.Tasks = tr.Tasks
	}
	return p
}
