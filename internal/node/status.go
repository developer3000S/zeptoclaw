package node

import (
	"context"
	"time"

	"github.com/developer3000S/zeptoclaw/internal/picoclaw"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/version"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// Status is the JSON shape served by the admin API and the probe socket.
type Status struct {
	NodeName    string    `json:"node_name"`
	PeerID      string    `json:"peer_id"`
	Version     string    `json:"version"`
	ProtocolVer string    `json:"protocol_version"`
	UptimeSec   int64     `json:"uptime_sec"`
	StartedAt   time.Time `json:"started_at"`
	Addrs       []string  `json:"addrs"`
	Skills      []string  `json:"skills"`
	ResourceCls string    `json:"resource_class"`
	Load        float64   `json:"load"`
	Running     int       `json:"running_tasks"`
	Tracked     int       `json:"tracked_tasks"`
	Neighbors   int       `json:"neighbors_total"`
	Connected   int       `json:"neighbors_connected"`
	Membership  int       `json:"membership_known"`
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

// Status renders the current node state. It is cheap enough for a 2s poll.
func (n *Node) Status() Status {
	hc, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	st := Status{
		NodeName:    n.Cfg.Node.Name,
		PeerID:      n.Identity.PeerID().String(),
		Version:     version.Version,
		ProtocolVer: version.ProtocolVersion,
		UptimeSec:   int64(time.Since(n.started).Seconds()),
		StartedAt:   n.started,
		Addrs:       n.DiscoverableAddrs(),
		Skills:      n.Cfg.EffectiveSkills(),
		ResourceCls: n.Cfg.Capabilities.ResourceClass,
		Load:        n.Manager.Load(),
		Running:     n.Manager.RunningCount(),
		Tracked:     n.Manager.TrackedCount(),
		Neighbors:   n.Table.Len(),
		Connected:   n.Table.ConnectedCount(),
	}
	if n.Membership != nil {
		st.Membership = n.Membership.Len()
	}
	if n.Bootstrap != nil {
		st.Bootstrap.Configured = n.Bootstrap.Count()
		st.Bootstrap.Reachable = len(n.Bootstrap.Healthy(n.Cfg.Discovery.Gossip.FailureTimeout.D() * 2))
	}
	st.Discovery.LocalRegistry = n.Registry != nil
	st.Discovery.MDNS = n.MDNS != nil
	st.Discovery.DHT = n.DHT != nil && n.DHT.Enabled()
	st.Discovery.Gossip = n.Cfg.Discovery.Gossip.Enabled
	if n.MDNS != nil {
		st.Discovery.MDNSFound = n.MDNS.FoundCount()
	}
	if n.DHT != nil {
		lookups, _, _ := n.DHT.Stats()
		st.Discovery.DHTLookups = int(lookups)
	}
	inf := picoclaw.Describe(hc, n.Adapter)
	st.Adapter.Name = inf.Name
	st.Adapter.Healthy = inf.Healthy
	st.Adapter.Detail = inf.Detail
	st.Adapter.Model = inf.Model
	st.Security.TrustMode = n.Policy.Mode().String()
	st.Security.RequireTaskSig = n.Cfg.Security.RequireTaskSignature
	st.Security.MinTrustForTasks = n.Policy.MinTrustForTasks().String()
	st.Security.RateLimitedPeers = n.Limiter.Size()
	st.Security.AuditLog = n.Audit.Path()
	if s, err := n.Store.Stats(); err == nil {
		st.Storage.Tasks = s.Tasks
		st.Storage.Peers = s.Peers
		st.Storage.DedupKeys = s.DedupKeys
	}
	if n.Scheduler != nil {
		if vs, err := n.Scheduler.Views(time.Now().UTC()); err == nil {
			st.Triggers = len(vs)
		}
	}
	return st
}

// Capabilities is the signed self-description served over the mesh protocol.
func (n *Node) Capabilities() *pb.Capabilities {
	caps := &pb.Capabilities{
		PeerId:              n.Identity.PeerID().String(),
		NodeName:            n.Cfg.Node.Name,
		Version:             version.Version,
		Skills:              n.Cfg.EffectiveSkills(),
		Models:              append([]string(nil), n.Cfg.Capabilities.Models...),
		MaxParallelTasks:    int32(n.Cfg.Tasks.MaxParallelTasks),
		RunningTasks:        int32(n.Manager.RunningCount()),
		Load:                n.Manager.Load(),
		AcceptExternalTasks: n.Cfg.Capabilities.AcceptExternalTasks,
		AllowShell:          n.Cfg.Capabilities.AllowShell,
		RelayCapable:        true,
		ResourceClass:       n.Cfg.Capabilities.ResourceClass,
		ListenAddrs:         n.DiscoverableAddrs(),
		Timestamp:           time.Now().UTC().Unix(),
		SkillsVersion:       n.Skills.Epoch(),
	}
	if n.Cfg.Capabilities.SkillExchange.Enabled {
		caps.SkillDocs = n.Skills.Descriptors()
	}
	if err := security.NewSigner(n.Identity).SignCaps(caps); err != nil {
		n.log.Error("caps_sign_failed", "err", err.Error())
	}
	return caps
}

func (n *Node) peerState() *pb.PeerState {
	return &pb.PeerState{
		PeerId:           n.Identity.PeerID().String(),
		Timestamp:        time.Now().UTC().Unix(),
		Skills:           n.Cfg.EffectiveSkills(),
		Load:             n.Manager.Load(),
		MaxParallelTasks: int32(n.Cfg.Tasks.MaxParallelTasks),
		Version:          version.Version,
		Status:           "active",
		Addrs:            n.DiscoverableAddrs(),
		SkillsVersion:    n.Skills.Epoch(),
	}
}
