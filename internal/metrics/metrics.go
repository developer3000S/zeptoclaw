// Package metrics exposes the node's Prometheus counters and gauges.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Collector holds every mesh metric. It implements p2p.MetricsRecorder.
type Collector struct {
	reg *prometheus.Registry

	PeersTotal      prometheus.Gauge
	PeersConnected  prometheus.Gauge
	TasksReceived   prometheus.Counter
	TasksCompleted  prometheus.Counter
	TasksFailed     prometheus.Counter
	TasksRejected   prometheus.Counter
	TasksRunning    prometheus.Gauge
	TasksDelegated  prometheus.Counter
	TasksDuplicate  prometheus.Counter
	TasksTimedOut   prometheus.Counter
	SearchRelays    prometheus.Counter
	SearchRelayHits prometheus.Counter
	FullRefreshes   prometheus.Counter
	TaskDuration    prometheus.Histogram
	TaskRouteHops   prometheus.Histogram
	DiscoveryMDNS   prometheus.Counter
	DiscoveryLocal  prometheus.Counter
	DHTLookups      prometheus.Counter
	DHTHits         prometheus.Counter
	GossipPublished prometheus.Counter
	GossipReceived  prometheus.Counter
	BytesSent       prometheus.Counter
	BytesReceived   prometheus.Counter
	PicoClawRunning prometheus.Gauge
	PicoClawErrors  prometheus.Counter
	SecurityEvents  *prometheus.CounterVec
	ForwardAttempts *prometheus.CounterVec
	NeighborScore   *prometheus.GaugeVec
	BuildInfo       prometheus.Gauge

	mu sync.Mutex
}

// New registers the metrics on a private registry, so a test binary can build
// several nodes without colliding on the default registry.
func New(namespace string) *Collector {
	if namespace == "" {
		namespace = "zeptomesh"
	}
	reg := prometheus.NewRegistry()
	c := &Collector{reg: reg}

	c.PeersTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "peers_total", Help: "Known neighbours in the table.",
	})
	c.PeersConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "peers_connected", Help: "Connected neighbours.",
	})
	c.TasksReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "tasks_received_total", Help: "Tasks received from any source.",
	})
	c.TasksCompleted = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "tasks_completed_total", Help: "Tasks that finished successfully.",
	})
	c.TasksFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "tasks_failed_total", Help: "Tasks that finished with an error.",
	})
	c.TasksRejected = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "tasks_rejected_total", Help: "Tasks refused by policy or validation.",
	})
	c.TasksRunning = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "tasks_running", Help: "Tasks currently executing locally.",
	})
	c.TasksDelegated = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "tasks_delegated_total", Help: "Tasks forwarded to another peer.",
	})
	c.TasksDuplicate = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "tasks_duplicate_total", Help: "Deliveries suppressed by deduplication.",
	})
	c.TasksTimedOut = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "tasks_timeout_total", Help: "Tasks that exceeded their deadline.",
	})
	c.SearchRelays = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "search_relays_total", Help: "Search-relay lookups started by this node.",
	})
	c.SearchRelayHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "search_relay_hits_total", Help: "Peers discovered via search relay.",
	})
	c.FullRefreshes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "skill_full_refreshes_total", Help: "Full skill-view reloads performed or requested.",
	})
	c.TaskDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Name: "task_duration_seconds", Help: "End-to-end task execution time.",
		Buckets: []float64{0.05, 0.25, 1, 5, 15, 30, 60, 120, 300, 600, 1800},
	})
	c.TaskRouteHops = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Name: "task_route_hops", Help: "Hops observed on completed tasks.",
		Buckets: []float64{0, 1, 2, 3, 5, 8, 13, 21},
	})
	c.DiscoveryMDNS = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "discovery_mdns_peers_found_total", Help: "Peers found via mDNS.",
	})
	c.DiscoveryLocal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "discovery_local_peers_found_total", Help: "Peers found in the local registry.",
	})
	c.DHTLookups = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "dht_lookups_total", Help: "DHT provider lookups.",
	})
	c.DHTHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "dht_lookup_hits_total", Help: "Peers returned by DHT lookups.",
	})
	c.GossipPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "gossip_published_total", Help: "Gossip messages published.",
	})
	c.GossipReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "gossip_received_total", Help: "Gossip messages accepted.",
	})
	c.BytesSent = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "network_bytes_sent_total", Help: "Bytes written to peers.",
	})
	c.BytesReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "network_bytes_received_total", Help: "Bytes read from peers.",
	})
	c.PicoClawRunning = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "picoclaw_processes_running", Help: "Concurrent local agent executions.",
	})
	c.PicoClawErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "picoclaw_errors_total", Help: "Local agent execution failures.",
	})
	c.SecurityEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "security_events_total", Help: "Security events by kind.",
	}, []string{"event"})
	c.ForwardAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "forward_attempts_total", Help: "Delegation attempts by outcome.",
	}, []string{"outcome"})
	c.NeighborScore = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Name: "neighbor_score", Help: "Last computed routing score per neighbour.",
	}, []string{"peer"})
	c.BuildInfo = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Name: "build_info", Help: "Constant 1; labels carry the build stamp.",
	})

	for _, col := range []prometheus.Collector{
		c.PeersTotal, c.PeersConnected, c.TasksReceived, c.TasksCompleted, c.TasksFailed,
		c.TasksRejected, c.TasksRunning, c.TasksDelegated, c.TasksDuplicate, c.TasksTimedOut,
		c.TaskDuration, c.TaskRouteHops, c.DiscoveryMDNS, c.DiscoveryLocal, c.DHTLookups,
		c.DHTHits, c.GossipPublished, c.GossipReceived, c.BytesSent, c.BytesReceived,
		c.PicoClawRunning, c.PicoClawErrors, c.SecurityEvents, c.ForwardAttempts,
		c.NeighborScore, c.BuildInfo, c.SearchRelays, c.SearchRelayHits, c.FullRefreshes,
	} {
		reg.MustRegister(col)
	}
	return c
}

// Registry returns the private collector registry.
func (c *Collector) Registry() *prometheus.Registry { return c.reg }

// AddBytesSent implements p2p.MetricsRecorder.
func (c *Collector) AddBytesSent(n uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.BytesSent.Add(float64(n))
}

// AddBytesReceived implements p2p.MetricsRecorder.
func (c *Collector) AddBytesReceived(n uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.BytesReceived.Add(float64(n))
}

// Security increments a named security event counter.
func (c *Collector) Security(event string) {
	if c == nil {
		return
	}
	c.SecurityEvents.WithLabelValues(event).Inc()
}

// Forward increments a delegation outcome counter.
func (c *Collector) Forward(outcome string) {
	if c == nil {
		return
	}
	c.ForwardAttempts.WithLabelValues(outcome).Inc()
}
