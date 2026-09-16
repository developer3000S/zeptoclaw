// Package config loads, defaults and validates node configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/developer3000S/zeptoclaw/internal/triggers"
	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from YAML duration strings.
type Duration time.Duration

// String implements fmt.Stringer.
func (d Duration) String() string { return time.Duration(d).String() }

// D returns the underlying time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// UnmarshalYAML accepts both Go duration strings and integer seconds.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("config: invalid duration %q: %w", s, err)
		}
		*d = Duration(v)
		return nil
	}
	var secs int64
	if err := value.Decode(&secs); err != nil {
		return fmt.Errorf("config: value is neither a duration string nor seconds: %w", err)
	}
	*d = Duration(time.Duration(secs) * time.Second)
	return nil
}

// MarshalYAML renders the duration as a Go duration string.
func (d Duration) MarshalYAML() (interface{}, error) { return d.String(), nil }

// Config is the root document of a node configuration file.
type Config struct {
	Node         NodeConfig         `yaml:"node"`
	Identity     IdentityConfig     `yaml:"identity"`
	Discovery    DiscoveryConfig    `yaml:"discovery"`
	Neighbors    NeighborsConfig    `yaml:"neighbors"`
	Tasks        TasksConfig        `yaml:"tasks"`
	Capabilities CapabilitiesConfig `yaml:"capabilities"`
	PicoClaw     PicoClawConfig     `yaml:"picoclaw"`
	Security     SecurityConfig     `yaml:"security"`
	API          APIConfig          `yaml:"api"`
	Telemetry    TelemetryConfig    `yaml:"telemetry"`
	Storage      StorageConfig      `yaml:"storage"`
	// Triggers are the fourth task source of ТЗ 6.6.1: cron-declared jobs the
	// node injects into its own pipeline. They are hot-reloadable because the
	// scheduler owns a live view of this list.
	Triggers []TriggerConfig `yaml:"triggers"`
}

// TriggerConfig is one scheduled job declared in YAML. The shape mirrors the
// admin API's trigger document so an operator reads the same fields in the
// config file, in the API answer and in the store.
type TriggerConfig struct {
	ID       string    `yaml:"id"`
	Name     string    `yaml:"name"`
	Schedule string    `yaml:"schedule"`
	Enabled  *bool     `yaml:"enabled"`
	MaxRuns  int       `yaml:"max_runs"`
	Job      JobConfig `yaml:"job"`
}

// JobConfig is the task a trigger submits, with the same names as the
// submitted task document.
type JobConfig struct {
	Instruction    string            `yaml:"instruction"`
	RequiredSkills []string          `yaml:"required_skills"`
	TTL            int32             `yaml:"ttl"`
	Priority       int32             `yaml:"priority"`
	TimeoutSeconds int32             `yaml:"timeout_seconds"`
	AllowShell     bool              `yaml:"allow_shell"`
	AllowNetwork   bool              `yaml:"allow_network_tools"`
	Labels         map[string]string `yaml:"labels"`
}

// ToDomain converts the YAML view into the scheduler's type. Keeping the
// mapping here means the config tags are the only place the wire shape and the
// domain shape are reconciled.
func (t TriggerConfig) ToDomain() *triggers.Trigger {
	return &triggers.Trigger{
		ID: t.ID, Name: t.Name, Schedule: t.Schedule,
		Enabled: t.Enabled, MaxRuns: t.MaxRuns,
		Job: triggers.Job{
			Instruction: t.Job.Instruction, RequiredSkills: t.Job.RequiredSkills,
			TTL: t.Job.TTL, Priority: t.Job.Priority,
			TimeoutSeconds: t.Job.TimeoutSeconds, AllowShell: t.Job.AllowShell,
			AllowNetwork: t.Job.AllowNetwork, Labels: t.Job.Labels,
		},
	}
}

// NodeConfig describes the daemon's own identity on disk and on the wire.
type NodeConfig struct {
	Name           string      `yaml:"name"`
	DataDir        string      `yaml:"data_dir"`
	Listen         []string    `yaml:"listen"`
	Announce       []string    `yaml:"announce"`
	PrivateNetwork string      `yaml:"private_network_psk"` // optional /1/<base32> PSK: isolates the mesh
	Relay          RelayConfig `yaml:"relay"`
}

// RelayConfig controls circuit-relay v2 (ТЗ 6.3.3.4): nodes behind NAT connect
// through peers that run the relay service. Relay is off by default; direct
// connections are always preferred, and the relay only carries the encrypted
// libp2p stream — it can never read task content.
type RelayConfig struct {
	// Enabled makes this node use relays when a direct connection fails.
	Enabled bool `yaml:"enabled"`
	// AdvertiseAsRelay turns this node into a relay for others.
	AdvertiseAsRelay bool `yaml:"advertise_as_relay"`
	// StaticRelays is an optional explicit list of relay AddrInfos.
	StaticRelays []string `yaml:"static_relays"`
	// Limit bounds the relay service: connections, reservations, bandwidth.
	Limit RelayLimitConfig `yaml:"limit"`
}

// RelayLimitConfig bounds what a relay service spends on foreign traffic.
type RelayLimitConfig struct {
	// MaxReservations caps how many behind-NAT peers may hold a slot here.
	MaxReservations int `yaml:"max_reservations"`
	// MaxCircuits caps simultaneously open relayed connections per peer.
	MaxCircuits int `yaml:"max_circuits"`
	// ReservationTTL is how long a reservation stays valid (refreshed by the
	// relayed peer). Default 1h.
	ReservationTTL Duration `yaml:"reservation_ttl"`
	// ConnDuration is the hard lifetime of one relayed connection (0 = 2min).
	ConnDuration Duration `yaml:"conn_duration"`
	// ConnDataBytes is the per-direction data cap of one relayed connection
	// before it is reset (0 = 128KB). This is the bandwidth bound of 6.3.3.4.
	ConnDataBytes int64 `yaml:"conn_data_bytes"`
}

// IdentityConfig points at the Ed25519 key material.
type IdentityConfig struct {
	KeyFile string `yaml:"key_file"`
}

// DiscoveryConfig toggles the layered peer discovery mechanisms.
type DiscoveryConfig struct {
	LocalRegistry     bool         `yaml:"local_registry"`
	LocalSocketDir    string       `yaml:"local_socket_dir"`
	MDNS              bool         `yaml:"mdns"`
	MDNSServiceName   string       `yaml:"mdns_service_name"`
	Bootstrap         []string     `yaml:"bootstrap"`
	DHT               bool         `yaml:"dht"`
	DHTMode           string       `yaml:"dht_mode"` // server | client | auto
	PeerExchange      bool         `yaml:"peer_exchange"`
	Gossip            GossipConfig `yaml:"gossip"`
	BootstrapInterval Duration     `yaml:"bootstrap_interval"`
}

// GossipConfig tunes the membership gossip protocol.
type GossipConfig struct {
	Enabled        bool     `yaml:"enabled"`
	Topic          string   `yaml:"topic"`
	Heartbeat      Duration `yaml:"heartbeat"`
	FullSync       Duration `yaml:"full_sync"`
	FailureTimeout Duration `yaml:"failure_timeout"`
}

// NeighborsConfig bounds the peer table.
type NeighborsConfig struct {
	Min    int `yaml:"min"`
	Target int `yaml:"target"`
	Max    int `yaml:"max"`
}

// TasksConfig bounds task handling and delegation.
type TasksConfig struct {
	DefaultTTL            int              `yaml:"default_ttl"`
	MaxTTL                int              `yaml:"max_ttl"`
	MaxParallelTasks      int              `yaml:"max_parallel_tasks"`
	MaxParallelPerPeer    int              `yaml:"max_parallel_tasks_per_peer"`
	DefaultTimeoutSeconds int              `yaml:"default_timeout_seconds"`
	MaxTimeoutSeconds     int              `yaml:"max_timeout_seconds"`
	MaxPayloadBytes       int64            `yaml:"max_task_payload_bytes"`
	MaxArtifactBytes      int64            `yaml:"max_artifact_bytes"`
	MaxWorkspaceBytes     int64            `yaml:"max_workspace_bytes"`
	MaxDiskFreeBytes      int64            `yaml:"min_free_disk_bytes"`
	MaxTaskMemoryBytes    int64            `yaml:"max_task_memory_bytes"`
	DedupWindow           Duration         `yaml:"dedup_window"`
	Forwarding            ForwardingConfig `yaml:"forwarding"`
	Retention             Duration         `yaml:"retention"`
}

// ForwardingConfig limits routing fan-out to prevent message storms.
type ForwardingConfig struct {
	MaxParallelCandidates int      `yaml:"max_parallel_candidates"`
	MaxFanout             int      `yaml:"max_fanout"`
	RetryInterval         Duration `yaml:"retry_interval_seconds"`
	MaxRetries            int      `yaml:"max_retries"`
	AttemptTimeout        Duration `yaml:"attempt_timeout"`
	// SearchRelay widens the executor search when the local neighbour view is
	// insufficient: a bounded skill-lookup fan-out that also instructs peers to
	// reload their full skill view (see tasks.SkillSource).
	SearchRelay SearchRelayConfig `yaml:"search_relay"`
}

// SearchRelayConfig bounds the skill-lookup fan-out used to find executors
// outside the local neighbour table.
type SearchRelayConfig struct {
	Enabled bool `yaml:"enabled"`
	// MaxDepth is how many further hops a lookup may traverse (relay_budget).
	MaxDepth int `yaml:"max_depth"`
	// Fanout is the number of peers asked in parallel per hop.
	Fanout int `yaml:"fanout"`
	// RequestTimeout bounds one lookup round trip.
	RequestTimeout Duration `yaml:"request_timeout"`
	// CacheTTL is how long a peer's refreshed skill answer is reused.
	CacheTTL Duration `yaml:"cache_ttl"`
	// Topic is the epidemic search plane (ТЗ 6.9.5 п.5): a pubsub topic peers
	// use when the addressed ladder (local → table → cache → relay RPC) found
	// no executor. Disabled by default — it is the widest blast radius step.
	Topic SearchTopicConfig `yaml:"topic"`
}

// SearchTopicConfig bounds the signed skill-search topic. Requests name
// skills only — never a task id or instruction — so a lookup cannot leak what
// is being executed even though every subscriber sees it.
type SearchTopicConfig struct {
	Enabled bool `yaml:"enabled"`
	// Name is the pubsub topic id. Peers must agree on it to see each other.
	Name string `yaml:"name"`
	// RequestTTL: requests older than this are not answered. It bounds how
	// long a delayed redelivery can still trigger work and forgives modest
	// clock skew in the friendly direction only (future timestamps refused).
	RequestTTL Duration `yaml:"request_ttl"`
	// MaxAnswers is how many matching replies the requester collects before
	// stopping, and how many records a responder may return per record list.
	MaxAnswers int `yaml:"max_answers"`
	// AnswerCooldown is the minimum time between two answers this node sends
	// for the same skill set, so a chatty search plane cannot become a reply
	// storm on a busy mesh.
	AnswerCooldown Duration `yaml:"answer_cooldown"`
}

// CapabilitiesConfig declares what this node can execute.
type CapabilitiesConfig struct {
	Skills              []string `yaml:"skills"`
	Models              []string `yaml:"models"`
	ResourceClass       string   `yaml:"resource_class"`
	AcceptExternalTasks bool     `yaml:"accept_external_tasks"`
	AllowShell          bool     `yaml:"allow_shell"`
	AllowNetworkTools   bool     `yaml:"allow_network_tools"`
	MaxParallelTasks    int      `yaml:"max_parallel_tasks"`
	DisabledSkills      []string `yaml:"disabled_skills"`
	// SkillDocs documents the advertised skills (description, version,
	// attributes) so neighbours can route on more than a bare name.
	SkillDocs []SkillDocConfig `yaml:"skill_docs"`
	// SkillExchange tunes sharing those descriptors with peers (ТЗ 6.3).
	SkillExchange SkillExchangeConfig `yaml:"skill_exchange"`
}

// SkillDocConfig is operator-authored documentation for one skill.
type SkillDocConfig struct {
	Name        string            `yaml:"name"`
	Version     int64             `yaml:"version"`
	Description string            `yaml:"description"`
	Models      []string          `yaml:"models"`
	Attributes  map[string]string `yaml:"attributes"`
}

// SkillExchangeConfig governs skill-descriptor exchange between peers.
type SkillExchangeConfig struct {
	// Enabled turns descriptor syncing on. With it off peers still exchange
	// bare skill names (the historical behaviour).
	Enabled bool `yaml:"enabled"`
	// DiscloseTo limits whose sync requests are answered: "trusted", "known"
	// (default) or "any". Skill documentation is operator metadata about this
	// node; disclosure is deliberately gateable.
	DiscloseTo string `yaml:"disclose_to"`
	// Interval is how often the node reconciles its neighbours' skill views.
	Interval Duration `yaml:"interval"`
	// MaxDescriptors caps one sync answer (storm/poisoning bound).
	MaxDescriptors int `yaml:"max_descriptors"`
	// ImportLimit caps how many peer-learned descriptors one node keeps total.
	ImportLimit int `yaml:"import_limit"`
	// Persist keeps the local descriptor set (and its version clock) across
	// restarts in <data_dir>/skills.json.
	Persist bool `yaml:"persist"`
}

// PicoClawConfig configures the agent adapter. Mode selects the transport used
// to reach a local PicoClaw instance; see internal/picoclaw for implementations.
type PicoClawConfig struct {
	Mode                string            `yaml:"mode"` // binary | http | stub
	Binary              string            `yaml:"binary"`
	ConfigFile          string            `yaml:"config"`
	WorkspaceRoot       string            `yaml:"workspace_root"`
	TimeoutSeconds      int               `yaml:"timeout_seconds"`
	MaxConcurrentAgents int               `yaml:"max_concurrent_agents"`
	Env                 map[string]string `yaml:"env"`
	// Model is the model this node asks PicoClaw to answer with (ТЗ 10.3
	// «модельные параметры»). It is a property of the node, not of a task: a
	// task asks for skills, and the operator decides which model serves them
	// here. Empty means PicoClaw's own configuration chooses.
	//
	// The remaining model knobs (temperature, max tokens…) are not a mesh
	// concern: `picoclaw agent` accepts only -d/-m/-s/--model and rejects an
	// unknown flag, so those are set with the PICOCLAW_AGENTS_DEFAULTS_*
	// variables of `env` or the PicoClaw config itself.
	Model string `yaml:"model"`
	// ExtraArgs are appended to every PicoClaw invocation, before the prompt.
	ExtraArgs []string `yaml:"extra_args"`
	// PromptTemplate controls how a task instruction is rendered for the CLI.
	// %s is replaced with the instruction. Empty means "pass it verbatim".
	PromptTemplate string             `yaml:"prompt_template"`
	HTTP           PicoClawHTTPConfig `yaml:"http"`
	Stub           StubConfig         `yaml:"stub"`
}

// PicoClawHTTPConfig targets a locally running PicoClaw gateway.
type PicoClawHTTPConfig struct {
	BaseURL        string `yaml:"base_url"`
	TokenEnv       string `yaml:"token_env"`
	Path           string `yaml:"path"`
	HealthPath     string `yaml:"health_path"`
	TimeoutSeconds int    `yaml:"timeout_seconds"`
}

// StubConfig configures the deterministic offline executor used for tests and
// for meshes without a PicoClaw installation.
type StubConfig struct {
	Latency Duration `yaml:"latency"`
	Echo    bool     `yaml:"echo"`
}

// SecurityConfig holds trust policy and access lists.
type SecurityConfig struct {
	TrustMode            string `yaml:"trust_mode"` // open | limited | private
	RequireTaskSignature bool   `yaml:"require_task_signature"`
	// RequireOriginSignature rejects a task whose authoring signature is
	// missing. A present-but-wrong one is always fatal — this knob only decides
	// whether nodes that predate the field stay acceptable (ТЗ 6.6.4).
	RequireOriginSignature bool            `yaml:"require_origin_signature"`
	DropInvalidSignatures  bool            `yaml:"drop_invalid_signatures"`
	AllowedPeersFile       string          `yaml:"allowed_peers_file"`
	BlockedPeersFile       string          `yaml:"blocked_peers_file"`
	MinTrustForTasks       string          `yaml:"min_trust_for_tasks"`
	RateLimit              RateLimitConfig `yaml:"rate_limit"`
	MaxMessageBytes        int64           `yaml:"max_message_bytes"`
}

// RateLimitConfig bounds per-peer request rates.
type RateLimitConfig struct {
	RequestsPerSecond float64 `yaml:"requests_per_second"`
	Burst             int     `yaml:"burst"`
}

// APIConfig configures the local administrative HTTP API.
type APIConfig struct {
	Enabled      bool     `yaml:"enabled"`
	Listen       string   `yaml:"listen"`
	AuthTokenEnv string   `yaml:"auth_token_env"`
	ReadTimeout  Duration `yaml:"read_timeout"`
	WriteTimeout Duration `yaml:"write_timeout"`
}

// TelemetryConfig configures metrics and logs.
type TelemetryConfig struct {
	PrometheusListen string        `yaml:"prometheus_listen"`
	LogLevel         string        `yaml:"log_level"`
	StructuredLogs   bool          `yaml:"structured_logs"`
	Tracing          TracingConfig `yaml:"tracing"`
}

// TracingConfig configures OpenTelemetry (ТЗ 14.3). The specification makes
// task_id the cross-cutting identifier and only *recommends* OpenTelemetry, so
// this is off by default: a node that never mentions it links no exporter and
// pays nothing for the possibility.
type TracingConfig struct {
	Enabled bool `yaml:"enabled"`
	// Endpoint is host:port for OTLP over HTTP, e.g. "localhost:4318". Empty
	// defers to the standard OTEL_EXPORTER_OTLP_ENDPOINT variables.
	Endpoint string `yaml:"endpoint"`
	// Insecure sends plaintext; with a scheme-less endpoint that is what a local
	// collector (Jaeger, Tempo) normally expects.
	Insecure bool `yaml:"insecure"`
	// SampleRatio is the head probability for traces this node starts, 0..1.
	// 0 is read as 1 (sample everything): a zero ratio means "unset", not
	// "sample nothing" — turning tracing off is `enabled: false`.
	SampleRatio float64 `yaml:"sample_ratio"`
	ServiceName string  `yaml:"service_name"`
	Environment string  `yaml:"environment"`
}

// StorageConfig selects the local key/value engine.
type StorageConfig struct {
	Engine         string `yaml:"engine"` // badger
	Dir            string `yaml:"dir"`
	MaxTaskRecords int    `yaml:"max_task_records"`
}

// Default returns a configuration with every field populated.
func Default() *Config {
	return &Config{
		Node: NodeConfig{
			Name:    "zeptomesh-node",
			DataDir: "./zeptomesh-data",
			Listen: []string{
				"/ip4/0.0.0.0/tcp/4001",
				"/ip4/0.0.0.0/udp/4001/quic-v1",
			},
			Relay: RelayConfig{
				Enabled:          false,
				AdvertiseAsRelay: false,
				Limit: RelayLimitConfig{
					MaxReservations: 128,
					MaxCircuits:     16,
					ReservationTTL:  Duration(time.Hour),
					ConnDuration:    Duration(2 * time.Minute),
					ConnDataBytes:   1 << 17, // 128 KB per direction
				},
			},
		},
		Identity: IdentityConfig{KeyFile: ""},
		Discovery: DiscoveryConfig{
			LocalRegistry:     true,
			LocalSocketDir:    "",
			MDNS:              true,
			MDNSServiceName:   "_zeptomesh._udp",
			DHT:               true,
			DHTMode:           "auto",
			PeerExchange:      true,
			BootstrapInterval: Duration(30 * time.Second),
			Gossip: GossipConfig{
				Enabled:        true,
				Topic:          "/zeptomesh/membership/0.1.0",
				Heartbeat:      Duration(3 * time.Second),
				FullSync:       Duration(60 * time.Second),
				FailureTimeout: Duration(15 * time.Second),
			},
		},
		Neighbors: NeighborsConfig{Min: 8, Target: 16, Max: 64},
		Tasks: TasksConfig{
			DefaultTTL:            5,
			MaxTTL:                10,
			MaxParallelTasks:      4,
			MaxParallelPerPeer:    2,
			DefaultTimeoutSeconds: 600,
			MaxTimeoutSeconds:     3600,
			MaxPayloadBytes:       1 << 20,
			MaxArtifactBytes:      100 << 20,
			MaxWorkspaceBytes:     512 << 20,
			MaxDiskFreeBytes:      1 << 30,
			MaxTaskMemoryBytes:    1 << 30,
			DedupWindow:           Duration(15 * time.Minute),
			Retention:             Duration(7 * 24 * time.Hour),
			Forwarding: ForwardingConfig{
				MaxParallelCandidates: 3,
				MaxFanout:             5,
				RetryInterval:         Duration(5 * time.Second),
				MaxRetries:            2,
				AttemptTimeout:        Duration(120 * time.Second),
				SearchRelay: SearchRelayConfig{
					Enabled:        true,
					MaxDepth:       2,
					Fanout:         3,
					RequestTimeout: Duration(8 * time.Second),
					CacheTTL:       Duration(60 * time.Second),
					Topic: SearchTopicConfig{
						Enabled:        false,
						Name:           "/zeptomesh/skillsearch/0.1.0",
						RequestTTL:     Duration(30 * time.Second),
						MaxAnswers:     8,
						AnswerCooldown: Duration(10 * time.Second),
					},
				},
			},
		},
		Capabilities: CapabilitiesConfig{
			Skills:              []string{"general"},
			ResourceClass:       "medium",
			AcceptExternalTasks: true,
			AllowShell:          false,
			AllowNetworkTools:   true,
			MaxParallelTasks:    4,
			SkillExchange: SkillExchangeConfig{
				Enabled:        true,
				DiscloseTo:     "known",
				Interval:       Duration(30 * time.Second),
				MaxDescriptors: 64,
				ImportLimit:    512,
				Persist:        true,
			},
		},
		PicoClaw: PicoClawConfig{
			Mode:                "stub",
			Binary:              "picoclaw",
			WorkspaceRoot:       "",
			TimeoutSeconds:      600,
			MaxConcurrentAgents: 4,
			HTTP: PicoClawHTTPConfig{
				// PicoClaw's gateway serves the Pico Protocol WebSocket on its own
				// port; there is no REST endpoint that accepts a prompt, so "http"
				// mode means "pico channel over WebSocket" here.
				BaseURL:        "http://127.0.0.1:18790",
				Path:           "/pico/ws",
				HealthPath:     "/health",
				TokenEnv:       "PICOCLAW_MESH_PICO_TOKEN",
				TimeoutSeconds: 600,
			},
			Stub: StubConfig{Latency: Duration(50 * time.Millisecond), Echo: true},
		},
		Security: SecurityConfig{
			TrustMode:              "limited",
			RequireTaskSignature:   true,
			RequireOriginSignature: true,
			DropInvalidSignatures:  true,
			MinTrustForTasks:       "limited",
			MaxMessageBytes:        4 << 20,
			RateLimit: RateLimitConfig{
				RequestsPerSecond: 20,
				Burst:             60,
			},
		},
		API: APIConfig{
			Enabled:      true,
			Listen:       "127.0.0.1:8081",
			ReadTimeout:  Duration(15 * time.Second),
			WriteTimeout: Duration(60 * time.Second),
		},
		Telemetry: TelemetryConfig{
			PrometheusListen: "",
			LogLevel:         "info",
			StructuredLogs:   true,
			Tracing: TracingConfig{
				Enabled:     false,
				SampleRatio: 1,
				ServiceName: "zeptomesh-node",
			},
		},
		Storage: StorageConfig{Engine: "badger", MaxTaskRecords: 100_000},
	}
}

// Load reads a YAML configuration file and merges it over the defaults.
func Load(path string) (*Config, error) {
	cfg := Default()
	if path == "" {
		cfg.applyEnvOverrides()
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	// Expand ${VAR} and ${VAR:-default} so one template can serve many
	// instances and secrets stay out of the file.
	raw = []byte(expandEnv(string(raw)))

	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(false)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	cfg.applyEnvOverrides()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: invalid in %s: %w", path, err)
	}
	return cfg, nil
}

// applyEnvOverrides applies settings that a YAML scalar expansion cannot
// express, because the target is a sequence: ZETOMESH_BOOTSTRAP is a comma
// separated list of multiaddrs appended to discovery.bootstrap, so one mounted
// template can serve every instance of a mesh without editing the file.
func (c *Config) applyEnvOverrides() {
	v := strings.TrimSpace(os.Getenv("ZETOMESH_BOOTSTRAP"))
	if v == "" {
		return
	}
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !slices.Contains(c.Discovery.Bootstrap, part) {
			c.Discovery.Bootstrap = append(c.Discovery.Bootstrap, part)
		}
	}
}

// expandEnv substitutes ${VAR} and ${VAR:-default} from the process
// environment. It exists so a single config template can be reused across many
// instances with different ports and data dirs.
//
// Default values may themselves contain references
// (${ZETOMESH_DATA:-./zeptomesh-data}/${ZETOMESH_INDEX:-0}), so the closing
// brace is found by depth counting, not by the first '}' — and the resolved
// value is expanded again, so a nested reference in a default is honoured.
func expandEnv(s string) string {
	var out strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '$' || i+1 >= len(s) || s[i+1] != '{' {
			out.WriteByte(s[i])
			i++
			continue
		}
		end, ok := findExprEnd(s[i:])
		if !ok {
			out.WriteByte(s[i])
			i++
			continue
		}
		expr := s[i+2 : i+end]
		i += end + 1
		name, def := expr, ""
		hasDefault := false
		if idx := strings.Index(expr, ":-"); idx >= 0 {
			name, def, hasDefault = expr[:idx], expr[idx+2:], true
		}
		if v, found := os.LookupEnv(strings.TrimSpace(name)); found && v != "" {
			out.WriteString(expandEnv(v))
			continue
		}
		if hasDefault {
			out.WriteString(expandEnv(def))
		}
	}
	return out.String()
}

// findExprEnd returns the offset of the '}' closing the expression that starts
// at s[0] == '$', s[1] == '{', counting every brace so that a default may hold
// nested ${...} references and literal braces alike. ok is false when unbalanced.
func findExprEnd(s string) (int, bool) {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return 0, false
}

// Validate checks invariants and derives path defaults relative to data_dir.
func (c *Config) Validate() error {
	var errs []error

	if c.Node.Name == "" {
		errs = append(errs, errors.New("node.name is required"))
	}
	if c.Node.DataDir == "" {
		errs = append(errs, errors.New("node.data_dir is required"))
	}
	if len(c.Node.Listen) == 0 {
		errs = append(errs, errors.New("node.listen must list at least one multiaddr"))
	}
	for _, a := range c.Node.Listen {
		if !strings.HasPrefix(a, "/") {
			errs = append(errs, fmt.Errorf("node.listen: %q is not a multiaddr", a))
		}
	}

	// Derived paths.
	if c.Identity.KeyFile == "" {
		c.Identity.KeyFile = filepath.Join(c.Node.DataDir, "keys", "peer.key")
	}
	if c.Storage.Dir == "" {
		c.Storage.Dir = filepath.Join(c.Node.DataDir, "db")
	}
	if c.Discovery.LocalSocketDir == "" {
		c.Discovery.LocalSocketDir = filepath.Join(c.Node.DataDir, "run")
	}
	if c.PicoClaw.WorkspaceRoot == "" {
		c.PicoClaw.WorkspaceRoot = filepath.Join(c.Node.DataDir, "picoclaw", "workspaces")
	}
	c.Node.DataDir = filepath.Clean(c.Node.DataDir)

	switch strings.ToLower(c.PicoClaw.Mode) {
	case "stub", "binary", "http":
	default:
		errs = append(errs, fmt.Errorf("picoclaw.mode %q must be one of stub|binary|http", c.PicoClaw.Mode))
	}
	if c.PicoClaw.MaxConcurrentAgents <= 0 {
		c.PicoClaw.MaxConcurrentAgents = 1
	}
	if c.PicoClaw.TimeoutSeconds <= 0 {
		c.PicoClaw.TimeoutSeconds = c.Tasks.DefaultTimeoutSeconds
	}

	switch strings.ToLower(c.Security.TrustMode) {
	case "open", "limited", "private":
	default:
		errs = append(errs, fmt.Errorf("security.trust_mode %q must be one of open|limited|private", c.Security.TrustMode))
	}

	if c.Tasks.MaxParallelTasks <= 0 {
		errs = append(errs, errors.New("tasks.max_parallel_tasks must be > 0"))
	}
	if c.Tasks.DefaultTTL <= 0 {
		errs = append(errs, errors.New("tasks.default_ttl must be > 0"))
	}
	if c.Tasks.MaxTTL < c.Tasks.DefaultTTL {
		errs = append(errs, errors.New("tasks.max_ttl must be >= tasks.default_ttl"))
	}
	if c.Tasks.Forwarding.MaxFanout <= 0 {
		errs = append(errs, errors.New("tasks.forwarding.max_fanout must be > 0"))
	}
	if c.Tasks.Forwarding.MaxParallelCandidates <= 0 {
		c.Tasks.Forwarding.MaxParallelCandidates = 1
	}
	if c.Tasks.Forwarding.MaxParallelCandidates > c.Tasks.Forwarding.MaxFanout {
		errs = append(errs, errors.New("tasks.forwarding.max_parallel_candidates must be <= max_fanout"))
	}
	if st := c.Tasks.Forwarding.SearchRelay.Topic; st.Enabled {
		if st.Name == "" || !strings.HasPrefix(st.Name, "/") {
			errs = append(errs, errors.New("tasks.forwarding.search_relay.topic.name must be a non-empty topic id starting with /"))
		}
		if st.RequestTTL <= 0 {
			errs = append(errs, errors.New("tasks.forwarding.search_relay.topic.request_ttl must be > 0"))
		}
		if st.MaxAnswers <= 0 {
			errs = append(errs, errors.New("tasks.forwarding.search_relay.topic.max_answers must be > 0"))
		}
		if st.AnswerCooldown < 0 {
			errs = append(errs, errors.New("tasks.forwarding.search_relay.topic.answer_cooldown must be >= 0"))
		}
	}
	if c.Tasks.MaxParallelPerPeer < 0 {
		errs = append(errs, errors.New("tasks.max_parallel_tasks_per_peer must be >= 0"))
	}
	if c.Tasks.DefaultTimeoutSeconds <= 0 {
		c.Tasks.DefaultTimeoutSeconds = 600
	}
	if c.Tasks.MaxTimeoutSeconds <= 0 {
		errs = append(errs, errors.New("tasks.max_timeout_seconds must be > 0"))
	}
	if c.Tasks.MaxTimeoutSeconds < c.Tasks.DefaultTimeoutSeconds {
		errs = append(errs, errors.New("tasks.max_timeout_seconds must be >= tasks.default_timeout_seconds"))
	}
	if c.Tasks.MaxPayloadBytes <= 0 {
		errs = append(errs, errors.New("tasks.max_task_payload_bytes must be > 0"))
	}
	if c.Tasks.MaxArtifactBytes <= 0 {
		errs = append(errs, errors.New("tasks.max_artifact_bytes must be > 0"))
	}
	if c.Tasks.MaxWorkspaceBytes < 0 || c.Tasks.MaxDiskFreeBytes < 0 || c.Tasks.MaxTaskMemoryBytes < 0 {
		errs = append(errs, errors.New("tasks resource limits must be >= 0 (0 disables the guard)"))
	}
	if c.Node.Relay.Limit.MaxReservations < 0 || c.Node.Relay.Limit.MaxCircuits < 0 {
		errs = append(errs, errors.New("node.relay.limit values must be >= 0"))
	}
	if c.Neighbors.Max < c.Neighbors.Target || c.Neighbors.Target < c.Neighbors.Min {
		errs = append(errs, errors.New("neighbors must satisfy min <= target <= max"))
	}
	if c.Discovery.Gossip.Heartbeat.D() <= 0 {
		errs = append(errs, errors.New("discovery.gossip.heartbeat must be > 0"))
	}
	if c.Discovery.Gossip.FailureTimeout.D() <= c.Discovery.Gossip.Heartbeat.D() {
		errs = append(errs, errors.New("discovery.gossip.failure_timeout must exceed heartbeat"))
	}
	if c.API.Enabled && c.API.Listen == "" {
		errs = append(errs, errors.New("api.listen is required when api.enabled"))
	}
	// Tracing (ТЗ 14.3): only checked when it is on — a disabled section must
	// never be able to stop the node. The format rules are the ones that would
	// otherwise surface as an opaque export failure at the first span.
	if c.Telemetry.Tracing.Enabled {
		t := c.Telemetry.Tracing
		if t.SampleRatio < 0 || t.SampleRatio > 1 {
			errs = append(errs, fmt.Errorf("telemetry.tracing.sample_ratio %v must be within 0..1", t.SampleRatio))
		}
		if ep := strings.TrimSpace(t.Endpoint); ep != "" {
			if strings.ContainsAny(ep, " \t") || strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") ||
				strings.Contains(ep, "/") || strings.HasSuffix(ep, ":") {
				errs = append(errs, fmt.Errorf("telemetry.tracing.endpoint %q must be host:port without a scheme or path (use telemetry.tracing.insecure for plaintext)", ep))
			}
		}
	}
	if c.Security.RateLimit.RequestsPerSecond <= 0 {
		c.Security.RateLimit.RequestsPerSecond = 20
	}
	if c.Security.RateLimit.Burst <= 0 {
		c.Security.RateLimit.Burst = 60
	}
	if c.Security.MaxMessageBytes <= 0 {
		c.Security.MaxMessageBytes = 4 << 20
	}
	if c.Tasks.DedupWindow.D() <= 0 {
		c.Tasks.DedupWindow = Duration(15 * time.Minute)
	}

	// Skills listed twice or disabled collide with the accepted set.
	seen := make(map[string]bool, len(c.Capabilities.Skills))
	for _, s := range c.Capabilities.Skills {
		if s == "" {
			errs = append(errs, errors.New("capabilities.skills contains an empty entry"))
			continue
		}
		if seen[s] {
			errs = append(errs, fmt.Errorf("capabilities.skills has duplicate %q", s))
		}
		seen[s] = true
	}
	for _, s := range c.Capabilities.DisabledSkills {
		delete(seen, s)
	}
	if len(seen) == 0 && c.Capabilities.AcceptExternalTasks {
		errs = append(errs, errors.New("capabilities.skills resolves to an empty set"))
	}
	if c.Capabilities.MaxParallelTasks <= 0 {
		c.Capabilities.MaxParallelTasks = c.Tasks.MaxParallelTasks
	}

	// Skill exchange: a descriptor must document a skill this node actually
	// advertises, otherwise it would describe a capability nobody serves here.
	advertised := make(map[string]bool, len(seen))
	for s := range seen {
		advertised[strings.ToLower(s)] = true
	}
	for _, d := range c.Capabilities.SkillDocs {
		name := strings.ToLower(strings.TrimSpace(d.Name))
		if name == "" {
			errs = append(errs, errors.New("capabilities.skill_docs entry has no name"))
			continue
		}
		if !advertised[name] {
			errs = append(errs, fmt.Errorf(
				"capabilities.skill_docs documents %q which is not in capabilities.skills", d.Name))
		}
	}
	switch strings.ToLower(strings.TrimSpace(c.Capabilities.SkillExchange.DiscloseTo)) {
	case "":
		c.Capabilities.SkillExchange.DiscloseTo = "known"
	case "any", "trusted", "known":
		c.Capabilities.SkillExchange.DiscloseTo = strings.ToLower(strings.TrimSpace(c.Capabilities.SkillExchange.DiscloseTo))
	default:
		errs = append(errs, fmt.Errorf(
			"capabilities.skill_exchange.disclose_to %q must be trusted|known|any",
			c.Capabilities.SkillExchange.DiscloseTo))
	}
	if c.Capabilities.SkillExchange.Enabled {
		if c.Capabilities.SkillExchange.Interval.D() <= 0 {
			c.Capabilities.SkillExchange.Interval = Duration(30 * time.Second)
		}
		if c.Capabilities.SkillExchange.MaxDescriptors <= 0 {
			c.Capabilities.SkillExchange.MaxDescriptors = 64
		}
		if c.Capabilities.SkillExchange.ImportLimit <= 0 {
			c.Capabilities.SkillExchange.ImportLimit = 512
		}
	}

	// Scheduled triggers: the schedule list is rejected as a whole before the
	// node starts, so a typo in a cron expression is a startup error rather
	// than a job that silently never runs.
	triggerIDs := make(map[string]bool, len(c.Triggers))
	for i, tc := range c.Triggers {
		if strings.TrimSpace(tc.ID) == "" {
			errs = append(errs, fmt.Errorf("triggers[%d]: id is required", i))
			continue
		}
		if triggerIDs[tc.ID] {
			errs = append(errs, fmt.Errorf("triggers: id %q is declared twice", tc.ID))
		}
		triggerIDs[tc.ID] = true
		if err := tc.ToDomain().Validate(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// ArtifactsDir returns the content-addressed artifact root.
func (c *Config) ArtifactsDir() string {
	return filepath.Join(c.Node.DataDir, "artifacts")
}

// TasksDir returns the per-task scratch root.
func (c *Config) TasksDir() string {
	return filepath.Join(c.Node.DataDir, "tasks")
}

// AuditDir returns the security journal root.
func (c *Config) AuditDir() string {
	return filepath.Join(c.Node.DataDir, "audit")
}

// ConfigTriggers renders the YAML-declared schedules as domain objects. The
// node's scheduler treats this list as the authoritative config view on a hot
// reload.
func (c *Config) ConfigTriggers() []*triggers.Trigger {
	if len(c.Triggers) == 0 {
		return nil
	}
	out := make([]*triggers.Trigger, 0, len(c.Triggers))
	for _, tc := range c.Triggers {
		out = append(out, tc.ToDomain())
	}
	return out
}

// EffectiveSkills returns declared skills minus operator-disabled ones.
func (c *Config) EffectiveSkills() []string {
	skip := make(map[string]bool, len(c.Capabilities.DisabledSkills))
	for _, s := range c.Capabilities.DisabledSkills {
		skip[s] = true
	}
	out := make([]string, 0, len(c.Capabilities.Skills))
	for _, s := range c.Capabilities.Skills {
		if !skip[s] {
			out = append(out, s)
		}
	}
	return out
}

// ReloadDiff classifies what changed between the running config and a freshly
// parsed one: some sections are honoured by live code (the task manager reads
// cfg.Tasks every call; trust decisions read cfg.Capabilities), others are
// baked in at startup (listeners, PSK, gossip topic, DHT, API bind).
//
// Anything that can only be honoured by a restart is listed honestly rather
// than half-applied: the admin must know the process is not yet using it.
type ReloadDiff struct {
	Hot             []string `json:"hot"`
	RequiresRestart []string `json:"requires_restart"`
}

// Diff compares the running config c with the candidate next (already
// defaulted and validated). Sections absent from both lists are unchanged.
func (c *Config) Diff(next *Config) ReloadDiff {
	var d ReloadDiff
	addHot := func(name string, changed bool) {
		if changed {
			d.Hot = append(d.Hot, name)
		}
	}
	addRestart := func(name string, changed bool) {
		if changed {
			d.RequiresRestart = append(d.RequiresRestart, name)
		}
	}
	addHot("security.trust_mode", c.Security.TrustMode != next.Security.TrustMode)
	addHot("security.min_trust_for_tasks", c.Security.MinTrustForTasks != next.Security.MinTrustForTasks)
	addHot("security.rate_limit", !reflect.DeepEqual(c.Security.RateLimit, next.Security.RateLimit))
	addHot("security.allowed_peers_file", c.Security.AllowedPeersFile != next.Security.AllowedPeersFile)
	addHot("security.blocked_peers_file", c.Security.BlockedPeersFile != next.Security.BlockedPeersFile)
	addHot("telemetry.log_level", c.Telemetry.LogLevel != next.Telemetry.LogLevel)
	// The scheduler keeps a live, synchronised copy of the config-declared
	// schedules; ReloadConfig pushes the new list through SetConfigTriggers,
	// which validates as a group before swapping.
	addHot("triggers", !reflect.DeepEqual(c.Triggers, next.Triggers))
	// "Hot" means the value has a synchronised live owner (Policy, Limiter,
	// slog.LevelVar) that ReloadConfig updates; the config struct itself is
	// never mutated, so readers of Cfg keep a consistent startup snapshot.
	// Everything else is read from unsynchronised hot paths and honestly
	// requires a restart.
	addRestart("tasks", !reflect.DeepEqual(c.Tasks, next.Tasks))
	addRestart("security.require_task_signature", c.Security.RequireTaskSignature != next.Security.RequireTaskSignature)
	addRestart("security.drop_invalid_signatures", c.Security.DropInvalidSignatures != next.Security.DropInvalidSignatures)
	addRestart("security.max_message_bytes", c.Security.MaxMessageBytes != next.Security.MaxMessageBytes)
	addRestart("capabilities", !reflect.DeepEqual(c.Capabilities, next.Capabilities))
	addRestart("node", !reflect.DeepEqual(c.Node, next.Node))
	addRestart("identity", c.Identity.KeyFile != next.Identity.KeyFile)
	addRestart("discovery", !reflect.DeepEqual(c.Discovery, next.Discovery))
	addRestart("api", !reflect.DeepEqual(c.API, next.API))
	addRestart("storage", !reflect.DeepEqual(c.Storage, next.Storage))
	addRestart("picoclaw", !reflect.DeepEqual(c.PicoClaw, next.PicoClaw))
	addRestart("neighbors", !reflect.DeepEqual(c.Neighbors, next.Neighbors))
	// The SDK installs its global providers once, at process start; turning
	// tracing on or off needs a restart — and the trace context already riding
	// in signed labels is untouched by it.
	addRestart("telemetry.tracing", !reflect.DeepEqual(c.Telemetry.Tracing, next.Telemetry.Tracing))
	return d
}
