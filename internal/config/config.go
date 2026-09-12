// Package config loads, defaults and validates node configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

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
}

// NodeConfig describes the daemon's own identity on disk and on the wire.
type NodeConfig struct {
	Name           string   `yaml:"name"`
	DataDir        string   `yaml:"data_dir"`
	Listen         []string `yaml:"listen"`
	Announce       []string `yaml:"announce"`
	PrivateNetwork string   `yaml:"private_network_psk"` // optional hex PSK: isolates the mesh
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
	DefaultTimeoutSeconds int              `yaml:"default_timeout_seconds"`
	MaxPayloadBytes       int64            `yaml:"max_task_payload_bytes"`
	MaxArtifactBytes      int64            `yaml:"max_artifact_bytes"`
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
	TrustMode             string          `yaml:"trust_mode"` // open | limited | private
	RequireTaskSignature  bool            `yaml:"require_task_signature"`
	DropInvalidSignatures bool            `yaml:"drop_invalid_signatures"`
	AllowedPeersFile      string          `yaml:"allowed_peers_file"`
	BlockedPeersFile      string          `yaml:"blocked_peers_file"`
	MinTrustForTasks      string          `yaml:"min_trust_for_tasks"`
	RateLimit             RateLimitConfig `yaml:"rate_limit"`
	MaxMessageBytes       int64           `yaml:"max_message_bytes"`
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
	PrometheusListen string `yaml:"prometheus_listen"`
	LogLevel         string `yaml:"log_level"`
	StructuredLogs   bool   `yaml:"structured_logs"`
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
			DefaultTimeoutSeconds: 600,
			MaxPayloadBytes:       1 << 20,
			MaxArtifactBytes:      100 << 20,
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
			TrustMode:             "limited",
			RequireTaskSignature:  true,
			DropInvalidSignatures: true,
			MinTrustForTasks:      "limited",
			MaxMessageBytes:       4 << 20,
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
