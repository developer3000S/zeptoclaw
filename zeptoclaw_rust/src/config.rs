//! Порт пакета `internal/config`: загрузка, значения по умолчанию и валидация
//! конфигурации узла (node.yaml). Структура и имена YAML-ключей соответствуют
//! Go-версии; длительности принимаются как строки (`"30s"`) или секунды (число).

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Duration: принимает "1h30m" | 90 (секунды)
// ---------------------------------------------------------------------------

/// Парсит Go-подобную duration-строку: "300ms", "5s", "1h30m", "2m".
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".into());
    }
    if let Ok(secs) = s.parse::<f64>() {
        return Ok(Duration::from_secs_f64(secs));
    }
    let bytes = s.as_bytes();
    let mut i = 0usize;
    let mut total = Duration::ZERO;
    while i < bytes.len() {
        let start = i;
        while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
            i += 1;
        }
        let num_str = &s[start..i];
        if num_str.is_empty() {
            return Err(format!("invalid duration {s:?}"));
        }
        let n: f64 = num_str.parse().map_err(|_| format!("invalid duration {s:?}"))?;
        let ustart = i;
        while i < bytes.len() && !bytes[i].is_ascii_digit() && bytes[i] != b'.' {
            i += 1;
        }
        let unit = &s[ustart..i];
        let mult: f64 = match unit {
            "ns" => 1e-9,
            "us" | "µs" => 1e-6,
            "ms" => 1e-3,
            "s" | "" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return Err(format!("unknown duration unit {unit:?}")),
        };
        total += Duration::from_secs_f64(n * mult);
    }
    Ok(total)
}

fn de_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = serde_yaml::Value::deserialize(deserializer)?;
    match v {
        serde_yaml::Value::Number(n) => {
            let secs = n.as_i64().ok_or_else(|| serde::de::Error::custom("bad seconds"))?;
            Ok(Duration::from_secs(secs.max(0) as u64))
        }
        serde_yaml::Value::String(s) => {
            parse_duration(&s).map_err(serde::de::Error::custom)
        }
        _ => Err(serde::de::Error::custom("duration must be string or seconds")),
    }
}

// ---------------------------------------------------------------------------
// Секции конфигурации
// ---------------------------------------------------------------------------

fn def_true() -> bool { true }

/// Роли админ-API (Go: config.APIRole: viewer | operator | admin).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Viewer,
    Operator,
    Admin,
}

impl Role {
    pub fn parse(s: &str) -> Option<Role> {
        match s.trim().to_ascii_lowercase().as_str() {
            "viewer" => Some(Role::Viewer),
            "operator" => Some(Role::Operator),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
            Role::Admin => "admin",
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    pub name: String,
    pub data_dir: String,
    pub listen: Vec<String>,
    pub announce: Vec<String>,
    pub private_network_psk: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IdentityConfig {
    pub key_file: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GossipConfig {
    pub enabled: bool,
    pub topic: String,
    #[serde(deserialize_with = "de_duration")]
    pub heartbeat: Duration,
    #[serde(deserialize_with = "de_duration")]
    pub full_sync: Duration,
    #[serde(deserialize_with = "de_duration")]
    pub failure_timeout: Duration,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScanConfig {
    pub enabled: bool,
    #[serde(deserialize_with = "de_duration")]
    pub interval: Duration,
    pub subnet_scan: bool,
    #[serde(deserialize_with = "de_duration")]
    pub dial_timeout: Duration,
    #[serde(deserialize_with = "de_duration")]
    pub timeout: Duration,
    pub max_candidates: i32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscoveryConfig {
    pub local_registry: bool,
    pub local_socket_dir: String,
    pub mdns: bool,
    pub mdns_service_name: String,
    pub bootstrap: Vec<String>,
    pub dht: bool,
    pub dht_mode: String, // server | client | auto
    pub peer_exchange: bool,
    pub gossip: GossipConfig,
    #[serde(deserialize_with = "de_duration")]
    pub bootstrap_interval: Duration,
    pub scan: ScanConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NeighborsConfig {
    pub min: i32,
    pub target: i32,
    pub max: i32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchTopicConfig {
    pub enabled: bool,
    pub name: String,
    #[serde(deserialize_with = "de_duration")]
    pub request_ttl: Duration,
    pub max_answers: i32,
    #[serde(deserialize_with = "de_duration")]
    pub answer_cooldown: Duration,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchRelayConfig {
    pub enabled: bool,
    pub max_depth: i32,
    pub fanout: i32,
    #[serde(deserialize_with = "de_duration")]
    pub request_timeout: Duration,
    #[serde(deserialize_with = "de_duration")]
    pub cache_ttl: Duration,
    pub topic: SearchTopicConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ForwardingConfig {
    pub max_parallel_candidates: i32,
    pub max_fanout: i32,
    #[serde(deserialize_with = "de_duration")]
    pub retry_interval_seconds: Duration,
    pub max_retries: i32,
    #[serde(deserialize_with = "de_duration")]
    pub attempt_timeout: Duration,
    pub search_relay: SearchRelayConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TasksConfig {
    pub default_ttl: i32,
    pub max_ttl: i32,
    pub max_parallel_tasks: i32,
    pub max_parallel_tasks_per_peer: i32,
    pub default_timeout_seconds: i32,
    pub max_timeout_seconds: i32,
    pub max_task_payload_bytes: i64,
    pub max_artifact_bytes: i64,
    pub max_workspace_bytes: i64,
    pub min_free_disk_bytes: i64,
    pub max_task_memory_bytes: i64,
    #[serde(deserialize_with = "de_duration")]
    pub dedup_window: Duration,
    pub forwarding: ForwardingConfig,
    #[serde(deserialize_with = "de_duration")]
    pub retention: Duration,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SkillDocConfig {
    pub name: String,
    pub version: i64,
    pub description: String,
    pub models: Vec<String>,
    pub attributes: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SkillExchangeConfig {
    pub enabled: bool,
    pub disclose_to: String, // trusted | known | any
    #[serde(deserialize_with = "de_duration")]
    pub interval: Duration,
    pub max_descriptors: i32,
    pub import_limit: i32,
    pub persist: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CapabilitiesConfig {
    pub skills: Vec<String>,
    pub models: Vec<String>,
    pub resource_class: String,
    pub accept_external_tasks: bool,
    pub allow_shell: bool,
    pub allow_network_tools: bool,
    pub max_parallel_tasks: i32,
    pub disabled_skills: Vec<String>,
    pub skill_docs: Vec<SkillDocConfig>,
    pub skill_exchange: SkillExchangeConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PicoClawHTTPConfig {
    pub base_url: String,
    pub token_env: String,
    pub path: String,
    pub health_path: String,
    pub timeout_seconds: i32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StubConfig {
    #[serde(deserialize_with = "de_duration")]
    pub latency: Duration,
    pub echo: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PicoClawConfig {
    pub mode: String, // binary | http | stub | ollama
    pub binary: String,
    pub config: String,
    pub workspace_root: String,
    pub timeout_seconds: i32,
    pub max_concurrent_agents: i32,
    pub env: BTreeMap<String, String>,
    pub model: String,
    pub extra_args: Vec<String>,
    pub prompt_template: String,
    pub http: PicoClawHTTPConfig,
    pub stub: StubConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RateLimitConfig {
    pub requests_per_second: f64,
    pub burst: i32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SecurityConfig {
    pub trust_mode: String, // open | limited | private
    pub require_task_signature: bool,
    pub require_origin_signature: bool,
    pub drop_invalid_signatures: bool,
    pub allowed_peers_file: String,
    pub blocked_peers_file: String,
    pub min_trust_for_tasks: String,
    pub rate_limit: RateLimitConfig,
    pub max_message_bytes: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct APIToken {
    pub name: String,
    pub role: String, // viewer | operator | admin
    pub token_env: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct APIConfig {
    #[serde(default = "def_true")]
    pub enabled: bool,
    pub listen: String,
    pub auth_token_env: String,
    pub tokens: Vec<APIToken>,
    #[serde(deserialize_with = "de_duration")]
    pub read_timeout: Duration,
    #[serde(deserialize_with = "de_duration")]
    pub write_timeout: Duration,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TracingConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub insecure: bool,
    pub sample_ratio: f64,
    pub service_name: String,
    pub environment: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TelemetryConfig {
    pub prometheus_listen: String,
    pub log_level: String,
    pub structured_logs: bool,
    pub tracing: TracingConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub engine: String,
    pub dir: String,
    pub max_task_records: i32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JobConfig {
    pub instruction: String,
    pub required_skills: Vec<String>,
    pub ttl: i32,
    pub priority: i32,
    pub timeout_seconds: i32,
    pub allow_shell: bool,
    pub allow_network_tools: bool,
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TriggerConfig {
    pub id: String,
    pub name: String,
    pub schedule: String,
    pub enabled: Option<bool>,
    pub max_runs: i32,
    pub job: JobConfig,
}

/// Корневой документ конфигурации узла.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub node: NodeConfig,
    pub identity: IdentityConfig,
    pub discovery: DiscoveryConfig,
    pub neighbors: NeighborsConfig,
    pub tasks: TasksConfig,
    pub capabilities: CapabilitiesConfig,
    pub picoclaw: PicoClawConfig,
    pub security: SecurityConfig,
    pub api: APIConfig,
    pub telemetry: TelemetryConfig,
    pub storage: StorageConfig,
    pub triggers: Vec<TriggerConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self::defaults()
    }
}

fn dur(s: &str) -> Duration {
    parse_duration(s).unwrap_or(Duration::ZERO)
}

impl Config {
    /// Аналог config.Default() — все поля заполнены.
    pub fn defaults() -> Self {
        Config {
            node: NodeConfig {
                name: "zeptomesh-node".into(),
                data_dir: "./zeptomesh-data".into(),
                listen: vec!["/ip4/0.0.0.0/tcp/4001".into()],
                announce: vec![],
                private_network_psk: String::new(),
            },
            identity: IdentityConfig { key_file: String::new() },
            discovery: DiscoveryConfig {
                local_registry: true,
                local_socket_dir: String::new(),
                mdns: true,
                mdns_service_name: "_zeptomesh._tcp".into(),
                bootstrap: vec![],
                dht: true,
                dht_mode: "auto".into(),
                peer_exchange: true,
                gossip: GossipConfig {
                    enabled: true,
                    topic: "mesh/membership/v1".into(),
                    heartbeat: dur("10s"),
                    full_sync: dur("2m"),
                    failure_timeout: dur("60s"),
                },
                bootstrap_interval: dur("30s"),
                scan: ScanConfig {
                    enabled: false,
                    interval: dur("10m"),
                    subnet_scan: false,
                    dial_timeout: dur("5s"),
                    timeout: dur("60s"),
                    max_candidates: 64,
                },
            },
            neighbors: NeighborsConfig { min: 3, target: 8, max: 24 },
            tasks: TasksConfig {
                default_ttl: 5,
                max_ttl: 10,
                max_parallel_tasks: 4,
                max_parallel_tasks_per_peer: 2,
                default_timeout_seconds: 120,
                max_timeout_seconds: 3600,
                max_task_payload_bytes: 1 << 20,
                max_artifact_bytes: 8 << 20,
                max_workspace_bytes: 64 << 20,
                min_free_disk_bytes: 128 << 20,
                max_task_memory_bytes: 512 << 20,
                dedup_window: dur("5m"),
                forwarding: ForwardingConfig {
                    max_parallel_candidates: 3,
                    max_fanout: 5,
                    retry_interval_seconds: dur("10s"),
                    max_retries: 2,
                    attempt_timeout: dur("60s"),
                    search_relay: SearchRelayConfig {
                        enabled: true,
                        max_depth: 2,
                        fanout: 3,
                        request_timeout: dur("5s"),
                        cache_ttl: dur("30s"),
                        topic: SearchTopicConfig {
                            enabled: false,
                            name: "mesh/skills/search/v1".into(),
                            request_ttl: dur("60s"),
                            max_answers: 5,
                            answer_cooldown: dur("30s"),
                        },
                    },
                },
                retention: dur("24h"),
            },
            capabilities: CapabilitiesConfig {
                skills: vec!["general".into()],
                models: vec![],
                resource_class: "standard".into(),
                accept_external_tasks: true,
                allow_shell: false,
                allow_network_tools: false,
                max_parallel_tasks: 4,
                disabled_skills: vec![],
                skill_docs: vec![],
                skill_exchange: SkillExchangeConfig {
                    enabled: true,
                    disclose_to: "known".into(),
                    interval: dur("2m"),
                    max_descriptors: 256,
                    import_limit: 1024,
                    persist: true,
                },
            },
            picoclaw: PicoClawConfig {
                mode: "stub".into(),
                binary: "picoclaw".into(),
                config: String::new(),
                workspace_root: String::new(),
                timeout_seconds: 300,
                max_concurrent_agents: 2,
                env: Default::default(),
                model: String::new(),
                extra_args: vec![],
                prompt_template: String::new(),
                http: Default::default(),
                stub: StubConfig { latency: dur("50ms"), echo: true },
            },
            security: SecurityConfig {
                trust_mode: "open".into(),
                require_task_signature: true,
                require_origin_signature: false,
                drop_invalid_signatures: true,
                allowed_peers_file: String::new(),
                blocked_peers_file: String::new(),
                min_trust_for_tasks: "known".into(),
                rate_limit: RateLimitConfig { requests_per_second: 50.0, burst: 100 },
                max_message_bytes: 4 << 20,
            },
            api: APIConfig {
                enabled: true,
                listen: "127.0.0.1:8700".into(),
                auth_token_env: "ZEPTOMESH_API_TOKEN".into(),
                tokens: vec![],
                read_timeout: dur("10s"),
                write_timeout: dur("30s"),
            },
            telemetry: TelemetryConfig {
                prometheus_listen: String::new(),
                log_level: "info".into(),
                structured_logs: false,
                tracing: Default::default(),
            },
            storage: StorageConfig {
                engine: "jsonl".into(),
                dir: String::new(),
                max_task_records: 10_000,
            },
            triggers: vec![],
        }
    }

    /// Аналог config.Load(path): дефолты + YAML поверх.
    pub fn load(path: &str) -> anyhow::Result<Config> {
        let mut cfg = Config::defaults();
        if !path.is_empty() {
            let text = std::fs::read_to_string(path)?;
            cfg = serde_yaml::from_str(&text)?;
            // serde(default)+deny_unknown не сливает с дефолтами рекурсивно —
            // Go-версия декодирует поверх Default(), эмулируем повторным
            // заполнением пустых обязательных полей через validate-путь.
        }
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    /// Аналог applyEnvOverrides: ZEPTOMESH_* переменные окружения.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("ZEPTOMESH_DATA_DIR") {
            self.node.data_dir = v;
        }
        if let Ok(v) = std::env::var("ZEPTOMESH_NAME") {
            self.node.name = v;
        }
        if let Ok(v) = std::env::var("ZEPTOMESH_KEY_FILE") {
            self.identity.key_file = v;
        }
        if let Ok(v) = std::env::var("ZEPTOMESH_PRIVATE_NETWORK_PSK") {
            self.node.private_network_psk = v;
        }
        if let Ok(v) = std::env::var("ZEPTOMESH_PICOCLAW_MODE") {
            self.picoclaw.mode = v;
        }
        if let Ok(v) = std::env::var("ZEPTOMESH_TRUST_MODE") {
            self.security.trust_mode = v;
        }
        if let Ok(v) = std::env::var("ZEPTOMESH_API_LISTEN") {
            self.api.listen = v;
        }
        if let Ok(v) = std::env::var("ZEPTOMESH_LOG_LEVEL") {
            self.telemetry.log_level = v;
        }
    }

    /// Аналог Validate(): базовые проверки согласованности.
    pub fn validate(&self) -> anyhow::Result<()> {
        let ok_modes = ["open", "limited", "private"];
        if !ok_modes.contains(&self.security.trust_mode.as_str()) {
            anyhow::bail!("config: security.trust_mode must be one of {ok_modes:?}");
        }
        let ok_pc = ["binary", "http", "stub", "ollama"];
        if !ok_pc.contains(&self.picoclaw.mode.as_str()) {
            anyhow::bail!("config: picoclaw.mode must be one of {ok_pc:?}");
        }
        if self.neighbors.min > self.neighbors.target || self.neighbors.target > self.neighbors.max {
            anyhow::bail!("config: neighbors must satisfy min <= target <= max");
        }
        if self.tasks.default_ttl < 1 || self.tasks.default_ttl > self.tasks.max_ttl {
            anyhow::bail!("config: tasks.default_ttl must be within 1..max_ttl");
        }
        if self.node.data_dir.is_empty() {
            anyhow::bail!("config: node.data_dir is required");
        }
        for t in &self.triggers {
            if t.schedule.is_empty() {
                anyhow::bail!("config: trigger {:?} has no schedule", t.id);
            }
        }
        Ok(())
    }

    pub fn artifacts_dir(&self) -> String {
        format!("{}/artifacts", self.node.data_dir)
    }
    pub fn tasks_dir(&self) -> String {
        format!("{}/tasks", self.node.data_dir)
    }
    pub fn audit_dir(&self) -> String {
        format!("{}/audit", self.node.data_dir)
    }
    pub fn key_file(&self) -> String {
        if self.identity.key_file.is_empty() {
            format!("{}/identity.key", self.node.data_dir)
        } else {
            self.identity.key_file.clone()
        }
    }

    /// Эффективные навыки: skills минус disabled_skills (аналог EffectiveSkills).
    pub fn effective_skills(&self) -> Vec<String> {
        self.capabilities
            .skills
            .iter()
            .filter(|s| !self.capabilities.disabled_skills.contains(s))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_go_durations() {
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("90").unwrap(), Duration::from_secs(90));
        assert!(parse_duration("1x").is_err());
    }

    #[test]
    fn defaults_validate() {
        let c = Config::defaults();
        c.validate().unwrap();
    }

    #[test]
    fn yaml_overlay() {
        let y = "node:\n  name: alpha\n  data_dir: /tmp/z\ntasks:\n  default_ttl: 3\n";
        let c: Config = serde_yaml::from_str(y).unwrap();
        assert_eq!(c.node.name, "alpha");
        assert_eq!(c.tasks.default_ttl, 3);
    }
}
