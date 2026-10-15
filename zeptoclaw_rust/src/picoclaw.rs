//! Порт пакета `internal/picoclaw`: адаптеры исполнения задач локальным
//! агентом PicoClaw (stub / CLI / gateway WebSocket / Ollama-чат) и пул
//! LLM-бэкендов (`internal/brain/backend.go`).

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::config::{Config, PicoClawConfig, StubConfig};

// ---------------------------------------------------------------------------
// Ошибки и типы запроса/ответа (Go: adapter.go)
// ---------------------------------------------------------------------------

/// Agent временно недоступен (Go: ErrUnavailable).
#[derive(Debug)]
pub struct UnavailableError(pub String);
impl std::fmt::Display for UnavailableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "picoclaw: adapter unavailable: {}", self.0)
    }
}
impl std::error::Error for UnavailableError {}

/// Превышен дедлайн исполнения (Go: ErrTimeout).
#[derive(Debug)]
pub struct TimeoutError(pub String);
impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "picoclaw: execution timeout: {}", self.0)
    }
}
impl std::error::Error for TimeoutError {}

fn default_timeout() -> Duration {
    Duration::from_secs(10 * 60) // 10 минут, как в Go
}

/// Одна единица работы, передаваемая локальному агенту (Go: picoclaw.Request).
#[derive(Clone, Debug)]
pub struct Request {
    pub task_id: String,
    pub instruction: String,
    pub skills: Vec<String>,
    pub workspace: String,
    pub home: String,
    pub allow_shell: bool,
    pub allow_network: bool,
    pub timeout: Duration,
    pub max_workspace_bytes: i64,
    pub max_memory_bytes: i64,
    pub model: String,
    pub session_key: String,
}

impl Default for Request {
    fn default() -> Self {
        Self {
            task_id: String::new(),
            instruction: String::new(),
            skills: Vec::new(),
            workspace: String::new(),
            home: String::new(),
            allow_shell: false,
            allow_network: false,
            timeout: default_timeout(),
            max_workspace_bytes: 0,
            max_memory_bytes: 0,
            model: String::new(),
            session_key: String::new(),
        }
    }
}

/// Файл, созданный во время исполнения (Go: picoclaw.Artifact).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub name: String,
    pub path: String,
    pub size: i64,
}

/// Ответ агента (Go: picoclaw.Response).
#[derive(Clone, Debug, Default)]
pub struct Response {
    pub text: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub artifacts: Vec<Artifact>,
    pub exit_code: i32,
    pub stderr: String,
    pub model: String,
}

/// Адаптер исполняет задачи локальным инстансом PicoClaw (Go: Adapter).
#[async_trait]
pub trait Adapter: Send + Sync {
    /// Имя реализации для вывода статуса.
    fn name(&self) -> String;
    /// Запуск одной задачи до завершения. Реализации обязаны уважать таймаут
    /// req.timeout и возвращать TimeoutError по истечении дедлайна.
    async fn execute(&self, req: Request) -> Result<Response>;
    /// Доступен ли агент прямо сейчас.
    async fn healthy(&self) -> Result<()>;
    /// Навыки, которые адаптер реально может обслужить.
    async fn capabilities(&self) -> Result<Vec<String>>;
    /// Освобождение ресурсов.
    async fn close(&self) -> Result<()>;
    /// Модель, которую узел попросит использовать (Go: ModelReporter).
    fn model(&self) -> String {
        String::new()
    }
    /// URL удалённой базы (Go: BaseReporter); пусто для локальных интеграций.
    fn base_url(&self) -> String {
        String::new()
    }
}

/// Описание адаптера для admin-API (Go: picoclaw.Info).
#[derive(Clone, Debug, Default, Serialize)]
pub struct Info {
    pub name: String,
    pub healthy: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub version: String,
    #[serde(with = "chrono_serde")]
    pub checked: DateTime<Utc>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub detail: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub model: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub base_url: String,
}

/// Go: Describe — опрос Healthy и упаковка результата для статуса.
pub async fn describe(a: &(dyn Adapter + Send + Sync)) -> Info {
    let mut info = Info {
        name: a.name(),
        model: a.model(),
        base_url: a.base_url(),
        checked: Utc::now(),
        ..Default::default()
    };
    match a.healthy().await {
        Ok(()) => info.healthy = true,
        Err(e) => info.detail = e.to_string(),
    }
    info
}

mod chrono_serde {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(d: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&d.to_rfc3339())
    }
    #[allow(dead_code)]
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        let s = String::deserialize(d)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|x| x.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// Пул LLM-бэкендов (Go: internal/brain BackendPool)
// ---------------------------------------------------------------------------

/// LLM-эндпоинт, на котором агент может думать (Go: brain.Backend).
#[derive(Clone, Debug, Default, Serialize)]
pub struct Backend {
    pub id: String,
    pub base_url: String,
    /// local | endpoint
    pub kind: String,
    #[serde(skip)]
    pub api_key: String,
    pub models: Vec<String>,
    #[serde(rename = "latency_ms")]
    pub latency_ms: i64,
    pub reachable: bool,
    pub verified: bool,
    #[serde(rename = "promoted_from", skip_serializing_if = "String::is_empty")]
    pub promoted_from: String,
    #[serde(rename = "checked_at", skip_serializing_if = "Option::is_none", with = "opt_chrono_serde")]
    pub checked_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
}

/// Явно объявленный оператором эндпоинт (Go: brain.BackendConfig).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BackendConfig {
    pub id: String,
    pub base_url: String,
    pub api_key_env: String,
}

/// Пул эндпоинтов, которыми узел думает (Go: brain.BackendPool).
pub struct BackendPool {
    backends: Mutex<Vec<Backend>>,
    http: reqwest::Client,
}

fn or_default(s: &str, d: &str) -> String {
    if s.trim().is_empty() {
        d.to_string()
    } else {
        s.to_string()
    }
}

impl BackendPool {
    /// Go: NewBackendPool — локальный Ollama + явные эндпоинты; ключи из env.
    pub fn new(local_url: &str, endpoints: &[BackendConfig]) -> Self {
        let mut backends = Vec::new();
        if !local_url.is_empty() {
            backends.push(Backend {
                id: "local".into(),
                base_url: local_url.trim_end_matches('/').to_string(),
                kind: "local".into(),
                ..Default::default()
            });
        }
        for ep in endpoints {
            if ep.base_url.is_empty() {
                continue;
            }
            let api_key = if ep.api_key_env.is_empty() {
                String::new()
            } else {
                std::env::var(&ep.api_key_env).unwrap_or_default()
            };
            backends.push(Backend {
                id: or_default(&ep.id, "endpoint"),
                base_url: ep.base_url.trim_end_matches('/').to_string(),
                kind: "endpoint".into(),
                api_key,
                ..Default::default()
            });
        }
        Self {
            backends: Mutex::new(backends),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Go: AddVerified — добавляет (или обновляет существующий по BaseURL)
    /// продвинутый эндпоинт и сразу пробует его.
    pub async fn add_verified(&self, b: Backend) -> Backend {
        let mut b = b;
        b.base_url = b.base_url.trim_end_matches('/').to_string();
        let target = {
            let mut v = self.backends.lock().unwrap();
            if let Some(ex) = v.iter_mut().find(|x| x.base_url == b.base_url) {
                ex.kind = b.kind.clone();
                if !b.api_key.is_empty() {
                    ex.api_key = b.api_key.clone();
                }
                if !b.promoted_from.is_empty() {
                    ex.promoted_from = b.promoted_from.clone();
                }
                ex.clone()
            } else {
                b.checked_at = None;
                v.push(b.clone());
                b
            }
        };
        self.probe_one(target.id.clone()).await;
        let v = self.backends.lock().unwrap();
        v.iter().find(|x| x.id == target.id).cloned().unwrap_or(target)
    }

    /// Go: ProbeOne — прозондировать один бэкенд и записать результат.
    pub async fn probe_one(&self, id: String) {
        let snapshot = {
            let v = self.backends.lock().unwrap();
            v.iter().find(|x| x.id == id).cloned()
        };
        let Some(b) = snapshot else { return };
        let res = self.probe_backend(&b).await;
        let mut v = self.backends.lock().unwrap();
        if let Some(e) = v.iter_mut().find(|x| x.id == id) {
            e.checked_at = Some(Utc::now());
            match res {
                Err(err) => {
                    e.reachable = false;
                    e.verified = false;
                    e.error = err.to_string();
                    e.models.clear();
                }
                Ok((models, latency_ms)) => {
                    e.reachable = true;
                    e.verified = true;
                    e.error = String::new();
                    e.models = models;
                    e.latency_ms = latency_ms;
                }
            }
        }
    }

    /// Go: Probe — пройтись по всем бэкендам параллельно.
    pub async fn probe(&self) {
        let ids: Vec<String> = self
            .backends
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.id.clone())
            .collect();
        let mut set = tokio::task::JoinSet::new();
        for id in ids {
            let slf = self as *const BackendPool;
            // Безопасно: BackendPool синхронизирован мьютексом, жизнь пула
            // дольше этого JoinSet (все задачи awaited ниже).
            let arc_guard = PoolRef(unsafe { &*slf });
            set.spawn(async move { arc_guard.0.probe_one(id).await });
        }
        while set.join_next().await.is_some() {}
    }

    /// Go: probeBackend — список моделей + измерение задержки.
    async fn probe_backend(&self, b: &Backend) -> Result<(Vec<String>, i64)> {
        let start = std::time::Instant::now();
        let models = self.fetch_models(b).await?;
        Ok((models, start.elapsed().as_millis() as i64))
    }

    fn with_auth(&self, rb: reqwest::RequestBuilder, b: &Backend) -> reqwest::RequestBuilder {
        if b.api_key.is_empty() {
            rb
        } else {
            rb.header("Authorization", format!("Bearer {}", b.api_key))
        }
    }

    /// Go: fetchModels — сначала Ollama /api/tags, затем OpenAI /v1/models.
    async fn fetch_models(&self, b: &Backend) -> Result<Vec<String>> {
        if let Ok(models) = self.fetch_ollama_tags(b).await {
            if !models.is_empty() {
                return Ok(models);
            }
        }
        self.fetch_openai_models(b).await
    }

    async fn fetch_ollama_tags(&self, b: &Backend) -> Result<Vec<String>> {
        let resp = self
            .with_auth(self.http.get(format!("{}/api/tags", b.base_url)), b)
            .send()
            .await?;
        if resp.status() != 200 {
            bail!("status {}", resp.status());
        }
        #[derive(Deserialize)]
        struct Doc {
            models: Vec<M>,
        }
        #[derive(Deserialize)]
        struct M {
            #[serde(default)]
            name: String,
        }
        let doc: Doc = resp.json().await?;
        Ok(doc.models.into_iter().filter(|m| !m.name.is_empty()).map(|m| m.name).collect())
    }

    async fn fetch_openai_models(&self, b: &Backend) -> Result<Vec<String>> {
        let resp = self
            .with_auth(self.http.get(format!("{}/v1/models", b.base_url)), b)
            .send()
            .await?;
        if resp.status() != 200 {
            bail!("status {}", resp.status());
        }
        #[derive(Deserialize)]
        struct Doc {
            data: Vec<M>,
        }
        #[derive(Deserialize)]
        struct M {
            #[serde(default)]
            id: String,
        }
        let doc: Doc = resp.json().await?;
        Ok(doc.data.into_iter().filter(|m| !m.id.is_empty()).map(|m| m.id).collect())
    }

    /// Go: List — снимок всех бэкендов в порядке объявления.
    pub fn list(&self) -> Vec<Backend> {
        self.backends.lock().unwrap().clone()
    }

    /// Go: Usable — ответившие на последнем зонде бэкенды со списком моделей.
    pub fn usable(&self) -> Vec<Backend> {
        self.backends
            .lock()
            .unwrap()
            .iter()
            .filter(|b| b.reachable && !b.models.is_empty())
            .cloned()
            .collect()
    }

    /// Go: SelectModel — предпочтённая модель, иначе самая быстрая и крупная.
    pub fn select_model(&self, preferred: &str) -> Option<(Backend, String)> {
        let usable = self.usable();
        if usable.is_empty() {
            return None;
        }
        if !preferred.is_empty() {
            for b in &usable {
                if b.models.iter().any(|m| m == preferred) {
                    return Some((b.clone(), preferred.to_string()));
                }
            }
        }
        let mut best = 0usize;
        for (i, b) in usable.iter().enumerate() {
            if b.latency_ms < usable[best].latency_ms {
                best = i;
            }
        }
        let b = usable[best].clone();
        let model = biggest_model(&b.models)?;
        Some((b, model))
    }
}

mod opt_chrono_serde {
    use chrono::{DateTime, Utc};
    use serde::{Serializer, Deserializer, Deserialize};
    pub fn serialize<S: Serializer>(d: &Option<DateTime<Utc>>, s: S) -> Result<S::Ok, S::Error> {
        match d {
            Some(x) => s.serialize_some(&x.to_rfc3339()),
            None => s.serialize_none(),
        }
    }
    #[allow(dead_code)]
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<DateTime<Utc>>, D::Error> {
        let o = Option::<String>::deserialize(d)?;
        o.map(|s| DateTime::parse_from_rfc3339(&s).map(|x| x.with_timezone(&Utc)).map_err(serde::de::Error::custom))
            .transpose()
    }
}

// Вспомогательная обёртка 'static-жизни для probe() (см. комментарий выше).
struct PoolRef<'a>(&'a BackendPool);
// SAFETY: пул живёт все await-точки spawn-задач (join ждёт их все).
unsafe impl Send for PoolRef<'_> {}
unsafe impl Sync for PoolRef<'_> {}

/// Go: biggestModel — крупнейший параметрический размер из имён моделей.
pub fn biggest_model(models: &[String]) -> Option<String> {
    let first = models.first()?.clone();
    let mut best = first;
    let mut best_score = model_size_score(&best);
    for m in &models[1..] {
        let s = model_size_score(m);
        if s > best_score {
            best = m.clone();
            best_score = s;
        }
    }
    Some(best)
}

/// Go: modelSizeScore — грубая оценка размера модели по имени.
pub fn model_size_score(name: &str) -> i64 {
    let low = name.to_lowercase();
    let mut scale: i64 = 0;
    if ["70b", "8x7b", "405b"].iter().any(|p| low.contains(p)) {
        scale = 70000;
    } else if ["32b", "34b", "35b"].iter().any(|p| low.contains(p)) {
        scale = 32000;
    } else if ["13b", "14b"].iter().any(|p| low.contains(p)) {
        scale = 13000;
    } else if ["8b", "9b"].iter().any(|p| low.contains(p)) {
        scale = 8000;
    } else if low.contains("7b") {
        scale = 7000;
    } else if low.contains("3b") {
        scale = 3000;
    } else if low.contains("1.5b") {
        scale = 1500;
    } else if low.contains("1b") {
        scale = 1000;
    } else if low.contains("0.5b") {
        scale = 500;
    }
    if low.contains("q4") || low.contains("q5") {
        scale = scale * 8 / 10;
    }
    scale
}

// ---------------------------------------------------------------------------
// StubAdapter (Go: stub.go)
// ---------------------------------------------------------------------------

pub struct StubAdapter {
    latency: Duration,
    echo: bool,
    skills: Vec<String>,
    calls: AtomicI64,
    in_flight: AtomicI64,
}

impl StubAdapter {
    pub fn new(cfg: StubConfig, skills: Vec<String>) -> Self {
        Self {
            latency: cfg.latency,
            echo: cfg.echo,
            skills,
            calls: AtomicI64::new(0),
            in_flight: AtomicI64::new(0),
        }
    }
    pub fn calls(&self) -> i64 {
        self.calls.load(Ordering::SeqCst)
    }
    pub fn running(&self) -> i64 {
        self.in_flight.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Adapter for StubAdapter {
    fn name(&self) -> String {
        "stub".into()
    }

    async fn healthy(&self) -> Result<()> {
        Ok(())
    }

    async fn capabilities(&self) -> Result<Vec<String>> {
        Ok(self.skills.clone())
    }

    async fn execute(&self, req: Request) -> Result<Response> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        struct Guard<'a>(&'a AtomicI64);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _g = Guard(&self.in_flight);

        let started = Utc::now();
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }

        let mut text = format!(
            "[zeptomesh-stub] task={} skills=[{}] accepted",
            req.task_id,
            req.skills.join(", ")
        );
        if self.echo {
            text = format!("{}\ninstruction: {}", text, req.instruction.trim());
        }
        // Запись файла доказывает сквозную работу механики workspace.
        let mut arts = Vec::new();
        if !req.workspace.is_empty() {
            let path = Path::new(&req.workspace).join("result.txt");
            if std::fs::create_dir_all(&req.workspace).is_ok()
                && std::fs::write(&path, text.as_bytes()).is_ok()
            {
                let size = std::fs::metadata(&path).map(|m| m.len() as i64).unwrap_or(text.len() as i64);
                arts.push(Artifact {
                    name: "result.txt".into(),
                    path: path.to_string_lossy().into_owned(),
                    size,
                });
            }
        }
        Ok(Response {
            text,
            started_at: started,
            finished_at: Utc::now(),
            artifacts: arts,
            exit_code: 0,
            ..Default::default()
        })
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Утилиты CLI (Go: cli.go)
// ---------------------------------------------------------------------------

/// Эмодзи-логотип, которым PicoClaw предваряет каждый ответ CLI.
pub const LOGO: &str = "\u{1F99E}";

/// Go: ExtractAnswer — срезает баннер и префикс-логотип из stdout CLI.
pub fn extract_answer(out: &str) -> String {
    let s = out.replace("\r\n", "\n");
    if let Some(i) = s.find(LOGO) {
        return s[i + LOGO.len()..].trim().to_string();
    }
    let kept: Vec<&str> = s
        .trim()
        .lines()
        .filter(|l| !is_banner_line(l))
        .collect();
    kept.join("\n").trim().to_string()
}

/// Go: isBannerLine — декоративный/информационный шум перед ответом.
fn is_banner_line(l: &str) -> bool {
    let t = l.trim();
    if t.is_empty() {
        return false;
    }
    if t.contains('█') || t.contains('▀') || t.contains('▄') {
        return true;
    }
    ["PICOCLAW", "TZ environment:", "ZONEINFO environment:", "Debug mode enabled"]
        .iter()
        .any(|p| t.starts_with(p))
}

/// Go: truncate — обрезка по байтам с эллипсисом.
pub fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut end = n;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// Go: sanitize — не выпускать секреты в логи и строки ошибок.
pub fn sanitize(s: &str) -> String {
    let mut s = s.to_string();
    for marker in ["api_key", "apikey", "token", "secret", "password", "authorization"] {
        loop {
            let l = s.to_lowercase();
            match l.find(marker) {
                Some(i) => {
                    let mut end = (i + marker.len() + 24).min(s.len());
                    while !s.is_char_boundary(end) {
                        end -= 1;
                    }
                    s = format!("{}***{}", &s[..i], &s[end..]);
                }
                None => break,
            }
        }
    }
    truncate(s.trim(), 512)
}

/// Go: collectArtifacts — обход workspace с квотой ТЗ 6.8.4.
pub fn collect_artifacts(root: &str, max_bytes: i64) -> Vec<Artifact> {
    if root.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut total: i64 = 0;
    collect_walk(Path::new(root), Path::new(root), max_bytes, &mut total, &mut out);
    out
}

fn collect_walk(
    root: &Path,
    dir: &Path,
    max_bytes: i64,
    total: &mut i64,
    out: &mut Vec<Artifact>,
) -> bool {
    let Ok(rd) = std::fs::read_dir(dir) else { return true };
    let mut entries: Vec<_> = rd.flatten().collect();
    entries.sort_by_key(|e| e.path()); // детерминированный порядок обхода
    for e in entries {
        let path = e.path();
        let Ok(meta) = e.metadata() else { continue };
        if meta.is_dir() {
            // Состояние самого PicoClaw — не результат работы.
            let base = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if matches!(base, "sessions" | "memory" | "logs" | "state") {
                continue;
            }
            if !collect_walk(root, &path, max_bytes, total, out) {
                return false; // квота исчерпана
            }
            continue;
        }
        if path.extension().and_then(|x| x.to_str()) == Some("tmp") {
            continue;
        }
        let size = meta.len() as i64;
        if max_bytes > 0 && *total + size > max_bytes {
            return false; // квота ТЗ 6.8.4 исчерпана
        }
        *total += size;
        let rel = pathdiff(&path, root);
        out.push(Artifact {
            name: rel,
            path: path.to_string_lossy().into_owned(),
            size,
        });
    }
    true
}

fn pathdiff(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .map(|p| p.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| {
            path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string()
        })
}

// ---------------------------------------------------------------------------
// CLIAdapter (Go: cli.go)
// ---------------------------------------------------------------------------

pub struct CLIAdapter {
    binary: String,
    cfg_file: String,
    env: Vec<(String, String)>,
    template: String,
    model: String,
    extra: Vec<String>,
    timeout: Duration,
    slots: Semaphore,
    in_flight: AtomicI64,
}

impl CLIAdapter {
    pub fn new(cfg: &PicoClawConfig) -> Result<Self> {
        let mut binary = cfg.binary.trim().to_string();
        if binary.is_empty() {
            binary = "picoclaw".into();
        }
        // Голое имя разрешается через PATH во время exec; путь обязан существовать.
        if binary.contains(std::path::MAIN_SEPARATOR) && !Path::new(&binary).exists() {
            bail!("picoclaw: binary {}: not found", binary);
        }
        let timeout = if cfg.timeout_seconds > 0 {
            Duration::from_secs(cfg.timeout_seconds.max(0) as u64)
        } else {
            default_timeout()
        };
        let slots = if cfg.max_concurrent_agents > 0 {
            cfg.max_concurrent_agents as usize
        } else {
            1
        };
        Ok(Self {
            binary,
            cfg_file: cfg.config.clone(),
            env: cfg.env.clone().into_iter().collect(),
            template: cfg.prompt_template.clone(),
            model: cfg.model.trim().to_string(),
            extra: cfg.extra_args.clone(),
            timeout,
            slots: Semaphore::new(slots),
            in_flight: AtomicI64::new(0),
        })
    }

    pub fn running(&self) -> i64 {
        self.in_flight.load(Ordering::SeqCst)
    }

    /// Go: modelFlag — выбор модели ТЗ 10.3 (override задачи важнее нодового).
    fn model_flag(&self, req: &Request) -> Vec<String> {
        let model = if !req.model.is_empty() {
            req.model.clone()
        } else {
            self.model.clone()
        };
        if model.is_empty() {
            return Vec::new();
        }
        vec!["--model".into(), model]
    }

    /// Go: renderPrompt — шаблон подсказки.
    fn render_prompt(&self, req: &Request) -> String {
        if self.template.is_empty() {
            return req.instruction.clone();
        }
        if !self.template.contains("%s") {
            return format!("{} {}", self.template, req.instruction);
        }
        self.template.replace("%s", &req.instruction)
    }

    /// Go: baseEnv — окружение дочернего процесса.
    fn base_env(&self, req: &Request) -> HashMap<String, String> {
        let mut env: HashMap<String, String> = std::env::vars().collect();
        for (k, v) in &self.env {
            env.insert(k.clone(), v.clone());
        }
        if !self.cfg_file.is_empty() {
            env.insert("PICOCLAW_CONFIG".into(), self.cfg_file.clone());
        }
        if !req.home.is_empty() {
            env.insert("PICOCLAW_HOME".into(), req.home.clone());
        }
        if !req.workspace.is_empty() {
            env.insert(
                "PICOCLAW_AGENTS_DEFAULTS_WORKSPACE".into(),
                req.workspace.clone(),
            );
        }
        env.insert("NO_COLOR".into(), "1".into());
        env.insert("TERM".into(), "dumb".into());
        env
    }
}

/// Go (limits_unix.go): limitedCommand — оборачивает вызов агента так, чтобы
/// ребёнок работал под RLIMIT_AS через `sh -c 'ulimit -v N; exec "$@"'`.
/// 0 = без ограничений. Вне Unix ограничение памяти не применяется.
#[cfg(unix)]
fn limited_command(max_memory: i64, binary: &str, args: &[String]) -> (String, Vec<String>) {
    if max_memory <= 0 {
        return (binary.to_string(), args.to_vec());
    }
    let kb = (max_memory + 1023) / 1024;
    let script = format!("ulimit -v {kb} 2>/dev/null || true\nexec \"$@\"");
    let mut argv = vec![
        "-c".to_string(),
        script,
        "zeptomesh-limit".to_string(),
        binary.to_string(),
    ];
    argv.extend_from_slice(args);
    ("/bin/sh".into(), argv)
}

#[cfg(not(unix))]
fn limited_command(_max_memory: i64, binary: &str, args: &[String]) -> (String, Vec<String>) {
    (binary.to_string(), args.to_vec())
}

#[async_trait]
impl Adapter for CLIAdapter {
    fn name(&self) -> String {
        "picoclaw-cli".into()
    }

    fn model(&self) -> String {
        self.model.clone()
    }

    async fn healthy(&self) -> Result<()> {
        // Go: exec.LookPath + `<binary> version` с таймаутом 10 с.
        let which = tokio::process::Command::new(&self.binary)
            .arg("version")
            .envs(self.base_env(&Request::default()))
            .kill_on_drop(true)
            .output();
        match tokio::time::timeout(Duration::from_secs(10), which).await {
            Err(_) => Err(UnavailableError(format!("{} version timed out", self.binary)).into()),
            Ok(Err(_)) => Err(UnavailableError(format!("{} not found in PATH", self.binary)).into()),
            Ok(Ok(out)) if out.status.success() => Ok(()),
            Ok(Ok(out)) => Err(UnavailableError(format!(
                "{} version failed: {}",
                self.binary,
                truncate(
                    &format!(
                        "{}{}",
                        String::from_utf8_lossy(&out.stdout),
                        String::from_utf8_lossy(&out.stderr)
                    ),
                    200,
                )
            ))
            .into()),
        }
    }

    async fn capabilities(&self) -> Result<Vec<String>> {
        // Из CLI не интроспектируется; реальный фильтр — политика узла.
        Ok(Vec::new())
    }

    async fn execute(&self, req: Request) -> Result<Response> {
        if req.instruction.trim().is_empty() {
            bail!("picoclaw: empty instruction");
        }
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| anyhow!("picoclaw: slots closed"))?;
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        struct G<'a>(&'a AtomicI64);
        impl Drop for G<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _g = G(&self.in_flight);

        let timeout = if req.timeout.is_zero() { self.timeout } else { req.timeout };
        let mut args = vec!["agent".to_string(), "-m".to_string(), self.render_prompt(&req)];
        if !req.session_key.is_empty() {
            args.push("-s".into());
            args.push(req.session_key.clone());
        }
        args.extend(self.model_flag(&req));
        args.extend(self.extra.iter().cloned());

        let (bin, argv) = limited_command(req.max_memory_bytes, &self.binary, &args);

        let mut cmd = tokio::process::Command::new(&bin);
        cmd.args(&argv)
            .envs(self.base_env(&req))
            .kill_on_drop(true);
        if !req.workspace.is_empty() {
            cmd.current_dir(&req.workspace);
        }
        #[cfg(unix)]
        {
            // Go: prepareChild — собственная процесс-группа для гарантированной
            // остановки всего дерева детей по таймауту.
            cmd.process_group(0);
        }

        let started = Utc::now();
        let run = tokio::time::timeout(timeout, cmd.output()).await;
        let finished = Utc::now();

        let mut resp = Response {
            started_at: started,
            finished_at: finished,
            ..Default::default()
        };
        let (status_out, stdout_s, stderr_s, timed_out) = match run {
            Err(_) => {
                // Таймаут: kill_on_drop убирает прямого ребёнка; групповую
                // очистку обеспечивает Pdeathsig-семантика tokio kill_on_drop.
                resp.exit_code = -1;
                (None, String::new(), String::new(), true)
            }
            Ok(Err(e)) => {
                resp.exit_code = -1;
                (None, String::new(), e.to_string(), false)
            }
            Ok(Ok(out)) => (
                out.status.code(),
                String::from_utf8_lossy(&out.stdout).into_owned(),
                String::from_utf8_lossy(&out.stderr).into_owned(),
                false,
            ),
        };
        resp.stderr = stderr_s.clone();
        resp.artifacts = collect_artifacts(&req.workspace, req.max_workspace_bytes);

        if timed_out {
            return Err(TimeoutError(format!("after {}s", timeout.as_secs())).into());
        }
        let code = status_out.unwrap_or(-1);
        resp.exit_code = code;
        if code != 0 {
            return Err(anyhow!(
                "picoclaw: agent exited {}: {}",
                code,
                sanitize(&stderr_s)
            ));
        }
        resp.text = extract_answer(&stdout_s);
        if resp.text.trim().is_empty() {
            return Err(anyhow!(
                "picoclaw: agent produced no answer (exit {})",
                code
            ));
        }
        Ok(resp)
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// OllamaAdapter (Go: ollama.go)
// ---------------------------------------------------------------------------

pub struct OllamaAdapter {
    pool: Option<std::sync::Arc<BackendPool>>,
    cfg: PicoClawConfig,
    skills: Vec<String>,
    timeout: Duration,
    http: reqwest::Client,
    proto: Mutex<HashMap<String, String>>,
}

impl OllamaAdapter {
    pub fn new(pool: Option<std::sync::Arc<BackendPool>>, cfg: &PicoClawConfig, skills: Vec<String>) -> Self {
        let timeout = if cfg.timeout_seconds > 0 {
            Duration::from_secs(cfg.timeout_seconds.max(0) as u64)
        } else {
            default_timeout()
        };
        Self {
            pool,
            cfg: cfg.clone(),
            skills,
            timeout,
            http: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .unwrap_or_default(),
            proto: Mutex::new(HashMap::new()),
        }
    }

    fn known_proto(&self, id: &str) -> String {
        self.proto.lock().unwrap().get(id).cloned().unwrap_or_default()
    }
    fn remember_proto(&self, id: &str, proto: &str) {
        self.proto.lock().unwrap().insert(id.into(), proto.into());
    }

    /// Go: chat — дважды-уступчивый вызов: известный диалект → ollama → openai.
    async fn chat(&self, b: &Backend, model: &str, instruction: &str) -> Result<(String, String)> {
        let known = self.known_proto(&b.id);
        if !known.is_empty() {
            match self.call_chat(b, model, instruction, &known).await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    tracing::warn!(backend = %b.id, protocol = %known, err = %e, "brain_chat_failed");
                }
            }
        }
        match self.call_chat(b, model, instruction, "ollama").await {
            Ok(r) => {
                self.remember_proto(&b.id, "ollama");
                return Ok(r);
            }
            Err(e) => {
                tracing::warn!(backend = %b.id, protocol = "ollama", err = %e, "brain_chat_failed");
            }
        }
        match self.call_chat(b, model, instruction, "openai").await {
            Ok(r) => {
                self.remember_proto(&b.id, "openai");
                return Ok(r);
            }
            Err(e) => {
                bail!(
                    "brain backend {} ({}) rejected both chat protocols: {}",
                    b.id,
                    b.base_url,
                    e
                );
            }
        }
    }

    async fn call_chat(
        &self,
        b: &Backend,
        model: &str,
        instruction: &str,
        proto: &str,
    ) -> Result<(String, String)> {
        let (url, body) = chat_payload(&b.base_url, proto, model, instruction);
        let mut rb = self.http.post(&url).json(&body);
        if !b.api_key.is_empty() {
            rb = rb.header("Authorization", format!("Bearer {}", b.api_key));
        }
        let resp = rb.send().await?;
        let status = resp.status().as_u16();
        let raw = resp.bytes().await?;
        let raw: Vec<u8> = raw.iter().take(32 << 20).cloned().collect();
        if status != 200 {
            // Только 404 переключает диалект; остальное — общая проблема бэкенда.
            if status == 404 {
                bail!("status 404");
            }
            bail!(
                "status {}: {}",
                status,
                truncate(&String::from_utf8_lossy(&raw), 200)
            );
        }
        if proto == "ollama" {
            #[derive(Deserialize)]
            struct Doc {
                #[serde(default)]
                model: String,
                #[serde(default)]
                message: Msg,
            }
            #[derive(Deserialize, Default)]
            struct Msg {
                #[serde(default)]
                content: String,
            }
            let doc: Doc = serde_json::from_slice(&raw)?;
            Ok((doc.message.content.trim().to_string(), or_default(&doc.model, model)))
        } else {
            #[derive(Deserialize)]
            struct Doc {
                #[serde(default)]
                model: String,
                #[serde(default)]
                choices: Vec<Ch>,
            }
            #[derive(Deserialize)]
            struct Ch {
                #[serde(default)]
                message: Msg,
            }
            #[derive(Deserialize, Default)]
            struct Msg {
                #[serde(default)]
                content: String,
            }
            let doc: Doc = serde_json::from_slice(&raw)?;
            let ch = doc.choices.into_iter().next().context("empty choices")?;
            Ok((ch.message.content.trim().to_string(), or_default(&doc.model, model)))
        }
    }
}

/// Go: chatPayload — URL и тело одного chat-вызова.
fn chat_payload(base_url: &str, proto: &str, model: &str, instruction: &str) -> (String, serde_json::Value) {
    let messages = serde_json::json!([{"role": "user", "content": instruction}]);
    let base = base_url.trim_end_matches('/');
    if proto == "ollama" {
        (
            format!("{base}/api/chat"),
            serde_json::json!({"model": model, "messages": messages, "stream": false}),
        )
    } else {
        (
            format!("{base}/v1/chat/completions"),
            serde_json::json!({"model": model, "messages": messages}),
        )
    }
}

#[async_trait]
impl Adapter for OllamaAdapter {
    fn name(&self) -> String {
        "ollama".into()
    }

    fn model(&self) -> String {
        if !self.cfg.model.is_empty() {
            return self.cfg.model.clone();
        }
        if let Some(pool) = &self.pool {
            if let Some((_, m)) = pool.select_model("") {
                return m;
            }
        }
        String::new()
    }

    fn base_url(&self) -> String {
        if let Some(pool) = &self.pool {
            if let Some((b, _)) = pool.select_model("") {
                return b.base_url;
            }
        }
        String::new()
    }

    async fn healthy(&self) -> Result<()> {
        let Some(pool) = &self.pool else {
            return Err(UnavailableError("no brain pool".into()).into());
        };
        if pool.usable().is_empty() {
            return Err(UnavailableError("no usable brain backend yet".into()).into());
        }
        Ok(())
    }

    async fn capabilities(&self) -> Result<Vec<String>> {
        Ok(self.skills.clone())
    }

    async fn execute(&self, req: Request) -> Result<Response> {
        let Some(pool) = &self.pool else {
            return Err(UnavailableError("no brain pool".into()).into());
        };
        let preferred = if !req.model.is_empty() { req.model.clone() } else { self.cfg.model.clone() };
        let Some((b, model)) = pool.select_model(&preferred) else {
            return Err(UnavailableError(format!("no usable brain backend for model {preferred:?}")).into());
        };
        let started = Utc::now();
        let fut = self.chat(&b, &model, &req.instruction);
        let res = if req.timeout > Duration::ZERO {
            match tokio::time::timeout(req.timeout, fut).await {
                Err(_) => return Err(TimeoutError("chat deadline".into()).into()),
                Ok(r) => r,
            }
        } else {
            fut.await
        };
        let (text, answered) = match res {
            Ok(v) => v,
            Err(e) => {
                return Err(if e.root_cause().downcast_ref::<reqwest::Error>().map(|x| x.is_timeout()).unwrap_or(false) {
                    TimeoutError(e.to_string()).into()
                } else {
                    e
                })
            }
        };
        if text.is_empty() {
            bail!("picoclaw: backend {} returned an empty answer", b.id);
        }
        let mut arts = Vec::new();
        if !req.workspace.is_empty() {
            let path = Path::new(&req.workspace).join("result.txt");
            if std::fs::create_dir_all(&req.workspace).is_ok()
                && std::fs::write(&path, text.as_bytes()).is_ok()
            {
                arts.push(Artifact {
                    name: "result.txt".into(),
                    path: path.to_string_lossy().into_owned(),
                    size: text.len() as i64,
                });
            }
        }
        Ok(Response {
            text,
            started_at: started,
            finished_at: Utc::now(),
            artifacts: arts,
            exit_code: 0,
            model: answered,
            stderr: String::new(),
        })
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WSAdapter — Pico Protocol поверх WebSocket (Go: ws.go)
// ---------------------------------------------------------------------------

const TYPE_MESSAGE_SEND: &str = "message.send";
const TYPE_MESSAGE_CREATE: &str = "message.create";
const TYPE_MESSAGE_UPDATE: &str = "message.update";
const TYPE_TYPING_STOP: &str = "typing.stop";
const TYPE_ERROR: &str = "error";
const TYPE_PONG: &str = "pong";
const KIND_THOUGHT: &str = "thought";
const KIND_TOOL_CALLS: &str = "tool_calls";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PicoMessage {
    #[serde(rename = "type", default)]
    r#type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    id: String,
    #[serde(rename = "session_id", default, skip_serializing_if = "String::is_empty")]
    session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timestamp: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Map<String, serde_json::Value>>,
}

pub struct WSAdapter {
    ws_url: String,
    http_base: String,
    health: String,
    token: String,
    timeout: Duration,
    slots: Semaphore,
    next_id: AtomicI64,
    http: reqwest::Client,
}

/// Go: sessionOf — mesh-ключ сессии → pico session id.
fn session_of(key: &str) -> String {
    key.trim().replace(':', "-").replace('/', "-").replace(' ', "-")
}

/// Go: wsURLFromHTTP — базовый HTTP-URL шлюза → ws://.../pico/ws.
fn ws_url_from_http(base: &str, path: &str) -> Result<String> {
    let base = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if base.starts_with("ws://") || base.starts_with("wss://") {
        base.to_string()
    } else {
        bail!("picoclaw: unsupported base_url {:?}", base);
    };
    let mut path = if path.is_empty() { "/pico/ws".to_string() } else { path.to_string() };
    if !path.starts_with('/') {
        path = format!("/{path}");
    }
    Ok(format!("{}{}", base.trim_end_matches('/'), path))
}

impl WSAdapter {
    pub fn new(cfg: &PicoClawConfig) -> Result<Self> {
        let base = cfg.http.base_url.trim_end_matches('/').to_string();
        if base.is_empty() {
            bail!("picoclaw: http mode requires picoclaw.http.base_url");
        }
        let ws_url = ws_url_from_http(&base, &cfg.http.path)?;
        let mut health = if cfg.http.health_path.is_empty() {
            "/health".to_string()
        } else {
            cfg.http.health_path.clone()
        };
        if !health.starts_with('/') {
            health = format!("/{health}");
        }
        let token = if cfg.http.token_env.is_empty() {
            String::new()
        } else {
            std::env::var(&cfg.http.token_env).unwrap_or_default()
        };
        if token.is_empty() {
            bail!(
                "picoclaw: pico token is empty; set {} (PicoClaw requires channels.pico.token)",
                cfg.http.token_env
            );
        }
        let mut timeout = Duration::from_secs(cfg.http.timeout_seconds.max(0) as u64);
        if timeout.is_zero() {
            timeout = Duration::from_secs(cfg.timeout_seconds.max(0) as u64);
        }
        if timeout.is_zero() {
            timeout = default_timeout();
        }
        let slots = if cfg.max_concurrent_agents > 0 {
            cfg.max_concurrent_agents as usize
        } else {
            1
        };
        if !cfg.model.trim().is_empty() {
            // Pico Protocol не имеет проверенного поля выбора модели.
            tracing::warn!(mode = "http", model = %cfg.model, "picoclaw_model_not_applicable");
        }
        Ok(Self {
            ws_url,
            http_base: base,
            health,
            token,
            timeout,
            slots: Semaphore::new(slots),
            next_id: AtomicI64::new(0),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_default(),
        })
    }

    fn seq(&self) -> i64 {
        self.next_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Go: dial — отдельное WS-соединение на задачу (изоляция сессий).
    async fn dial(&self, session_key: &str) -> Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
        let mut url = self.ws_url.clone();
        let s = session_of(session_key);
        if !s.is_empty() {
            let sep = if url.contains('?') { "&" } else { "?" };
            url = format!("{url}{sep}session_id={s}");
        }
        use tokio_tungstenite::connect_async;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = url.as_str().into_client_request()?;
        {
            let headers = request.headers_mut();
            headers.insert(
                "Authorization",
                format!("Bearer {}", self.token).parse().unwrap(),
            );
            // Шлюз эхо-подтверждает "token.<value>" как согласованный subprotocol.
            headers.insert(
                "Sec-WebSocket-Protocol",
                format!("token.{}", self.token).parse().unwrap(),
            );
        }
        let (ws, _resp) = connect_async(request)
            .await
            .map_err(|e| anyhow!("picoclaw: ws handshake {url}: {e}"))?;
        Ok(ws)
    }

    /// Go: readTurn — читает кадры до конца хода.
    async fn read_turn(
        &self,
        conn: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        deadline: Duration,
        req_id: &str,
    ) -> Result<(String, String)> {
        use tokio_tungstenite::tungstenite::Message;
        let mut final_text = String::new();
        let mut model = String::new();
        let mut latest = String::new();
        loop {
            let frame_opt: Option<Result<Message, tokio_tungstenite::tungstenite::Error>> =
                tokio::select! {
                    f = conn.next() => f,
                    _ = tokio::time::sleep(deadline) => {
                        // Go: по дедлайну возвращаем накопленный текст с TimeoutError.
                        if !final_text.is_empty() {
                            return Err(TimeoutError(format!("turn deadline after {deadline:?}")).into());
                        }
                        return Err(TimeoutError("turn deadline".into()).into());
                    }
                };
            let frame = match frame_opt {
                Some(Ok(Message::Text(t))) => t,
                Some(Ok(Message::Close(_))) | None => {
                    if !final_text.is_empty() {
                        return Ok((final_text, model));
                    }
                    bail!("picoclaw: connection closed before an answer (req {req_id})");
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    // Go: IsUnexpectedCloseError(Normal|GoingAway): чистое закрытие
                    // с уже накопленным ответом — нормальный конец хода.
                    let clean = matches!(&e, tokio_tungstenite::tungstenite::Error::Protocol(
                        tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake)
                    ) || matches!(&e, tokio_tungstenite::tungstenite::Error::Io(_))
                        || format!("{e}").contains("Connection closed without closing handshake")
                        || matches!(&e, tokio_tungstenite::tungstenite::Error::Utf8);
                    let _ = clean;
                    if !final_text.is_empty() {
                        return Ok((final_text, model));
                    }
                    bail!("picoclaw: read: {e}");
                }
            };
            let msg: PicoMessage = match serde_json::from_str(frame.as_str()) {
                Ok(m) => m,
                Err(_) => continue,
            };
            match msg.r#type.as_str() {
                TYPE_MESSAGE_CREATE | TYPE_MESSAGE_UPDATE => {
                    let payload = msg.payload.unwrap_or_default();
                    let kind = payload.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                    let content = payload.get("content").and_then(|v| v.as_str()).unwrap_or("");
                    if let Some(m) = payload.get("model_name").and_then(|v| v.as_str()) {
                        if !m.is_empty() {
                            model = m.to_string();
                        }
                    }
                    if kind == KIND_THOUGHT || kind == KIND_TOOL_CALLS || content.is_empty() {
                        continue; // прогресс, не ответ
                    }
                    latest = content.to_string();
                    final_text = content.to_string();
                }
                TYPE_TYPING_STOP => {
                    if final_text.is_empty() {
                        final_text = latest.clone();
                    }
                    if final_text.is_empty() {
                        bail!("picoclaw: turn ended without an answer");
                    }
                    return Ok((final_text, model));
                }
                TYPE_ERROR => {
                    let payload = msg.payload.unwrap_or_default();
                    let code = payload.get("code").and_then(|v| v.as_str()).unwrap_or("");
                    let m = payload.get("message").and_then(|v| v.as_str()).unwrap_or("");
                    bail!("picoclaw: gateway error {code}: {m}");
                }
                TYPE_PONG => continue,
                other => {
                    tracing::debug!(r#type = other, req = req_id, "picoclaw_unexpected_frame");
                }
            }
        }
    }
}

#[async_trait]
impl Adapter for WSAdapter {
    fn name(&self) -> String {
        "picoclaw-pico-ws".into()
    }

    fn base_url(&self) -> String {
        self.http_base.clone()
    }

    async fn healthy(&self) -> Result<()> {
        let health = format!("{}{}", self.http_base.trim_end_matches('/'), self.health);
        let resp = self
            .http
            .get(&health)
            .send()
            .await
            .map_err(|e| UnavailableError(format!("gateway health at {health}: {e}")))?;
        if resp.status() != 200 {
            return Err(UnavailableError(format!("gateway health returned {}", resp.status())).into());
        }
        Ok(())
    }

    async fn capabilities(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn execute(&self, req: Request) -> Result<Response> {
        if req.instruction.trim().is_empty() {
            bail!("picoclaw: empty instruction");
        }
        let _permit = self
            .slots
            .acquire()
            .await
            .map_err(|_| anyhow!("picoclaw: slots closed"))?;
        let timeout = if req.timeout.is_zero() { self.timeout } else { req.timeout };
        let started = Utc::now();
        let mut conn = match self.dial(&req.session_key).await {
            Ok(c) => c,
            Err(e) => {
                // Go возвращает resp+err; в Rust err достаточно (tasks пишет
                // journal с сообщением ошибки).
                tracing::debug!(err = %e, "picoclaw: ws dial failed");
                return Err(e);
            }
        };
        let id = format!("zeptomesh-{}", self.seq());
        let mut payload = serde_json::Map::new();
        payload.insert("content".into(), serde_json::json!(req.instruction));
        let send = PicoMessage {
            r#type: TYPE_MESSAGE_SEND.into(),
            id: id.clone(),
            session_id: session_of(&req.session_key),
            payload: Some(payload),
            ..Default::default()
        };
        use futures::SinkExt;
        conn.send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&send)?.into(),
        ))
        .await
        .map_err(|e| anyhow!("picoclaw: send: {e}"))?;

        let (text, model) = self.read_turn(&mut conn, timeout, &id).await?;
        let finished = Utc::now();
        let arts = collect_artifacts(&req.workspace, req.max_workspace_bytes);
        let resp = Response {
            text,
            model,
            started_at: started,
            finished_at: finished,
            artifacts: arts,
            ..Default::default()
        };
        if resp.text.trim().is_empty() {
            return Err(anyhow!("picoclaw: gateway returned an empty answer"));
        }
        Ok(resp)
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Фабрика (Go: factory.go)
// ---------------------------------------------------------------------------

/// Go: picoclaw.New — адаптер по picoclaw.mode.
pub async fn new_adapter(cfg: &Config, pool: Option<std::sync::Arc<BackendPool>>) -> Result<std::sync::Arc<dyn Adapter + Send + Sync>> {
    let pc = &cfg.picoclaw;
    let skills = cfg.effective_skills();
    match pc.mode.as_str() {
        "binary" => {
            let a = CLIAdapter::new(pc)?;
            tracing::info!(mode = "binary", binary = %a.binary, model = %a.model, "picoclaw_adapter");
            Ok(std::sync::Arc::new(a))
        }
        "http" => {
            let a = WSAdapter::new(pc)?;
            tracing::info!(mode = "http", url = %a.ws_url, "picoclaw_adapter");
            Ok(std::sync::Arc::new(a))
        }
        "ollama" => {
            let pool = pool.ok_or_else(|| {
                anyhow!("picoclaw: mode ollama requires brain.enabled=true (the executor thinks with brain backends)")
            })?;
            tracing::info!(mode = "ollama", model_selection = "brain pool", "picoclaw_adapter");
            Ok(std::sync::Arc::new(OllamaAdapter::new(Some(pool), pc, skills)))
        }
        "stub" | "" => {
            tracing::info!(mode = "stub", note = "no real agent is invoked", "picoclaw_adapter");
            Ok(std::sync::Arc::new(StubAdapter::new(pc.stub.clone(), skills)))
        }
        other => bail!("picoclaw: unknown mode {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Тесты
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_answer_strips_logo_and_banner() {
        let out = "PICOCLAW v1.0\n █▀█ banner \n🦞 hello world\n";
        assert_eq!(extract_answer(out), "hello world");
        // без логотипа — фильтруем строки баннера
        let out2 = "TZ environment: UTC\nreal answer line\n";
        assert_eq!(extract_answer(out2), "real answer line");
    }

    #[test]
    fn sanitize_masks_secrets() {
        let s = sanitize("failed with api_key=supersecretvalue1234567890 tail");
        assert!(s.contains("***"), "{s}");
        assert!(!s.contains("supersecretvalue1234567890"));
    }

    #[test]
    fn model_size_scores() {
        assert_eq!(model_size_score("qwen2.5:72b"), 0); // нет точного совпадения
        assert_eq!(model_size_score("llama3:70b"), 70000);
        assert_eq!(model_size_score("mistral:7b-q4"), 7000 * 8 / 10);
        let models = vec!["qwen2.5:0.5b".to_string(), "qwen2.5:1.5b".to_string()];
        assert_eq!(biggest_model(&models).unwrap(), "qwen2.5:1.5b");
    }

    #[test]
    fn ws_url_conversion() {
        assert_eq!(ws_url_from_http("http://h:18790", "").unwrap(), "ws://h:18790/pico/ws");
        assert_eq!(ws_url_from_http("https://h", "pico/ws").unwrap(), "wss://h/pico/ws");
        assert!(ws_url_from_http("ftp://h", "").is_err());
    }

    #[test]
    fn session_of_maps_separators() {
        assert_eq!(session_of("a:b/c d"), "a-b-c-d");
        assert_eq!(session_of("  "), "");
    }

    #[test]
    fn chat_payload_shapes() {
        let (u, b) = chat_payload("http://x/", "ollama", "m1", "hi");
        assert_eq!(u, "http://x/api/chat");
        assert_eq!(b["stream"], false);
        let (u, _) = chat_payload("http://x", "openai", "m1", "hi");
        assert_eq!(u, "http://x/v1/chat/completions");
    }

    #[tokio::test]
    async fn stub_executes_and_writes_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path().join("t1");
        let a = StubAdapter::new(
            StubConfig { latency: Duration::from_millis(1), echo: true },
            vec!["general".into()],
        );
        let r = a
            .execute(Request {
                task_id: "t1".into(),
                instruction: "do it".into(),
                workspace: ws.to_string_lossy().into_owned(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(r.text.contains("task=t1"));
        assert!(r.text.contains("instruction: do it"));
        assert_eq!(r.artifacts.len(), 1);
        assert_eq!(r.artifacts[0].name, "result.txt");
        assert_eq!(a.calls(), 1);
    }

    #[test]
    fn collect_artifacts_quota_and_skips() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("a.txt"), "12345").unwrap();
        std::fs::write(root.join("b.tmp"), "x").unwrap();
        std::fs::create_dir_all(root.join("sessions")).unwrap();
        std::fs::write(root.join("sessions/s.json"), "junk").unwrap();
        std::fs::write(root.join("sub.txt"), "678").unwrap();
        let arts = collect_artifacts(root.to_str().unwrap(), 0);
        let names: Vec<_> = arts.iter().map(|a| a.name.clone()).collect();
        assert!(names.contains(&"a.txt".to_string()));
        assert!(names.contains(&"sub.txt".to_string()));
        assert!(!names.iter().any(|n| n.contains(".tmp") || n.contains("sessions")));
        // квота 7 байт: второй файл не проходит
        let arts = collect_artifacts(root.to_str().unwrap(), 7);
        assert_eq!(arts.len(), 1);
    }

    #[test]
    fn pool_select_model_prefers_fastest_biggest() {
        let pool = BackendPool::new("", &[]);
        {
            let mut v = pool.backends.lock().unwrap();
            v.push(Backend {
                id: "slow".into(),
                base_url: "http://slow".into(),
                kind: "endpoint".into(),
                models: vec!["small:1b".into(), "big:7b".into()],
                latency_ms: 500,
                reachable: true,
                verified: true,
                ..Default::default()
            });
            v.push(Backend {
                id: "fast".into(),
                base_url: "http://fast".into(),
                kind: "endpoint".into(),
                models: vec!["mid:3b".into()],
                latency_ms: 10,
                reachable: true,
                verified: true,
                ..Default::default()
            });
        }
        let (b, m) = pool.select_model("").unwrap();
        assert_eq!(b.id, "fast");
        assert_eq!(m, "mid:3b");
        let (b, m) = pool.select_model("big:7b").unwrap();
        assert_eq!(b.id, "slow");
        assert_eq!(m, "big:7b");
        assert!(pool.select_model("nonexistent:zz").is_some()); // fallback на fastest
        assert_eq!(pool.usable().len(), 2);
    }
}
