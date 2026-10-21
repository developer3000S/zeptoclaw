//! ZeptoClaw Mesh Dashboard — BFF (backend-for-frontend) на Rust.
//!
//! Дашборд обслуживает SPA, ведёт реестр управляемых нод на диске
//! (`nodes.json`), агрегирует состояние mesh-сети в единый граф и
//! проксирует операторские вызовы к admin-API узлов.
//!
//! Токены к узлам в браузер не попадают: фронтенд знает только идентификатор
//! узла, все обращения к admin-API идут через этот BFF. Это снимает проблему
//! CORS (admin-API узла его не выставляет) и не даёт токену утечь в клиент.
//!
//! ## Конфигурация (env)
//!
//! | Переменная                  | По умолчанию             | Назначение                        |
//! |-----------------------------|--------------------------|-----------------------------------|
//! | `ZEPTOMESH_UI_LISTEN`       | `127.0.0.1:28090`        | адрес прослушивания               |
//! | `ZEPTOMESH_UI_DATA`         | `./zeptomesh-ui-data`    | каталог данных                    |
//! | `ZEPTOMESH_UI_NODES_FILE`   | `<data>/nodes.json`      | реестр управляемых нод            |
//! | `ZEPTOMESH_UI_NODES`        | пусто                    | стартовый список нод (seed)       |
//! | `ZEPTOMESH_UI_TOKEN`        | пусто                    | токен доступа к самому дашборду   |
//! | `ZEPTOMESH_UI_API_TOKEN`    | пусто                    | bearer для нод без своего токена  |
//! | `ZEPTOMESH_UI_POLL_INTERVAL`| `5`                      | интервал опроса нод, сек          |
//! | `ZEPTOMESH_UI_DISCOVERY_PORTS` | `8081-8085,14001-14010` | порты admin-API для обхода сети   |

pub mod discovery;

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock, Semaphore};

use discovery::IpFilter;

/// Максимум тела запроса (8 MiB) — как у admin-API узла.
const MAX_BODY: usize = 8 << 20;

/// Тип ответа BFF: JSON либо статика.
type Resp = Response<Full<Bytes>>;

// ---------------------------------------------------------------------------
// Конфигурация
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct UiConfig {
    /// Адрес прослушивания.
    pub listen: String,
    /// Каталог данных дашборда.
    pub data_dir: PathBuf,
    /// Файл реестра управляемых нод.
    pub nodes_file: PathBuf,
    /// Токен доступа к самому дашборду (пусто — без авторизации).
    pub token: String,
    /// Bearer-токен для нод, у которых нет собственного.
    pub api_token: String,
    /// Интервал опроса нод, сек.
    pub poll_interval: u64,
    /// Порты admin-API, которые обходятся при обнаружении.
    pub discovery_ports: Vec<u16>,
}

fn env_or(key: &str, default: &str) -> String {
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => default.to_string(),
    }
}

impl UiConfig {
    /// Собрать конфигурацию из окружения.
    pub fn from_env() -> Result<Self> {
        let data_dir = PathBuf::from(env_or("ZEPTOMESH_UI_DATA", "./zeptomesh-ui-data"));
        let nodes_file = match std::env::var("ZEPTOMESH_UI_NODES_FILE") {
            Ok(v) if !v.trim().is_empty() => PathBuf::from(v.trim()),
            _ => data_dir.join("nodes.json"),
        };
        let poll_interval = env_or("ZEPTOMESH_UI_POLL_INTERVAL", "5")
            .parse::<u64>()
            .context("ZEPTOMESH_UI_POLL_INTERVAL: ожидалось целое число секунд")?;
        let discovery_ports = parse_ports(&env_or(
            "ZEPTOMESH_UI_DISCOVERY_PORTS",
            "8081,8082,8083,8084,8085,14001,14002,14003,14004,14005,14006,14007,14008,14009,14010",
        ));
        Ok(UiConfig {
            listen: env_or("ZEPTOMESH_UI_LISTEN", "127.0.0.1:28090"),
            data_dir,
            nodes_file,
            token: env_or("ZEPTOMESH_UI_TOKEN", ""),
            api_token: env_or("ZEPTOMESH_UI_API_TOKEN", ""),
            poll_interval,
            discovery_ports,
        })
    }
}

/// Разобрать список портов: `8081,8082` или `8081-8085`.
fn parse_ports(spec: &str) -> Vec<u16> {
    let mut out = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((a, b)) = part.split_once('-') {
            if let (Ok(a), Ok(b)) = (a.trim().parse::<u16>(), b.trim().parse::<u16>()) {
                for p in a..=b {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
            }
            continue;
        }
        if let Ok(p) = part.parse::<u16>() {
            if !out.contains(&p) {
                out.push(p);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Реестр управляемых нод
// ---------------------------------------------------------------------------

/// Запись реестра управляемых нод.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeEntry {
    /// Стабильный идентификатор записи (не путать с peer_id узла).
    pub id: String,
    /// Отображаемое имя.
    #[serde(default)]
    pub name: String,
    /// Базовый URL admin-API узла.
    pub url: String,
    /// Собственный токен узла (если отличается от общего).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Участвует ли нода в опросе и графе.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Произвольные метки для группировки и поиска.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Когда заведён в реестре.
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
}

fn default_true() -> bool {
    true
}

/// Реестр управляемых нод с сохранением на диск.
pub struct Registry {
    path: PathBuf,
    nodes: RwLock<Vec<NodeEntry>>,
}

impl Registry {
    /// Загрузить реестр из файла; отсутствующий файл — пустой реестр.
    pub fn load(path: PathBuf) -> Result<Self> {
        let nodes = match std::fs::read_to_string(&path) {
            Ok(s) if !s.trim().is_empty() => serde_json::from_str::<Vec<NodeEntry>>(&s)
                .with_context(|| format!("ui: разбор реестра нод {}", path.display()))?,
            _ => Vec::new(),
        };
        Ok(Registry {
            path,
            nodes: RwLock::new(nodes),
        })
    }

    /// Снимок реестра.
    pub async fn list(&self) -> Vec<NodeEntry> {
        self.nodes.read().await.clone()
    }

    /// Найти запись по идентификатору.
    pub async fn get(&self, id: &str) -> Option<NodeEntry> {
        self.nodes.read().await.iter().find(|n| n.id == id).cloned()
    }

    /// Найти запись по базовому URL.
    pub async fn find_by_url(&self, url: &str) -> Option<NodeEntry> {
        let url = url.trim_end_matches('/');
        self.nodes
            .read()
            .await
            .iter()
            .find(|n| n.url.trim_end_matches('/') == url)
            .cloned()
    }

    /// Добавить ноду. Дубликаты по URL игнорируются.
    pub async fn add(&self, mut entry: NodeEntry) -> Result<NodeEntry> {
        let mut guard = self.nodes.write().await;
        let url = entry.url.trim().trim_end_matches('/').to_string();
        if guard.iter().any(|n| n.url.trim_end_matches('/') == url) {
            return Err(anyhow!("ui: нода {} уже есть в реестре", url));
        }
        if entry.id.is_empty() {
            entry.id = uuid::Uuid::new_v4().to_string();
        }
        if entry.name.trim().is_empty() {
            entry.name = host_of(&entry.url).unwrap_or_else(|| entry.url.clone());
        }
        if entry.created_at.is_none() {
            entry.created_at = Some(Utc::now());
        }
        guard.push(entry.clone());
        let snapshot = guard.clone();
        drop(guard);
        self.save(&snapshot).await?;
        Ok(entry)
    }

    /// Обновить ноду по идентификатору.
    pub async fn update(&self, id: &str, patch: NodePatch) -> Result<NodeEntry> {
        let mut guard = self.nodes.write().await;
        let Some(node) = guard.iter_mut().find(|n| n.id == id) else {
            return Err(anyhow!("ui: нода {} не найдена", id));
        };
        if let Some(v) = patch.name {
            node.name = v;
        }
        if let Some(v) = patch.url {
            let v = v.trim().trim_end_matches('/').to_string();
            if v.is_empty() {
                return Err(anyhow!("ui: url ноды не может быть пустым"));
            }
            node.url = v;
        }
        if let Some(v) = patch.token {
            node.token = if v.trim().is_empty() { None } else { Some(v) };
        }
        if let Some(v) = patch.enabled {
            node.enabled = v;
        }
        if let Some(v) = patch.tags {
            node.tags = v;
        }
        let out = node.clone();
        let snapshot = guard.clone();
        drop(guard);
        self.save(&snapshot).await?;
        Ok(out)
    }

    /// Удалить ноду по идентификатору.
    pub async fn remove(&self, id: &str) -> Result<()> {
        let mut guard = self.nodes.write().await;
        let before = guard.len();
        guard.retain(|n| n.id != id);
        if guard.len() == before {
            return Err(anyhow!("ui: нода {} не найдена", id));
        }
        let snapshot = guard.clone();
        drop(guard);
        self.save(&snapshot).await?;
        Ok(())
    }

    /// Сохранить реестр на диск (атомарно).
    async fn save(&self, nodes: &[NodeEntry]) -> Result<()> {
        if let Some(dir) = self.path.parent() {
            tokio::fs::create_dir_all(dir).await.ok();
        }
        let body = serde_json::to_vec_pretty(nodes)?;
        let tmp = self.path.with_extension("json.tmp");
        tokio::fs::write(&tmp, &body).await.ok();
        tokio::fs::rename(&tmp, &self.path).await.ok();
        Ok(())
    }
}

/// Частичное обновление ноды (из JSON тела запроса).
#[derive(Clone, Debug, Default, Deserialize)]
pub struct NodePatch {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
}

/// Извлечь хост из базового URL (без схемы, порта и пути).
pub fn host_of(url: &str) -> Option<String> {
    let rest = url.split("://").nth(1).unwrap_or(url);
    let rest = rest.split('/').next().unwrap_or(rest);
    if rest.starts_with('[') {
        return rest.find(']').map(|i| rest[1..i].to_string());
    }
    Some(rest.split(':').next().unwrap_or(rest).to_string())
}

// ---------------------------------------------------------------------------
// Кэш снимка сети
// ---------------------------------------------------------------------------

/// Кэш агрегированного снимка сети: пересобирается не чаще `poll_interval`.
struct MeshCache {
    ttl: Duration,
    last: Mutex<Option<Instant>>,
    data: RwLock<Option<Value>>,
}

impl MeshCache {
    fn new(ttl: Duration) -> Self {
        MeshCache {
            ttl,
            last: Mutex::new(None),
            data: RwLock::new(None),
        }
    }

    async fn get(&self) -> Option<Value> {
        let fresh = {
            let last = self.last.lock().await;
            last.map(|t| t.elapsed() < self.ttl).unwrap_or(false)
        };
        if !fresh {
            return None;
        }
        self.data.read().await.clone()
    }

    async fn put(&self, v: Value) {
        *self.last.lock().await = Some(Instant::now());
        *self.data.write().await = Some(v);
    }
}

// ---------------------------------------------------------------------------
// Сервер
// ---------------------------------------------------------------------------

/// Состояние BFF.
pub struct Server {
    cfg: UiConfig,
    reg: Registry,
    http: reqwest::Client,
    mesh: MeshCache,
}

impl Server {
    fn new(cfg: UiConfig, reg: Registry) -> Result<Arc<Self>> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(8))
            .user_agent(concat!("zeptomesh-ui/", "0.1"))
            .build()
            .context("ui: сборка http-клиента")?;
        let ttl = Duration::from_secs(cfg.poll_interval.max(1));
        Ok(Arc::new(Server {
            cfg,
            reg,
            http,
            mesh: MeshCache::new(ttl),
        }))
    }

    /// Bearer-токен для обращения к конкретной ноде.
    fn token_for(&self, node: &NodeEntry) -> Option<String> {
        node.token
            .clone()
            .filter(|t| !t.trim().is_empty())
            .or_else(|| {
                let t = self.cfg.api_token.trim();
                if t.is_empty() {
                    None
                } else {
                    Some(t.to_string())
                }
            })
    }

    /// Запрос к admin-API узла с подстановкой bearer-токена.
    async fn call(
        &self,
        node: &NodeEntry,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value)> {
        let url = format!("{}{}", node.url.trim_end_matches('/'), path);
        let mut req = self.http.request(method, &url);
        if let Some(t) = self.token_for(node) {
            req = req.bearer_auth(t);
        }
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.map_err(|e| anyhow!("{e}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        let value = serde_json::from_str::<Value>(&text)
            .unwrap_or_else(|_| Value::String(text.clone()));
        Ok((status, value))
    }

    // -----------------------------------------------------------------
    // Обработка запросов
    // -----------------------------------------------------------------

    async fn handle(&self, req: Request<Incoming>) -> Resp {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let query = req.uri().query().unwrap_or("").to_string();

        // Авторизация самого дашборда.
        if !self.cfg.token.is_empty() && path != "/healthz" {
            let presented = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .unwrap_or("")
                .to_string();
            if !const_eq(presented.as_bytes(), self.cfg.token.as_bytes()) {
                return err_resp(StatusCode::UNAUTHORIZED, "неверный токен дашборда");
            }
        }

        let body = match read_body(req).await {
            Ok(b) => b,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, e),
        };

        // --- служебные ---
        if method == Method::GET && path == "/healthz" {
            return json_resp(StatusCode::OK, json!({"status": "ok"}));
        }
        if method == Method::GET && path == "/api/v1/ui/config" {
            return json_resp(
                StatusCode::OK,
                json!({
                    "poll_interval": self.cfg.poll_interval,
                    "discovery_ports": self.cfg.discovery_ports,
                    "auth_required": !self.cfg.token.is_empty(),
                }),
            );
        }

        // --- реестр нод ---
        if path == "/api/v1/nodes" {
            match method {
                Method::GET => return self.nodes_list().await,
                Method::POST => return self.node_create(body).await,
                _ => return err_resp(StatusCode::METHOD_NOT_ALLOWED, "метод не поддерживается"),
            }
        }
        if path == "/api/v1/nodes/search" && method == Method::GET {
            return self.nodes_search(&query).await;
        }
        if path == "/api/v1/nodes/import" && method == Method::POST {
            return self.nodes_import(body).await;
        }
        if let Some(rest) = path.strip_prefix("/api/v1/nodes/") {
            let id = rest.trim_end_matches('/');
            if id.is_empty() {
                return err_resp(StatusCode::NOT_FOUND, "не указан идентификатор ноды");
            }
            match method {
                Method::GET => return self.node_get(id).await,
                Method::PUT | Method::PATCH => return self.node_update(id, body).await,
                Method::DELETE => return self.node_delete(id).await,
                _ => return err_resp(StatusCode::METHOD_NOT_ALLOWED, "метод не поддерживается"),
            }
        }

        // --- обнаружение нод ---
        if path == "/api/v1/discover" && method == Method::POST {
            return self.discover(body).await;
        }

        // --- снимок сети ---
        if path == "/api/v1/mesh" && method == Method::GET {
            return self.mesh_snapshot().await;
        }

        // --- задачи ---
        if path == "/api/v1/tasks" {
            match method {
                Method::GET => return self.tasks_list(&query).await,
                Method::POST => return self.tasks_submit(body).await,
                _ => return err_resp(StatusCode::METHOD_NOT_ALLOWED, "метод не поддерживается"),
            }
        }
        if let Some(rest) = path.strip_prefix("/api/v1/tasks/") {
            // /api/v1/tasks/{node_id}/{task_id}/cancel
            let parts: Vec<&str> = rest.split('/').filter(|s| !s.is_empty()).collect();
            if parts.len() == 3 && parts[2] == "cancel" && method == Method::POST {
                return self.task_cancel(parts[0], parts[1]).await;
            }
            return err_resp(StatusCode::NOT_FOUND, "неизвестный маршрут задач");
        }

        // --- прокси к admin-API ноды ---
        if let Some(rest) = path.strip_prefix("/api/v1/proxy/") {
            let mut parts = rest.splitn(2, '/');
            let node_id = parts.next().unwrap_or("").to_string();
            let sub = parts.next().unwrap_or("").to_string();
            let sub = if sub.is_empty() {
                "api/v1/status".to_string()
            } else {
                format!("api/v1/{sub}")
            };
            return self.proxy(&node_id, method, &sub, &query, body).await;
        }

        // --- статика ---
        if (method == Method::GET || method == Method::HEAD) && (path == "/" || path == "/index.html") {
            let body = if method == Method::HEAD {
                Bytes::new()
            } else {
                Bytes::from_static(INDEX_HTML.as_bytes())
            };
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/html; charset=utf-8")
                .header("content-length", INDEX_HTML.len().to_string())
                .header("cache-control", "no-store")
                .body(Full::new(body))
                .unwrap();
        }

        err_resp(StatusCode::NOT_FOUND, "маршрут не найден")
    }

    // -----------------------------------------------------------------
    // Ноды
    // -----------------------------------------------------------------

    async fn nodes_list(&self) -> Resp {
        let nodes = self.reg.list().await;
        json_resp(StatusCode::OK, json!({"nodes": nodes}))
    }

    async fn node_get(&self, id: &str) -> Resp {
        match self.reg.get(id).await {
            Some(n) => json_resp(StatusCode::OK, serde_json::to_value(n).unwrap_or(Value::Null)),
            None => err_resp(StatusCode::NOT_FOUND, "нода не найдена"),
        }
    }

    async fn node_create(&self, body: Bytes) -> Resp {
        let Ok(mut patch) = serde_json::from_slice::<NodePatch>(&body) else {
            return err_resp(StatusCode::BAD_REQUEST, "некорректный JSON");
        };
        let Some(url) = patch.url.take() else {
            return err_resp(StatusCode::BAD_REQUEST, "не указан url ноды");
        };
        let url = url.trim().to_string();
        if url.is_empty() {
            return err_resp(StatusCode::BAD_REQUEST, "url ноды не может быть пустым");
        }
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return err_resp(StatusCode::BAD_REQUEST, "url должен начинаться с http:// или https://");
        }
        let entry = NodeEntry {
            id: String::new(),
            name: patch.name.unwrap_or_default(),
            url: url.trim_end_matches('/').to_string(),
            token: patch.token,
            enabled: patch.enabled.unwrap_or(true),
            tags: patch.tags.unwrap_or_default(),
            created_at: None,
        };
        match self.reg.add(entry).await {
            Ok(n) => json_resp(StatusCode::CREATED, serde_json::to_value(n).unwrap_or(Value::Null)),
            Err(e) => err_resp(StatusCode::CONFLICT, e.to_string()),
        }
    }

    async fn node_update(&self, id: &str, body: Bytes) -> Resp {
        let Ok(patch) = serde_json::from_slice::<NodePatch>(&body) else {
            return err_resp(StatusCode::BAD_REQUEST, "некорректный JSON");
        };
        match self.reg.update(id, patch).await {
            Ok(n) => json_resp(StatusCode::OK, serde_json::to_value(n).unwrap_or(Value::Null)),
            Err(e) => err_resp(StatusCode::NOT_FOUND, e.to_string()),
        }
    }

    async fn node_delete(&self, id: &str) -> Resp {
        match self.reg.remove(id).await {
            Ok(()) => json_resp(StatusCode::OK, json!({"deleted": id})),
            Err(e) => err_resp(StatusCode::NOT_FOUND, e.to_string()),
        }
    }

    /// Поиск нод: по IP (одиночному, CIDR или диапазону) либо по подстроке.
    async fn nodes_search(&self, query: &str) -> Resp {
        let params = parse_query(query);
        let q = params.get("q").cloned().unwrap_or_default();
        let q = q.trim();
        let nodes = self.reg.list().await;

        let matched: Vec<NodeEntry> = if q.is_empty() {
            nodes
        } else if let Some(filter) = IpFilter::parse(q) {
            nodes
                .into_iter()
                .filter(|n| {
                    host_of(&n.url)
                        .and_then(|h| h.parse::<IpAddr>().ok())
                        .map(|ip| filter.matches(ip))
                        .unwrap_or(false)
                })
                .collect()
        } else {
            let needle = q.to_lowercase();
            nodes
                .into_iter()
                .filter(|n| {
                    n.name.to_lowercase().contains(&needle)
                        || n.url.to_lowercase().contains(&needle)
                        || n.tags.iter().any(|t| t.to_lowercase().contains(&needle))
                })
                .collect()
        };

        json_resp(
            StatusCode::OK,
            json!({
                "query": q,
                "count": matched.len(),
                "nodes": matched,
            }),
        )
    }

    /// Импортировать найденные обходом ноды в реестр.
    async fn nodes_import(&self, body: Bytes) -> Resp {
        let Ok(v) = serde_json::from_slice::<Value>(&body) else {
            return err_resp(StatusCode::BAD_REQUEST, "некорректный JSON");
        };
        let items = v
            .get("nodes")
            .and_then(|n| n.as_array())
            .cloned()
            .unwrap_or_default();
        let mut added = Vec::new();
        let mut skipped = Vec::new();
        for item in items {
            let url = item
                .get("url")
                .and_then(|u| u.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if url.is_empty() {
                continue;
            }
            let name = item
                .get("node_name")
                .and_then(|u| u.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    item.get("ip")
                        .and_then(|u| u.as_str())
                        .unwrap_or(&url)
                        .to_string()
                });
            let entry = NodeEntry {
                id: String::new(),
                name,
                url: url.trim_end_matches('/').to_string(),
                token: None,
                enabled: true,
                tags: vec!["discovered".to_string()],
                created_at: None,
            };
            match self.reg.add(entry).await {
                Ok(n) => added.push(n),
                Err(_) => skipped.push(url),
            }
        }
        json_resp(
            StatusCode::OK,
            json!({"added": added.len(), "skipped": skipped, "nodes": added}),
        )
    }

    /// Обход сети: найти admin-API узлы в диапазоне адресов.
    async fn discover(&self, body: Bytes) -> Resp {
        let v = serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({}));
        let timeout_ms = v
            .get("timeout_ms")
            .and_then(|x| x.as_u64())
            .unwrap_or(700)
            .clamp(100, 5000);
        let concurrency = v
            .get("concurrency")
            .and_then(|x| x.as_u64())
            .unwrap_or(64)
            .clamp(1, 512) as usize;

        let ports: Vec<u16> = match v.get("ports").and_then(|p| p.as_array()) {
            Some(arr) => arr
                .iter()
                .filter_map(|x| x.as_u64().map(|n| n as u16))
                .filter(|p| *p > 0)
                .collect(),
            None => self.cfg.discovery_ports.clone(),
        };
        if ports.is_empty() {
            return err_resp(StatusCode::BAD_REQUEST, "не заданы порты для обхода");
        }

        // Цели: диапазон адресов × порты.
        let mut targets: Vec<(IpAddr, u16)> = Vec::new();
        let mut ranges: Vec<IpFilter> = Vec::new();
        if let Some(s) = v.get("cidr").and_then(|x| x.as_str()) {
            if let Some(f) = IpFilter::parse(s) {
                ranges.push(f);
            }
        }
        if let Some(s) = v.get("range").and_then(|x| x.as_str()) {
            if let Some(f) = IpFilter::parse(s) {
                ranges.push(f);
            }
        }
        if let (Some(a), Some(b)) = (
            v.get("from").and_then(|x| x.as_str()),
            v.get("to").and_then(|x| x.as_str()),
        ) {
            if let Some(f) = IpFilter::parse(&format!("{a}-{b}")) {
                ranges.push(f);
            }
        }
        // Без явного диапазона обходим только локальный хост.
        if ranges.is_empty() {
            ranges.push(IpFilter::Exact("127.0.0.1".parse().unwrap()));
        }
        for r in &ranges {
            for ip in discovery::expand(r) {
                for p in &ports {
                    targets.push((ip, *p));
                }
            }
        }
        if targets.len() > discovery::MAX_SWEEP_TARGETS * 4 {
            targets.truncate(discovery::MAX_SWEEP_TARGETS * 4);
        }

        let found = discovery::sweep(
            &self.http,
            targets,
            concurrency,
            Duration::from_millis(timeout_ms),
        )
        .await;

        json_resp(
            StatusCode::OK,
            json!({
                "found": found,
                "count": found.len(),
            }),
        )
    }

    // -----------------------------------------------------------------
    // Снимок сети
    // -----------------------------------------------------------------

    async fn mesh_snapshot(&self) -> Resp {
        if let Some(cached) = self.mesh.get().await {
            return json_resp(StatusCode::OK, cached);
        }
        let snapshot = self.build_mesh().await;
        self.mesh.put(snapshot.clone()).await;
        json_resp(StatusCode::OK, snapshot)
    }

    async fn build_mesh(&self) -> Value {
        let all = self.reg.list().await;
        let enabled: Vec<NodeEntry> = all.iter().filter(|n| n.enabled).cloned().collect();

        // Параллельный опрос всех нод.
        let sem = Arc::new(Semaphore::new(32));
        let mut futs = Vec::new();
        for node in &enabled {
            let sem = sem.clone();
            let http = self.http.clone();
            let node = node.clone();
            futs.push(async move {
                let _permit = sem.acquire_owned().await.ok()?;
                Some(probe_node(&http, &node).await)
            });
        }
        let probes = futures::future::join_all(futs).await;

        // Управляемые ноды.
        let mut nodes: Vec<Value> = Vec::new();
        let mut peer_to_node: HashMap<String, String> = HashMap::new();
        for (i, node) in enabled.iter().enumerate() {
            let Some(probe) = probes[i].as_ref() else {
                continue;
            };
            let status = probe.status.clone();
            let peer_id = status
                .as_ref()
                .and_then(|s| s.get("peer_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if !peer_id.is_empty() {
                peer_to_node.insert(peer_id.clone(), node.id.clone());
            }
            nodes.push(json!({
                "id": node.id,
                "kind": "managed",
                "name": node.name,
                "url": node.url,
                "enabled": node.enabled,
                "tags": node.tags,
                "online": probe.error.is_none(),
                "error": probe.error.clone(),
                "peer_id": peer_id,
                "status": status,
                "peers": probe.peers.clone().unwrap_or_else(|| json!([])),
            }));
        }

        // Обнаруженные пиры: не управляются напрямую, но видны в списках соседей.
        let mut discovered: HashMap<String, Value> = HashMap::new();
        for probe in probes.iter().flatten() {
            let Some(peers) = probe.peers.as_ref().and_then(|p| p.as_array()) else {
                continue;
            };
            for peer in peers {
                let pid = peer
                    .get("peer_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if pid.is_empty() || peer_to_node.contains_key(&pid) {
                    continue;
                }
                let addrs = peer
                    .get("addrs")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|x| x.as_str())
                            .map(|s| s.to_string())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                discovered
                    .entry(pid.clone())
                    .or_insert_with(|| {
                        json!({
                            "id": format!("peer:{pid}"),
                            "kind": "discovered",
                            "name": format!("peer:{}", &pid.chars().take(12).collect::<String>()),
                            "peer_id": pid,
                            "url": addrs.first().cloned().unwrap_or_default(),
                            "addrs": addrs,
                            "online": peer.get("connected").and_then(|c| c.as_bool()).unwrap_or(false),
                            "status": null,
                            "peers": json!([]),
                        })
                    });
            }
        }

        // Рёбра графа: управляемая нода → её сосед.
        let mut links: Vec<Value> = Vec::new();
        let mut seen: HashSet<(String, String)> = HashSet::new();
        for (i, node) in enabled.iter().enumerate() {
            let Some(probe) = probes[i].as_ref() else {
                continue;
            };
            let Some(peers) = probe.peers.as_ref().and_then(|p| p.as_array()) else {
                continue;
            };
            for peer in peers {
                let pid = peer
                    .get("peer_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if pid.is_empty() {
                    continue;
                }
                let target = match peer_to_node.get(&pid) {
                    Some(id) => id.clone(),
                    None => format!("peer:{pid}"),
                };
                if target == node.id {
                    continue;
                }
                let key = (node.id.clone(), target.clone());
                if !seen.insert(key) {
                    continue;
                }
                links.push(json!({
                    "source": node.id,
                    "target": target,
                    "peer_id": pid,
                    "connected": peer.get("connected").and_then(|c| c.as_bool()).unwrap_or(false),
                }));
            }
        }

        // Итоговая статистика.
        let online = nodes
            .iter()
            .filter(|n| n["online"].as_bool().unwrap_or(false))
            .count();
        let total_peers: usize = nodes
            .iter()
            .map(|n| n["peers"].as_array().map(|a| a.len()).unwrap_or(0))
            .sum();
        let discovered_count = discovered.len();
        let link_count = links.len();
        let discovered: Vec<Value> = discovered.into_values().collect();

        json!({
            "updated_at": Utc::now().to_rfc3339(),
            "nodes": nodes,
            "discovered": discovered,
            "links": links,
            "stats": {
                "managed": all.len(),
                "enabled": enabled.len(),
                "online": online,
                "discovered": discovered_count,
                "links": link_count,
                "total_peers": total_peers,
            },
        })
    }

    // -----------------------------------------------------------------
    // Задачи
    // -----------------------------------------------------------------

    /// Список задач по всем (или выбранной) нодам.
    async fn tasks_list(&self, query: &str) -> Resp {
        let params = parse_query(query);
        let limit = params
            .get("limit")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(50)
            .clamp(1, 500);
        let node_filter = params.get("node_id").cloned().unwrap_or_default();

        let all = self.reg.list().await;
        let targets: Vec<NodeEntry> = all
            .iter()
            .filter(|n| n.enabled)
            .filter(|n| node_filter.is_empty() || n.id == node_filter)
            .cloned()
            .collect();

        let mut futs = Vec::new();
        for node in &targets {
            futs.push(async move {
                let (status, value) = self.call(node, Method::GET, "/api/v1/tasks?limit=50", None).await?;
                Ok::<_, anyhow::Error>((node.id.clone(), status, value))
            });
        }
        let results = futures::future::join_all(futs).await;

        let mut tasks: Vec<Value> = Vec::new();
        let mut errors: Vec<Value> = Vec::new();
        for r in results {
            match r {
                Ok((node_id, status, value)) => {
                    if !status.is_success() {
                        errors.push(json!({"node_id": node_id, "error": value.to_string()}));
                        continue;
                    }
                    let list = value
                        .get("tasks")
                        .and_then(|t| t.as_array())
                        .cloned()
                        .unwrap_or_default();
                    for mut t in list {
                        if let Some(obj) = t.as_object_mut() {
                            obj.insert("node_id".into(), json!(node_id));
                        }
                        tasks.push(t);
                    }
                }
                Err(e) => errors.push(json!({"error": e.to_string()})),
            }
        }
        tasks.truncate(limit);
        json_resp(
            StatusCode::OK,
            json!({"tasks": tasks, "errors": errors, "count": tasks.len()}),
        )
    }

    /// Постановка задачи выбранным нодам.
    async fn tasks_submit(&self, body: Bytes) -> Resp {
        let Ok(v) = serde_json::from_slice::<Value>(&body) else {
            return err_resp(StatusCode::BAD_REQUEST, "некорректный JSON");
        };
        let instruction = v
            .get("instruction")
            .and_then(|i| i.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if instruction.is_empty() {
            return err_resp(StatusCode::BAD_REQUEST, "не указана инструкция задачи");
        }
        let node_ids: Vec<String> = v
            .get("node_ids")
            .and_then(|n| n.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        if node_ids.is_empty() {
            return err_resp(StatusCode::BAD_REQUEST, "не выбраны ноды-получатели");
        }

        // Тело задачи в формате admin-API узла.
        let mut task = json!({
            "instruction": instruction,
            "required_skills": v.get("required_skills").cloned().unwrap_or(json!([])),
            "ttl": v.get("ttl").and_then(|x| x.as_i64()).unwrap_or(0),
            "priority": v.get("priority").and_then(|x| x.as_i64()).unwrap_or(0),
            "timeout_seconds": v.get("timeout_seconds").and_then(|x| x.as_i64()).unwrap_or(0),
            "allow_shell": v.get("allow_shell").and_then(|x| x.as_bool()).unwrap_or(false),
            "allow_network": v.get("allow_network").and_then(|x| x.as_bool()).unwrap_or(true),
            "labels": v.get("labels").cloned().unwrap_or(json!({})),
        });
        if let Some(sub) = v.get("subtasks") {
            if sub.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                task["subtasks"] = sub.clone();
            }
        }

        let mut results = Vec::new();
        let mut failures = 0usize;
        for id in &node_ids {
            let Some(node) = self.reg.get(id).await else {
                results.push(json!({"node_id": id, "ok": false, "error": "нода не найдена"}));
                failures += 1;
                continue;
            };
            match self.call(&node, Method::POST, "/api/v1/tasks", Some(task.clone())).await {
                Ok((status, value)) => {
                    let ok = status.is_success();
                    if !ok {
                        failures += 1;
                    }
                    results.push(json!({
                        "node_id": id,
                        "node": node.name,
                        "ok": ok,
                        "status": status.as_u16(),
                        "result": value,
                    }));
                }
                Err(e) => {
                    failures += 1;
                    results.push(json!({"node_id": id, "node": node.name, "ok": false, "error": e.to_string()}));
                }
            }
        }
        let status = if failures == 0 {
            StatusCode::CREATED
        } else if failures < results.len() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::BAD_GATEWAY
        };
        json_resp(
            status,
            json!({"submitted": results.len() - failures, "failed": failures, "results": results}),
        )
    }

    /// Отмена задачи на конкретной ноде.
    async fn task_cancel(&self, node_id: &str, task_id: &str) -> Resp {
        let Some(node) = self.reg.get(node_id).await else {
            return err_resp(StatusCode::NOT_FOUND, "нода не найдена");
        };
        let path = format!(
            "/api/v1/tasks/{}/{}/cancel?reason=dashboard",
            node_id, task_id
        );
        match self.call(&node, Method::POST, &path, None).await {
            Ok((status, value)) => {
                if status.is_success() {
                    json_resp(StatusCode::OK, value)
                } else {
                    err_resp(status, value.to_string())
                }
            }
            Err(e) => err_resp(StatusCode::BAD_GATEWAY, e.to_string()),
        }
    }

    // -----------------------------------------------------------------
    // Прокси к admin-API ноды
    // -----------------------------------------------------------------

    async fn proxy(
        &self,
        node_id: &str,
        method: Method,
        sub: &str,
        query: &str,
        body: Bytes,
    ) -> Resp {
        let Some(node) = self.reg.get(node_id).await else {
            return err_resp(StatusCode::NOT_FOUND, "нода не найдена");
        };
        let path = if query.is_empty() {
            format!("/{sub}")
        } else {
            format!("/{sub}?{query}")
        };
        let payload = if body.is_empty() {
            None
        } else {
            serde_json::from_slice::<Value>(&body).ok()
        };
        match self.call(&node, method, &path, payload).await {
            Ok((status, value)) => json_resp(status, value),
            Err(e) => err_resp(StatusCode::BAD_GATEWAY, e.to_string()),
        }
    }
}

/// Опрос одной ноды: статус и список соседей.
#[derive(Clone, Debug, Default)]
struct NodeProbe {
    status: Option<Value>,
    peers: Option<Value>,
    error: Option<String>,
}

/// Опрашивает admin-API ноды: `/api/v1/status` и `/api/v1/peers`.
async fn probe_node(http: &reqwest::Client, node: &NodeEntry) -> NodeProbe {
    let base = node.url.trim_end_matches('/');
    let status = match http.get(format!("{base}/api/v1/status")).send().await {
        Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
        Ok(r) => {
            return NodeProbe {
                error: Some(format!("HTTP {}", r.status().as_u16())),
                ..Default::default()
            }
        }
        Err(e) => {
            return NodeProbe {
                error: Some(e.to_string()),
                ..Default::default()
            }
        }
    };
    let peers = match http.get(format!("{base}/api/v1/peers")).send().await {
        Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
        _ => None,
    };
    NodeProbe {
        status,
        peers,
        error: None,
    }
}

// ---------------------------------------------------------------------------
// Запуск
// ---------------------------------------------------------------------------

/// Запустить BFF дашборда.
pub async fn serve(cfg: UiConfig) -> Result<()> {
    tokio::fs::create_dir_all(&cfg.data_dir)
        .await
        .ok();
    let reg = Registry::load(cfg.nodes_file.clone())?;
    let seed = std::env::var("ZEPTOMESH_UI_NODES").unwrap_or_default();
    let srv = Server::new(cfg.clone(), reg)?;

    // Стартовый список нод из env — только если реестр пуст.
    if srv.reg.list().await.is_empty() && !seed.trim().is_empty() {
        for url in seed.split(',') {
            let url = url.trim();
            if url.is_empty() {
                continue;
            }
            let entry = NodeEntry {
                id: String::new(),
                name: host_of(url).unwrap_or_else(|| url.to_string()),
                url: url.trim_end_matches('/').to_string(),
                token: None,
                enabled: true,
                tags: vec!["seed".to_string()],
                created_at: None,
            };
            if let Err(e) = srv.reg.add(entry).await {
                tracing::warn!(error = %e, "ui_seed_add_failed");
            }
        }
    }

    let listener = TcpListener::bind(&cfg.listen)
        .await
        .map_err(|e| anyhow!("ui: listen {}: {e}", cfg.listen))?;
    tracing::info!(
        addr = %cfg.listen,
        nodes_file = %cfg.nodes_file.display(),
        auth = !cfg.token.is_empty(),
        "dashboard_listening"
    );

    loop {
        let (stream, remote) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "ui accept failed");
                continue;
            }
        };
        let srv = srv.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req: Request<Incoming>| {
                let srv = srv.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(
                        srv.handle(req).await,
                    )
                }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                tracing::debug!(error = %e, peer = %remote, "ui connection error");
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Вспомогательные функции
// ---------------------------------------------------------------------------

/// SPA-страница дашборда (встраивается в бинарь).
const INDEX_HTML: &str = include_str!("static/index.html");

/// Сравнение строк в постоянном времени.
fn const_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
}

/// Разобрать query-строку в карту.
fn parse_query(q: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in q.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(url_decode(k), url_decode(v));
    }
    out
}

/// Раскодировать percent-encoding.
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Прочитать тело запроса с ограничением размера.
async fn read_body(req: Request<Incoming>) -> Result<Bytes, String> {
    let collected = req
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("чтение тела: {e}"))?;
    let bytes = collected.to_bytes();
    if bytes.len() > MAX_BODY {
        return Err("тело запроса слишком велико".into());
    }
    Ok(bytes)
}

/// JSON-ответ.
fn json_resp(status: StatusCode, v: Value) -> Resp {
    let body = serde_json::to_vec(&v).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

/// Ответ с ошибкой.
fn err_resp(status: StatusCode, msg: impl Into<String>) -> Resp {
    json_resp(status, json!({"error": msg.into()}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_extraction() {
        assert_eq!(host_of("http://127.0.0.1:14002").as_deref(), Some("127.0.0.1"));
        assert_eq!(host_of("http://node.local/api").as_deref(), Some("node.local"));
        assert_eq!(host_of("http://[::1]:8081").as_deref(), Some("::1"));
        assert_eq!(host_of("https://10.0.0.7").as_deref(), Some("10.0.0.7"));
    }

    #[test]
    fn ports_parsing() {
        let p = parse_ports("8081,8082,14001-14003");
        assert!(p.contains(&8081));
        assert!(p.contains(&8082));
        assert!(p.contains(&14001));
        assert!(p.contains(&14003));
        assert_eq!(p.len(), 5);
    }

    #[test]
    fn query_parsing() {
        let q = parse_query("limit=10&status=RUNNING&q=10.0.0.1");
        assert_eq!(q.get("limit").map(String::as_str), Some("10"));
        assert_eq!(q.get("status").map(String::as_str), Some("RUNNING"));
        assert_eq!(q.get("q").map(String::as_str), Some("10.0.0.1"));
    }

    #[tokio::test]
    async fn registry_crud_and_persistence() {
        let dir = std::env::temp_dir().join(format!("ui-reg-{}", uuid::Uuid::new_v4()));
        let path = dir.join("nodes.json");
        let reg = Registry::load(path.clone()).unwrap();

        let added = reg
            .add(NodeEntry {
                id: String::new(),
                name: String::new(),
                url: "http://10.0.0.1:14002".into(),
                token: None,
                enabled: true,
                tags: vec![],
                created_at: None,
            })
            .await
            .unwrap();
        assert!(!added.id.is_empty());
        assert_eq!(added.name, "10.0.0.1");

        // Дубликат по URL отклоняется.
        let dup = reg
            .add(NodeEntry {
                id: String::new(),
                name: "dup".into(),
                url: "http://10.0.0.1:14002".into(),
                token: None,
                enabled: true,
                tags: vec![],
                created_at: None,
            })
            .await;
        assert!(dup.is_err());

        // Персистентность: перечитываем файл.
        let reg2 = Registry::load(path.clone()).unwrap();
        assert_eq!(reg2.list().await.len(), 1);

        reg.update(&added.id, NodePatch { name: Some("node-a".into()), ..Default::default() })
            .await
            .unwrap();
        assert_eq!(reg.get(&added.id).await.unwrap().name, "node-a");

        reg.remove(&added.id).await.unwrap();
        assert!(reg.get(&added.id).await.is_none());
        assert!(reg.remove(&added.id).await.is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn search_by_ip_and_substring() {
        let dir = std::env::temp_dir().join(format!("ui-search-{}", uuid::Uuid::new_v4()));
        let path = dir.join("nodes.json");
        let reg = Registry::load(path).unwrap();
        for (name, url) in [
            ("alpha", "http://10.0.0.5:14002"),
            ("beta", "http://10.0.0.9:14003"),
            ("gamma", "http://192.168.5.5:14004"),
        ] {
            reg.add(NodeEntry {
                id: String::new(),
                name: name.into(),
                url: url.into(),
                token: None,
                enabled: true,
                tags: vec![],
                created_at: None,
            })
            .await
            .unwrap();
        }

        let filter = IpFilter::parse("10.0.0.0/24").unwrap();
        let matched: Vec<_> = reg
            .list()
            .await
            .into_iter()
            .filter(|n| {
                host_of(&n.url)
                    .and_then(|h| h.parse::<IpAddr>().ok())
                    .map(|ip| filter.matches(ip))
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(matched.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
