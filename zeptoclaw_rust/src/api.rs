//! Порт пакета `internal/api`: HTTP/JSON админ-API узла.
//!
//! Реализовано поверх hyper (Go: net/http + http.ServeMux). Совместимы путь,
//! методы, коды ответов и JSON-поля; brain/chat/triggers-ручки Go-версии
//! (зависят от пакетов internal/brain, internal/chat, internal/triggers) в
//! Rust-порте возвращают 501 Not Implemented.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::net::TcpListener;

use crate::config::Config;
use crate::node::Node;
use crate::tasks::{result_json, SubmitRequest, SubtaskSpec};

const MAX_BODY: usize = 8 << 20; // 8 MiB (Go: limitBody)

/// Сокращённый hex-дайджест (8 символов + "…") для отображения.
/// Соответствует Go-функции `shortHex` / `hexShort` из ui/.
fn hex_abbrev(s: &str) -> String {
    if s.len() <= 8 {
        s.to_string()
    } else {
        format!("{}…", &s[..8])
    }
}

// ---------------------------------------------------------------------------
// Роли и токены (Go: config.APIRole / resolveTokens)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Viewer,
    Operator,
    Admin,
}

impl Role {
    pub fn parse(s: &str) -> Option<Role> {
        match s {
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

#[derive(Clone)]
struct Identity {
    name: String,
    role: Role,
}

/// Разрешает токены из конфига (значения берутся из env по token_env).
fn resolve_tokens(cfg: &Config) -> HashMap<String, Identity> {
    let mut out = HashMap::new();
    if !cfg.api.auth_token_env.is_empty() {
        if let Ok(t) = std::env::var(&cfg.api.auth_token_env) {
            if !t.trim().is_empty() {
                out.insert(
                    t.trim().to_string(),
                    Identity { name: "default".into(), role: Role::Admin },
                );
            }
        }
    }
    for tk in &cfg.api.tokens {
        let val = std::env::var(&tk.token_env).unwrap_or_default();
        if val.trim().is_empty() {
            tracing::warn!(name = %tk.name, env = %tk.token_env, "api_token_missing");
            continue;
        }
        let role = Role::parse(&tk.role).unwrap_or(Role::Viewer);
        out.insert(
            val.trim().to_string(),
            Identity { name: tk.name.clone(), role },
        );
    }
    out
}

/// Constant-time сравнение токенов (Go: subtle.ConstantTimeCompare).
fn const_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
}

/// Go: roleAllows — viewer читает; operator читает и мутирует задачи/скиллы,
/// но /api/v1/admin/* остаётся admin-only (кроме scan); admin может всё.
fn role_allows(role: Role, method: &Method, path: &str) -> bool {
    match role {
        Role::Admin => true,
        Role::Viewer => *method == Method::GET,
        Role::Operator => {
            if *method == Method::GET {
                return true;
            }
            if path.starts_with("/api/v1/admin/") {
                return path == "/api/v1/admin/scan";
            }
            true
        }
    }
}

// ---------------------------------------------------------------------------
// Сервер
// ---------------------------------------------------------------------------

pub struct Server {
    node: Arc<Node>,
    cfg: Arc<Config>,
    tokens: HashMap<String, Identity>,
}

/// Точка входа Serve: слушает cfg.api.listen, принимает соединения до
/// завершения shutdown-сигнала (ctx.Done в Go).
pub async fn serve(node: Arc<Node>, cfg: Arc<Config>) -> Result<()> {
    if !cfg.api.enabled {
        return Ok(());
    }
    let tokens = resolve_tokens(&cfg);
    if tokens.is_empty() && cfg.security.trust_mode == "private" {
        tracing::warn!(
            hint = "configure api.tokens (or api.auth_token_env) or bind api.listen to localhost",
            "admin_api_unauthenticated"
        );
    }
    let srv = Arc::new(Server { node, cfg, tokens });

    let listener = TcpListener::bind(&srv.cfg.api.listen)
        .await
        .map_err(|e| anyhow::anyhow!("api: listen {}: {e}", srv.cfg.api.listen))?;
    tracing::info!(
        addr = %srv.cfg.api.listen,
        auth = !srv.tokens.is_empty(),
        "admin_api_listening"
    );

    let shutdown = srv.node.shutdown_token();
    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, remote) = match accept {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "api accept failed");
                        continue;
                    }
                };
                let srv = srv.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let srv = srv.clone();
                        async move { Result::<Response<BoxBody<Bytes, Infallible>>, Infallible>::Ok(srv.handle(req, remote).await) }
                    });
                    if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                        tracing::debug!(error = %e, "api connection error");
                    }
                });
            }
            _ = wait_shutdown(shutdown.clone()) => {
                tracing::info!("api stopped");
                return Ok(());
            }
        }
    }
}

/// Ожидание сигнала shutdown (watch-канал).
async fn wait_shutdown(mut rx: tokio::sync::watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

type Resp = Response<BoxBody<Bytes, Infallible>>;

fn json_resp(status: StatusCode, v: serde_json::Value) -> Resp {
    text_resp(status, "application/json", v.to_string())
}
fn text_resp(status: StatusCode, ct: &str, body: String) -> Resp {
    Response::builder()
        .status(status)
        .header("content-type", ct)
        .body(Full::new(Bytes::from(body)).boxed())
        .unwrap()
}
fn err_resp(status: StatusCode, msg: impl Into<String>) -> Resp {
    json_resp(status, serde_json::json!({ "error": msg.into() }))
}
fn not_found() -> Resp {
    err_resp(StatusCode::NOT_FOUND, "not found")
}

impl Server {
    /// Идентификация по Bearer-токену; без настроенных токенов — loopback-admin.
    fn identify(&self, req: &Request<Incoming>) -> Option<Identity> {
        if self.tokens.is_empty() {
            return Some(Identity { name: "loopback".into(), role: Role::Admin });
        }
        let auth = req.headers().get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("");
        let mut got = auth.strip_prefix("Bearer ").unwrap_or("").trim();
        // Совместимость с Go-портом и smoke-тестом: токен также принимается
        // из query-параметра ?token= (для curl/скриптов).
        if got.is_empty() {
            if let Some(q) = req.uri().query() {
                for kv in q.split('&') {
                    if let Some(v) = kv.strip_prefix("token=") {
                        got = v;
                        break;
                    }
                }
            }
        }
        if got.is_empty() {
            return None;
        }
        for (raw, ident) in &self.tokens {
            if const_eq(got.as_bytes(), raw.as_bytes()) {
                return Some(ident.clone());
            }
        }
        None
    }

    async fn handle(&self, req: Request<Incoming>, remote: std::net::SocketAddr) -> Resp {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let query = req.uri().query().unwrap_or("").to_string();

        // Middleware-эквиваленты Go: limitBody проверяется при чтении тела.
        let ident = match self.identify(&req) {
            Some(i) => i,
            None => {
                tracing::info!(event = "api_auth_denied", remote = %remote, route = %format!("{method} {path}"), "security");
                return err_resp(StatusCode::UNAUTHORIZED, "missing or bad bearer token");
            }
        };
        if !role_allows(ident.role, &method, &path) {
            tracing::info!(event = "api_role_denied", actor = %ident.name, remote = %remote, route = %format!("{method} {path}"), "security");
            return err_resp(StatusCode::FORBIDDEN, "insufficient permissions");
        }
        if method != Method::GET {
            tracing::info!(event = "api_mutation", actor = %ident.name, remote = %remote, route = %format!("{method} {path}"), "audit");
        }

        let body = read_body(req).await;
        let ctx = Ctx { method, path, query, body };
        self.route(&ctx).await
    }

    /// Таблица маршрутов (Go: http.ServeMux c паттернами "METHOD /path").
    async fn route(&self, c: &Ctx) -> Resp {
        let m = c.method.clone();
        let p = c.path.as_str();
        match (m.as_str(), p) {
            ("GET", "/healthz") => return self.health(),
            ("GET", "/api/v1/status") => return json_resp(StatusCode::OK, self.node.status_json()),
            ("GET", "/api/v1/peers") => return self.peers(&c.query),
            ("GET", "/api/v1/capabilities") => return self.capabilities(),
            ("POST", "/api/v1/tasks") => return self.submit(c).await,
            ("GET", "/api/v1/tasks") => return self.list_tasks(&c.query),
            ("GET", "/metrics") => return self.metrics(),
            _ => {}
        }
        // Пути с параметром {id}: /api/v1/tasks/{id}, .../cancel, .../resubmit
        if let Some(rest) = p.strip_prefix("/api/v1/tasks/") {
            let id = rest.split('/').next().unwrap_or("");
            if id.is_empty() {
                return not_found();
            }
            let suffix = &rest[id.len()..];
            match (m.as_str(), suffix) {
                ("GET", "") => return self.get_task(id),
                ("DELETE", "") => return self.cancel_task(id, &c.query).await,
                ("POST", "/cancel") => return self.cancel_task(id, &c.query).await,
                ("POST", "/resubmit") => return self.resubmit(id).await,
                _ => return not_found(),
            }
        }
        match (m.as_str(), p) {
            ("GET", "/api/v1/config") => self.config_view(),
            ("POST", "/api/v1/admin/leave") | ("POST", "/api/v1/shutdown") => {
                self.shutdown_alias().await
            }
            // Scan (Go: internal/discovery LAN/subnet sweep)
            ("POST", "/api/v1/admin/scan") => self.run_scan().await,
            ("GET", "/api/v1/admin/scan") => json_resp(StatusCode::OK, self.node.scan_status_json()),
            // Chat (Go: internal/chat поверх brain-пула)
            ("POST", "/api/v1/chat") => self.chat_send(c).await,
            ("GET", "/api/v1/chat/history") => self.chat_history(&c.query),
            // Skills-реестр (Go: internal/skills)
            ("GET", "/api/v1/skills") => self.skills_list(),
            ("POST", "/api/v1/skills") => self.skills_add(c),
            ("POST", "/api/v1/skills/sync") => self.skills_sync(c).await,
            // Triggers (Go: internal/triggers)
            ("GET", "/api/v1/triggers") => self.triggers_list(),
            ("POST", "/api/v1/triggers") => self.triggers_add(c),
            // Brain / LLM-бэкенды (Go: internal/brain)
            ("GET", "/api/v1/brain/backends") => self.brain_backends(),
            ("POST", "/api/v1/brain/scan") => self.brain_probe().await,
            ("POST", "/api/v1/candidates/promote") => self.candidates_promote(c).await,
            ("GET", "/api/v1/brain/candidates") => self.brain_candidates(),
            // Admin lifecycle
            ("POST", "/api/v1/admin/reload-config") => self.reload_config(),
            // Ручки Go-версии, не имеющие аналога в Rust-порте (перевод ключей
            // между узлами/друзьями и rebinds): остаются 501 осознанно.
            ("GET", "/api/v1/rebinds")
            | ("POST", "/api/v1/admin/rotate-key")
            | ("POST", "/api/v1/admin/revoke")
            | ("POST", "/api/v1/keys/exchange")
            | ("POST", "/api/v1/keys/share")
            | ("POST", "/api/v1/friends/promote") => err_resp(
                StatusCode::NOT_IMPLEMENTED,
                "endpoint not available in the Rust port (no Go analogue implemented)",
            ),
            _ => not_found(),
        }
    }

    // ---- scan ----

    async fn run_scan(&self) -> Resp {
        match self.node.clone().run_scan().await {
            Ok(found) => json_resp(
                StatusCode::OK,
                serde_json::json!({ "dialed": found, "neighbors_total": self.node.table.len() }),
            ),
            Err(e) => err_resp(StatusCode::CONFLICT, e.to_string()),
        }
    }

    // ---- chat ----

    async fn chat_send(&self, c: &Ctx) -> Resp {
        let svc = match &self.node.chat {
            Some(s) => s.clone(),
            None => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "chat disabled: configure picoclaw.local_url or picoclaw.backends",
                )
            }
        };
        #[derive(Deserialize)]
        struct Req {
            #[serde(default)]
            conversation: String,
            #[serde(default)]
            message: String,
            #[serde(default)]
            model: Option<String>,
        }
        let body = match c.json_body() {
            Ok(b) => b,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, e),
        };
        let r: Req = match serde_json::from_value(body) {
            Ok(r) => r,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}")),
        };
        if r.message.trim().is_empty() {
            return err_resp(StatusCode::BAD_REQUEST, "message is required");
        }
        let conv = if r.conversation.is_empty() { "default".to_string() } else { r.conversation };
        match svc.send(&conv, &r.message, r.model.as_deref()).await {
            Ok((reply, backend_id)) => json_resp(
                StatusCode::OK,
                serde_json::json!({
                    "conversation": conv,
                    "reply": reply.content,
                    "backend": backend_id,
                    "created_at": reply.created_at,
                }),
            ),
            Err(e) => err_resp(StatusCode::BAD_GATEWAY, e.to_string()),
        }
    }

    fn chat_history(&self, query: &str) -> Resp {
        let svc = match &self.node.chat {
            Some(s) => s.clone(),
            None => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "chat disabled: configure picoclaw.local_url or picoclaw.backends",
                )
            }
        };
        let q = parse_query(query);
        let conv = q.get("conversation").cloned().unwrap_or_else(|| "default".into());
        let msgs = svc.history(&conv);
        json_resp(
            StatusCode::OK,
            serde_json::json!({ "conversation": conv, "count": msgs.len(), "messages": msgs }),
        )
    }

    // ---- skills ----

    fn skills_list(&self) -> Resp {
        let reg = match &self.node.skills {
            Some(r) => r.clone(),
            None => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "skills exchange disabled (capabilities.skill_exchange.enabled=false)",
                )
            }
        };
        let docs = reg.list();
        json_resp(
            StatusCode::OK,
            serde_json::json!({
                "version": reg.version(),
                "count": docs.len(),
                "skills": docs.iter().map(|d| serde_json::json!({
                    "name": d.name,
                    "version": d.version,
                    "updated_at": d.updated_at,
                    "digest": hex_abbrev(&d.digest),
                    "description": d.description,
                    "models": d.models,
                    "attributes": d.attributes,
                })).collect::<Vec<_>>(),
            }),
        )
    }

    fn skills_add(&self, c: &Ctx) -> Resp {
        let reg = match &self.node.skills {
            Some(r) => r.clone(),
            None => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "skills exchange disabled (capabilities.skill_exchange.enabled=false)",
                )
            }
        };
        #[derive(Deserialize)]
        struct Req {
            name: String,
            #[serde(default)]
            description: String,
            #[serde(default)]
            models: Vec<String>,
        }
        let body = match c.json_body() {
            Ok(b) => b,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, e),
        };
        let r: Req = match serde_json::from_value(body) {
            Ok(v) => v,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}")),
        };
        if r.name.trim().is_empty() {
            return err_resp(StatusCode::BAD_REQUEST, "name is required");
        }
        // Дескриптор с текущим временем; digest — sha256 от канонического тела.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let mut d = crate::proto::SkillDescriptor {
            name: r.name.clone(),
            version: now,
            updated_at: now,
            digest: String::new(),
            description: r.description,
            models: r.models,
            attributes: Default::default(),
        };
        d.digest = hex::encode(crate::wire::sha256_bytes(&crate::wire::skill_digest_body(&d)));
        reg.upsert_own(d);
        json_resp(StatusCode::ACCEPTED, serde_json::json!({ "name": r.name, "version": now }))
    }

    async fn skills_sync(&self, c: &Ctx) -> Resp {
        let reg = match &self.node.skills {
            Some(r) => r.clone(),
            None => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "skills exchange disabled (capabilities.skill_exchange.enabled=false)",
                )
            }
        };
        #[derive(Deserialize)]
        struct Req {
            #[serde(default)]
            peer_id: String,
            #[serde(default)]
            full: bool,
        }
        let body = match c.json_body() {
            Ok(b) => b,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, e),
        };
        let r: Req = match serde_json::from_value(body) {
            Ok(v) => v,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}")),
        };
        if r.peer_id.is_empty() || !self.node.table.contains(&r.peer_id) {
            return err_resp(StatusCode::NOT_FOUND, format!("unknown peer: {}", r.peer_id));
        }
        // Отправить SkillsSyncRequest по активному конну к пиру и дождаться ответа.
        let req_msg = reg.build_request(&r.peer_id, r.full);
        match self.node.rpc_skills_sync(&r.peer_id, req_msg).await {
            Ok(imported) => json_resp(
                StatusCode::OK,
                serde_json::json!({ "peer_id": r.peer_id, "imported": imported, "version": reg.version() }),
            ),
            Err(e) => err_resp(StatusCode::BAD_GATEWAY, e.to_string()),
        }
    }

    // ---- triggers ----

    fn triggers_list(&self) -> Resp {
        let sch = match &self.node.scheduler {
            Some(s) => s.clone(),
            None => return json_resp(StatusCode::OK, serde_json::json!({ "count": 0, "triggers": [] })),
        };
        let list = sch.list();
        json_resp(StatusCode::OK, serde_json::json!({ "count": list.len(), "triggers": list }))
    }

    fn triggers_add(&self, c: &Ctx) -> Resp {
        let sch = match &self.node.scheduler {
            Some(s) => s.clone(),
            None => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "triggers scheduler inactive (no triggers configured at startup)",
                )
            }
        };
        let body = match c.json_body() {
            Ok(b) => b,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, e),
        };
        let tc: crate::config::TriggerConfig = match serde_json::from_value(body) {
            Ok(v) => v,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}")),
        };
        match sch.add(tc.clone()) {
            Ok(next) => json_resp(
                StatusCode::ACCEPTED,
                serde_json::json!({ "id": tc.id, "next_run": next }),
            ),
            Err(e) => err_resp(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
        }
    }

    // ---- brain ----

    fn pool_or_err(&self) -> Option<Arc<crate::picoclaw::BackendPool>> {
        self.node.pool.clone()
    }

    fn brain_backends(&self) -> Resp {
        let pool = match self.pool_or_err() {
            Some(p) => p,
            None => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "brain disabled: configure picoclaw.local_url or picoclaw.backends",
                )
            }
        };
        let list = pool.list();
        json_resp(
            StatusCode::OK,
            serde_json::json!({ "count": list.len(), "backends": list }),
        )
    }

    async fn brain_probe(&self) -> Resp {
        let pool = match self.pool_or_err() {
            Some(p) => p,
            None => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "brain disabled: configure picoclaw.local_url or picoclaw.backends",
                )
            }
        };
        pool.probe().await;
        let list = pool.list();
        let usable = pool.usable().len();
        json_resp(
            StatusCode::OK,
            serde_json::json!({ "probed": list.len(), "usable": usable, "backends": list }),
        )
    }

    async fn candidates_promote(&self, c: &Ctx) -> Resp {
        #[derive(Deserialize)]
        struct Req {
            base_url: String,
            #[serde(default)]
            api_key_env: String,
            #[serde(default)]
            promoted_from: String,
        }
        let body = match c.json_body() {
            Ok(b) => b,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, e),
        };
        let r: Req = match serde_json::from_value(body) {
            Ok(v) => v,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}")),
        };
        if r.base_url.trim().is_empty() || !(r.base_url.starts_with("http://") || r.base_url.starts_with("https://")) {
            return err_resp(StatusCode::BAD_REQUEST, "base_url must be an http(s) URL");
        }
        match self.node.promote_backend(&r.base_url, &r.api_key_env, &r.promoted_from).await {
            Ok(b) => json_resp(StatusCode::OK, serde_json::json!({ "backend": b })),
            Err(e) => err_resp(StatusCode::CONFLICT, e.to_string()),
        }
    }

    fn brain_candidates(&self) -> Resp {
        // Кандидаты = найденные scan'ом эндпоинты ещё не добавлены в пул
        // автоматически (промоуток через /api/v1/candidates/promote). Список
        // формируется из записей соседей с объявленными моделями.
        let mut out: Vec<serde_json::Value> = Vec::new();
        for n in self.node.table.list() {
            if n.models.is_empty() {
                continue;
            }
            out.push(serde_json::json!({
                "peer_id": n.peer_id,
                "node_name": n.node_name,
                "models": n.models,
                "addrs": n.addrs,
            }));
        }
        json_resp(StatusCode::OK, serde_json::json!({ "count": out.len(), "candidates": out }))
    }

    fn reload_config(&self) -> Resp {
        match self.node.reload_config() {
            Ok(()) => json_resp(
                StatusCode::OK,
                serde_json::json!({ "status": "reloaded", "note": "restart required for non-hot fields" }),
            ),
            Err(e) => err_resp(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
        }
    }

    // ---- ручки ----

    fn health(&self) -> Resp {
        json_resp(
            StatusCode::OK,
            serde_json::json!({ "status": "ok", "peer_id": self.node.self_id() }),
        )
    }

    fn peers(&self, query: &str) -> Resp {
        let q = parse_query(query);
        let limit = get_int(&q, "limit", 200);
        let minimal = q.get("minimal").map(|s| s == "true").unwrap_or(false);
        let mut out: Vec<serde_json::Value> = Vec::new();
        for n in self.node.table.list() {
            let mut v = serde_json::json!({
                "peer_id": n.peer_id,
                "trust": self.node.table.trust_of(&n.peer_id).to_string(),
                "category": n.category,
                "connected": n.connected,
                "left": n.left,
                "successes": n.successes,
                "failures": n.failures,
                "last_seen_unix": n.last_seen,
                "rtt_ms": n.rtt_ms,
            });
            if !n.addrs.is_empty() {
                v["addrs"] = serde_json::json!(n.addrs);
            }
            if !minimal {
                v["skills"] = serde_json::json!(n.skills);
                v["version"] = serde_json::json!(n.version);
                v["load"] = serde_json::json!(n.load);
            }
            out.push(v);
            if out.len() >= limit {
                break;
            }
        }
        json_resp(StatusCode::OK, serde_json::json!({ "count": out.len(), "peers": out }))
    }

    fn capabilities(&self) -> Resp {
        let caps = self.node.manager.capabilities();
        json_resp(
            StatusCode::OK,
            serde_json::json!({
                "peer_id": caps.peer_id,
                "node_name": caps.node_name,
                "version": caps.version,
                "skills": caps.skills,
                "models": caps.models,
                "max_parallel_tasks": caps.max_parallel_tasks,
                "running_tasks": caps.running_tasks,
                "load": caps.load,
                "accept_external_tasks": caps.accept_external_tasks,
                "allow_shell": caps.allow_shell,
                "relay_capable": caps.relay_capable,
                "resource_class": caps.resource_class,
                "listen_addrs": caps.listen_addrs,
            }),
        )
    }

    async fn submit(&self, c: &Ctx) -> Resp {
        #[derive(Deserialize)]
        struct SubReq {
            #[serde(default)]
            instruction: String,
            #[serde(default)]
            required_skills: Vec<String>,
            #[serde(default)]
            ttl: i32,
            #[serde(default)]
            priority: i32,
            #[serde(default)]
            timeout_seconds: i32,
            #[serde(default)]
            allow_shell: bool,
            #[serde(default)]
            allow_network_tools: Option<bool>,
            #[serde(default)]
            allow_delegation: Option<bool>,
            #[serde(default)]
            allow_subtasks: Option<bool>,
            #[serde(default)]
            labels: std::collections::BTreeMap<String, String>,
            #[serde(default)]
            subtasks: Vec<Subtask>,
            #[serde(default)]
            wait: bool,
            #[serde(default)]
            wait_seconds: i64,
        }
        #[derive(Deserialize)]
        struct Subtask {
            #[serde(default)]
            instruction: String,
            #[serde(default)]
            required_skills: Vec<String>,
            #[serde(default)]
            ttl: i32,
            #[serde(default)]
            priority: i32,
        }
        let body = match c.json_body() {
            Ok(b) => b,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, e),
        };
        let req: SubReq = match serde_json::from_value(body) {
            Ok(r) => r,
            Err(e) => return err_resp(StatusCode::BAD_REQUEST, format!("invalid JSON body: {e}")),
        };
        if req.instruction.trim().is_empty() {
            return err_resp(StatusCode::BAD_REQUEST, "instruction is required");
        }
        if req.priority < 0 || req.priority > 9 {
            return err_resp(StatusCode::BAD_REQUEST, "priority must be within 1..9 (0 = default)");
        }
        if req.subtasks.len() > crate::tasks::MAX_SUBTASKS {
            return err_resp(
                StatusCode::BAD_REQUEST,
                format!("too many subtasks (max {})", crate::tasks::MAX_SUBTASKS),
            );
        }
        let sr = SubmitRequest {
            instruction: req.instruction,
            required_skills: req.required_skills,
            ttl: req.ttl,
            priority: req.priority,
            timeout_seconds: req.timeout_seconds,
            allow_shell: req.allow_shell,
            allow_network: req.allow_network_tools.unwrap_or(true),
            allow_deleg: req.allow_delegation,
            allow_subtasks: req.allow_subtasks,
            labels: req.labels,
            subtasks: req
                .subtasks
                .into_iter()
                .map(|s| SubtaskSpec {
                    instruction: s.instruction,
                    required_skills: s.required_skills,
                    ttl: s.ttl,
                    priority: s.priority,
                })
                .collect(),
        };
        let id = match self.node.submit(sr).await {
            Ok(id) => id,
            Err(e) => return err_resp(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
        };
        let mut resp = serde_json::json!({ "task_id": id });
        if req.wait {
            let wait_for = if req.wait_seconds > 0 {
                req.wait_seconds
            } else {
                // Go: DefaultTimeoutSeconds + DefaultTTL * AttemptTimeout(сек)
                (self.cfg.tasks.default_timeout_seconds
                    + self.cfg.tasks.default_ttl
                        * self.cfg.tasks.forwarding.attempt_timeout.as_secs() as i32)
                    as i64
            };
            let deadline = Utc::now() + chrono::Duration::seconds(wait_for.max(1));
            loop {
                if let Some(r) = self.node.manager.get_result(&id) {
                    resp["result"] = result_json(&r);
                    break;
                }
                if Utc::now() >= deadline {
                    resp["error"] = serde_json::json!("timed out waiting for result");
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        json_resp(StatusCode::ACCEPTED, resp)
    }

    fn list_tasks(&self, query: &str) -> Resp {
        let q = parse_query(query);
        let limit = get_int(&q, "limit", 50);
        let status = q.get("status").map(|s| s.to_uppercase()).unwrap_or_default();
        let recs = self
            .node
            .store
            .list_tasks(limit, &status)
            .unwrap_or_default();
        json_resp(StatusCode::OK, serde_json::json!({ "count": recs.len(), "tasks": recs }))
    }

    fn get_task(&self, id: &str) -> Resp {
        let rec = match self.node.store.get_task(id) {
            Ok(r) => r,
            Err(_) => return err_resp(StatusCode::NOT_FOUND, format!("task not found: {id}")),
        };
        // Как в Go: record сериализуется с полем status верхнего уровня.
        let mut out = serde_json::json!({ "task": rec, "status": rec.status });
        if let Some(res) = self.node.manager.get_result(id) {
            out["result"] = result_json(&res);
        } else if let Ok(rr) = self.node.store.get_result(id) {
            out["result"] = serde_json::json!(rr);
        }
        json_resp(StatusCode::OK, out)
    }

    async fn cancel_task(&self, id: &str, query: &str) -> Resp {
        let q = parse_query(query);
        let reason = q.get("reason").cloned().unwrap_or_else(|| "canceled via admin API".into());
        match self.node.manager.cancel(id).await {
            Ok(()) => {
                tracing::info!(task = %id, reason = %reason, "task_canceled");
                json_resp(StatusCode::OK, serde_json::json!({ "task_id": id, "status": "canceled" }))
            }
            Err(e) => err_resp(StatusCode::CONFLICT, e.to_string()),
        }
    }

    async fn resubmit(&self, id: &str) -> Resp {
        let rec = match self.node.store.get_task(id) {
            Ok(r) => r,
            Err(_) => return err_resp(StatusCode::NOT_FOUND, format!("task not found: {id}")),
        };
        let instruction = match self.node.instruction_of(id) {
            Some(i) => i,
            None => {
                return err_resp(
                    StatusCode::CONFLICT,
                    format!("cannot recover instruction for {id}"),
                )
            }
        };
        let new_id = match self
            .node
            .submit(SubmitRequest {
                instruction,
                required_skills: rec.required_skills.clone(),
                priority: rec.priority,
                labels: std::collections::BTreeMap::from([("resubmit_of".to_string(), id.to_string())]),
                ..Default::default()
            })
            .await
        {
            Ok(i) => i,
            Err(e) => return err_resp(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
        };
        json_resp(StatusCode::ACCEPTED, serde_json::json!({ "task_id": new_id, "resubmit_of": id }))
    }

    fn config_view(&self) -> Resp {
        // Go: секьюрная конфигурация исключена из ответа.
        match serde_json::to_value(self.cfg.as_ref()) {
            Ok(mut v) => {
                if let Some(sec) = v.get_mut("security") {
                    *sec = serde_json::json!({ "trust_mode": self.cfg.security.trust_mode });
                }
                json_resp(StatusCode::OK, v)
            }
            Err(e) => err_resp(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    }

    async fn leave(&self) -> Resp {
        self.node.request_shutdown();
        json_resp(StatusCode::OK, serde_json::json!({ "status": "leaving" }))
    }

    /// Совместимость со smoke-тестом/скриптами: /api/v1/shutdown — алиас leave.
    async fn shutdown_alias(&self) -> Resp {
        self.leave().await
    }

    /// Минимальный Prometheus-textformat вместо prometheus/client-go:
    /// счётчики процессов + метрики mesh из менеджера/таблицы.
    fn metrics(&self) -> Resp {
        let (completed, failed, rejected, forwarded) = self.node.manager.counters();
        let stats = self.node.store.stats().unwrap_or_default();
        let body = format!(
            "# HELP zeptomesh_up Node liveness.\n\
             # TYPE zeptomesh_up gauge\n\
             zeptomesh_up 1\n\
             # HELP zeptomesh_peers Known neighbors.\n\
             # TYPE zeptomesh_peers gauge\n\
             zeptomesh_peers {}\n\
             # HELP zeptomesh_tasks_running Currently executing tasks.\n\
             # TYPE zeptomesh_tasks_running gauge\n\
             zeptomesh_tasks_running {}\n\
             # HELP zeptomesh_tasks_completed_total Completed tasks.\n\
             # TYPE zeptomesh_tasks_completed_total counter\n\
             zeptomesh_tasks_completed_total {}\n\
             # HELP zeptomesh_tasks_failed_total Failed tasks.\n\
             # TYPE zeptomesh_tasks_failed_total counter\n\
             zeptomesh_tasks_failed_total {}\n\
             # HELP zeptomesh_tasks_rejected_total Rejected tasks.\n\
             # TYPE zeptomesh_tasks_rejected_total counter\n\
             zeptomesh_tasks_rejected_total {}\n\
             # HELP zeptomesh_tasks_forwarded_total Forwarded tasks.\n\
             # TYPE zeptomesh_tasks_forwarded_total counter\n\
             zeptomesh_tasks_forwarded_total {}\n\
             # HELP zeptomesh_storage_tasks Journal task records.\n\
             # TYPE zeptomesh_storage_tasks gauge\n\
             zeptomesh_storage_tasks {}\n\
             # HELP zeptomesh_storage_peers Persisted peer records.\n\
             # TYPE zeptomesh_storage_peers gauge\n\
             zeptomesh_storage_peers {}\n",
            self.node.table.len(),
            self.node.manager.running(),
            completed,
            failed,
            rejected,
            forwarded,
            stats.tasks,
            stats.peers,
        );
        text_resp(StatusCode::OK, "text/plain; version=0.0.4; charset=utf-8", body)
    }
}

// ---------------------------------------------------------------------------
// Утилиты запросов
// ---------------------------------------------------------------------------

struct Ctx {
    method: Method,
    path: String,
    query: String,
    body: Result<Bytes, String>,
}

impl Ctx {
    fn json_body(&self) -> Result<serde_json::Value, String> {
        let b = self.body.as_ref().map_err(|e| e.clone())?;
        serde_json::from_slice(b).map_err(|e| format!("invalid JSON body: {e}"))
    }
}

async fn read_body(req: Request<Incoming>) -> Result<Bytes, String> {
    let (parts, incoming) = req.into_parts();
    let max = parts
        .headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    if max > MAX_BODY {
        return Err("request body too large".into());
    }
    let collected = incoming
        .collect()
        .await
        .map_err(|e| format!("read body: {e}"))?;
    let b = collected.to_bytes();
    if b.len() > MAX_BODY {
        return Err("request body too large".into());
    }
    Ok(b)
}

fn parse_query(q: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in q.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("").to_string();
        let v = it.next().unwrap_or("").to_string();
        if !k.is_empty() {
            out.insert(k, v);
        }
    }
    out
}

fn get_int(q: &HashMap<String, String>, key: &str, def: usize) -> usize {
    q.get(key).and_then(|v| v.parse().ok()).unwrap_or(def)
}

// ---------------------------------------------------------------------------
// Тесты
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_matrix() {
        assert!(role_allows(Role::Viewer, &Method::GET, "/api/v1/tasks"));
        assert!(!role_allows(Role::Viewer, &Method::POST, "/api/v1/tasks"));
        assert!(role_allows(Role::Operator, &Method::POST, "/api/v1/tasks"));
        assert!(role_allows(Role::Operator, &Method::POST, "/api/v1/admin/scan"));
        assert!(!role_allows(Role::Operator, &Method::POST, "/api/v1/admin/leave"));
        assert!(role_allows(Role::Admin, &Method::POST, "/api/v1/admin/leave"));
    }

    #[test]
    fn query_parsing() {
        let q = parse_query("limit=7&status=failed&minimal=true");
        assert_eq!(get_int(&q, "limit", 50), 7);
        assert_eq!(q.get("status").unwrap(), "failed");
        assert_eq!(get_int(&q, "absent", 50), 50);
    }

    #[test]
    fn const_time_eq_basics() {
        assert!(const_eq(b"token", b"token"));
        assert!(!const_eq(b"token", b"tokne"));
        assert!(!const_eq(b"a", b"ab"));
    }
}
