//! zeptomesh-node — демон узлов ZeptoClaw Agent Mesh и админ-клиент
//! локального узла (порт Go: `cmd/zeptomesh-node`).
//!
//!     zeptomesh-node run        -c configs/node.yaml   # запуск узла
//!     zeptomesh-node version                            # версия сборки
//!     zeptomesh-node genkey     --out FILE [--print-id] # создать идентичность
//!     zeptomesh-node submit     -i "instruction" [-w]   # инъекция задачи
//!     zeptomesh-node status|peers|tasks|get|cancel|resubmit|leave
//!
//! Команды клиента обращаются к HTTP admin-API запущенного локального узла
//! (Go: clientXxx + net/http). Brain/chat/triggers-команды Go-версии зависят
//! от ещё не перенесённых пакетов и в Rust-порте пока не предоставляются.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde_json::Value;

use zeptomesh::api;
use zeptomesh::config::Config;
use zeptomesh::node::Node;
use zeptomesh::security::Identity;
use zeptomesh::version;

/// systemd sd_notify: «выход для перезапуска» (Restart=always/on-failure).
const EX_RESTART: i32 = 75;

const USAGE: &str = "zeptomesh-node — ZeptoClaw Agent Mesh node daemon";

#[derive(Parser)]
#[command(name = "zeptomesh-node", version = version::VERSION, about = USAGE)]
struct Cli {
    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Запустить mesh-узел
    Run {
        /// Путь к YAML-конфигурации (или ZEPTOMESH_CONFIG)
        #[arg(short = 'c', long, env = "ZEPTOMESH_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Версия сборки
    Version,
    /// Создать файл Ed25519-идентичности
    Genkey {
        /// Файл ключа для создания (обязателен)
        #[arg(long)]
        out: PathBuf,
        /// Перезаписать существующий ключ
        #[arg(long)]
        force: bool,
        /// Напечатать полученный peer id
        #[arg(long, default_value_t = true)]
        print_id: bool,
    },
    /// Статус локального узла (GET /api/v1/status)
    Status {
        #[arg(long)]
        api: Option<String>,
        #[arg(long)]
        token: Option<String>,
    },
    /// Список известных соседей (GET /api/v1/peers)
    Peers {
        #[arg(long)]
        api: Option<String>,
        #[arg(long)]
        token: Option<String>,
    },
    /// Инъекция задачи (POST /api/v1/tasks)
    Submit {
        /// Инструкция исполнителю
        #[arg(short = 'i', long)]
        instruction: String,
        /// Требуемые навыки (повторяемый флаг или через запятую)
        #[arg(long)]
        skill: Vec<String>,
        /// TTL в hops
        #[arg(long, default_value_t = 0)]
        ttl: i32,
        /// Приоритет 0..255 (-1 — по умолчанию узла)
        #[arg(long, default_value_t = -1)]
        priority: i32,
        /// Таймаут исполнения, сек (0 — по умолчанию узла)
        #[arg(long, default_value_t = 0)]
        timeout: i32,
        /// Разрешить shell-инструменты
        #[arg(long)]
        shell: bool,
        /// Разрешить сетевые инструменты
        #[arg(long, default_value_t = true)]
        network: bool,
        /// Метка key=value (повторяемая)
        #[arg(long)]
        label: Vec<String>,
        /// Ждать результата
        #[arg(short = 'w', long)]
        wait: bool,
        /// Максимум ожидания результата, сек (0 — рассчитает узел)
        #[arg(long, default_value_t = 0)]
        wait_seconds: i64,
        #[arg(long)]
        api: Option<String>,
        #[arg(long)]
        token: Option<String>,
    },
    /// Список недавних задач (GET /api/v1/tasks)
    Tasks {
        #[arg(long, default_value_t = 50)]
        limit: u32,
        /// Фильтр по статусу (SUCCEEDED, FAILED, ...)
        #[arg(long, default_value = "")]
        status: String,
        #[arg(long)]
        api: Option<String>,
        #[arg(long)]
        token: Option<String>,
    },
    /// Показать одну задачу (GET /api/v1/tasks/<id>)
    Get {
        id: String,
        #[arg(long)]
        api: Option<String>,
        #[arg(long)]
        token: Option<String>,
    },
    /// Отменить задачу (POST /api/v1/tasks/<id>/cancel)
    Cancel {
        id: String,
        #[arg(long, default_value = "canceled via CLI")]
        reason: String,
        #[arg(long)]
        api: Option<String>,
        #[arg(long)]
        token: Option<String>,
    },
    /// Повторно инжектировать failed/timed-out задачу
    Resubmit {
        id: String,
        #[arg(long)]
        api: Option<String>,
        #[arg(long)]
        token: Option<String>,
    },
    /// Остановить узел (POST /api/v1/shutdown)
    Leave {
        #[arg(long)]
        api: Option<String>,
        #[arg(long)]
        token: Option<String>,
    },
}

fn main() {
    // `zeptomesh-node -c x.yaml` без подкоманды трактуется как `run`
    // (Go: ведущий флаг ⇒ вызов run с теми же аргументами).
    let cli = match Cli::try_parse() {
        Ok(c) => c,
        Err(e) => {
            if e.kind() == clap::error::ErrorKind::InvalidSubcommand
                || e.kind() == clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            {
                let mut argv: Vec<String> = std::env::args().collect();
                argv.insert(1, "run".to_string());
                Cli::try_parse_from(&argv).unwrap_or_else(|e2| e2.exit())
            } else {
                e.exit();
            }
        }
    };
    let cmd = match cli.command {
        Some(c) => c,
        None => {
            eprintln!("zeptomesh-node: no command given\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    let code = match dispatch(cmd) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("zeptomesh-node: {e:#}");
            1
        }
    };
    std::process::exit(code);
}

fn dispatch(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Run { config } => run_node(config),
        Cmd::Version => {
            println!("zeptomesh-node {}", version::version());
            Ok(())
        }
        Cmd::Genkey { out, force, print_id } => gen_key(&out, force, print_id),
        Cmd::Status { api, token } => {
            let v = client_get(&api, &token, "/api/v1/status")?;
            print_json(&v)
        }
        Cmd::Peers { api, token } => {
            let v = client_get(&api, &token, "/api/v1/peers")?;
            print_peers(&v)
        }
        Cmd::Tasks { limit, status, api, token } => {
            let path = format!("/api/v1/tasks?limit={}&status={}", limit, urlenc(&status));
            let v = client_get(&api, &token, &path)?;
            print_tasks(&v)
        }
        Cmd::Get { id, api, token } => {
            let v = client_get(&api, &token, &format!("/api/v1/tasks/{}", urlenc(&id)))?;
            print_json(&v)
        }
        Cmd::Cancel { id, reason, api, token } => {
            let path = format!(
                "/api/v1/tasks/{}/cancel?reason={}",
                urlenc(&id),
                urlenc(&reason)
            );
            let v = client_post(&api, &token, &path, Value::Null)?;
            print_json(&v)
        }
        Cmd::Resubmit { id, api, token } => {
            let path = format!("/api/v1/tasks/{}/resubmit", urlenc(&id));
            let v = client_post(&api, &token, &path, Value::Null)?;
            print_json(&v)
        }
        Cmd::Leave { api, token } => {
            let v = client_post(&api, &token, "/api/v1/shutdown", Value::Null)?;
            print_json(&v)
        }
        Cmd::Submit {
            instruction,
            skill,
            ttl,
            priority,
            timeout,
            shell,
            network,
            label,
            wait,
            wait_seconds,
            api,
            token,
        } => {
            let required_skills: Vec<String> = skill
                .iter()
                .flat_map(|s| s.split(','))
                .map(|p| p.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let mut labels = BTreeMap::new();
            for l in &label {
                match l.split_once('=') {
                    Some((k, v)) => {
                        labels.insert(k.to_string(), v.to_string());
                    }
                    None => bail!("bad --label {l:?} (want key=value)"),
                }
            }
            let mut body = serde_json::json!({
                "instruction": instruction,
                "required_skills": required_skills,
                "allow_shell": shell,
                "allow_network_tools": network,
                "labels": labels,
                "wait": wait,
                "wait_seconds": wait_seconds,
            });
            if ttl != 0 {
                body["ttl"] = serde_json::json!(ttl);
            }
            if priority >= 0 {
                body["priority"] = serde_json::json!(priority);
            }
            if timeout != 0 {
                body["timeout_seconds"] = serde_json::json!(timeout);
            }
            let v = client_post(&api, &token, "/api/v1/tasks", body)?;
            print_json(&v)
        }
    }
}

// ---------- run ----------

fn init_logging(level: &str, structured: bool) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(if level.is_empty() { "info" } else { level }));
    // structured_logs=true в Go включает JSON-формат; в порте подписчик собран
    // без feature "json" — используем компактный текстовый формат для обоих.
    let _ = structured;
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn run_node(cfg_path: Option<PathBuf>) -> Result<()> {
    let cfg_path = cfg_path.context("-config is required (or set ZEPTOMESH_CONFIG)")?;
    let p = cfg_path.to_str().unwrap_or("").to_string();
    let cfg = Config::load(&p).with_context(|| format!("load config {}", cfg_path.display()))?;
    init_logging(&cfg.telemetry.log_level, cfg.telemetry.structured_logs);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(run_node_async(cfg))
}

async fn run_node_async(cfg: Config) -> Result<()> {
    let cfg = Arc::new(cfg);
    let node = Node::new((*cfg).clone()).await?;
    node.start().await?;

    if cfg.api.enabled {
        let n = node.clone();
        let c = cfg.clone();
        tokio::spawn(async move {
            if let Err(e) = api::serve(n, c).await {
                tracing::error!(error = %e, "admin_api");
            }
        });
    }

    tracing::info!("zeptomesh_node_running");

    // SIGINT/SIGTERM — остановка; SIGHUP — рестарт под супервизором
    // (Go: signal.NotifyContext + exit code 75 после /admin/leave).
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;

    let mut shutdown = node.shutdown_token();
    let restart = loop {
        tokio::select! {
            _ = sigint.recv() => { tracing::info!("shutdown_signal_received"); break false; }
            _ = sigterm.recv() => { tracing::info!("shutdown_signal_received"); break false; }
            _ = sighup.recv() => {
                // Go: SIGHUP перечитывает тот же файл, что и admin-эндпоинт.
                // Hot-reload значений конфига в порте не реализован —
                // просим супервизор перезапустить процесс.
                tracing::warn!("sighup_received_restart_required");
                break true;
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow_and_update() {
                    // leave/shutdown запрошен через API: чистый рестарт.
                    tracing::info!("leave_requested_restarting_process");
                    break true;
                }
            }
        }
    };

    // Graceful stop с таймаутом 15с (Go: context.WithTimeout в stopNode).
    let stop = async { node.shutdown().await };
    if tokio::time::timeout(std::time::Duration::from_secs(15), stop)
        .await
        .is_err()
    {
        tracing::error!("node_stop_timed_out");
    }
    if restart {
        std::process::exit(EX_RESTART);
    }
    Ok(())
}

// ---------- genkey ----------

fn gen_key(out: &PathBuf, force: bool, print_id: bool) -> Result<()> {
    if out.exists() && !force {
        bail!("{} exists (use --force to replace)", out.display());
    }
    let id = Identity::load_or_generate(out)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(out, std::fs::Permissions::from_mode(0o600))?;
        if let Some(dir) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
        }
    }
    if print_id {
        println!("{}", id.peer_id());
    }
    Ok(())
}

// ---------- client commands ----------

fn api_base(flag: &Option<String>) -> String {
    if let Some(v) = flag {
        return v.trim_end_matches('/').to_string();
    }
    if let Ok(v) = std::env::var("ZEPTOMESH_API") {
        if !v.is_empty() {
            return v.trim_end_matches('/').to_string();
        }
    }
    "http://127.0.0.1:7789".to_string()
}

fn api_token(flag: &Option<String>) -> String {
    if let Some(v) = flag {
        return v.clone();
    }
    std::env::var("ZEPTOMESH_API_TOKEN").unwrap_or_default()
}

fn urlenc(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn print_json(v: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

fn print_peers(v: &Value) -> Result<()> {
    let arr = v.as_array().cloned().unwrap_or_default();
    if arr.is_empty() {
        println!("no known peers");
        return Ok(());
    }
    println!("{:<12} {:<10} {:>9} {:>7} {:>8}  {}", "PEER", "TRUST", "CONNECTED", "LOAD", "RTT_MS", "SKILLS");
    for p in arr {
        let pid = short_id(p["peer_id"].as_str().unwrap_or("?"));
        let trust = p["trust"].as_str().unwrap_or("?");
        let conn = if p["connected"].as_bool().unwrap_or(false) { "yes" } else { "no" };
        let load = p["load"].as_f64().map(|f| format!("{:.2}", f)).unwrap_or_default();
        let rtt = p["rtt_ms"].as_i64().map(|i| i.to_string()).unwrap_or_else(|| "-".into());
        let skills: Vec<&str> = p["skills"]
            .as_array()
            .map(|a| a.iter().filter_map(|s| s.as_str()).collect())
            .unwrap_or_default();
        println!("{pid:<12} {trust:<10} {conn:>9} {load:>7} {rtt:>8}  {}", skills.join(","));
    }
    Ok(())
}

fn print_tasks(v: &Value) -> Result<()> {
    let arr = v["tasks"].as_array().cloned().unwrap_or_default();
    if arr.is_empty() {
        println!("no tasks");
        return Ok(());
    }
    println!("{:<12} {:<18} {:>8}  {}", "TASK", "STATUS", "ATTEMPTS", "INSTRUCTION");
    for t in arr {
        let id = short_id(t["task_id"].as_str().unwrap_or("?"));
        let st = t["status"].as_str().unwrap_or("?");
        let at = t["attempts"].as_i64().map(|i| i.to_string()).unwrap_or_else(|| "-".into());
        let instr = truncate(t["instruction"].as_str().unwrap_or(""), 60);
        println!("{id:<12} {st:<18} {at:>8}  {instr}");
    }
    Ok(())
}

fn short_id(s: &str) -> String {
    if s.chars().count() <= 10 {
        s.to_string()
    } else {
        let head: String = s.chars().take(10).collect();
        format!("{head}…")
    }
}

fn truncate(s: &str, max: usize) -> String {
    let one = s.lines().next().unwrap_or("").trim();
    if one.chars().count() <= max {
        one.to_string()
    } else {
        let cut: String = one.chars().take(max - 1).collect();
        format!("{cut}…")
    }
}

/// Блокирующий клиент admin-API поверх текущего tokio-рантайма (Go: net/http).
fn client_request(
    api: &Option<String>,
    token: &Option<String>,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> Result<Value> {
    let base = api_base(api);
    let url = format!("{base}{path}");
    let token = api_token(token);
    // Клиентские команды выполняются вне рантайма `run` — поднимаем свой.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let text = rt.block_on(async move {
        let mut req = reqwest::Client::new()
            .request(method, &url)
            .timeout(std::time::Duration::from_secs(300));
        if !token.is_empty() {
            req = req.bearer_auth(token);
        }
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("cannot reach local node API at {base} (is the node running?)"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            let detail = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| v["error"].as_str().map(|s| s.to_string()))
                .unwrap_or(text);
            bail!("HTTP {status}: {detail}");
        }
        Result::<String, anyhow::Error>::Ok(text)
    })?;
    if text.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str::<Value>(&text).with_context(|| format!("bad JSON response: {text}"))
}

fn client_get(api: &Option<String>, token: &Option<String>, path: &str) -> Result<Value> {
    client_request(api, token, reqwest::Method::GET, path, None)
}

fn client_post(api: &Option<String>, token: &Option<String>, path: &str, body: Value) -> Result<Value> {
    client_request(api, token, reqwest::Method::POST, path, Some(body))
}
