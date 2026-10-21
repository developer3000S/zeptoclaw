//! zeptomesh-ui — ZeptoClaw Mesh Dashboard (BFF + SPA).
//!
//!     zeptomesh-ui            # запуск дашборда (всё из env)
//!     zeptomesh-ui --listen 0.0.0.0:28090
//!
//! Конфигурация — через переменные окружения:
//!
//! | Переменная                     | По умолчанию          |
//! |--------------------------------|----------------------|
//! | `ZEPTOMESH_UI_LISTEN`          | `127.0.0.1:28090`     |
//! | `ZEPTOMESH_UI_DATA`            | `./zeptomesh-ui-data` |
//! | `ZEPTOMESH_UI_NODES_FILE`      | `<data>/nodes.json`   |
//! | `ZEPTOMESH_UI_NODES`           | пусто (стартовый seed)|
//! | `ZEPTOMESH_UI_TOKEN`           | пусто (без авторизации)|
//! | `ZEPTOMESH_UI_API_TOKEN`       | пусто                |
//! | `ZEPTOMESH_UI_POLL_INTERVAL`   | `5` (сек)            |
//! | `ZEPTOMESH_UI_DISCOVERY_PORTS` | `8081-8085,14001-14010` |

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use zeptomesh::ui;

const USAGE: &str = "zeptomesh-ui — дашборд ZeptoClaw Agent Mesh (BFF + SPA)";

#[derive(Parser)]
#[command(name = "zeptomesh-ui", version = zeptomesh::version::VERSION, about = USAGE)]
struct Cli {
    /// Адрес прослушивания (перекрывает ZEPTOMESH_UI_LISTEN).
    #[arg(long, env = "ZEPTOMESH_UI_LISTEN")]
    listen: Option<String>,
    /// Каталог данных (перекрывает ZEPTOMESH_UI_DATA).
    #[arg(long, env = "ZEPTOMESH_UI_DATA")]
    data_dir: Option<String>,
    /// Файл реестра нод (перекрывает ZEPTOMESH_UI_NODES_FILE).
    #[arg(long, env = "ZEPTOMESH_UI_NODES_FILE")]
    nodes_file: Option<String>,
    /// Токен доступа к дашборду (перекрывает ZEPTOMESH_UI_TOKEN).
    #[arg(long, env = "ZEPTOMESH_UI_TOKEN")]
    token: Option<String>,
    /// Bearer-токен для нод без собственного (перекрывает ZEPTOMESH_UI_API_TOKEN).
    #[arg(long, env = "ZEPTOMESH_UI_API_TOKEN")]
    api_token: Option<String>,
    /// Интервал опроса нод, сек (перекрывает ZEPTOMESH_UI_POLL_INTERVAL).
    #[arg(long, env = "ZEPTOMESH_UI_POLL_INTERVAL")]
    poll_interval: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let log_level = std::env::var("ZEPTOMESH_UI_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&log_level).unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .init();

    let mut cfg = ui::UiConfig::from_env()?;
    if let Some(v) = cli.listen {
        cfg.listen = v;
    }
    if let Some(v) = cli.data_dir {
        cfg.data_dir = v.into();
    }
    if let Some(v) = cli.nodes_file {
        cfg.nodes_file = v.into();
    }
    if let Some(v) = cli.token {
        cfg.token = v;
    }
    if let Some(v) = cli.api_token {
        cfg.api_token = v;
    }
    if let Some(v) = cli.poll_interval {
        cfg.poll_interval = v;
    }

    ui::serve(cfg).await.context("запуск дашборда")
}
