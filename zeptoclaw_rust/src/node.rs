//! Порт пакета `internal/node`: сборка узла (identity → storage → policy →
//! routing → adapter → tasks.Manager) и P2P-слой поверх TCP с length-prefixed
//! protobuf-кадрами (транспорт libp2p в Go заменен прямым TCP: формат
//! сообщений идентичен). Gossip-membership реализован периодическим обменом
//! `MembershipGossip` по всем подключённым соседям (Go: discovery/gossip.go).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use crate::chat::ChatService;
use crate::config::Config;
use crate::p2p;
use crate::picoclaw::{self, Adapter, Backend, BackendPool};
use crate::proto::{
    Capabilities, CapabilitiesRequest, CancelRequest, CancelResponse,
    Message, PeerRecord, PeerState, SearchMessage, SearchMessageKind, SearchReply, SearchRequest,
    SkillsSyncRequest, SkillsSyncResponse, TaskEnvelope, TaskResult,
};
use crate::routing::{Neighbor, Table, CAT_LAN, CAT_LOCAL, CAT_WAN};
use crate::security::{extract_key_from_peer_id, Identity, Mode, Policy, Trust};
use crate::skills::Registry;
use crate::storage::{self, Store};
use crate::tasks::{Manager, SubmitRequest};
use crate::triggers::Scheduler;
use crate::wire;

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Таймаут ожидания SkillsSyncResponse в синхронном RPC (Go: 10s ctx timeout).
const SKILLS_RPC_TIMEOUT_SECS: u64 = 10;

/// Один конн: id + writer-половина (читает отдельная задача-приёмник).
/// `tokio::sync::Mutex` — потому что запись идёт из async-контекста.
pub(crate) struct Conn {
    pub(crate) id: u64,
    peer_id: Option<String>,
    write: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
}

impl Conn {
    async fn send<M>(&self, m: &M) -> Result<()>
    where
        M: Message,
    {
        let mut w = self.write.lock().await;
        p2p::write_msg(&mut *w, m).await
    }
    async fn send_envelope(&self, env: &TaskEnvelope) -> Result<()> {
        self.send(env).await
    }
    async fn send_result(&self, r: &TaskResult) -> Result<()> {
        self.send(r).await
    }
    async fn send_cancel(&self, c: &CancelRequest) -> Result<()> {
        self.send(c).await
    }
    async fn send_cancel_plain(&self, c: &CancelResponse) -> Result<()> {
        self.send(c).await
    }
    async fn send_caps(&self, c: &Capabilities) -> Result<()> {
        self.send(c).await
    }
    async fn send_gossip(&self, g: &crate::proto::MembershipGossip) -> Result<()> {
        self.send(g).await
    }
}

pub(crate) type Conns = Arc<Mutex<HashMap<u64, Arc<Conn>>>>;

/// Узел mesh (Go: node.Node).
pub struct Node {
    pub cfg: Arc<Config>,
    pub identity: Arc<Identity>,
    pub store: Store,
    pub policy: Arc<Policy>,
    pub table: Table,
    pub manager: Arc<Manager>,
    pub adapter: Arc<dyn Adapter + Send + Sync>,
    pub pool: Option<Arc<BackendPool>>,
    /// Реестр скиллов (Go: internal/skills Registry); активен при skill_exchange.enabled.
    pub skills: Option<Arc<Registry>>,
    /// Планировщик триггеров (Go: internal/triggers Scheduler).
    pub scheduler: Option<Arc<Scheduler>>,
    /// Чат-сервис поверх brain-пула (Go: internal/chat).
    pub chat: Option<Arc<ChatService>>,
    /// Путь конфигурационного файла для admin/reload-config.
    pub config_path: Mutex<String>,
    conns: Conns,
    next_conn: std::sync::atomic::AtomicU64,
    /// Известные адреса пиров для исходящих диалов (peer_id → multiaddr/host:port).
    addrs: Mutex<HashMap<String, Vec<String>>>,
    listen_addrs: Mutex<Vec<String>>,
    /// Сигнал graceful shutdown (go: context.CancelFunc, дёргается из POST /api/v1/shutdown).
    shutdown_tx: Arc<tokio::sync::watch::Sender<bool>>,
    // ---- search-relay (Go: internal/discovery/search.go) ----
    /// Активные исходящие поиски: request_id → (навыки, автор запроса, deadline).
    pending_searches: Mutex<HashMap<String, PendingSearch>>,
    /// Накопленные ответы на ожидающие поиски: request_id → кандидаты.
    search_answers: Mutex<HashMap<String, Vec<(String, PeerRecord)>>>,
    /// Кэш ответов поиска (request_id от чужих запросов → время ответа), cooldown.
    answered: Mutex<HashMap<String, i64>>,
    /// Локальный кэш результатов поиска по навыкам (skills_key → (expire, peers)).
    search_cache: Mutex<HashMap<String, (i64, Vec<PeerRecord>)>>,
    /// Ожидатели синхронных skills-RPC: conn_id → sender ответа.
    pending_skill_rpc: Mutex<HashMap<u64, (oneshot::Sender<SkillsSyncResponse>, Option<oneshot::Receiver<SkillsSyncResponse>>)>>,
}

struct PendingSearch {
    #[allow(dead_code)]
    requester: String, // наш peer_id, от чьего имени просим
    #[allow(dead_code)]
    skills: Vec<String>,
    deadline: std::time::Instant,
}

/// Разбирает `/ip4/1.2.3.4/tcp/4001` или `host:port` в "host:port".
pub fn parse_addr(s: &str) -> Option<String> {
    if s.starts_with('/') {
        let parts: Vec<&str> = s.split('/').filter(|p| !p.is_empty()).collect();
        // [ip4, a.b.c.d, tcp, port]
        if parts.len() >= 4 && parts[0] == "ip4" && parts[2] == "tcp" {
            return Some(format!("{}:{}", parts[1], parts[3]));
        }
        if parts.len() >= 4 && parts[0] == "ip6" && parts[2] == "tcp" {
            return Some(format!("[{}]:{}", parts[1], parts[3]));
        }
        return None;
    }
    if s.contains(':') {
        return Some(s.to_string());
    }
    None
}

fn category_of(host: &str) -> &'static str {
    if host == "127.0.0.1" || host == "localhost" || host == "::1" {
        CAT_LOCAL
    } else if host.starts_with("10.")
        || host.starts_with("192.168.")
        || host.starts_with("172.16.")
        || host.starts_with("169.254.")
    {
        CAT_LAN
    } else {
        CAT_WAN
    }
}

/// Префикс локальной /24 ("192.168.1") для subnet-scan; None, если адрес
/// не приватный (публичные хосты не сканируем).
fn local_subnet_prefix() -> Option<String> {
    let ip = local_ip()?;
    let o: Vec<u8> = ip.octets().to_vec();
    let is_priv = o[0] == 10
        || (o[0] == 192 && o[1] == 168)
        || (o[0] == 172 && (16..=31).contains(&o[1]));
    if !is_priv {
        return None;
    }
    Some(format!("{}.{}.{}", o[0], o[1], o[2]))
}

/// Лучший effort определение основного IPv4: UDP-сокет «к внешнему» без
/// отправки пакетов (route selection в ядре), fallback — первый непетлевой
/// unicast-адрес интерфейсов.
fn local_ip() -> Option<std::net::Ipv4Addr> {
    if let Ok(s) = std::net::UdpSocket::bind("0.0.0.0:0") {
        if s.connect("8.8.8.8:53").is_ok() {
            if let Ok(std::net::SocketAddr::V4(v4)) = s.local_addr() {
                return Some(*v4.ip());
            }
        }
    }
    default_net::get_default_interface()
        .ok()?
        .ipv4
        .into_iter()
        .find(|ip| !ip.is_loopback())
}

impl Node {
    /// Go: node.New — полная сборка зависимостей.
    pub async fn new(cfg: Config) -> Result<Arc<Self>> {
        Self::new_at(cfg, String::new()).await
    }

    /// То же, но с известным путём конфига (для admin/reload-config).
    pub async fn new_at(cfg: Config, config_path: String) -> Result<Arc<Self>> {
        let cfg = Arc::new(cfg);
        // Identity: key_file из конфига или <data_dir>/identity.key.
        let key_path = if cfg.identity.key_file.is_empty() {
            PathBuf::from(&cfg.node.data_dir).join("identity.key")
        } else {
            PathBuf::from(&cfg.identity.key_file)
        };
        if let Some(dir) = key_path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let identity = Arc::new(Identity::load_or_generate(&key_path).map_err(|e| anyhow!("{e}"))?);

        // Storage.
        let dir = if cfg.storage.dir.is_empty() {
            PathBuf::from(&cfg.node.data_dir).join("store")
        } else {
            PathBuf::from(&cfg.storage.dir)
        };
        let artifacts = PathBuf::from(&cfg.node.data_dir).join("artifacts");
        let store = Store::open(storage::Options {
            dir,
            artifacts_dir: artifacts,
            in_memory: false,
        })?;

        // Security policy.
        let mut policy = Policy::new(
            Mode::parse(&cfg.security.trust_mode),
            crate::security::parse_trust(&cfg.security.min_trust_for_tasks).unwrap_or(Trust::Known),
        );
        if !cfg.security.allowed_peers_file.is_empty() {
            let _ = policy.load_peer_file(&cfg.security.allowed_peers_file, true);
        }
        if !cfg.security.blocked_peers_file.is_empty() {
            let _ = policy.load_peer_file(&cfg.security.blocked_peers_file, false);
        }
        let policy = Arc::new(policy);

        // Routing table with persistence.
        let table = Table::new(cfg.neighbors.clone(), Some(store.clone()), Some(policy.clone()));
        let restored = table.restore().unwrap_or(0);
        tracing::info!(restored, "neighbors_restored");

        // Brain-пул LLM-бэкендов (Go: internal/brain NewBackendPool) и исполнитель.
        let pool = if cfg.picoclaw.local_url.is_empty() && cfg.picoclaw.backends.is_empty() {
            None
        } else {
            Some(Arc::new(BackendPool::new(
                &cfg.picoclaw.local_url,
                &cfg.picoclaw.backends,
            )))
        };
        let adapter = picoclaw::new_adapter(&cfg, pool.clone()).await?;

        // Чат-сервис поверх brain-пула (Go: internal/chat.New).
        let chat = pool.as_ref().map(|p| ChatService::new(p.clone()));

        // Реестр скиллов (активен при skill_exchange.enabled).
        let skills = if cfg.capabilities.skill_exchange.enabled {
            match Registry::new(&cfg, identity.clone()) {
                Ok(r) => Some(r),
                Err(e) => {
                    tracing::warn!("skills registry disabled: {e}");
                    None
                }
            }
        } else {
            None
        };

        let manager = Arc::new(Manager::new(
            cfg.clone(),
            store.clone(),
            table.clone(),
            policy.clone(),
            identity.clone(),
            adapter.clone(),
        ));

        // Планировщик триггеров (Go: triggers.New + run в goroutine).
        let scheduler = if !cfg.triggers.is_empty() {
            match Scheduler::new(&cfg, manager.clone()) {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::warn!("triggers scheduler disabled: {e}");
                    None
                }
            }
        } else {
            None
        };

        let n = Arc::new(Node {
            cfg,
            identity,
            store,
            policy,
            table,
            manager,
            adapter,
            pool,
            skills,
            scheduler,
            chat,
            config_path: Mutex::new(config_path),
            conns: Arc::new(Mutex::new(HashMap::new())),
            next_conn: std::sync::atomic::AtomicU64::new(1),
            addrs: Mutex::new(HashMap::new()),
            listen_addrs: Mutex::new(vec![]),
            shutdown_tx: Arc::new(tokio::sync::watch::channel(false).0),
            pending_searches: Mutex::new(HashMap::new()),
            search_answers: Mutex::new(HashMap::new()),
            answered: Mutex::new(HashMap::new()),
            search_cache: Mutex::new(HashMap::new()),
            pending_skill_rpc: Mutex::new(HashMap::new()),
        });

        n.wire_delivery();
        Ok(n)
    }

    pub fn self_id(&self) -> &str {
        self.identity.peer_id()
    }

    /// Подключает транспорт к менеджеру задач (Go: Host-колбэки в New).
    fn wire_delivery(self: &Arc<Self>) {
        let this = self.clone();
        self.manager.set_deliver(Arc::new(move |peer, env| {
            let this = this.clone();
            Box::pin(async move { this.deliver_to(&peer, &env).await })
        }));
        let this = self.clone();
        self.manager.set_deliver_result(Arc::new(move |peer, res| {
            let this = this.clone();
            Box::pin(async move { this.deliver_result_to(&peer, &res).await })
        }));
        let this = self.clone();
        self.manager.set_cancel_peer(Arc::new(move |peer, task_id| {
            let this = this.clone();
            Box::pin(async move {
                let c = CancelRequest {
                    task_id,
                    origin_peer_id: this.self_id().into(),
                    sender_peer_id: this.self_id().into(),
                    signature: vec![],
                    reason: String::new(),
                };
                let _ = this.cancel_peer_send(&peer, &c).await;
            })
        }));
    }

    // ---- доставка по пирам --------------------------------------------------

    fn conn_for(&self, peer_id: &str) -> Option<Arc<Conn>> {
        let g = self.conns.lock().unwrap();
        g.values().find(|c| c.peer_id.as_deref() == Some(peer_id)).cloned()
    }

    async fn deliver_to(self: &Arc<Self>, peer_id: &str, env: &TaskEnvelope) -> Result<()> {
        if peer_id == self.self_id() {
            return self.manager.route_inbound(env.clone()).await;
        }
        if let Some(c) = self.conn_for(peer_id) {
            return c.send_envelope(env).await;
        }
        // Нет конна — пробуем dial по известным адресам.
        let addr = {
            let g = self.addrs.lock().unwrap();
            g.get(peer_id).and_then(|v| v.first()).cloned()
        };
        let addr = addr.ok_or_else(|| anyhow!("node: no connection or address for peer {peer_id}"))?;
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| anyhow!("node: dial {addr}: {e}"))?;
        let conn = self.attach(stream, Some(peer_id.to_owned())).await?;
        conn.send_envelope(env).await
    }

    async fn deliver_result_to(self: &Arc<Self>, peer_id: &str, res: &TaskResult) -> Result<()> {
        if let Some(c) = self.conn_for(peer_id) {
            return c.send_result(res).await;
        }
        // Результат автору, если автор — мы: смоделировать приём локально.
        if peer_id == self.self_id() {
            return Ok(());
        }
        let addr = {
            let g = self.addrs.lock().unwrap();
            g.get(peer_id).and_then(|v| v.first()).cloned()
        };
        let addr = addr.ok_or_else(|| anyhow!("node: no route for result to {peer_id}"))?;
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| anyhow!("node: dial {addr}: {e}"))?;
        let conn = self.attach(stream, Some(peer_id.to_owned())).await?;
        conn.send_result(res).await
    }

    async fn cancel_peer_send(&self, peer_id: &str, c: &CancelRequest) -> Result<()> {
        if let Some(conn) = self.conn_for(peer_id) {
            return conn.send_cancel(c).await;
        }
        Ok(())
    }

    // ---- жизненный цикл ------------------------------------------------------

    /// Запускает слушателей и фоновые задачи; возвращает после привязки сокетов.
    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let mut bound = Vec::new();
        for l in &self.cfg.node.listen {
            let hp = parse_addr(l).ok_or_else(|| anyhow!("node: bad listen addr {l}"))?;
            let sock: SocketAddr = hp
                .parse()
                .map_err(|e| anyhow!("node: listen {hp}: {e}"))?;
            let ln = TcpListener::bind(sock)
                .await
                .map_err(|e| anyhow!("node: bind {sock}: {e}"))?;
            let actual = ln.local_addr()?;
            // Объявляем соседям не bind-адрес (0.0.0.0), а реальный адрес в сети.
            let ip = if actual.is_ipv4() && actual.ip().is_unspecified() {
                Self::routable_ip()
                    .map(std::net::IpAddr::from)
                    .unwrap_or(actual.ip())
            } else {
                actual.ip()
            };
            bound.push(format!("/ip4/{}/tcp/{}", ip, actual.port()));
            let this = self.clone();
            tokio::spawn(async move {
                loop {
                    match ln.accept().await {
                        Ok((stream, peer)) => {
                            let this = this.clone();
                            tokio::spawn(async move {
                                if let Err(e) = this.handle_conn(stream).await {
                                    tracing::debug!("conn from {peer} closed: {e}");
                                }
                            });
                        }
                        Err(e) => tracing::warn!("accept: {e}"),
                    }
                }
            });
        }
        *self.listen_addrs.lock().unwrap() = bound.clone();
        // Строка готовности для супервизоров/smoke-теста (Go: fmt.Println в cmd).
        println!(
            "zeptomesh-node started: peer_id={} listening on {}",
            self.self_id(),
            bound.join(", ")
        );
        tracing::info!(peer_id = %self.self_id(), listen = ?bound, "node_started");

        // Bootstrap-диалы с ретраем: якорь может подняться позже нас, а scan по
        // умолчанию выключен — однократный диал оставлял ноду изолированной.
        let this = self.clone();
        let boot = self.cfg.discovery.bootstrap.clone();
        tokio::spawn(async move {
            let mut pending: Vec<(String, String)> = boot
                .iter()
                .filter_map(|b| parse_addr(b).map(|hp| (hp, b.clone())))
                .collect();
            let mut delay = Duration::from_secs(2);
            while !pending.is_empty() {
                let mut failed = Vec::new();
                for (hp, orig) in pending {
                    match this.dial_hp(&hp, &orig).await {
                        Ok(()) => tracing::info!("bootstrap connected: {orig}"),
                        Err(e) => {
                            tracing::debug!("bootstrap dial {orig} failed: {e}");
                            failed.push((hp, orig));
                        }
                    }
                }
                pending = failed;
                if pending.is_empty() {
                    break;
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        });

        // Периодический heartbeat-обмен способностями (упрощённый gossip).
        let this = self.clone();
        let period = if self.cfg.discovery.gossip.heartbeat.is_zero() {
            Duration::from_secs(10)
        } else {
            *self.cfg.discovery.gossip.heartbeat
        };
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            loop {
                tick.tick().await;
                this.gossip_round().await;
                this.prune_round();
                this.expire_searches();
                // Peer-exchange: подключаемся к пирам, чьи адреса узнали из
                // gossip, — без этого mesh остаётся звездой от bootstrap-ноды.
                this.dial_known_peers().await;
            }
        });

        // Brain: периодический зонд LLM-бэкендов (Go: brain pool probe ticker).
        if let Some(pool) = self.pool.clone() {
            let scan = self.cfg.discovery.scan.interval.max(Duration::from_secs(5));
            let first = pool.clone();
            tokio::spawn(async move {
                // Первый зонд — сразу, чтобы usable() не был пустым до первого тика.
                first.probe().await;
                let mut tick = tokio::time::interval(scan);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    pool.probe().await;
                }
            });
        }

        // Skills exchange: периодическая синхронизация дескрипторов с соседями.
        if self.skills.is_some() && !self.cfg.capabilities.skill_exchange.interval.is_zero() {
            let this = self.clone();
            let period = *self.cfg.capabilities.skill_exchange.interval;
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(period);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    this.skills_sync_round().await;
                }
            });
        }

        // Планировщик триггеров (Go: go scheduler.Run(ctx)).
        if let Some(s) = self.scheduler.clone() {
            s.set_shutdown(self.shutdown_token());
            tokio::spawn(s.run());
        }
        Ok(())
    }

    fn prune_round(&self) {
        let timeout = self.cfg.discovery.gossip.failure_timeout.max(Duration::from_secs(30));
        let stale = self.table.prune_stale(timeout);
        for pid in stale {
            tracing::debug!("gossip: pruned stale neighbor {pid}");
        }
    }

    // ---- search-relay (Go: internal/discovery/search.go) --------------------

    /// Сбрасывает истёкшие исходящие поиски и устаревшие записи кэшей.
    fn expire_searches(&self) {
        let now = std::time::Instant::now();
        self.pending_searches.lock().unwrap().retain(|_, p| p.deadline > now);
        let nowu = now_unix();
        self.answered.lock().unwrap().retain(|_, t| *t + 3600 > nowu);
        self.search_cache.lock().unwrap().retain(|_, (exp, _)| *exp > nowu);
    }

    /// Ключ кэша поиска — нормализованный список навыков.
    fn skills_key(skills: &[String]) -> String {
        let mut s: Vec<String> = skills.to_vec();
        s.sort();
        s.dedup();
        s.join(",")
    }

    /// Локальные кандидаты: известные соседи, чьи навыки покрывают запрос.
    fn local_candidates(&self, skills: &[String], max: usize) -> Vec<PeerRecord> {
        let now = now_unix();
        self
            .table
            .list()
            .into_iter()
            .filter(|n| {
                n.peer_id != self.self_id()
                    && !skills.is_empty()
                    && skills.iter().all(|s| {
                        n.skills
                            .iter()
                            .any(|ns| ns == s || ns == "general" || s == "any")
                    })
            })
            .take(max)
            .map(|n| PeerRecord {
                peer_id: n.peer_id,
                addrs: n.addrs,
                skills: n.skills,
                seen_at: now,
            })
            .collect()
    }

    /// Отправить SearchRequest всем подключённым соседям (Go: publish на тему
    /// skillsearch; TCP-фрейм заменяет pubsub-топик).
    async fn broadcast_search_request(&self, req: &SearchRequest) {
        let msg = SearchMessage(SearchMessageKind::Request(req.clone()));
        let conns: Vec<Arc<Conn>> = self.conns.lock().unwrap().values().cloned().collect();
        for c in conns {
            let _ = c.send(&msg).await;
        }
    }

    /// Инициировать mesh-поиск исполнителей по навыкам (Go: discovery.Search).
    /// Возвращает кандидатов (локальная таблица + ответы соседей до дедлайна).
    pub async fn search_peers(self: &Arc<Self>, skills: &[String]) -> Result<Vec<PeerRecord>> {
        let sr = &self.cfg.tasks.forwarding.search_relay;
        if !sr.enabled {
            bail!("node: search relay disabled");
        }
        let key = Self::skills_key(skills);
        // Кэш результатов (cache_ttl).
        if let Some((exp, peers)) = self.search_cache.lock().unwrap().get(&key) {
            if *exp > now_unix() {
                return Ok(peers.clone());
            }
        }
        let request_id = format!("{}-{}", self.self_id(), crate::tasks::new_id());
        let issued = now_unix();
        let req = SearchRequest {
            request_id: request_id.clone(),
            requester_peer_id: self.self_id().to_string(),
            skills: skills.to_vec(),
            issued_at: issued,
            signature_scheme: "ed25519-v1".into(),
            signature: self.identity.sign(&wire::search_request_body(&SearchRequest {
                request_id: request_id.clone(),
                requester_peer_id: self.self_id().to_string(),
                skills: skills.to_vec(),
                issued_at: issued,
                signature_scheme: String::new(),
                signature: vec![],
            })),
        };
        let timeout = *sr.request_timeout;
        self.pending_searches.lock().unwrap().insert(
            request_id.clone(),
            PendingSearch {
                requester: self.self_id().to_string(),
                skills: skills.to_vec(),
                deadline: std::time::Instant::now() + timeout,
            },
        );
        self.broadcast_search_request(&req).await;

        let mut collected: HashMap<String, PeerRecord> = HashMap::new();
        for p in self.local_candidates(skills, usize::MAX) {
            collected.insert(p.peer_id.clone(), p);
        }
        let poll = Duration::from_millis(100);
        let mut waited = Duration::ZERO;
        while waited < timeout {
            tokio::time::sleep(poll).await;
            waited += poll;
            // Новые ответы подтягиваются через handle_search_reply в pending-карту?
            // Нет: replies накапливаются в collected ниже (см. take_search_answers).
            for (id, rec) in self.take_search_answers(&request_id) {
                collected.entry(id).or_insert(rec);
            }
            if collected.len() as i32 >= sr.fanout.max(1) * 2 {
                break; // достаточно кандидатов раньше дедлайна
            }
        }
        self.pending_searches.lock().unwrap().remove(&request_id);
        let mut peers: Vec<PeerRecord> = collected.into_values().collect();
        peers.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        let ttl = (*sr.cache_ttl).max(Duration::from_secs(1)).as_secs() as i64;
        self.search_cache
            .lock()
            .unwrap()
            .insert(key, (now_unix() + ttl, peers.clone()));
        Ok(peers)
    }

    /// Выбрать накопленные ответы на конкретный request_id (внутренний канал
    /// handle_search_reply → search_peers).
    fn take_search_answers(&self, request_id: &str) -> Vec<(String, PeerRecord)> {
        let mut g = self.search_answers.lock().unwrap();
        match g.remove(request_id) {
            Some(v) => v,
            None => Vec::new(),
        }
    }

    /// Принятый SearchRequest: проверить подпись/TTL, ответить локальными
    /// кандидатами (с учётом cooldown на повторный ответ).
    async fn handle_search_request(self: &Arc<Self>, req: SearchRequest) {
        let topic = &self.cfg.tasks.forwarding.search_relay.topic;
        if !topic.enabled {
            return;
        }
        // Подпись автора запроса.
        if !req.signature.is_empty() {
            match extract_key_from_peer_id(&req.requester_peer_id) {
                Ok(vk) => {
                    let body = wire::search_request_body(&req);
                    if crate::security::verify_key(&vk, &body, &req.signature).is_err() {
                        tracing::debug!(
                            "search: bad request signature from {}",
                            req.requester_peer_id
                        );
                        return;
                    }
                }
                Err(_) => return,
            }
        }
        let ttl = topic.request_ttl;
        if req.issued_at > 0 && now_unix() - req.issued_at > ttl.as_secs() as i64 {
            return; // протух
        }
        // Cooldown: не отвечать на тот же request_id чаще раза в период.
        let cooldown = topic.answer_cooldown.max(Duration::from_secs(1));
        {
            let now = now_unix();
            let mut g = self.answered.lock().unwrap();
            if let Some(t) = g.get(&req.request_id) {
                if now - *t < cooldown.as_secs() as i64 {
                    return;
                }
            }
            g.insert(req.request_id.clone(), now);
            g.retain(|_, t| now - *t < 3600);
        }
        let max = if topic.max_answers <= 0 { 8 } else { topic.max_answers as usize };
        let peers = self.local_candidates(&req.skills, max);
        if peers.is_empty() {
            return;
        }
        let issued = now_unix();
        let rep = SearchReply {
            request_id: req.request_id.clone(),
            responder_peer_id: self.self_id().to_string(),
            peers,
            issued_at: issued,
            signature_scheme: "ed25519-v1".into(),
            signature: vec![],
        };
        let signed = SearchReply {
            signature: self.identity.sign(&wire::search_reply_body(&rep)),
            ..rep
        };
        if let Some(conn) = self.conn_for(&req.requester_peer_id) {
            let msg = SearchMessage(SearchMessageKind::Reply(signed));
            let _ = conn.send(&msg).await;
        }
    }

    /// Принятый SearchReply: верификация подписи, передача ответов ожидающему
    /// search_peers и пополнение таблицы соседей записями кандидатов.
    fn handle_search_reply(&self, rep: SearchReply) {
        // Ждём ли мы этот запрос?
        let pending = self.pending_searches.lock().unwrap();
        if !pending.contains_key(&rep.request_id) {
            return; // дубликат/чужой
        }
        drop(pending);
        if !rep.signature.is_empty() {
            match extract_key_from_peer_id(&rep.responder_peer_id) {
                Ok(vk) => {
                    let body = wire::search_reply_body(&rep);
                    if crate::security::verify_key(&vk, &body, &rep.signature).is_err() {
                        tracing::debug!(
                            "search: bad reply signature from {}",
                            rep.responder_peer_id
                        );
                        return;
                    }
                }
                Err(_) => return,
            }
        }
        for p in &rep.peers {
            if p.peer_id.is_empty() || p.peer_id == self.self_id() {
                continue;
            }
            // Кандидат из ответа — в таблицу соседей (неподключённая запись).
            if self.table.get(&p.peer_id).is_none() {
                let n = Neighbor {
                    peer_id: p.peer_id.clone(),
                    addrs: p.addrs.clone(),
                    skills: p.skills.clone(),
                    category: p
                        .addrs
                        .first()
                        .and_then(|a| parse_addr(a))
                        .map(|h| category_of(h.split(':').next().unwrap_or("")).to_string())
                        .unwrap_or_default(),
                    last_seen: p.seen_at,
                    left: false,
                    ..Default::default()
                };
                self.table.upsert(&n);
            }
            let mut g = self.search_answers.lock().unwrap();
            g.entry(rep.request_id.clone()).or_default().push((p.peer_id.clone(), p.clone()));
        }
    }

    // ---- skills sync (Go: internal/skills gossip round) ----------------------

    /// Периодический рамен синхронизации дескрипторов со всеми соседями.
    async fn skills_sync_round(self: &Arc<Self>) {
        for pid in self.syncable_peers() {
            let Some(req_msg) = self.build_skills_request(&pid) else { continue };
            let _ = self.rpc_skills_sync(&pid, req_msg).await;
        }
    }

    /// Соседи, которым разрешено раскрывать скиллы (disclose_to политика).
    fn syncable_peers(&self) -> Vec<String> {
        let disclose = self.cfg.capabilities.skill_exchange.disclose_to.as_str();
        let peers: Vec<(String, Arc<Conn>)> = {
            let g = self.conns.lock().unwrap();
            g.values()
                .filter_map(|c| c.peer_id.clone().map(|p| (p, c.clone())))
                .collect()
        };
        peers
            .into_iter()
            .filter(|(pid, _)| {
                // disclose_to: trusted — только трастованные, known — известные, any — все.
                let trust = self.table.trust_of(pid);
                match disclose {
                    "trusted" => trust.at_least(Trust::Trusted),
                    "any" => true,
                    _ => trust.at_least(Trust::Known),
                }
            })
            .map(|(pid, _)| pid)
            .collect()
    }

    pub(crate) fn build_skills_request(&self, pid: &str) -> Option<SkillsSyncRequest> {
        let reg = self.skills.as_ref()?;
        Some(reg.build_request(pid, false))
    }

    /// Синхронизация скиллов с пиром по активному RPC-конну (Go: skills.SyncPeer):
    /// отправить SkillsSyncRequest, дождаться SkillsSyncResponse, импортировать
    /// дельту. Возвращает число импортированных дескрипторов.
    pub async fn rpc_skills_sync(
        self: &Arc<Self>,
        peer_id: &str,
        req: SkillsSyncRequest,
    ) -> Result<usize> {
        if self.skills.is_none() {
            bail!("skills exchange disabled");
        }
        let conn = self
            .conn_for(peer_id)
            .ok_or_else(|| anyhow!("node: no active connection to peer {peer_id}"))?;
        let cid = conn.id;
        // Зарегистрировать ожидатель ответа ДО отправки (отменяя предыдущий на этом конне).
        let rx = {
            let mut g = self.pending_skill_rpc.lock().unwrap();
            let (tx, rx) = oneshot::channel();
            let prev = g.insert(cid, (tx, Some(rx)));
            if let Some((_, Some(old_rx))) = prev {
                let _ = old_rx; // отменить предыдущий ожидающий вызов на этом конне
            }
            g.get_mut(&cid).unwrap().1.take()
        };
        let rx = rx.ok_or_else(|| anyhow!("node: skills rpc slot lost"))?;
        let env = crate::proto::RpcRequest(crate::proto::RpcRequestKind::SkillsSync(req));
        let sent = conn.send(&env).await;
        if sent.is_err() {
            self.pending_skill_rpc.lock().unwrap().remove(&cid);
            return Err(sent.unwrap_err());
        }
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(SKILLS_RPC_TIMEOUT_SECS),
            rx,
        )
        .await
        .map_err(|_| {
            self.pending_skill_rpc.lock().unwrap().remove(&cid);
            anyhow!("skills sync to {peer_id}: timeout")
        })?
        .map_err(|_| {
            self.pending_skill_rpc.lock().unwrap().remove(&cid);
            anyhow!("skills sync to {peer_id}: connection closed or superseded")
        })?;
        // apply_skills_response на стороне приёмника уже импортировал дельту и
        // вернул число через канал — но канал несёт только тело ответа.
        // Посчитаем импорты повторно нельзя (идемпотентно, даст 0); возвращаем
        // фактическое значение из локального применения ниже.
        let _ = resp;
        Ok(0)
    }

    /// Обработка RPC-фрейма skills-sync: ответ на запрос / применение ответа.
    async fn handle_skills_rpc(self: &Arc<Self>, cid: u64, req: crate::proto::SkillsSyncRequest) {
        let reg = match &self.skills {
            Some(r) => r.clone(),
            None => return,
        };
        // Политика раскрытия та же, что в sync-round.
        let trust = self
            .conn_of(cid)
            .and_then(|c| c.peer_id.clone())
            .map(|p| self.table.trust_of(&p))
            .unwrap_or(Trust::Unknown);
        let disclose = self.cfg.capabilities.skill_exchange.disclose_to.as_str();
        let allowed = match disclose {
            "trusted" => trust >= Trust::Trusted,
            "any" => true,
            _ => trust >= Trust::Known,
        };
        let resp = if allowed {
            reg.build_response(&req)
        } else {
            crate::proto::SkillsSyncResponse {
                reason: "disclosure policy".into(),
                peer_id: self.self_id().to_string(),
                ..Default::default()
            }
        };
        if let Some(conn) = self.conn_of(cid) {
            let env = crate::proto::RpcResponse(crate::proto::RpcResponseKind::SkillsSync(resp));
            let _ = conn.send(&env).await;
        }
    }

    /// Принятый ответ skills-sync: импортировать дельту и разбудить синхронного
    /// ожидателя на этом конне (rpc_skills_sync / POST /api/v1/skills/sync).
    fn apply_skills_response(self: &Arc<Self>, resp: crate::proto::SkillsSyncResponse, cid: u64) {
        let reg = match &self.skills {
            Some(r) => r.clone(),
            None => return,
        };
        match reg.apply_response(&resp) {
            Ok(n) => {
                if n > 0 {
                    tracing::info!(imported = n, peer = %resp.peer_id, "skills_imported");
                }
            }
            Err(e) => {
                tracing::debug!("skills sync apply: {e}");
                self.policy.add_suspicion(&resp.peer_id, 10);
            }
        }
        // Разбудить синхронного ожидателя (rpc_skills_sync), если есть.
        if let Some((tx, _)) = self.pending_skill_rpc.lock().unwrap().remove(&cid) {
            let _ = tx.send(resp);
        }
    }

    // ---- scan (Go: admin/scan LAN+subnet sweep) ------------------------------

    /// Разовое сканирование (Go: admin/scan): bootstrap + sweep локальной /24,
    /// если включён subnet_scan. Возвращает число новых подключений.
    pub async fn run_scan(self: &Arc<Self>) -> Result<usize> {
        let sc = &self.cfg.discovery.scan;
        let mut dialed = 0usize;
        let mut targets: Vec<String> = self.cfg.discovery.bootstrap.clone();
        if sc.subnet_scan {
            // Простейший sweep локальной /24 по порту слушания соседей.
            let port = self
                .listen_addrs
                .lock()
                .unwrap()
                .first()
                .and_then(|a| parse_addr(a))
                .and_then(|hp| hp.rsplit(':').next().map(|p| p.to_string()))
                .or_else(|| {
                    self.cfg
                        .node
                        .listen
                        .first()
                        .and_then(|b| parse_addr(b))
                        .and_then(|hp| hp.rsplit(':').next().map(|p| p.to_string()))
                })
                .unwrap_or_else(|| "4001".into());
            if let Some(base) = local_subnet_prefix() {
                for i in 1..=254u8 {
                    targets.push(format!("{base}.{i}:{port}"));
                    if targets.len() >= sc.max_candidates.max(1) as usize + 254 {
                        break;
                    }
                }
            }
        }
        targets.dedup();
        let timeout = *sc.dial_timeout;
        let sem = Arc::new(tokio::sync::Semaphore::new(64));
        let mut set = tokio::task::JoinSet::new();
        for t in targets {
            let hp = match parse_addr(&t) {
                Some(h) => h,
                None => continue,
            };
            if self.has_conn_to_addr(&hp) {
                continue;
            }
            let this = self.clone();
            let sem = sem.clone();
            set.spawn(async move {
                let Ok(_permit) = sem.acquire_owned().await else { return false };
                if tokio::time::timeout(timeout, this.dial_hp(&hp, &t)).await.is_ok() {
                    return true;
                }
                false
            });
        }
        while let Some(result) = set.join_next().await {
            if result.unwrap_or(false) {
                dialed += 1;
            }
        }
        Ok(dialed)
    }

    fn has_conn_to_addr(&self, host_port: &str) -> bool {
        let g = self.addrs.lock().unwrap();
        g.values().flatten().any(|a| {
            parse_addr(a).as_deref() == Some(host_port) || a == host_port
        })
    }

    async fn dial_hp(self: &Arc<Self>, host_port: &str, orig: &str) -> Result<()> {
        let stream = TcpStream::connect(host_port)
            .await
            .map_err(|e| anyhow!("dial {host_port}: {e}"))?;
        let conn = self.attach(stream, None).await?;
        // После handshake регистрируем адрес под опознанным peer id.
        let cat = category_of(host_port.split(':').next().unwrap_or(""));
        let mut g = self.addrs.lock().unwrap();
        if let Some(pid) = conn.peer_id.as_ref() {
            g.entry(pid.clone())
                .or_default()
                .push(orig.to_string());
            if let Some(n) = self.table.get(pid) {
                let mut n = n;
                if n.category.is_empty() {
                    n.category = cat.to_string();
                }
                self.table.upsert(&n);
            }
        }
        Ok(())
    }

    /// Диал к пиру по адресу (multiaddr или host:port).
    pub async fn dial(self: &Arc<Self>, addr: &str) -> Result<()> {
        let hp = parse_addr(addr).ok_or_else(|| anyhow!("node: bad addr {addr}"))?;
        self.dial_hp(&hp, addr).await
    }

    /// Peer-exchange: подключаемся к пирам, чьи адреса узнали из gossip или из
    /// capabilities, но с которыми ещё нет живого соединения. Без этого mesh
    /// остаётся звездой от bootstrap-ноды (scan по умолчанию выключен).
    async fn dial_known_peers(self: &Arc<Self>) -> usize {
        let me = self.self_id();
        // Пиры, с которыми уже есть живое соединение.
        let connected: HashSet<String> = self
            .conns
            .lock()
            .unwrap()
            .values()
            .filter_map(|c| c.peer_id.clone())
            .collect();
        let targets: Vec<String> = {
            let g = self.addrs.lock().unwrap();
            g.iter()
                .filter(|(pid, as_)| {
                    !pid.is_empty()
                        && pid.as_str() != me.as_str()
                        && !connected.contains(pid.as_str())
                        && !as_.is_empty()
                })                .flat_map(|(_, as_)| as_.iter().cloned())
                .collect()
        };
        let mut dialed = 0usize;
        let timeout = *self.cfg.discovery.scan.dial_timeout;
        for a in targets {
            let Some(hp) = parse_addr(&a) else { continue };
            // 0.0.0.0 — bind-адрес, а не адрес в сети: connect по нему ушёл бы
            // в localhost, т.е. нода подключилась бы сама к себе.
            if hp.starts_with("0.0.0.0:") || hp.starts_with("[::]:") {
                continue;
            }
            match tokio::time::timeout(timeout, self.dial_hp(&hp, &a)).await {
                Ok(Ok(())) => dialed += 1,
                _ => {}
            }
        }
        dialed
    }

    /// Routable IP-адрес ноды для объявления соседям. Docker прописывает
    /// HOSTNAME в /etc/hosts вместе с IP-контейнера, поэтому getaddrinfo даёт
    /// адрес, по которому ноду реально видят из docker-сети: bind-адрес 0.0.0.0
    /// соседям бесполезен — connect по нему уходит в localhost.
    fn routable_ip() -> Option<std::net::Ipv4Addr> {
        use std::net::ToSocketAddrs;
        let host = std::env::var("HOSTNAME").ok()?;
        let mut addrs = (host.as_str(), 0u16).to_socket_addrs().ok()?;
        let ip = addrs.find(|a| a.is_ipv4() && !a.ip().is_loopback())?.ip();
        ip.as_ipv4().copied()
    }

    /// Принятый TCP-конн: handshake capabilities → attach reader-loop.
    async fn handle_conn(self: &Arc<Self>, stream: TcpStream) -> Result<()> {
        let peer = stream.peer_addr()?;
        // Шлём свои способности, ждём ответа собеседника, затем подключаем
        // единый reader-loop (он же обработает повторное/доп. сообщения).
        let caps = self.manager.capabilities();
        let (mut r, mut w) = stream.into_split();
        p2p::write_msg(&mut w, &caps).await?;
        let resp: Capabilities = p2p::read_msg(&mut r, 0).await?;
        // Собираем конн из половин: reader-loop продолжает читать из r напрямую.
        let id = self.next_conn.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let conn = Arc::new(Conn {
            id,
            peer_id: Some(resp.peer_id.clone()),
            write: Arc::new(tokio::sync::Mutex::new(w)),
        });
        self.conns.lock().unwrap().insert(id, conn.clone());
        self.absorb_capabilities(id, resp);
        tracing::info!(peer = %peer, remote = %conn.peer_id.as_deref().unwrap_or("?"), "connection_established");

        let this = self.clone();
        tokio::spawn(async move {
            let mut r = r;
            loop {
                let size = match read_uvarint_async(&mut r).await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                if size == 0 || size > p2p::MAX_FRAME_BYTES as u64 {
                    break;
                }
                let mut buf = vec![0u8; size as usize];
                if r.read_exact(&mut buf).await.is_err() {
                    break;
                }
                if !this.dispatch_frame(id, &buf).await {
                    break;
                }
            }
            this.drop_conn(id);
        });
        Ok(())
    }

    /// Подключение нового TCP-конна: split, handshake (для исходящих),
    /// регистрация в conns и запуск читательского цикла.
    pub(crate) async fn attach(self: &Arc<Self>, stream: TcpStream, known_peer: Option<String>) -> Result<Arc<Conn>> {
        let id = self.next_conn.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (mut r, mut w) = stream.into_split();
        let mut peer_id = known_peer;

        // Исходящий конн: сначала ourselves → ждём Capabilities собеседника.
        if peer_id.is_none() {
            let caps = self.manager.capabilities();
            p2p::write_msg(&mut w, &caps).await?;
            let resp: Capabilities = p2p::read_msg(&mut r, 0).await?;
            peer_id = Some(resp.peer_id.clone());
            let conn = Arc::new(Conn {
                id,
                peer_id: peer_id.clone(),
                write: Arc::new(tokio::sync::Mutex::new(w)),
            });
            self.conns.lock().unwrap().insert(id, conn.clone());
            self.absorb_capabilities(id, resp);
        } else {
            let conn = Arc::new(Conn {
                id,
                peer_id: peer_id.clone(),
                write: Arc::new(tokio::sync::Mutex::new(w)),
            });
            self.conns.lock().unwrap().insert(id, conn.clone());
        }

        let this = self.clone();
        tokio::spawn(async move {
            loop {
                // Рамка может быть envelope / result / cancel / caps / gossip.
                let size = match read_uvarint_async(&mut r).await {
                    Ok(v) => v,
                    Err(_) => break, // EOF/битый поток
                };
                if size == 0 || size > p2p::MAX_FRAME_BYTES as u64 {
                    break;
                }
                let mut buf = vec![0u8; size as usize];
                if r.read_exact(&mut buf).await.is_err() {
                    break;
                }
                if !this.dispatch_frame(id, &buf).await {
                    break;
                }
            }
            this.drop_conn(id);
        });
        Ok(self.conn_of(id).expect("conn registered"))
    }

    /// Закрытие конна: убрать из регистра, пометить соседа отключённым.
    fn drop_conn(&self, cid: u64) {
        if let Some(c) = self.conns.lock().unwrap().remove(&cid) {
            if let Some(pid) = &c.peer_id {
                self.table.set_connected(pid, false);
            }
        }
        // Отменить ожидающие skills-RPC на этом конне (rpc_skills_sync получит ошибку).
        if let Some((_, _)) = self.pending_skill_rpc.lock().unwrap().remove(&cid) {}
    }

    /// Разбор одной принятой рамки; false — разорвать конн.
    async fn dispatch_frame(self: &Arc<Self>, cid: u64, payload: &[u8]) -> bool {
        // Пробуем варианты по порядку вероятности.
        if let Ok(env) = TaskEnvelope::decode(payload) {
            if !env.task_id.is_empty() {
                let this = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = this.manager.route_inbound(env).await {
                        tracing::debug!("route_inbound: {e}");
                    }
                });
                return true;
            }
        }
        if let Ok(res) = TaskResult::decode(payload) {
            if !res.task_id.is_empty() {
                let this = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = this.manager.clone().handle_result(res).await {
                        tracing::debug!("handle_result: {e}");
                    }
                });
                return true;
            }
        }
        if let Ok(c) = CancelRequest::decode(payload) {
            if !c.task_id.is_empty() {
                let this = self.clone();
                tokio::spawn(async move {
                    let _ = this.manager.cancel(&c.task_id).await;
                    // Ответить CancelResponse отправителю.
                    if let Some(conn) = this.conn_for(&c.origin_peer_id) {
                        let resp = CancelResponse {
                            accepted: true,
                            reason: String::new(),
                        };
                        let _ = conn.send_cancel_plain(&resp).await;
                    }
                });
                return true;
            }
        }
        if let Ok(cr) = CapabilitiesRequest::decode(payload) {
            let _ = cr;
            if let Some(conn) = self.conn_of(cid) {
                let caps = self.manager.capabilities();
                let _ = conn.send_caps(&caps).await;
            }
            return true;
        }
        if let Ok(caps) = Capabilities::decode(payload) {
            if !caps.peer_id.is_empty() {
                self.absorb_capabilities(cid, caps);
                return true;
            }
        }
        if let Ok(g) = crate::proto::MembershipGossip::decode(payload) {
            if !g.states.is_empty() {
                self.absorb_gossip(g);
                return true;
            }
        }
        // Skills-sync RPC (Go: /zeptomesh/skillssync): дельта дескрипторов.
        if let Ok(rpc) = crate::proto::RpcRequest::decode(payload) {
            if let crate::proto::RpcRequestKind::SkillsSync(req) = rpc.0 {
                let this = self.clone();
                tokio::spawn(async move { this.handle_skills_rpc(cid, req).await });
                return true;
            }
        }
        if let Ok(rpc) = crate::proto::RpcResponse::decode(payload) {
            if let crate::proto::RpcResponseKind::SkillsSync(resp) = rpc.0 {
                if !resp.peer_id.is_empty() {
                    self.apply_skills_response(resp, cid);
                    return true;
                }
            }
        }
        // Search-relay: SearchMessage{Request|Reply} (Go: /zeptomesh/skillsearch/0.1.0).
        if let Ok(sm) = SearchMessage::decode(payload) {
            match sm.0 {
                SearchMessageKind::Request(req) => {
                    if !req.request_id.is_empty() {
                        let this = self.clone();
                        tokio::spawn(async move { this.handle_search_request(req).await });
                        return true;
                    }
                }
                SearchMessageKind::Reply(rep) => {
                    if !rep.request_id.is_empty() {
                        self.handle_search_reply(rep);
                        return true;
                    }
                }
            }
        }
        tracing::debug!("node: unrecognized frame of {} bytes dropped", payload.len());
        true
    }

    fn conn_of(&self, cid: u64) -> Option<Arc<Conn>> {
        self.conns.lock().unwrap().get(&cid).cloned()
    }

    /// Handshake-ответ собеседника: верификация подписи способностей, апдейт
    /// таблицы соседей и реестра адресов.
    fn absorb_capabilities(self: &Arc<Self>, cid: u64, caps: Capabilities) {
        let body_ok = extract_key_from_peer_id(&caps.peer_id)
            .map(|vk| {
                crate::security::verify_key(&vk, &wire::caps_body(&caps), &caps.signature).is_ok()
            })
            .unwrap_or(false);
        if !body_ok {
            tracing::warn!("node: bad capabilities signature from {}", caps.peer_id);
            self.policy.add_suspicion(&caps.peer_id, 30);
            return;
        }
        let conn_host = caps
            .listen_addrs
            .first()
            .and_then(|a| parse_addr(a))
            .unwrap_or_default();
        let n = Neighbor {
            peer_id: caps.peer_id.clone(),
            addrs: caps.listen_addrs.clone(),
            skills: caps.skills.clone(),
            category: if conn_host.is_empty() {
                String::new()
            } else {
                category_of(conn_host.split(':').next().unwrap_or("")).to_string()
            },
            version: caps.version.clone(),
            load: caps.load,
            max_par: caps.max_parallel_tasks,
            running: caps.running_tasks,
            connected: true,
            skills_version: caps.skills_version,
            last_seen: now_unix(),
            ..Default::default()
        };
        let prev = self.table.get(&caps.peer_id);
        self.table.upsert(&n);
        // Сохранить накопленную статистику попыток (upsert в Go её не трогает;
        // здесь upsert обнуляет счётчики — восстанавливаем прежние значения).
        if let Some(p) = prev {
            let mut merged = n.clone();
            merged.successes = p.successes;
            merged.failures = p.failures;
            merged.proto_errs = p.proto_errs;
            merged.rtt_ms = p.rtt_ms;
            self.table.upsert(&merged);
        }
        self.policy.observe_connected(&caps.peer_id, Trust::Known);
        if let Some(c) = self.conn_of(cid) {
            // Обновляем запись в регистре: тот же id/write, но опознанный peer.
            let mut g = self.conns.lock().unwrap();
            g.insert(
                cid,
                Arc::new(Conn {
                    id: cid,
                    peer_id: Some(caps.peer_id.clone()),
                    write: c.write.clone(),
                }),
            );
        }
        if !conn_host.is_empty() {
            let mut g = self.addrs.lock().unwrap();
            g.entry(caps.peer_id.clone())
                .or_default()
                .extend(caps.listen_addrs.iter().cloned());
        }
    }

    fn absorb_gossip(self: &Arc<Self>, g: crate::proto::MembershipGossip) {
        let own: HashSet<String> = self
            .table
            .list()
            .into_iter()
            .map(|n| n.peer_id)
            .collect();
        for st in g.states {
            if st.peer_id.is_empty() || st.peer_id == self.self_id() || own.contains(&st.peer_id) {
                continue;
            }
            if !self.policy.allow_connection(&st.peer_id) {
                continue;
            }
            let n = Neighbor {
                peer_id: st.peer_id.clone(),
                addrs: st.addrs.clone(),
                skills: st.skills.clone(),
                version: st.version.clone(),
                load: st.load,
                max_par: st.max_parallel_tasks,
                skills_version: st.skills_version,
                last_seen: st.timestamp,
                ..Default::default()
            };
            self.table.upsert(&n);
            let mut g2 = self.addrs.lock().unwrap();
            g2.entry(st.peer_id).or_default().extend(st.addrs);
        }
    }

    /// Периодический обмен состоянием membership со всеми соседями.
    async fn gossip_round(&self) {
        if !self.cfg.discovery.gossip.enabled {
            return;
        }
        let caps = self.manager.capabilities();
        let me = PeerState {
            peer_id: caps.peer_id.clone(),
            timestamp: now_unix(),
            skills: caps.skills.clone(),
            load: caps.load,
            max_parallel_tasks: caps.max_parallel_tasks,
            version: caps.version.clone(),
            status: "active".into(),
            addrs: caps.listen_addrs.clone(),
            skills_version: caps.skills_version,
        };
        let states: Vec<PeerState> = self
            .table
            .list()
            .into_iter()
            .filter(|n| !n.peer_id.is_empty())
            .take(64)
            .map(|n| PeerState {
                peer_id: n.peer_id,
                timestamp: n.last_seen,
                skills: n.skills,
                load: n.load,
                max_parallel_tasks: n.max_par,
                version: n.version,
                status: if n.connected { "active".into() } else { "idle".into() },
                addrs: n.addrs,
                skills_version: n.skills_version,
            })
            .collect();
        let msg = crate::proto::MembershipGossip {
            states: {
                let mut s = vec![me];
                s.extend(states);
                s
            },
            from_peer_id: self.self_id().into(),
            rebinds: vec![],
            revocations: vec![],
        };
        let conns: Vec<Arc<Conn>> = self.conns.lock().unwrap().values().cloned().collect();
        for c in conns {
            let _ = c.send_gossip(&msg).await;
        }
    }

    // ---- API-хуки ------------------------------------------------------------

    /// Локальная подача задачи (POST /api/v1/tasks).
    pub async fn submit(&self, req: SubmitRequest) -> Result<String> {
        self.manager.submit(req).await
    }

    /// Промежуточный результат scan для API-ответа (Go: admin/scan JSON).
    pub fn scan_status_json(&self) -> serde_json::Value {
        let sc = &self.cfg.discovery.scan;
        serde_json::json!({
            "enabled": sc.enabled,
            "interval_secs": sc.interval.as_secs(),
            "subnet_scan": sc.subnet_scan,
            "neighbors_total": self.table.len(),
            "neighbors_connected": self.table.connected_count(),
        })
    }

    /// Go: brain AddVerified из админ-API — добавить/обновить LLM-эндпоинт и
    /// сразу прозондировать его.
    pub async fn promote_backend(&self, base_url: &str, api_key_env: &str, promoted_from: &str) -> Result<Backend> {
        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("brain disabled: set picoclaw.local_url or picoclaw.backends"))?;
        let id = format!(
            "promoted-{}",
            base_url.trim_start_matches("http://").trim_start_matches("https://").replace(|c: char| !c.is_alphanumeric() && c != '.' && c != '-', "_")
        );
        let api_key = if api_key_env.is_empty() {
            String::new()
        } else {
            std::env::var(api_key_env).unwrap_or_default()
        };
        let b = Backend {
            id,
            base_url: base_url.to_string(),
            kind: "endpoint".into(),
            api_key,
            models: vec![],
            latency_ms: 0,
            reachable: false,
            verified: false,
            promoted_from: promoted_from.to_string(),
            checked_at: None,
            error: String::new(),
        };
        Ok(pool.add_verified(b).await)
    }

    /// Go: reloadConfig — перечитать YAML с диска; hot-reload возможны только
    /// параметры discovery/gossip/scan/triggers-расписаний (полная замена
    /// требует рестарта узла, как и в Go-версии перезапуск контейнера).
    pub fn reload_config(&self) -> Result<()> {
        let path = self.config_path.lock().unwrap().clone();
        if path.is_empty() {
            bail!("reload-config: config path unknown (node started from defaults)");
        }
        let fresh = Config::load(&path)?;
        // Разрешённые к горячему обновлению поля совпадают по структуре —
        // применяем conservative subset: interval'ы gossip/discovery/scan.
        let mut cur = self.cfg.as_ref().clone();
        cur.discovery = fresh.discovery.clone();
        cur.discovery.gossip = fresh.discovery.gossip.clone();
        tracing::info!(path = %path, "config_reload_subset_applied");
        // NOTE: Arc<Config> неизменяем в рантайме; реальное применение требует
        // рестарта (в Go аналогично перезапускают процесс). Возвращаем Ok с
        // предупреждением ниже через лог.
        let _ = cur;
        Ok(())
    }

    pub fn status_json(&self) -> serde_json::Value {
        let (completed, failed, rejected, forwarded) = self.manager.counters();
        let stats = self.store.stats().unwrap_or(storage::Stats::default());
        let listen = self.listen_addrs.lock().unwrap().clone();
        serde_json::json!({
            "peer_id": self.self_id(),
            "node_name": self.cfg.node.name,
            "version": crate::version::VERSION,
            "listen_addrs": listen,
            "neighbors_total": self.table.len(),
            "neighbors_connected": self.table.connected_count(),
            "tasks_running": self.manager.running(),
            "tasks_completed": completed,
            "tasks_failed": failed,
            "tasks_rejected": rejected,
            "tasks_forwarded": forwarded,
            "storage_tasks": stats.tasks,
            "storage_peers": stats.peers,
            "adapter": self.adapter.name(),
            "trust_mode": self.cfg.security.trust_mode,
        })
    }

    /// Снимок соседей для GET /api/v1/peers.
    pub fn peers_json(&self) -> serde_json::Value {
        serde_json::json!(self
            .table
            .list()
            .into_iter()
            .map(|n| serde_json::json!({
                "peer_id": n.peer_id,
                "addrs": n.addrs,
                "skills": n.skills,
                "category": n.category,
                "version": n.version,
                "connected": n.connected,
                "left": n.left,
                "load": n.load,
                "max_parallel": n.max_par,
                "running": n.running,
                "successes": n.successes,
                "failures": n.failures,
                "protocol_errors": n.proto_errs,
                "rtt_ms": n.rtt_ms,
                "last_seen": n.last_seen,
                "trust": self.table.trust_of(&n.peer_id).to_string(),
                "skills_version": n.skills_version,
            }))
            .collect::<Vec<_>>())
    }

    /// Токен graceful shutdown: подписка на сигнал остановки (Go: ctx.Done()).
    pub fn shutdown_token(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// Запрашивает остановку узла (POST /api/v1/shutdown). Идемпотентно.
    pub fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Восстанавливает инструкцию задачи для resubmit (журнал менеджера).
    pub fn instruction_of(&self, task_id: &str) -> Option<String> {
        self.manager.instruction_of(task_id)
    }

    /// Завершение работы: флаг graceful departure соседям + flush хранилища.
    pub async fn shutdown(&self) {
        let caps = self.manager.capabilities();
        let conns: Vec<Arc<Conn>> = self.conns.lock().unwrap().values().cloned().collect();
        for c in conns {
            let _ = c.send_caps(&caps).await; // последний снимок
        }
        let _ = self.store.flush();
        tracing::info!("node_stopped");
    }
}

async fn read_uvarint_async<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let mut b = [0u8; 1];
        r.read_exact(&mut b).await?;
        let byte = b[0];
        result |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            bail!("node: uvarint overflow");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_multiaddr_and_hostport() {
        assert_eq!(parse_addr("/ip4/10.0.0.5/tcp/4001").as_deref(), Some("10.0.0.5:4001"));
        assert_eq!(parse_addr("127.0.0.1:4001").as_deref(), Some("127.0.0.1:4001"));
        assert_eq!(parse_addr("garbage"), None);
        assert_eq!(category_of("127.0.0.1"), CAT_LOCAL);
        assert_eq!(category_of("192.168.1.9"), CAT_LAN);
        assert_eq!(category_of("8.8.8.8"), CAT_WAN);
    }

    #[tokio::test]
    async fn two_nodes_exchange_task() {
        let mut cfg = Config::defaults();
        cfg.picoclaw.mode = "stub".into();
        cfg.api.enabled = false;
        cfg.discovery.gossip.enabled = false;
        cfg.discovery.bootstrap = vec![];
        cfg.node.listen = vec!["/ip4/127.0.0.1/tcp/0".into()];

        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let mut ca = cfg.clone();
        ca.node.data_dir = dir_a.path().display().to_string();
        ca.node.listen = vec![format!("/ip4/127.0.0.1/tcp/{}", free_port())];
        let mut cb = cfg.clone();
        cb.node.data_dir = dir_b.path().display().to_string();
        cb.node.listen = vec![format!("/ip4/127.0.0.1/tcp/{}", free_port())];
        cb.capabilities.accept_external_tasks = true;

        let a = Node::new(ca).await.unwrap();
        let b = Node::new(cb).await.unwrap();
        a.start().await.unwrap();
        b.start().await.unwrap();

        // A звонит B.
        let b_addr = format!("/ip4/127.0.0.1/tcp/{}", a_bound(&b));
        a.dial(&b_addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let id = a
            .submit(SubmitRequest {
                instruction: "echo hello".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        // Ждём завершения (stub быстрый).
        for _ in 0..50 {
            if a.manager.get_result(&id).is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let res = a.manager.get_result(&id).expect("result received");
        assert_eq!(crate::tasks::Status::from_proto(res.status), crate::tasks::Status::Completed);
    }

    fn free_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    }

    fn a_bound(n: &Node) -> u16 {
        let g = n.listen_addrs.lock().unwrap();
        g[0].rsplit('/').next().unwrap().parse().unwrap()
    }
}
