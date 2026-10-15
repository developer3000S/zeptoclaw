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

use crate::config::Config;
use crate::p2p;
use crate::picoclaw::{self, Adapter, BackendPool};
use crate::proto::{
    Capabilities, CapabilitiesRequest, CancelRequest, CancelResponse,
    Message, PeerState, TaskEnvelope, TaskResult,
};
use crate::routing::{Neighbor, Table, CAT_LAN, CAT_LOCAL, CAT_WAN};
use crate::security::{extract_key_from_peer_id, Identity, Mode, Policy, Trust};
use crate::storage::{self, Store};
use crate::tasks::{Manager, SubmitRequest};
use crate::wire;

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Один конн: id + writer-половина (читает отдельная задача-приёмник).
/// `tokio::sync::Mutex` — потому что запись идёт из async-контекста.
pub(crate) struct Conn {
    #[allow(dead_code)] // идентификатор конна (ключ в self.conns; отладка/логи)
    id: u64,
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

pub type Conns = Arc<Mutex<HashMap<u64, Arc<Conn>>>>;

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
    conns: Conns,
    next_conn: std::sync::atomic::AtomicU64,
    /// Известные адреса пиров для исходящих диалов (peer_id → multiaddr/host:port).
    addrs: Mutex<HashMap<String, Vec<String>>>,
    listen_addrs: Mutex<Vec<String>>,
    /// Сигнал graceful shutdown (go: context.CancelFunc, дёргается из POST /api/v1/shutdown).
    shutdown_tx: Arc<tokio::sync::watch::Sender<bool>>,
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

impl Node {
    /// Go: node.New — полная сборка зависимостей.
    pub async fn new(cfg: Config) -> Result<Arc<Self>> {
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

        // Исполнитель.
        let pool = None; // brain-каталог в Rust-порте пока не включается
        let adapter = picoclaw::new_adapter(&cfg, pool.clone()).await?;

        let manager = Arc::new(Manager::new(
            cfg.clone(),
            store.clone(),
            table.clone(),
            policy.clone(),
            identity.clone(),
            adapter.clone(),
        ));

        let n = Arc::new(Node {
            cfg,
            identity,
            store,
            policy,
            table,
            manager,
            adapter,
            pool,
            conns: Arc::new(Mutex::new(HashMap::new())),
            next_conn: std::sync::atomic::AtomicU64::new(1),
            addrs: Mutex::new(HashMap::new()),
            listen_addrs: Mutex::new(vec![]),
            shutdown_tx: Arc::new(tokio::sync::watch::channel(false).0),
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
            bound.push(format!("/ip4/{}/tcp/{}", actual.ip(), actual.port()));
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

        // Bootstrap-диалы.
        let this = self.clone();
        let boot = self.cfg.discovery.bootstrap.clone();
        tokio::spawn(async move {
            for b in boot {
                if let Some(hp) = parse_addr(&b) {
                    let _ = this.dial_hp(&hp, &b).await;
                }
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
            }
        });
        Ok(())
    }

    fn prune_round(&self) {
        let timeout = self.cfg.discovery.gossip.failure_timeout.max(Duration::from_secs(30));
        let stale = self.table.prune_stale(timeout);
        for pid in stale {
            tracing::debug!("gossip: pruned stale neighbor {pid}");
        }
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
    pub async fn attach(self: &Arc<Self>, stream: TcpStream, known_peer: Option<String>) -> Result<Arc<Conn>> {
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
                crate::security::verify(
                    &vk.to_bytes().to_vec(),
                    &wire::caps_body(&caps),
                    &caps.signature,
                )
                .is_ok()
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
        self.table.upsert(&n);
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
            if st.peer_id == self.self_id() || own.contains(&st.peer_id) {
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
