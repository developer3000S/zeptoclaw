//! Порт пакета `internal/routing`: таблица соседей, скоринг кандидатов и
//! выбор исполнителя для задачи. Веса скоринга и правила merge-наблюдений
//! идентичны Go; доверие всегда читается из `security::Policy` — в таблице
//! поля trust нет (в Go удалено по той же причине: два представления одного
//! факта неизбежно расходятся).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::seq::SliceRandom;
use rand::SeedableRng;

use crate::config::NeighborsConfig;
use crate::security::{Policy, Trust};
use crate::storage::{PeerRecord, Store};

/// Категории соседей относительно этого узла.
pub const CAT_LOCAL: &str = "local";
pub const CAT_LAN: &str = "lan";
pub const CAT_WAN: &str = "wan";
pub const CAT_BOOTSTRAP: &str = "bootstrap";

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// In-memory представление пира (Go: routing.Neighbor).
#[derive(Debug, Clone, Default)]
pub struct Neighbor {
    pub peer_id: String,
    pub addrs: Vec<String>,
    pub skills: Vec<String>,
    pub category: String,
    pub version: String,
    pub load: f64,
    pub max_par: i32,
    pub running: i32,
    pub successes: u64,
    pub failures: u64,
    pub proto_errs: u64,
    /// Скользящее среднее RTT в миллисекундах.
    pub rtt_ms: i64,
    pub last_seen: i64,
    pub connected: bool,
    /// Монотонная эпоха навыков пира (ТЗ 6.3).
    pub skills_version: i64,
    /// Пир объявил graceful departure.
    pub left: bool,
    /// Объявленные модели (для candidate-promotion).
    pub models: Vec<String>,
    /// Имя узла (из Capabilities).
    pub node_name: String,
}

impl Neighbor {
    /// Доля свободных слотов; неизвестная ёмкость — нейтральные 0.5.
    fn available_capacity(&self) -> f64 {
        if self.max_par <= 0 {
            return 0.5;
        }
        let free = (self.max_par - self.running) as f64;
        if free < 0.0 {
            return 0.0;
        }
        free / self.max_par as f64
    }

    /// Доля успехов; 0.5 если ничего не пробовали.
    fn success_rate(&self) -> f64 {
        let total = self.successes + self.failures;
        if total == 0 {
            return 0.5;
        }
        self.successes as f64 / total as f64
    }
}

fn trust_score(t: Trust) -> f64 {
    match t {
        Trust::Trusted => 1.0,
        Trust::Known => 0.75,
        Trust::Limited => 0.5,
        Trust::Untrusted => 0.2,
        Trust::Blocked => 0.0,
        Trust::Unknown => 0.1,
    }
}

/// RTT → [0,1]: 50мс ≈ идеально, 2с ≈ бесполезно.
fn latency_score(rtt_ms: i64) -> f64 {
    if rtt_ms <= 0 {
        return 0.5;
    }
    const GOOD: i64 = 50;
    const BAD: i64 = 2000;
    if rtt_ms <= GOOD {
        return 1.0;
    }
    if rtt_ms >= BAD {
        return 0.0;
    }
    1.0 - (rtt_ms - GOOD) as f64 / (BAD - GOOD) as f64
}

/// Веса скоринга из спецификации (идентичны Go).
const W_SKILL: f64 = 0.40;
const W_TRUST: f64 = 0.25;
const W_CAPACITY: f64 = 0.20;
const W_LATENCY: f64 = 0.10;
const W_SUCCESS: f64 = 0.05;

/// Кандидат на исполнение задачи с оценкой.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub neighbor: Neighbor,
    pub score: f64,
    pub skill_match: f64,
    pub reasons: Vec<String>,
}

fn normalize_skill(s: &str) -> String {
    s.trim().to_lowercase()
}

/// Совпадение навыков (Go: skillsMatch): наличие general/any покрывает всё;
/// иначе нужны все want. Возвращает (matched, ok).
fn skills_match(have: &[String], want: &[String]) -> (usize, bool) {
    let set: HashSet<String> = have.iter().map(|s| normalize_skill(s)).collect();
    if set.contains("general") || set.contains("any") {
        return (want.len(), true);
    }
    if want.is_empty() {
        return (0, true);
    }
    let matched = want
        .iter()
        .filter(|w| set.contains(&normalize_skill(w)))
        .count();
    (matched, matched == want.len())
}

struct TableInner {
    neighbors: HashMap<String, Neighbor>,
    rng: rand::rngs::StdRng,
}

/// Таблица соседей с опциональным хуком персистентности (Go: routing.Table).
#[derive(Clone)]
pub struct Table {
    inner: Arc<Mutex<TableInner>>,
    cfg: NeighborsConfig,
    store: Option<Store>,
    policy: Option<Arc<Policy>>,
}

fn status_label(n: &Neighbor) -> &'static str {
    if n.left {
        "left"
    } else if n.connected {
        "active"
    } else {
        "idle"
    }
}

impl Table {
    /// Новая пустая таблица. policy может быть None (тогда все peers untrusted).
    pub fn new(cfg: NeighborsConfig, store: Option<Store>, policy: Option<Arc<Policy>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TableInner {
                neighbors: HashMap::new(),
                rng: rand::rngs::StdRng::seed_from_u64(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().subsec_nanos() as u64),
            })),
            cfg,
            store,
            policy,
        }
    }

    /// Конфиг таблицы (min/target/max соседей).
    pub fn cfg(&self) -> &NeighborsConfig {
        &self.cfg
    }

    /// TrustOf — сквозной вызов политики, а не кэш (Go: Table.TrustOf).
    pub fn trust_of(&self, pid: &str) -> Trust {
        match &self.policy {
            Some(p) => p.trust_of(pid),
            None => Trust::Untrusted,
        }
    }

    fn persist_locked(&self, n: &Neighbor) {
        let Some(st) = &self.store else { return };
        let rec = PeerRecord {
            peer_id: n.peer_id.clone(),
            addrs: n.addrs.clone(),
            skills: n.skills.clone(),
            // Диагностическое поле: обратно отсюда доверие не читается (Restore).
            trust: self.trust_of(&n.peer_id).to_string(),
            category: n.category.clone(),
            version: n.version.clone(),
            load: n.load,
            max_parallel: n.max_par,
            running: n.running,
            successes: n.successes,
            failures: n.failures,
            protocol_errors: n.proto_errs,
            avg_rtt_ms: n.rtt_ms,
            seen_at: n.last_seen,
            connected: n.connected,
            last_status: status_label(n).to_string(),
            skills_version: n.skills_version,
        };
        let _ = st.put_peer(&rec);
    }

    /// Merg-наблюдение в таблицу с сохранением накопленных метрик (Go: Upsert).
    pub fn upsert(&self, n: &Neighbor) -> Neighbor {
        let mut g = self.inner.lock().unwrap();
        let cur = g.neighbors.entry(n.peer_id.clone()).or_default();
        // Ключ карты — peer_id, но и в самой записи поле должно быть заполнено:
        // иначе сосед остаётся с пустым id (gossip, trust, выбор кандидата, API).
        cur.peer_id = n.peer_id.clone();
        if !n.addrs.is_empty() {
            cur.addrs = n.addrs.clone();
        }
        if !n.skills.is_empty() {
            cur.skills = n.skills.clone();
        }
        if !n.category.is_empty() {
            cur.category = n.category.clone();
        }
        if !n.version.is_empty() {
            cur.version = n.version.clone();
        }
        if n.max_par > 0 {
            cur.max_par = n.max_par;
        }
        // Эпоха навыков монотонна: пир, вернувшийся со старым номером, лжёт
        // или misconfigured — откатывать наше представление нельзя.
        if n.skills_version > cur.skills_version {
            cur.skills_version = n.skills_version;
        }
        cur.running = n.running;
        cur.load = n.load;
        cur.connected = n.connected;
        cur.left = n.left;
        cur.last_seen = now_unix();
        let out = cur.clone();
        drop(g);
        self.persist_locked(&out);
        out
    }

    pub fn set_connected(&self, pid: &str, connected: bool) {
        let mut g = self.inner.lock().unwrap();
        let n = g.neighbors.entry(pid.to_string()).or_default();
        n.peer_id = pid.to_string();
        n.connected = connected;
        n.last_seen = now_unix();
        if connected {
            n.left = false;
        }
    }

    /// Скользящее среднее RTT: 0.7*старое + 0.3*новое (как в Go).
    pub fn record_rtt(&self, pid: &str, rtt: Duration) {
        let mut g = self.inner.lock().unwrap();
        if let Some(n) = g.neighbors.get_mut(pid) {
            let ms = rtt.as_millis() as i64;
            if n.rtt_ms <= 0 {
                n.rtt_ms = ms;
            } else {
                n.rtt_ms = (n.rtt_ms as f64 * 0.7 + ms as f64 * 0.3) as i64;
            }
        }
    }

    pub fn record_success(&self, pid: &str) {
        self.record(pid, true);
    }

    pub fn record_failure(&self, pid: &str) {
        self.record(pid, false);
    }

    fn record(&self, pid: &str, ok: bool) {
        {
            let mut g = self.inner.lock().unwrap();
            let Some(n) = g.neighbors.get_mut(pid) else { return };
            if ok {
                n.successes += 1;
            } else {
                n.failures += 1;
            }
        }
        if let Some(p) = &self.policy {
            if ok {
                p.reduce_suspicion(pid, 20);
            } else {
                p.add_suspicion(pid, 20);
            }
        }
    }

    /// Счётчик протокольных ошибок (malformed/policy-violating сообщение).
    pub fn record_protocol_error(&self, pid: &str) {
        let mut g = self.inner.lock().unwrap();
        if let Some(n) = g.neighbors.get_mut(pid) {
            n.proto_errs += 1;
        }
    }

    pub fn get(&self, pid: &str) -> Option<Neighbor> {
        self.inner.lock().unwrap().neighbors.get(pid).cloned()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().neighbors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Проверка наличия пира в таблице.
    pub fn contains(&self, pid: &str) -> bool {
        self.inner.lock().unwrap().neighbors.contains_key(pid)
    }

    /// Снимок всех соседей: connected первыми, затем newest-last-seen (как Go).
    pub fn list(&self) -> Vec<Neighbor> {
        let mut out: Vec<Neighbor> = self
            .inner
            .lock()
            .unwrap()
            .neighbors
            .values()
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            if a.connected != b.connected {
                return if a.connected {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                };
            }
            b.last_seen.cmp(&a.last_seen)
        });
        out
    }

    pub fn connected_count(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .neighbors
            .values()
            .filter(|n| n.connected && !n.left)
            .count()
    }

    pub fn remove(&self, pid: &str) {
        self.inner.lock().unwrap().neighbors.remove(pid);
    }

    /// Соседи, молчащие дольше timeout — без удаления (Go: Suspects).
    /// Молчание само по себе не карается: живой, но тихий пир подтверждается
    /// прямым зондом (ТЗ 6.5.3).
    pub fn suspects(&self, timeout: Duration) -> Vec<Neighbor> {
        let cutoff = now_unix() - timeout.as_secs() as i64;
        let mut out: Vec<Neighbor> = self
            .inner
            .lock()
            .unwrap()
            .neighbors
            .values()
            .filter(|n| n.last_seen < cutoff)
            .cloned()
            .collect();
        out.sort_by_key(|n| n.last_seen);
        out
    }

    /// Подтверждение живости: обновляет LastSeen и состояние соединения.
    pub fn touch(&self, pid: &str, connected: bool) {
        let mut g = self.inner.lock().unwrap();
        if let Some(n) = g.neighbors.get_mut(pid) {
            n.last_seen = now_unix();
            n.connected = connected;
            if connected {
                n.left = false;
            }
        }
    }

    /// Удаляет молчащих дольше timeout, возвращает изгнанных.
    pub fn prune_stale(&self, timeout: Duration) -> Vec<String> {
        let cutoff = now_unix() - timeout.as_secs() as i64;
        let mut g = self.inner.lock().unwrap();
        let gone: Vec<String> = g
            .neighbors
            .iter()
            .filter(|(_, n)| n.last_seen < cutoff)
            .map(|(k, _)| k.clone())
            .collect();
        for id in &gone {
            g.neighbors.remove(id);
        }
        gone
    }

    /// До k случайных соседей (gossip / peer exchange).
    pub fn sample(&self, k: usize) -> Vec<Neighbor> {
        let all = self.list();
        if k >= all.len() {
            return all;
        }
        let mut g = self.inner.lock().unwrap();
        let idx: Vec<usize> = (0..all.len()).collect();
        let mut perm = idx;
        perm.shuffle(&mut g.rng);
        perm.truncate(k);
        perm.into_iter().map(|i| all[i].clone()).collect()
    }

    /// Загрузка персистентных пиров при старте (Go: Restore).
    /// Connected=false и trust не восстанавливаются: доверие зарабатывается
    /// заново за процесс handshake'ами.
    pub fn restore(&self) -> Result<usize, anyhow::Error> {
        let Some(st) = &self.store else {
            return Ok(0);
        };
        let recs = st.list_peers()?;
        let mut g = self.inner.lock().unwrap();
        for r in recs {
            // Запись без peer_id — мусор: такого «пира» нельзя ни адресовать,
            // ни проверить подписью, в таблицу его тащить незачем.
            if r.peer_id.is_empty() {
                continue;
            }
            g.neighbors.insert(
                r.peer_id.clone(),
                Neighbor {
                    peer_id: r.peer_id,
                    addrs: r.addrs,
                    skills: r.skills,
                    category: r.category,
                    version: r.version,
                    load: r.load,
                    max_par: r.max_parallel,
                    running: r.running,
                    successes: r.successes,
                    failures: r.failures,
                    proto_errs: r.protocol_errors,
                    rtt_ms: r.avg_rtt_ms,
                    last_seen: r.seen_at,
                    connected: false,
                    skills_version: r.skills_version,
                    left: false,
                    models: vec![],
                    node_name: String::new(),
                },
            );
        }
        Ok(g.neighbors.len())
    }

    /// Оценивает всех подходящих соседей для задачи и возвращает лучшие
    /// fanout-кандидатов (Go: Select). exclude — отказавшие ранее пиры.
    pub fn select(
        &self,
        want_skills: &[String],
        policy: Option<&Policy>,
        self_pid: &str,
        fanout: usize,
        exclude: &HashSet<String>,
    ) -> Vec<Candidate> {
        let snap: Vec<Neighbor> = self
            .inner
            .lock()
            .unwrap()
            .neighbors
            .values()
            .cloned()
            .collect();
        let mut cands: Vec<Candidate> = Vec::new();
        // Go-аналог: Table.Select делает снимок политики (Snapshot) перед
        // циклом, чтобы оценка была консистентной. Arc<Policy> — это ссылка,
        // поэтому снимаем копию сами; None -> fallback-снимок из own policy
        // либо Open-политика (в open-режиме untrusted разрешены к делегированию).
        let snap_owned: Policy = match policy {
            Some(p) => (*p).clone(),
            None => match &self.policy {
                Some(p) => p.as_ref().clone(),
                None => Policy::new(crate::security::Mode::Open, Trust::Untrusted),
            },
        };
        let pol = &snap_owned;
        for n in snap {
            if n.peer_id == self_pid || n.left {
                continue;
            }
            if exclude.contains(&n.peer_id) {
                continue;
            }
            if !pol.allow_delegation_to(&n.peer_id) {
                continue;
            }
            let (matched, ok) = skills_match(&n.skills, want_skills);
            if !ok {
                continue;
            }
            let skill_score = if want_skills.is_empty() {
                1.0
            } else {
                matched as f64 / want_skills.len() as f64
            };
            // Оцениваем текущим вердиктом политики, а не кэшированной копией.
            let trust = pol.trust_of(&n.peer_id);
            let mut score = skill_score * W_SKILL
                + trust_score(trust) * W_TRUST
                + n.available_capacity() * W_CAPACITY
                + latency_score(n.rtt_ms) * W_LATENCY
                + n.success_rate() * W_SUCCESS;
            if !n.connected {
                score *= 0.85; // диал стоит round-trip
            }
            let cap = n.available_capacity();
            let rtt = n.rtt_ms;
            cands.push(Candidate {
                neighbor: n,
                score: (score * 10000.0).round() / 10000.0,
                skill_match: skill_score,
                reasons: vec![
                    format!("skill={:.2}", skill_score),
                    format!("trust={}", trust),
                    format!("cap={:.2}", cap),
                    format!("rtt={}ms", rtt),
                ],
            });
        }
        cands.sort_by(|a, b| {
            if a.score == b.score {
                return a.neighbor.peer_id.cmp(&b.neighbor.peer_id);
            }
            b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal)
        });
        if fanout > 0 && cands.len() > fanout {
            cands.truncate(fanout);
        }
        cands
    }

    /// Есть ли подключённый сосед, рекламирующий навыки (Go: AnyWithSkills).
    pub fn any_with_skills(&self, want: &[String]) -> bool {
        !self.select(want, None, "", 1, &HashSet::new()).is_empty()
    }
}






#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::{Mode, Policy};

    fn cfg() -> NeighborsConfig {
        NeighborsConfig { min: 3, target: 8, max: 24 }
    }

    #[test]
    fn upsert_and_get() {
        let t = Table::new(cfg(), None, None);
        let n = Neighbor {
            peer_id: "12D3KooTest".into(),
            ..Default::default()
        };
        t.upsert(&n);
        assert_eq!(t.len(), 1);
        assert!(t.get("12D3KooTest").is_some());
    }

    #[test]
    fn connected_count_and_trust() {
        let t = Table::new(cfg(), None, None);
        let n = Neighbor { peer_id: "p1".into(), ..Default::default() };
        t.upsert(&n);
        t.set_connected("p1", true);
        assert_eq!(t.connected_count(), 1);
        t.record_success("p1");
        assert_eq!(t.trust_of("p1"), Trust::Untrusted);
    }

    #[test]
    fn remove_and_prune() {
        let t = Table::new(cfg(), None, None);
        t.upsert(&Neighbor { peer_id: "p1".into(), ..Default::default() });
        t.remove("p1");
        assert!(t.is_empty());
    }

    #[test]
    fn select_prefers_connected_with_skills() {
        let t = Table::new(cfg(), None, None);
        let mut n = Neighbor {
            peer_id: "p1".into(),
            skills: vec!["shell".into()],
            ..Default::default()
        };
        t.upsert(&n);
        t.set_connected("p1", true);
        n.peer_id = "p2".into();
        n.skills = vec!["python".into()];
        t.upsert(&n);
        let cands = t.select(&["shell".into()], None, "", 0, &HashSet::new());
        assert!(!cands.is_empty());
        assert_eq!(cands[0].neighbor.peer_id, "p1");
        assert!(t.any_with_skills(&["shell".into()]));
        assert!(!t.any_with_skills(&["rust".into()]));
    }
}

