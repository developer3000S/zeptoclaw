//! Порт пакета `internal/storage`. Хранит локальное состояние узла: журнал
//! задач, записи пиров, ключи дедупликации и content-addressed артефакты.
//!
//! Go-версия использует BadgerDB; Rust-порт реализует то же keyspace-хранилище
//! поверх in-BTree + JSONL-снимка на диске (формат: одна запись `{"k":..,"v":"<base64>"}`
//! на строку). Публичный API повторяет Go: префиксы бакетов, транзакционные
//! операции PutPeer/ClaimDedup, атомарная запись артефактов через tmp+rename.

use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ---- Префиксы бакетов (идентично Go) ----
const PFX_TASK: &str = "t:";
const PFX_RESULT: &str = "r:";
const PFX_PEER: &str = "p:";
const PFX_DEDUP: &str = "d:";
const PFX_SKILL: &str = "s:";
const PFX_META: &str = "m:";
const PFX_CHILD: &str = "c:";
const PFX_PARENT_OF: &str = "o:";

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ---- Записи (JSON-поля совпадают с Go-тегами) ----

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct TaskRecord {
    pub task_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub parent_task_id: String,
    #[serde(default)]
    pub origin_peer_id: String,
    #[serde(default)]
    pub sender_peer_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_skills: Vec<String>,
    #[serde(default)]
    pub received_at: f64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub finished_at: f64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub worker_peer_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub delegated_to: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub result_digest: String,
    #[serde(default)]
    pub attempts: i32,
    #[serde(default)]
    pub ttl: i32,
    #[serde(default)]
    pub priority: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub payload_hash: String,
    #[serde(default)]
    pub updated_at: i64,
}

fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ArtifactInfo {
    pub name: String,
    pub hash: String,
    #[serde(default)]
    pub size: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ResultRecord {
    pub task_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error_message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub worker_peer_id: String,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub started_at: f64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub finished_at: f64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactInfo>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signature: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_signature: Vec<u8>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub result_digest: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub aggregated: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error_class: String,
}

fn is_false(v: &bool) -> bool {
    !*v
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct PeerRecord {
    pub peer_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addrs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skills: Vec<String>,
    #[serde(default)]
    pub trust: String,
    #[serde(default)]
    pub category: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    #[serde(default)]
    pub load: f64,
    #[serde(default)]
    pub max_parallel: i32,
    #[serde(default)]
    pub running: i32,
    #[serde(default)]
    pub successes: u64,
    #[serde(default)]
    pub failures: u64,
    #[serde(default)]
    pub protocol_errors: u64,
    #[serde(default)]
    pub avg_rtt_ms: i64,
    #[serde(default)]
    pub seen_at: i64,
    #[serde(default)]
    pub connected: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_status: String,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub skills_version: i64,
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct DedupRecord {
    pub task_id: String,
    pub first_seen_unix: i64,
    #[serde(default)]
    pub count: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Stats {
    pub tasks: i64,
    pub peers: i64,
    pub dedup_keys: i64,
    pub opened_at: i64,
}

/// Опции открытия хранилища (аналог storage.Options).
pub struct Options {
    pub dir: PathBuf,
    pub artifacts_dir: PathBuf,
    pub in_memory: bool,
}

/// Внутреннее состояние: keyspace + путь снапшота.
struct Inner {
    kv: BTreeMap<Vec<u8>, Vec<u8>>,
    snapshot: Option<PathBuf>,
    opened_at: i64,
}

/// Store — хранилище состояния узла (клонировать можно, всё под одним замком).
#[derive(Clone)]
pub struct Store {
    inner: Arc<Mutex<Inner>>,
    art_root: PathBuf,
}

impl Store {
    /// Создаёт или загружает хранилище.
    pub fn open(opts: Options) -> Result<Self> {
        let art_root = opts.artifacts_dir.clone();
        if !opts.in_memory {
            std::fs::create_dir_all(&opts.dir).context("storage: mkdir db dir")?;
            std::fs::create_dir_all(&art_root).context("storage: mkdir artifacts")?;
        }
        let snapshot = if opts.in_memory {
            None
        } else {
            Some(opts.dir.join("store.jsonl"))
        };
        let mut kv = BTreeMap::new();
        if let Some(path) = &snapshot {
            if path.exists() {
                let data = std::fs::read_to_string(path).context("storage: read snapshot")?;
                for line in data.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let rec: SnapshotEntry = serde_json::from_str(line)
                        .with_context(|| format!("storage: corrupt snapshot line"))?;
                    kv.insert(base64_decode(&rec.k)?, base64_decode(&rec.v)?);
                }
            }
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                kv,
                snapshot,
                opened_at: now_unix(),
            })),
            art_root,
        })
    }

    /// Сохраняет снимок на диск (аналог flush у Badger; вызывается вместо Close).
    pub fn flush(&self) -> Result<()> {
        let g = self.inner.lock().unwrap();
        if let Some(path) = &g.snapshot {
            let mut buf = String::new();
            for (k, v) in &g.kv {
                let e = SnapshotEntry {
                    k: base64_encode(k),
                    v: base64_encode(v),
                };
                buf.push_str(&serde_json::to_string(&e)?);
                buf.push('\n');
            }
            // Атомарная замена: tmp + rename.
            let tmp = path.with_extension("jsonl.tmp");
            std::fs::write(&tmp, buf.as_bytes())?;
            std::fs::rename(&tmp, path)?;
        }
        Ok(())
    }

    fn set(&self, key: &[u8], val: Vec<u8>) {
        self.inner.lock().unwrap().kv.insert(key.to_vec(), val);
    }

    fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.lock().unwrap().kv.get(key).cloned()
    }

    fn delete(&self, key: &[u8]) -> bool {
        self.inner.lock().unwrap().kv.remove(key).is_some()
    }

    /// Итерация по префиксу в лексикографическом порядке (как Badger prefix-iterator).
    fn iterate(&self, prefix: &str, mut f: impl FnMut(&[u8], &[u8])) {
        let p = prefix.as_bytes();
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = {
            let g = self.inner.lock().unwrap();
            g.kv
                .range(p.to_vec()..)
                .take_while(|(k, _)| k.starts_with(p))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        for (k, v) in pairs {
            f(&k, &v);
        }
    }

    fn put_json<T: Serialize>(&self, key: &str, v: &T) -> Result<()> {
        let b = serde_json::to_vec(v).context("storage: marshal")?;
        self.set(key.as_bytes(), b);
        Ok(())
    }

    fn get_json<T: DeserializeOwned>(&self, key: &str) -> Result<T> {
        let b = self
            .get_raw(key.as_bytes())
            .ok_or_else(|| anyhow::anyhow!("storage: not found: {key}"))?;
        Ok(serde_json::from_slice(&b)?)
    }

    // ---- Журнал задач ----

    /// Upsert записи задачи; обновляет `updated_at`.
    pub fn put_task(&self, rec: &TaskRecord) -> Result<()> {
        if rec.task_id.is_empty() {
            bail!("storage: task record requires task_id");
        }
        let mut rec = rec.clone();
        rec.updated_at = now_unix();
        self.put_json(&format!("{PFX_TASK}{}", rec.task_id), &rec)
    }

    pub fn get_task(&self, task_id: &str) -> Result<TaskRecord> {
        self.get_json(&format!("{PFX_TASK}{task_id}"))
    }

    /// Список задач newest-first с опциональным фильтром по статусу.
    pub fn list_tasks(&self, limit: usize, status_filter: &str) -> Result<Vec<TaskRecord>> {
        let limit = if limit == 0 { 100 } else { limit };
        let mut out: Vec<TaskRecord> = Vec::new();
        self.iterate(PFX_TASK, |_, v| {
            if let Ok(rec) = serde_json::from_slice::<TaskRecord>(v) {
                // повреждённую запись терпим, а не роняем листинг (как в Go)
                if !status_filter.is_empty() && rec.status != status_filter {
                    return;
                }
                out.push(rec);
            }
        });
        out.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        out.truncate(limit);
        Ok(out)
    }

    pub fn put_result(&self, rec: &ResultRecord) -> Result<()> {
        if rec.task_id.is_empty() {
            bail!("storage: result record requires task_id");
        }
        self.put_json(&format!("{PFX_RESULT}{}", rec.task_id), rec)
    }

    pub fn get_result(&self, task_id: &str) -> Result<ResultRecord> {
        self.get_json(&format!("{PFX_RESULT}{task_id}"))
    }

    // ---- Пиры и индекс навыков ----

    fn normalize_skill(s: &str) -> String {
        s.trim().to_lowercase()
    }

    fn normalized_skills(skills: &[String]) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for sk in skills {
            let n = Self::normalize_skill(sk);
            if n.is_empty() || !seen.insert(n.clone()) {
                continue;
            }
            out.push(n);
        }
        out
    }

    fn skill_key(skill: &str, peer_id: &str) -> String {
        format!("{PFX_SKILL}{skill}/{peer_id}")
    }

    /// Upsert записи пира с перестройкой индекса навыков (одна «транзакция»).
    pub fn put_peer(&self, rec: &PeerRecord) -> Result<()> {
        if rec.peer_id.is_empty() {
            bail!("storage: peer record requires peer_id");
        }
        let mut rec = rec.clone();
        rec.seen_at = now_unix();
        // Удаляем прежние индексные записи (включая ненормализованные имена).
        if let Some(prev_bytes) = self.get_raw(format!("{PFX_PEER}{}", rec.peer_id).as_bytes()) {
            if let Ok(prev) = serde_json::from_slice::<PeerRecord>(&prev_bytes) {
                for sk in &prev.skills {
                    for name in [sk.as_str(), Self::normalize_skill(sk).as_str()] {
                        if name.is_empty() {
                            continue;
                        }
                        self.delete(Self::skill_key(name, &rec.peer_id).as_bytes());
                    }
                }
            }
        }
        self.put_json(&format!("{PFX_PEER}{}", rec.peer_id), &rec)?;
        for sk in Self::normalized_skills(&rec.skills) {
            self.set(Self::skill_key(&sk, &rec.peer_id).as_bytes(), vec![1]);
        }
        Ok(())
    }

    pub fn get_peer(&self, peer_id: &str) -> Result<PeerRecord> {
        self.get_json(&format!("{PFX_PEER}{peer_id}"))
    }

    pub fn list_peers(&self) -> Result<Vec<PeerRecord>> {
        let mut out = Vec::new();
        self.iterate(PFX_PEER, |_, v| {
            if let Ok(rec) = serde_json::from_slice::<PeerRecord>(v) {
                out.push(rec);
            }
        });
        Ok(out)
    }

    /// Пиры по навыку; запрос нормализуется так же, как при записи.
    pub fn peers_by_skill(&self, skill: &str) -> Result<Vec<String>> {
        let pre = format!("{PFX_SKILL}{}/", Self::normalize_skill(skill));
        let mut out = Vec::new();
        self.iterate(&pre, |k, _| {
            let ks = String::from_utf8_lossy(k).to_string();
            out.push(ks[pre.len()..].to_string());
        });
        Ok(out)
    }

    /// Обновляет timestamp живости пира (молча пропускает отсутствующего).
    pub fn mark_seen(&self, peer_id: &str) -> Result<()> {
        let key = format!("{PFX_PEER}{peer_id}");
        if let Some(bytes) = self.get_raw(key.as_bytes()) {
            if let Ok(mut rec) = serde_json::from_slice::<PeerRecord>(&bytes) {
                rec.seen_at = now_unix();
                self.put_json(&key, &rec)?;
            }
        }
        Ok(())
    }

    // ---- Дедупликация ----

    /// Атомарно резервирует task_id в окне дедупликации.
    /// Возвращает (claimed_first, previous_record).
    pub fn claim_dedup(&self, task_id: &str, window: Duration) -> Result<(bool, DedupRecord)> {
        if task_id.is_empty() {
            bail!("storage: empty task id");
        }
        let now = now_unix();
        let key = format!("{PFX_DEDUP}{task_id}");
        let mut prev = DedupRecord::default();
        let mut guard = self.inner.lock().unwrap();
        if let Some(bytes) = guard.kv.get(key.as_bytes()) {
            let rec: DedupRecord = serde_json::from_slice(bytes)?;
            prev = rec;
            if now - prev.first_seen_unix < window.as_secs() as i64 {
                // внутри окна: дубликат доставки
                drop(guard);
                return Ok((false, prev));
            }
        }
        let rec = DedupRecord {
            task_id: task_id.to_string(),
            first_seen_unix: now,
            count: prev.count + 1,
        };
        guard
            .kv
            .insert(key.into_bytes(), serde_json::to_vec(&rec)?);
        drop(guard);
        Ok((true, prev))
    }

    // ---- Дерево подзадач ----

    pub fn link_child(&self, parent_id: &str, child_id: &str) -> Result<()> {
        self.set(
            format!("{PFX_CHILD}{parent_id}/{child_id}").as_bytes(),
            vec![1],
        );
        self.set(
            format!("{PFX_PARENT_OF}{child_id}").as_bytes(),
            parent_id.as_bytes().to_vec(),
        );
        Ok(())
    }

    pub fn children_of(&self, parent_id: &str) -> Result<Vec<String>> {
        let pre = format!("{PFX_CHILD}{parent_id}/");
        let mut out = Vec::new();
        self.iterate(&pre, |k, _| {
            let ks = String::from_utf8_lossy(k).to_string();
            out.push(ks[pre.len()..].to_string());
        });
        Ok(out)
    }

    pub fn parent_of(&self, child_id: &str) -> Result<String> {
        let b = self
            .get_raw(format!("{PFX_PARENT_OF}{child_id}").as_bytes())
            .ok_or_else(|| anyhow::anyhow!("storage: not found"))?;
        Ok(String::from_utf8(b)?)
    }

    // ---- Мета ----

    pub fn put_meta(&self, key: &str, val: &[u8]) -> Result<()> {
        self.set(format!("{PFX_META}{key}").as_bytes(), val.to_vec());
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Vec<u8>> {
        self.get_raw(format!("{PFX_META}{key}").as_bytes())
            .ok_or_else(|| anyhow::anyhow!("storage: not found"))
    }

    // ---- Артефакты ----

    /// Пишет content-addressed байты, возвращает ("sha256:<hex>", размер).
    pub fn store_artifact(&self, data: &[u8]) -> Result<(String, i64)> {
        let digest = Sha256::digest(data);
        let name = hex::encode(digest);
        let hash = format!("sha256:{name}");
        let dir = self.art_root.join("sha256").join(&name[..2]);
        std::fs::create_dir_all(&dir).context("storage: artifact dir")?;
        let path = dir.join(&name);
        if path.exists() {
            return Ok((hash, data.len() as i64));
        }
        let tmp = path.with_file_name(format!("{name}.tmp"));
        std::fs::write(&tmp, data)?;
        match std::fs::rename(&tmp, &path) {
            Ok(()) => {}
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(e).context("storage: rename artifact");
            }
        }
        Ok((hash, data.len() as i64))
    }

    pub fn load_artifact(&self, hash: &str) -> Result<Vec<u8>> {
        let path = self.artifact_path(hash)?;
        std::fs::read(&path).with_context(|| format!("storage: artifact {hash} not found"))
    }

    /// Разрешает хэш артефакта в путь на диске с валидацией формата.
    pub fn artifact_path(&self, hash: &str) -> Result<PathBuf> {
        let name = hash.strip_prefix("sha256:").unwrap_or(hash);
        if name.len() != 64 || hex::decode(name).is_err() {
            bail!("storage: malformed artifact hash {hash:?}");
        }
        Ok(self.art_root.join("sha256").join(&name[..2]).join(name))
    }

    // ---- Обслуживание ----

    /// Удаляет завершённые журнальные записи старше `keep`; возвращает число удалений.
    pub fn prune(&self, keep: Duration) -> Result<usize> {
        let keep = if keep.is_zero() {
            Duration::from_secs(7 * 24 * 3600)
        } else {
            keep
        };
        let cutoff = now_unix() - keep.as_secs() as i64;
        let mut del: Vec<Vec<u8>> = Vec::new();
        self.iterate(PFX_TASK, |k, v| {
            if let Ok(rec) = serde_json::from_slice::<TaskRecord>(v) {
                if rec.finished_at != 0.0 && (rec.finished_at as i64) < cutoff {
                    del.push(k.to_vec());
                    del.push(format!("{PFX_RESULT}{}", rec.task_id).into_bytes());
                }
            }
        });
        let dedup_cutoff = now_unix() - keep.as_secs().max(15 * 60) as i64;
        self.iterate(PFX_DEDUP, |k, v| {
            if let Ok(rec) = serde_json::from_slice::<DedupRecord>(v) {
                if rec.first_seen_unix < dedup_cutoff {
                    del.push(k.to_vec());
                }
            }
        });
        let mut removed = 0usize;
        {
            let mut g = self.inner.lock().unwrap();
            for k in &del {
                if g.kv.remove(k).is_some() {
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    /// Приблизительные счётчики записей для админ-API.
    pub fn stats(&self) -> Result<Stats> {
        let count = |pfx: &str| -> i64 {
            let mut n = 0i64;
            self.iterate(pfx, |_, _| n += 1);
            n
        };
        Ok(Stats {
            tasks: count(PFX_TASK),
            peers: count(PFX_PEER),
            dedup_keys: count(PFX_DEDUP),
            opened_at: self.inner.lock().unwrap().opened_at,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct SnapshotEntry {
    k: String,
    v: String,
}

// ---- Минимальный base64 (стандартный алфавит, с паддингом) ----

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { B64[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { B64[n as usize & 63] as char } else { '=' });
    }
    out
}

fn base64_decode(s: &str) -> Result<Vec<u8>> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut buf: u32 = 0;
    let mut bits = 0u32;
    for &b in bytes {
        if b == b'=' {
            break;
        }
        let v = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => bail!("storage: invalid base64 byte"),
        };
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem_store() -> Store {
        Store::open(Options {
            dir: PathBuf::from("/unused"),
            artifacts_dir: PathBuf::from("/unused/artifacts"),
            in_memory: true,
        })
        .unwrap()
    }

    #[test]
    fn task_journal_roundtrip_and_listing() {
        let s = mem_store();
        s.put_task(&TaskRecord {
            task_id: "t1".into(),
            status: "done".into(),
            ..Default::default()
        })
        .unwrap();
        s.put_task(&TaskRecord {
            task_id: "t2".into(),
            status: "failed".into(),
            ..Default::default()
        })
        .unwrap();
        let rec = s.get_task("t1").unwrap();
        assert_eq!(rec.status, "done");
        assert!(rec.updated_at > 0);
        let all = s.list_tasks(10, "").unwrap();
        assert_eq!(all.len(), 2);
        let done = s.list_tasks(10, "done").unwrap();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].task_id, "t1");
    }

    #[test]
    fn put_task_requires_id() {
        let s = mem_store();
        assert!(s.put_task(&TaskRecord::default()).is_err());
    }

    #[test]
    fn peer_skill_index_is_normalized_and_rebuilt() {
        let s = mem_store();
        s.put_peer(&PeerRecord {
            peer_id: "p1".into(),
            skills: vec![" OCR ".into(), "ocr".into(), "Search".into()],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(s.peers_by_skill("OCR").unwrap(), vec!["p1".to_string()]);
        assert_eq!(s.peers_by_skill(" search ").unwrap(), vec!["p1".to_string()]);
        // Перезапись без "search" должна убрать его из индекса.
        s.put_peer(&PeerRecord {
            peer_id: "p1".into(),
            skills: vec!["ocr".into()],
            ..Default::default()
        })
        .unwrap();
        assert!(s.peers_by_skill("search").unwrap().is_empty());
        assert_eq!(s.peers_by_skill("ocr").unwrap(), vec!["p1".to_string()]);
    }

    #[test]
    fn dedup_window_claims_once() {
        let s = mem_store();
        let w = Duration::from_secs(60);
        let (first, _) = s.claim_dedup("t1", w).unwrap();
        assert!(first);
        let (dup, prev) = s.claim_dedup("t1", w).unwrap();
        assert!(!dup);
        assert_eq!(prev.task_id, "t1");
    }

    #[test]
    fn parent_child_links() {
        let s = mem_store();
        s.link_child("p", "c1").unwrap();
        s.link_child("p", "c2").unwrap();
        assert_eq!(s.children_of("p").unwrap(), vec!["c1", "c2"]);
        assert_eq!(s.parent_of("c1").unwrap(), "p");
        assert!(s.parent_of("missing").is_err());
    }

    #[test]
    fn artifacts_are_content_addressed() {
        let dir = std::env::temp_dir().join(format!("zepto-art-{}-{}", now_unix(), std::process::id()));
        let s = Store::open(Options {
            dir: dir.join("db"),
            artifacts_dir: dir.join("art"),
            in_memory: false,
        })
        .unwrap();
        let data = b"hello world".to_vec();
        let (hash, size) = s.store_artifact(&data).unwrap();
        assert!(hash.starts_with("sha256:"));
        assert_eq!(size, 11);
        assert_eq!(s.load_artifact(&hash).unwrap(), data);
        assert!(s.artifact_path("sha256:zz").is_err());
        s.flush().unwrap();
        // Снимок перезагружается.
        let s2 = Store::open(Options {
            dir: dir.join("db"),
            artifacts_dir: dir.join("art"),
            in_memory: false,
        })
        .unwrap();
        s2.put_task(&TaskRecord { task_id: "x".into(), ..Default::default() })
            .unwrap();
        s2.flush().unwrap();
        let s3 = Store::open(Options {
            dir: dir.join("db"),
            artifacts_dir: dir.join("art"),
            in_memory: false,
        })
        .unwrap();
        assert_eq!(s3.get_task("x").unwrap().task_id, "x");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn base64_matches_std() {
        // известные векторы RFC 4648
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        for s in ["", "Zg==", "Zm9vYmFy"] {
            let dec = base64_decode(s).unwrap();
            assert_eq!(base64_encode(&dec), s);
        }
    }
}
