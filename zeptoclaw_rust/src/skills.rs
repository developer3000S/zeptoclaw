//! Порт подсистемы реестра скиллов (Go: internal/skills + capability exchange).
//!
//! Хранит дескрипторы умений узла с монотонным `skills_version`, персистит
//! импорт из сети в `<data_dir>/skills.json`, формирует дельты для
//! `SkillsSyncRequest/SkillsSyncResponse` и проверяет подписи ответов пира.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::proto::{SkillDescriptor, SkillVersion, SkillsSyncRequest, SkillsSyncResponse};
use crate::security::Identity;
use crate::wire;

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Запись реестра: имя → дескриптор + локальная версия публикации.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Entry {
    pub descriptor: SkillDescriptor,
    pub version: i64,
}

/// Реестр скиллов узла (Go: skills.Registry + persist <data_dir>/skills.json).
pub struct Registry {
    self_id: String,
    identity: Arc<Identity>,
    persist: bool,
    path: PathBuf,
    import_limit: usize,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Свои умения (из конфига capabilities.skill_docs), name → entry.
    own: BTreeMap<String, Entry>,
    /// Импортированные из сети дескрипторы, key = "peer/name".
    imported: BTreeMap<String, SkillDescriptor>,
    epoch: i64,
}

impl Registry {
    pub fn new(cfg: &Config, identity: Arc<Identity>) -> Result<Arc<Self>> {
        let mut own = BTreeMap::new();
        for d in &cfg.capabilities.skill_docs {
            let mut desc = SkillDescriptor {
                name: d.name.clone(),
                version: 1,
                updated_at: now_unix(),
                digest: String::new(),
                description: d.description.clone(),
                models: d.models.clone(),
                attributes: d.attributes.clone(),
            };
            desc.digest = wire::skill_descriptor_digest(&desc);
            own.insert(
                d.name.clone(),
                Entry { descriptor: desc, version: 1 },
            );
        }
        let path = PathBuf::from(&cfg.node.data_dir).join("skills.json");
        let persist = cfg.capabilities.skill_exchange.persist;
        let import_limit = cfg.capabilities.skill_exchange.import_limit.max(0) as usize;
        let r = Arc::new(Self {
            self_id: identity.peer_id().to_string(),
            identity,
            persist,
            path,
            import_limit,
            inner: Mutex::new(Inner {
                own,
                imported: BTreeMap::new(),
                epoch: 1,
            }),
        });
        if persist {
            r.load();
        }
        Ok(r)
    }

    fn load(&self) {
        let Ok(data) = std::fs::read(&self.path) else { return };
        #[derive(Deserialize)]
        struct Disk {
            #[serde(default)]
            imported: BTreeMap<String, SkillDescriptor>,
            #[serde(default)]
            epoch: i64,
        }
        if let Ok(d) = serde_json::from_str::<Disk>(&String::from_utf8_lossy(&data)) {
            let mut g = self.inner.lock().unwrap();
            g.imported = d.imported;
            if d.epoch > g.epoch {
                g.epoch = d.epoch;
            }
        }
    }

    fn save(&self) {
        if !self.persist {
            return;
        }
        let g = self.inner.lock().unwrap();
        #[derive(Serialize)]
        struct Disk<'a> {
            imported: &'a BTreeMap<String, SkillDescriptor>,
            epoch: i64,
        }
        let json = serde_json::to_string_pretty(&Disk {
            imported: &g.imported,
            epoch: g.epoch,
        })
        .unwrap_or_default();
        let tmp = self.path.with_extension("json.tmp");
        if std::fs::write(&tmp, json).is_ok() {
            let _ = std::fs::rename(&tmp, &self.path);
        }
    }

    /// Текущая версия каталога (пишется в Capabilities.skills_version).
    pub fn version(&self) -> i64 {
        self.inner.lock().unwrap().epoch
    }

    /// Дескрипторы своих умений (для gossip Capabilities.skill_docs).
    pub fn own_docs(&self) -> Vec<SkillDescriptor> {
        let g = self.inner.lock().unwrap();
        g.own.values().map(|e| e.descriptor.clone()).collect()
    }

    /// Все известные имена умений (свои + импортированные) — для skills-ручек API.
    pub fn list(&self) -> Vec<SkillDescriptor> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<SkillDescriptor> = g.own.values().map(|e| e.descriptor.clone()).collect();
        out.extend(g.imported.values().cloned());
        out.sort_by(|a, b| a.name.cmp(&b.name).then(a.version.cmp(&b.version)));
        out
    }

    /// Путь персистентного кэша импортов (`<data_dir>/skills.json`).
    pub fn persist_path(&self) -> &std::path::Path {
        &self.path
    }

    /// Сформировать запрос синхронизации: что мы уже знаем о скиллах пира.
    pub fn build_request(&self, peer_id: &str, full: bool) -> SkillsSyncRequest {
        let g = self.inner.lock().unwrap();
        let prefix = format!("{peer_id}/");
        let known: Vec<SkillVersion> = g
            .imported
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, d)| SkillVersion {
                name: d.name.clone(),
                version: d.version,
                digest: d.digest.clone(),
            })
            .collect();
        let is_full = full || known.is_empty();
        SkillsSyncRequest {
            known,
            full: is_full,
        }
    }

    /// Обработать ответ пира: проверить подпись, импортировать дельту.
    /// Возвращает число обновлённых дескрипторов.
    pub fn apply_response(&self, resp: &SkillsSyncResponse) -> Result<usize> {
        if resp.peer_id.is_empty() {
            anyhow::bail!("skills: response without peer_id");
        }
        // Подпись покрывает каноническое тело (Go: VerifySignature(skills_sync_body)).
        let body = wire::skills_sync_body(resp);
        let pk = crate::security::extract_key_from_peer_id(&resp.peer_id)?;
        if crate::security::verify(&crate::security::marshal_public_key(&pk), &body, &resp.signature).is_err() {
            anyhow::bail!("skills: bad signature from {}", resp.peer_id);
        }
        let mut g = self.inner.lock().unwrap();
        let before = g.imported.len();
        for d in &resp.skills {
            if d.name.is_empty() {
                continue;
            }
            let key = format!("{}/{}", resp.peer_id, d.name);
            let cur_ver = g
                .imported
                .get(&key)
                .map(|x| x.version)
                .unwrap_or(-1);
            if d.version > cur_ver {
                g.imported.insert(key, d.clone());
            }
        }
        // Ограничение размера импорта (Go: import_limit).
        while g.imported.len() > self.import_limit {
            if let Some((k, _)) = g
                .imported
                .iter()
                .min_by_key(|(_, d)| (d.updated_at, d.version))
            {
                let k = k.clone();
                g.imported.remove(&k);
            } else {
                break;
            }
        }
        let added = g.imported.len().saturating_sub(before.min(g.imported.len()));
        let changed = resp.skills.iter().any(|d| {
            g.imported
                .get(&format!("{}/{}", resp.peer_id, d.name))
                .map(|x| x.version == d.version)
                .unwrap_or(false)
        });
        if changed || added > 0 {
            g.epoch += 1;
        }
        drop(g);
        self.save();
        Ok(added)
    }

    /// Подписать ответ дельтой для пира, приславшего `req`.
    pub fn build_response(&self, req: &SkillsSyncRequest) -> SkillsSyncResponse {
        let g = self.inner.lock().unwrap();
        let mut skills: Vec<SkillDescriptor> = Vec::new();
        for e in g.own.values() {
            let known = req.known.iter().any(|k| {
                k.name == e.descriptor.name && k.version >= e.descriptor.version
                    && (k.digest.is_empty() || k.digest == e.descriptor.digest)
            });
            if !known || req.full {
                skills.push(e.descriptor.clone());
            }
        }
        let mut resp = SkillsSyncResponse {
            skills,
            skills_version: g.epoch,
            peer_id: self.self_id.clone(),
            signature_scheme: "ed25519-v1".into(),
            ..Default::default()
        };
        let body = wire::skills_sync_body(&resp);
        resp.signature = self.identity.sign(&body);
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn cfg_with_docs(dir: &std::path::Path) -> Config {
        let mut cfg = Config::default();
        cfg.node.data_dir = dir.to_string_lossy().into_owned();
        cfg.capabilities.skill_docs = vec![crate::config::SkillDocConfig {
            name: "pdf-summarize".into(),
            version: 1,
            description: "summarize pdf".into(),
            models: vec!["llama3".into()],
            attributes: Default::default(),
        }];
        cfg
    }

    #[test]
    fn registry_roundtrip_delta_sync() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let id_a = Arc::new(Identity::load_or_generate(&dir_a.path().join("id.key")).unwrap());
        let id_b = Arc::new(Identity::load_or_generate(&dir_b.path().join("id.key")).unwrap());
        let reg_a = Registry::new(&cfg_with_docs(dir_a.path()), id_a.clone()).unwrap();
        let reg_b = Registry::new(&cfg_with_docs(dir_b.path()), id_b.clone()).unwrap();

        assert_eq!(reg_a.own_docs().len(), 1);
        assert!(!reg_a.own_docs()[0].digest.is_empty());

        // B просит у A всё, что не знает.
        let req = reg_b.build_request(id_a.peer_id(), true);
        let resp = reg_a.build_response(&req);
        assert_eq!(resp.skills.len(), 1);
        let n = reg_b.apply_response(&resp).unwrap();
        assert_eq!(n, 1);
        // list() = свои (cfg-доки) + импортированные от A.
        assert_eq!(reg_b.list().len(), 2);
        assert!(reg_b.version() > 1);

        // Повторная синхронизация — дельта пуста.
        let req2 = reg_b.build_request(id_a.peer_id(), false);
        let resp2 = reg_a.build_response(&req2);
        assert!(resp2.skills.is_empty(), "delta must be empty");
    }

    #[test]
    fn rejects_bad_signature() {
        let dir = tempfile::tempdir().unwrap();
        let id = Arc::new(Identity::load_or_generate(&dir.path().join("id.key")).unwrap());
        let reg = Registry::new(&cfg_with_docs(dir.path()), id.clone()).unwrap();
        let other = Identity::load_or_generate(&dir.path().join("other.key")).unwrap();
        let mut resp = reg.build_response(&SkillsSyncRequest { known: vec![], full: true });
        // Подписано нами, но peer_id подставлен чужой — подпись не сходится.
        resp.peer_id = other.peer_id().to_string();
        assert!(reg.apply_response(&resp).is_err());
    }
}
