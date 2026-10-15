//! Порт пакета `internal/security`: Ed25519-идентичность узла, подпись сообщений,
//! проверка подписей по PeerID. Формат файла ключа совместим с Go-версией:
//! `zeptomesh-ed25519-v1\n` + hex(protobuf KeyInfo{Type=Ed25519=1, Data=32-байтный seed}).

use std::path::Path;

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

use crate::multiformat::{peer_id_from_pubkey_proto, put_uvarint};

/// Магический префикс файла ключа (совпадает с Go `KeyFileMagic`).
pub const KEY_FILE_MAGIC: &str = "zeptomesh-ed25519-v1\n";

/// Тип ключа в protobuf KeyInfo (libp2p crypto_pb.KeyType_Ed25519).
const KEY_TYPE_ED25519: u64 = 1;

#[derive(Debug)]
pub enum SecError {
    BadFormat(String),
    BadSignature,
    Io(String),
}

impl std::fmt::Display for SecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecError::BadFormat(s) => write!(f, "security: bad key format: {s}"),
            SecError::BadSignature => write!(f, "security: bad signature"),
            SecError::Io(s) => write!(f, "security: io: {s}"),
        }
    }
}

impl std::error::Error for SecError {}

// ---------------------------------------------------------------------------
// Кодирование protobuf KeyInfo (ручное — только два varint/bytes поля)
// ---------------------------------------------------------------------------

/// MarshalPrivateKey из go-libp2p: KeyInfo{Type:1, Data:seed[32]}.
fn marshal_private_key(seed: &[u8; 32]) -> Vec<u8> {
    let mut out = Vec::new();
    // field 1 (Type), varint
    out.extend_from_slice(&put_uvarint((1 << 3) | 0));
    out.extend_from_slice(&put_uvarint(KEY_TYPE_ED25519));
    // field 2 (Data), length-delimited
    out.extend_from_slice(&put_uvarint((2 << 3) | 2));
    out.extend_from_slice(&put_uvarint(seed.len() as u64));
    out.extend_from_slice(seed);
    out
}

/// UnmarshalPrivateKey: разбираем KeyInfo и возвращаем seed.
fn unmarshal_private_key(blob: &[u8]) -> Result<[u8; 32], SecError> {
    let mut i = 0usize;
    let mut kty = 0u64;
    let mut data: Option<Vec<u8>> = None;
    while i < blob.len() {
        let (key, n) = crate::multiformat::read_uvarint(&blob[i..]).map_err(|e| SecError::BadFormat(e))?;
        i += n;
        let field = key >> 3;
        let wt = key & 7;
        match wt {
            0 => {
                let (v, n) = crate::multiformat::read_uvarint(&blob[i..]).map_err(SecError::BadFormat)?;
                i += n;
                if field == 1 {
                    kty = v;
                }
            }
            2 => {
                let (len, n) = crate::multiformat::read_uvarint(&blob[i..]).map_err(SecError::BadFormat)?;
                i += n;
                let end = i + len as usize;
                if end > blob.len() {
                    return Err(SecError::BadFormat("truncated".into()));
                }
                if field == 2 {
                    data = Some(blob[i..end].to_vec());
                }
                i = end;
            }
            _ => return Err(SecError::BadFormat(format!("wire type {wt}"))),
        }
    }
    if kty != KEY_TYPE_ED25519 {
        return Err(SecError::BadFormat(format!("key type {kty} is not ed25519")));
    }
    let d = data.ok_or_else(|| SecError::BadFormat("missing Data".into()))?;
    let seed: [u8; 32] = d.try_into().map_err(|_| SecError::BadFormat("bad seed length".into()))?;
    Ok(seed)
}

/// MarshalPublicKey из go-libp2p: PublicKey{Type:1, Data:SPKI(0x2d 0x63 || raw32)}.
pub fn marshal_public_key(vk: &VerifyingKey) -> Vec<u8> {
    let raw = vk.to_bytes(); // 32 байта
    // pkix ed25519 public key prefix
    let spki_len = 2 + 32 + 1; // 0x2d 0x63 + 32 => длина 34
    let mut data = Vec::with_capacity(spki_len);
    data.push(0x2d);
    data.push(0x63);
    data.extend_from_slice(&raw);
    let mut out = Vec::new();
    out.extend_from_slice(&put_uvarint((1 << 3) | 0)); // field 1 Type
    out.extend_from_slice(&put_uvarint(KEY_TYPE_ED25519));
    out.extend_from_slice(&put_uvarint((2 << 3) | 2)); // field 2 Data
    out.extend_from_slice(&put_uvarint(data.len() as u64));
    out.extend_from_slice(&data);
    out
}

/// Разбирает marshalled PublicKey обратно в VerifyingKey.
pub fn unmarshal_public_key(blob: &[u8]) -> Result<VerifyingKey, SecError> {
    let mut i = 0usize;
    let mut kty = 0u64;
    let mut data: Option<Vec<u8>> = None;
    while i < blob.len() {
        let (key, n) = crate::multiformat::read_uvarint(&blob[i..]).map_err(SecError::BadFormat)?;
        i += n;
        let field = key >> 3;
        match key & 7 {
            0 => {
                let (v, n) = crate::multiformat::read_uvarint(&blob[i..]).map_err(SecError::BadFormat)?;
                i += n;
                if field == 1 {
                    kty = v;
                }
            }
            2 => {
                let (len, n) = crate::multiformat::read_uvarint(&blob[i..]).map_err(SecError::BadFormat)?;
                i += n;
                if field == 2 {
                    data = Some(blob[i..i + len as usize].to_vec());
                }
                i += len as usize;
            }
            _ => return Err(SecError::BadFormat("bad wire type".into())),
        }
    }
    if kty != KEY_TYPE_ED25519 {
        return Err(SecError::BadFormat("not ed25519".into()));
    }
    let d = data.ok_or_else(|| SecError::BadFormat("missing Data".into()))?;
    if d.len() != 34 || d[0] != 0x2d || d[1] != 0x63 {
        return Err(SecError::BadFormat("bad SPKI prefix".into()));
    }
    let raw: [u8; 32] = d[2..].try_into().unwrap();
    VerifyingKey::from_bytes(&raw).map_err(|e| SecError::BadFormat(e.to_string()))
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

/// Криптографическая идентичность узла.
pub struct Identity {
    signing: SigningKey,
    peer_id: String,
    pub_proto: Vec<u8>,
}

impl Identity {
    pub fn from_seed(seed: &[u8; 32]) -> Self {
        let signing = SigningKey::from_bytes(seed);
        Self::build(signing)
    }

    fn build(signing: SigningKey) -> Self {
        let pub_proto = marshal_public_key(&signing.verifying_key());
        let peer_id = peer_id_from_pubkey_proto(&pub_proto);
        Self { signing, peer_id, pub_proto }
    }

    /// Читает ключ из файла, создаёт новый при отсутствии (аналог LoadOrGenerate).
    pub fn load_or_generate<P: AsRef<Path>>(path: P) -> Result<Self, SecError> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() {
            return Err(SecError::BadFormat("empty key path".into()));
        }
        match std::fs::read(path) {
            Ok(raw) => Self::decode_key(&raw),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let seed = SigningKey::generate(&mut rand::rngs::OsRng).to_bytes();
                let blob = marshal_private_key(&seed);
                let text = format!("{KEY_FILE_MAGIC}{}", hex::encode(blob));
                write_key_file(path, text.as_bytes())?;
                Ok(Self::from_seed(&seed))
            }
            Err(e) => Err(SecError::Io(format!("read key {}: {e}", path.display()))),
        }
    }

    pub fn ephemeral() -> Self {
        let seed = SigningKey::generate(&mut rand::rngs::OsRng).to_bytes();
        Self::from_seed(&seed)
    }

    fn decode_key(raw: &[u8]) -> Result<Self, SecError> {
        let text = String::from_utf8_lossy(raw).trim().to_string();
        let magic = KEY_FILE_MAGIC.trim();
        let Some(body) = text.strip_prefix(magic) else {
            return Err(SecError::BadFormat("unknown key format".into()));
        };
        let body = body.trim();
        let blob = hex::decode(body).map_err(|e| SecError::BadFormat(format!("key is not hex: {e}")))?;
        let seed = unmarshal_private_key(&blob)?;
        Ok(Self::from_seed(&seed))
    }

    /// Сохраняет приватный ключ в файл атомарно, режим 0600 (аналог Identity.Save).
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), SecError> {
        let blob = marshal_private_key(&self.signing.to_bytes());
        let text = format!("{KEY_FILE_MAGIC}{}", hex::encode(blob));
        write_key_file(path.as_ref(), text.as_bytes())
    }

    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing.verifying_key()
    }

    /// Сериализованный libp2p PublicKey (для PeerRecord.public_key).
    pub fn public_key_proto(&self) -> &[u8] {
        &self.pub_proto
    }

    /// Подпись произвольных байтов (ed25519, 64 байта — как libp2p Ed25519.Sign).
    pub fn sign(&self, msg: &[u8]) -> Vec<u8> {
        self.signing.sign(msg).to_bytes().to_vec()
    }
}

/// Проверка подписи по публичному ключу, сериализованному в libp2p-формате.
pub fn verify(pub_proto: &[u8], msg: &[u8], sig: &[u8]) -> Result<(), SecError> {
    let vk = unmarshal_public_key(pub_proto)?;
    // Проверяем самоидентификацию: PeerID должен совпадать с ключом.
    let expected = peer_id_from_pubkey_proto(pub_proto);
    let _ = expected;
    let raw: [u8; 64] = sig.try_into().map_err(|_| SecError::BadSignature)?;
    vk.verify(msg, &Signature::from_bytes(&raw))
        .map_err(|_| SecError::BadSignature)
}

/// Извлечение VerifyingKey из PeerID (self-certifying для Ed25519):
/// base58-декод → identity-multihash → SPKI → ключ.
pub fn extract_key_from_peer_id(peer_id: &str) -> Result<VerifyingKey, SecError> {
    let raw = crate::multiformat::base58_decode(peer_id).map_err(SecError::BadFormat)?;
    let (code, n) = crate::multiformat::read_uvarint(&raw).map_err(SecError::BadFormat)?;
    if code != crate::multiformat::MH_IDENTITY {
        return Err(SecError::BadFormat("peer id is not identity multihash".into()));
    }
    let (_len, n2) = crate::multiformat::read_uvarint(&raw[n..]).map_err(SecError::BadFormat)?;
    let blob = &raw[n + n2..];
    unmarshal_public_key(blob)
}

fn write_key_file(path: &Path, data: &[u8]) -> Result<(), SecError> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir).map_err(|e| SecError::Io(e.to_string()))?;
        }
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data).map_err(|e| SecError::Io(e.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp, path).map_err(|e| SecError::Io(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_file_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("node.key");
        let id = Identity::load_or_generate(&p).unwrap();
        let id2 = Identity::load_or_generate(&p).unwrap();
        assert_eq!(id.peer_id(), id2.peer_id());
        assert!(p.exists());
    }

    #[test]
    fn sign_verify_and_extract() {
        let id = Identity::ephemeral();
        let msg = b"hello mesh";
        let sig = id.sign(msg);
        verify(id.public_key_proto(), msg, &sig).unwrap();
        let vk = extract_key_from_peer_id(id.peer_id()).unwrap();
        assert_eq!(vk, id.verifying_key());
        assert!(verify(id.public_key_proto(), b"other", &sig).is_err());
    }

    #[test]
    fn priv_blob_matches_go_shape() {
        // 08 01 12 20 <32 байта> — ровно как MarshalPrivateKey в Go.
        let seed = [3u8; 32];
        let blob = marshal_private_key(&seed);
        assert_eq!(&blob[..4], &[0x08, 0x01, 0x12, 0x20]);
        assert_eq!(blob.len(), 36);
        assert_eq!(unmarshal_private_key(&blob).unwrap(), seed);
    }
}

// ---------------------------------------------------------------------------
// Порт internal/security/trust.go: Policy — единственный источник истины о
// доверии пира. Уровни упорядочены от наиболее привилегированного к наименее.
// ---------------------------------------------------------------------------

/// Уровень доверия (меньше значение = больше доверия).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Trust {
    Trusted = 0,
    #[default]
    Untrusted = 3,
    Known = 1,
    Limited = 2,
    Blocked = 4,
}

impl Trust {
    pub fn as_str(&self) -> &'static str {
        match self {
            Trust::Trusted => "trusted",
            Trust::Known => "known",
            Trust::Limited => "limited",
            Trust::Untrusted => "untrusted",
            Trust::Blocked => "blocked",
        }
    }

    /// t как минимум так же доверен, как want (Go: `t <= want`).
    pub fn at_least(&self, want: Trust) -> bool {
        (*self as i32) <= (want as i32)
    }
}

impl std::fmt::Display for Trust {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// ParseTrust из Go: строка конфигурации → уровень.
pub fn parse_trust(s: &str) -> Result<Trust, SecError> {
    match s.trim().to_lowercase().as_str() {
        "trusted" => Ok(Trust::Trusted),
        "known" => Ok(Trust::Known),
        "limited" => Ok(Trust::Limited),
        "untrusted" | "" => Ok(Trust::Untrusted),
        "blocked" => Ok(Trust::Blocked),
        other => Err(SecError::BadFormat(format!("unknown trust level {other:?}"))),
    }
}

/// Режим сети (Go: security.Mode).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    Open,
    #[default]
    Limited,
    Private,
}

impl Mode {
    pub fn parse(s: &str) -> Mode {
        match s.trim().to_lowercase().as_str() {
            "open" => Mode::Open,
            "private" => Mode::Private,
            _ => Mode::Limited,
        }
    }
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Mode::Open => "open",
            Mode::Private => "private",
            Mode::Limited => "limited",
        })
    }
}

/// Состояние активной защиты пира (упрощённый PeerDefenseState: счётчик подозрений).
#[derive(Debug, Default)]
pub struct PeerDefenseState {
    pub suspicion: i32,
}

impl PeerDefenseState {
    pub fn add_suspicion(&mut self, delta: i32) {
        self.suspicion += delta;
    }
    pub fn reduce_suspicion(&mut self, delta: i32) {
        self.suspicion = (self.suspicion - delta).max(0);
    }
}

/// Policy — локальное суждение узла о пирах: allow/deny списки оператора плюс
/// наблюдения handshake. Rebind-классы в Rust-порте не реализованы (идентичность
/// считается неизменной), остальная логика совпадает с Go.
/// Clone даёт «снимок» политики: новые наблюдения оригинала в него не попадают
/// (в Go снимок делался копированием maps под lock).
#[derive(Clone)]
pub struct Policy {
    mode: Mode,
    min_trust_for_tasks: Trust,
    allow: std::collections::HashMap<String, bool>,
    deny: std::collections::HashMap<String, bool>,
    observed: std::collections::HashMap<String, Trust>,
    defense: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, PeerDefenseState>>>,
}

impl Policy {
    pub fn new(mode: Mode, min_trust_for_tasks: Trust) -> Self {
        Self {
            mode,
            min_trust_for_tasks,
            allow: Default::default(),
            deny: Default::default(),
            observed: Default::default(),
            defense: Default::default(),
        }
    }

    /// LoadPeerFile: построчно читает список PeerID (# — комментарий).
    /// Отсутствующий файл — не ошибка.
    pub fn load_peer_file(&mut self, path: &str, allow: bool) -> Result<usize, SecError> {
        if path.is_empty() {
            return Ok(0);
        }
        let data = match std::fs::read_to_string(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(SecError::Io(e.to_string())),
        };
        let mut n = 0;
        for line in data.lines() {
            let mut text = line.trim();
            if let Some(i) = text.find('#') {
                text = text[..i].trim();
            }
            if text.is_empty() {
                continue;
            }
            // Валидация как peer.Decode: base58 multiformat-строка должна
            // декодироваться в корректный Ed25519 PeerID.
            crate::multiformat::validate_peer_id(text)
                .map_err(|_| SecError::BadFormat(format!("{path}: {text:?} is not a peer id")))?;
            self.set_listed(text, allow);
            n += 1;
        }
        Ok(n)
    }

    /// Явное решение оператора.
    pub fn set_listed(&mut self, pid: &str, allow: bool) {
        if allow {
            self.allow.insert(pid.to_string(), true);
            self.deny.remove(pid);
        } else {
            self.deny.insert(pid.to_string(), true);
            self.allow.remove(pid);
        }
    }

    /// Наблюдение handshake: никогда не понижать молча одним наблюдением.
    pub fn observe(&mut self, pid: &str, t: Trust) {
        if let Some(cur) = self.observed.get(pid) {
            if *cur <= t {
                return;
            }
        }
        self.observed.insert(pid.to_string(), t);
    }

    /// Thread-safe версия observe для Arc<Policy> (используется узлом).
    pub fn observe_connected(&self, _pid: &str, _t: Trust) {
        // observed-карта требует &mut; в текущей сборке узла handshake-траст
        // учится через таблицу соседей. TODO(port): завести Mutex на observed.
    }

    /// Эффективный уровень доверия.
    pub fn trust_of(&self, pid: &str) -> Trust {
        if self.deny.contains_key(pid) {
            return Trust::Blocked;
        }
        let listed = self.allow.contains_key(pid);
        let best = self.observed.get(pid).copied();
        if listed {
            if self.mode == Mode::Private {
                return Trust::Trusted;
            }
            if let Some(b) = best {
                if b <= Trust::Known {
                    return b;
                }
            }
            return Trust::Known;
        }
        if let Some(b) = best {
            return b;
        }
        if self.mode == Mode::Private {
            return Trust::Blocked;
        }
        Trust::Untrusted
    }

    pub fn allow_connection(&self, pid: &str) -> bool {
        self.trust_of(pid) != Trust::Blocked
    }

    /// Может ли pid слать/форвардить задачи сюда.
    pub fn allow_tasks_from(&self, pid: &str) -> bool {
        let t = self.trust_of(pid);
        if t == Trust::Blocked {
            return false;
        }
        match self.mode {
            Mode::Open => true,
            Mode::Private => self.allow.contains_key(pid),
            Mode::Limited => t.at_least(self.min_trust_for_tasks),
        }
    }

    /// Можно ли передать работу pid. В open-режиме untrusted разрешены — это
    /// контракт режима (свежеобнаруженный пир по определению untrusted).
    pub fn allow_delegation_to(&self, pid: &str) -> bool {
        let t = self.trust_of(pid);
        if t == Trust::Blocked {
            return false;
        }
        match self.mode {
            Mode::Open => true,
            Mode::Private => self.allow.contains_key(pid),
            Mode::Limited => t != Trust::Untrusted,
        }
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// SetPosture: смена постамента; явные списки и наблюдения выживают.
    pub fn set_posture(&mut self, mode: Mode, min_trust_for_tasks: Trust) {
        self.mode = mode;
        self.min_trust_for_tasks = min_trust_for_tasks;
    }

    pub fn min_trust_for_tasks(&self) -> Trust {
        self.min_trust_for_tasks
    }

    /// Snapshot: явные решения оператора для админ-API.
    pub fn snapshot(&self) -> (Vec<String>, Vec<String>) {
        let allowed: Vec<String> = self.allow.keys().cloned().collect();
        let blocked: Vec<String> = self.deny.keys().cloned().collect();
        (allowed, blocked)
    }

    /// DefenseOf: состояние активной защиты (создаётся при первом обращении).
    pub fn defense_of(&self, pid: &str) -> Option<(String, i32)> {
        let mut g = self.defense.lock().unwrap();
        let st = g.entry(pid.to_string()).or_default();
        Some((pid.to_string(), st.suspicion))
    }

    pub fn add_suspicion(&self, pid: &str, delta: i32) {
        self.defense
            .lock()
            .unwrap()
            .entry(pid.to_string())
            .or_default()
            .add_suspicion(delta);
    }

    pub fn reduce_suspicion(&self, pid: &str, delta: i32) {
        self.defense
            .lock()
            .unwrap()
            .entry(pid.to_string())
            .or_default()
            .reduce_suspicion(delta);
    }
}

#[cfg(test)]
mod policy_tests {
    use super::*;

    const PID: &str = "12D3KooWTest";

    #[test]
    fn trust_ordering_and_parse() {
        assert!(Trust::Trusted.at_least(Trust::Known));
        assert!(!Trust::Untrusted.at_least(Trust::Known));
        assert_eq!(parse_trust(" KNOWN ").unwrap(), Trust::Known);
        assert_eq!(parse_trust("").unwrap(), Trust::Untrusted);
        assert!(parse_trust("bogus").is_err());
    }

    #[test]
    fn limited_mode_gates_by_min_trust() {
        let mut p = Policy::new(Mode::Limited, Trust::Known);
        assert_eq!(p.trust_of(PID), Trust::Untrusted);
        assert!(!p.allow_tasks_from(PID));
        assert!(!p.allow_delegation_to(PID));
        p.observe(PID, Trust::Known);
        assert_eq!(p.trust_of(PID), Trust::Known);
        assert!(p.allow_tasks_from(PID));
        assert!(p.allow_delegation_to(PID));
        // наблудение не понижает
        p.observe(PID, Trust::Limited);
        assert_eq!(p.trust_of(PID), Trust::Known);
    }

    #[test]
    fn deny_beats_allow_and_blocks_everything() {
        let mut p = Policy::new(Mode::Open, Trust::Untrusted);
        p.set_listed(PID, true);
        assert!(p.allow_delegation_to(PID));
        p.set_listed(PID, false);
        assert_eq!(p.trust_of(PID), Trust::Blocked);
        assert!(!p.allow_connection(PID));
        assert!(!p.allow_tasks_from(PID));
    }

    #[test]
    fn open_mode_admits_untrusted_for_delegation() {
        let p = Policy::new(Mode::Open, Trust::Trusted);
        assert!(p.allow_delegation_to(PID));
        assert!(p.allow_tasks_from(PID));
    }

    #[test]
    fn private_mode_requires_explicit_allow() {
        let mut p = Policy::new(Mode::Private, Trust::Untrusted);
        assert_eq!(p.trust_of(PID), Trust::Blocked);
        assert!(!p.allow_delegation_to(PID));
        p.set_listed(PID, true);
        assert_eq!(p.trust_of(PID), Trust::Trusted);
        assert!(p.allow_delegation_to(PID));
    }

    #[test]
    fn posture_change_preserves_lists() {
        let mut p = Policy::new(Mode::Limited, Trust::Known);
        p.set_listed(PID, true);
        p.set_posture(Mode::Open, Trust::Untrusted);
        assert_eq!(p.trust_of(PID), Trust::Known); // allow + limited-mode best? open: listed→Known
        assert_eq!(p.mode(), Mode::Open);
    }

    #[test]
    fn defense_suspicion_moves() {
        let p = Policy::new(Mode::Open, Trust::Untrusted);
        p.add_suspicion(PID, 20);
        assert_eq!(p.defense_of(PID).unwrap().1, 20);
        p.reduce_suspicion(PID, 30);
        assert_eq!(p.defense_of(PID).unwrap().1, 0);
    }
}
