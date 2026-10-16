//! Канонические энкодеры (порт `internal/wire/canonical.go`).
//!
//! Кодирование length-prefixed и упорядоченное по полям, чтобы никакая
//! комбинация значений не сдвигала байты другого поля. Байт-в-байт
//! совместимо с Go-реализацией: дайджесты и подписи взаимно проверяемы.

use crate::proto::{Capabilities, KeyRebind, SkillDescriptor, SkillsSyncResponse, TaskAck, TaskEnvelope, TaskResult};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

/// Накопитель канонического побайтового кодирования.
#[derive(Default)]
pub struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn out(&self) -> &[u8] {
        &self.buf
    }
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }
    pub fn raw(&mut self, p: &[u8]) {
        self.buf.extend_from_slice(p);
    }
    pub fn str(&mut self, s: &str) {
        self.uvarint(s.len() as u64);
        self.buf.extend_from_slice(s.as_bytes());
    }
    /// zigzag + varint — как binary.PutVarint в Go.
    pub fn varint(&mut self, v: i64) {
        let mut x = ((v << 1) ^ (v >> 63)) as u64;
        loop {
            let b = (x & 0x7f) as u8;
            x >>= 7;
            if x == 0 {
                self.buf.push(b);
                return;
            }
            self.buf.push(b | 0x80);
        }
    }
    pub fn uvarint(&mut self, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                self.buf.push(b);
                return;
            }
            self.buf.push(b | 0x80);
        }
    }
    pub fn boolean(&mut self, v: bool) {
        self.buf.push(if v { 1 } else { 0 });
    }
    pub fn str_list(&mut self, vals: &[String]) {
        self.uvarint(vals.len() as u64);
        for v in vals {
            self.str(v);
        }
    }
    /// map кодируется отсортированным по ключу (BTreeMap гарантирует порядок).
    pub fn str_map(&mut self, m: &BTreeMap<String, String>) {
        self.uvarint(m.len() as u64);
        for (k, v) in m {
            self.str(k);
            self.str(v);
        }
    }
}

/// Доменный sha256 над схемой и телом: `sha256("zeptomesh/" + scheme + 0x00 + body)`.
pub fn digest(scheme: &str, body: &[u8]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(b"zeptomesh/");
    h.update(scheme.as_bytes());
    h.update([0u8]);
    h.update(body);
    h.finalize().to_vec()
}

/// Всё, что авторизовал origin: идентичность, скиллы, payload, constraints.
/// Исключает мутрируемые маршрутные поля (sender, ttl, route_stack).
pub fn task_content(env: &TaskEnvelope) -> Vec<u8> {
    let mut e = Encoder::new();
    e.str(&env.task_id);
    e.str(&env.parent_task_id);
    e.str(&env.origin_peer_id);
    e.varint(env.created_at);
    e.varint(env.priority as i64);
    e.str_list(&env.required_skills);

    let pl = env.payload();
    e.str(&pl.instruction);
    e.str(&pl.context_digest);
    e.uvarint(pl.attachments.len() as u64);
    for a in &pl.attachments {
        e.str(&a.name);
        e.str(&a.hash);
        e.varint(a.size);
        e.str(&a.media_type);
    }
    e.str_map(&pl.labels);

    let c = env.constraints();
    e.varint(c.max_duration_seconds as i64);
    e.boolean(c.allow_network_tools);
    e.boolean(c.allow_shell);
    e.boolean(c.allow_delegation);
    e.boolean(c.allow_subtasks);
    e.into_vec()
}

/// Полное подписываемое тело envelope: content + маршрутные поля.
pub fn task_body(env: &TaskEnvelope) -> Vec<u8> {
    let content = task_content(env);
    let mut e = Encoder::new();
    e.raw(&content);
    e.str(&env.sender_peer_id);
    e.varint(env.ttl as i64);
    e.str_list(&env.route_stack);
    e.into_vec()
}

/// Тело, которое авторизовал worker (независимо от маршрутизации).
pub fn result_content(r: &TaskResult) -> Vec<u8> {
    let mut e = Encoder::new();
    e.str(&r.task_id);
    e.str(&r.worker_peer_id);
    e.varint(r.status.code());
    e.str(&r.text);
    e.raw(&r.result_digest);
    e.str(&r.error_message);
    e.varint(r.started_at);
    e.varint(r.finished_at);
    e.uvarint(r.artifacts.len() as u64);
    for a in &r.artifacts {
        e.str(&a.name);
        e.str(&a.hash);
        e.varint(a.size);
        e.str(&a.media_type);
    }
    e.varint(r.error_class.code());
    e.boolean(r.aggregated);
    e.into_vec()
}

/// Подписываемое тело результата: content + sender/route (переписывается ретрансляторами).
pub fn result_body(r: &TaskResult) -> Vec<u8> {
    let content = result_content(r);
    let mut e = Encoder::new();
    e.raw(&content);
    e.str(&r.sender_peer_id);
    e.str_list(&r.route_stack);
    e.into_vec()
}

/// Дайджест содержимого результата: sha256 по worker-телу с пустым result_digest.
pub fn result_digest(r: &TaskResult) -> Vec<u8> {
    let mut clone = r.clone();
    clone.result_digest = Vec::new();
    digest("result-content", &result_content(&clone))
}

/// Подписываемое тело отмены задачи.
pub fn cancel_body(c: &crate::proto::CancelRequest) -> Vec<u8> {
    let mut e = Encoder::new();
    e.str(&c.task_id);
    e.str(&c.origin_peer_id);
    e.str(&c.sender_peer_id);
    e.str(&c.reason);
    e.into_vec()
}

/// Подписываемое тело самоописания узла.
pub fn caps_body(c: &Capabilities) -> Vec<u8> {
    let mut e = Encoder::new();
    e.str(&c.peer_id);
    e.str(&c.node_name);
    e.str(&c.version);
    e.str_list(&c.skills);
    e.str_list(&c.models);
    e.varint(c.max_parallel_tasks as i64);
    e.varint(c.running_tasks as i64);
    e.str(&format_float(c.load));
    e.boolean(c.accept_external_tasks);
    e.boolean(c.allow_shell);
    e.boolean(c.relay_capable);
    e.str(&c.resource_class);
    e.str_list(&c.listen_addrs);
    e.varint(c.timestamp);
    e.varint(c.skills_version);
    let docs = skill_docs_sorted(&c.skill_docs);
    e.uvarint(docs.len() as u64);
    for d in &docs {
        e.raw(&skill_descriptor_body(d));
    }
    e.into_vec()
}

/// Каноническое тело дескриптора скилла.
pub fn skill_descriptor_body(d: &SkillDescriptor) -> Vec<u8> {
    let mut e = Encoder::new();
    e.str(&d.name);
    e.varint(d.version);
    e.varint(d.updated_at);
    e.str(&d.description);
    e.str_list(&d.models);
    e.str_map(&d.attributes);
    e.into_vec()
}

/// "sha256:<hex>" контент-id дескриптора.
/// sha256 от произвольных байт (hex-совместимо с Go hex(sha256.Sum256)).
pub fn sha256_bytes(b: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(b).to_vec()
}

/// Hex-дайджест дескриптора умения (та же схема, что и в `skill_descriptor_digest`).
pub fn skill_digest_body(d: &SkillDescriptor) -> Vec<u8> {
    digest("skill-descriptor-v1", &skill_descriptor_body(d))
}

pub fn skill_descriptor_digest(d: &SkillDescriptor) -> String {
    format!(
        "sha256:{}",
        hex::encode(digest("skill-descriptor-v1", &skill_descriptor_body(d)))
    )
}

fn skill_docs_sorted(in_: &[SkillDescriptor]) -> Vec<SkillDescriptor> {
    let mut out: Vec<SkillDescriptor> = in_.to_vec();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Тело заявления о ротации/отзыве ключа (подписывают оба ключа).
pub fn rebind_body(k: &KeyRebind) -> Vec<u8> {
    let mut e = Encoder::new();
    e.str(&k.old_peer_id);
    e.str(&k.new_peer_id);
    e.raw(&k.new_pubkey);
    e.varint(k.sequence);
    e.varint(k.issued_at);
    e.str(&k.reason);
    e.into_vec()
}

/// Тело ответа синхронизации скиллов.
pub fn skills_sync_body(r: &SkillsSyncResponse) -> Vec<u8> {
    let mut e = Encoder::new();
    e.str(&r.peer_id);
    e.varint(r.skills_version);
    let docs = skill_docs_sorted(&r.skills);
    e.uvarint(docs.len() as u64);
    for d in &docs {
        e.raw(&skill_descriptor_body(d));
    }
    e.into_vec()
}

/// Тело запроса поиска исполнителей по навыкам (search-request-v1).
pub fn search_request_body(r: &crate::proto::SearchRequest) -> Vec<u8> {
    let mut e = Encoder::new();
    e.str(&r.request_id);
    e.str(&r.requester_peer_id);
    e.str_list(&r.skills);
    e.varint(r.issued_at);
    e.into_vec()
}

/// Тело ответа на поиск (search-reply-v1): request_id + отсортированные
/// записи пиров (peer_id, addrs, skills, seen_at).
pub fn search_reply_body(r: &crate::proto::SearchReply) -> Vec<u8> {
    let mut e = Encoder::new();
    e.str(&r.request_id);
    e.str(&r.responder_peer_id);
    e.uvarint(r.peers.len() as u64);
    for p in &r.peers {
        e.str(&p.peer_id);
        e.str_list(&p.addrs);
        e.str_list(&p.skills);
        e.varint(p.seen_at);
    }
    e.varint(r.issued_at);
    e.into_vec()
}

/// Тело подтверждения задачи.
pub fn ack_body(a: &TaskAck) -> Vec<u8> {
    let mut e = Encoder::new();
    e.str(&a.task_id);
    e.varint(a.status.code());
    e.str(&a.reason);
    e.str(&a.accepted_by);
    e.varint(a.timestamp);
    e.into_vec()
}

/// Детерминированное float→string: значение округляется до 4 знаков
/// (basis points) и кодируется без показателя степени — стабильно между
/// языками для малых load-значений, которые отдаёт mesh.
pub fn format_float(mut f: f64) -> String {
    if f.is_nan() {
        return "nan".into();
    }
    if f > 1.0 {
        f = 1.0;
    }
    if f < 0.0 {
        f = 0.0;
    }
    let i = (f * 10000.0 + 0.5).floor() as i64;
    format!("bp:{i}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{TaskConstraints, TaskPayload};

    #[test]
    fn format_float_parity() {
        assert_eq!(format_float(0.5), "bp:5000");
        assert_eq!(format_float(1.5), "bp:10000"); // clamp
        assert_eq!(format_float(-2.0), "bp:0"); // clamp
        assert_eq!(format_float(f64::NAN), "nan");
        assert_eq!(format_float(0.3), "bp:3000");
        assert_eq!(format_float(0.0), "bp:0");
        assert_eq!(format_float(1.0 / 3.0), "bp:3333");
    }

    #[test]
    fn encoder_primitives_go_parity() {
        let mut e = Encoder::new();
        e.str("abc");
        e.varint(-1);
        e.varint(300);
        e.uvarint(128);
        e.boolean(true);
        // "abc" -> len 3 + bytes; -1 -> zigzag 1; 300 -> zigzag 600 = 0xd8 0x04; 128 -> 0x80 0x01; true -> 1
        assert_eq!(e.out(), vec![3, b'a', b'b', b'c', 1, 0xd8, 0x04, 0x80, 0x01, 1]);
    }

    #[test]
    fn digest_domain_separation() {
        let d = digest("zeptomesh-task-v1", b"body");
        let mut h = Sha256::new();
        h.update(b"zeptomesh/zeptomesh-task-v1\0body");
        assert_eq!(d, h.finalize().to_vec());
    }

    #[test]
    fn task_body_matches_manual_encoding() {
        let env = TaskEnvelope {
            task_id: "T".into(),
            parent_task_id: "".into(),
            origin_peer_id: "O".into(),
            sender_peer_id: "S".into(),
            created_at: 7,
            ttl: 2,
            priority: 9,
            required_skills: vec!["a".into()],
            payload: Some(TaskPayload {
                instruction: "i".into(),
                ..Default::default()
            }),
            constraints: Some(TaskConstraints {
                allow_shell: true,
                max_duration_seconds: 5,
                ..Default::default()
            }),
            route_stack: vec!["R".into()],
            ..Default::default()
        };
        let mut want = Encoder::new();
        want.str("T");
        want.str("");
        want.str("O");
        want.varint(7);
        want.varint(9);
        want.str_list(&["a".to_string()]);
        want.str("i");
        want.str("");
        want.uvarint(0);
        want.str_map(&BTreeMap::new());
        want.varint(5);
        want.boolean(false);
        want.boolean(true);
        want.boolean(false);
        want.boolean(false);
        let mut full = Encoder::new();
        full.raw(want.out());
        full.str("S");
        full.varint(2);
        full.str_list(&["R".to_string()]);
        assert_eq!(task_body(&env), full.out());
    }

    #[test]
    fn result_digest_ignores_digest_field() {
        let mut r = TaskResult {
            task_id: "T".into(),
            text: "ok".into(),
            status: crate::proto::TaskStatus::Completed,
            ..Default::default()
        };
        let d = result_digest(&r);
        r.result_digest = d.clone();
        assert_eq!(result_digest(&r), d, "digest стабилен после простановки поля");
    }


    #[test]
    fn skill_digest_prefixed_hex() {
        let d = SkillDescriptor {
            name: "shell".into(),
            version: 1,
            updated_at: 100,
            description: "d".into(),
            ..Default::default()
        };
        let s = skill_descriptor_digest(&d);
        assert!(s.starts_with("sha256:") && s.len() == 7 + 64);
    }
}
