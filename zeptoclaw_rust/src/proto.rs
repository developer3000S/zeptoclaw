//! Protobuf-модели протокола mesh — Rust-порт `gen/zeptomesh/v1`.
//!
//! Номера полей и wire-типы точно соответствуют `api/proto/zeptomesh/v1/mesh.proto`,
//! поэтому сообщения бинарно совместимы с Go-узлами. Кодек реализован вручную
//! (varint + length-delimited), без внешних зависимостей от protoc.

use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Низкоуровневый кодек
// ---------------------------------------------------------------------------

/// Wire-типы protobuf.
const WT_VARINT: u8 = 0;
const WT_LEN: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PbErr {
    Truncated,
    VarintTooLong,
    BadWireType,
    FieldOverflow,
}

impl std::fmt::Display for PbErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "protobuf decode error: {:?}", self)
    }
}
impl std::error::Error for PbErr {}

/// Значение поля после декодирования.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Varint(i64),
    Bytes(Vec<u8>),
}

/// Буфер для кодирования сообщений.
#[derive(Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    fn key(&mut self, field: u32, wt: u8) {
        write_varint(&mut self.buf, (field as u64) << 3 | wt as u64);
    }

    /// int32/int64/enum/bool (varint, отрицательные — как i64 zigzag? нет: proto3 sign-extended).
    pub fn varint(&mut self, field: u32, v: i64) {
        if v == 0 {
            return; // proto3: скалярные значения по умолчанию не кодируются
        }
        self.key(field, WT_VARINT);
        write_varint(&mut self.buf, v as u64);
    }

    pub fn boolean(&mut self, field: u32, v: bool) {
        self.varint(field, v as i64);
    }

    pub fn double(&mut self, field: u32, v: f64) {
        if v == 0.0 {
            return;
        }
        self.key(field, 1);
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn string(&mut self, field: u32, v: &str) {
        if v.is_empty() {
            return;
        }
        self.key(field, WT_LEN);
        write_varint(&mut self.buf, v.len() as u64);
        self.buf.extend_from_slice(v.as_bytes());
    }

    pub fn bytes(&mut self, field: u32, v: &[u8]) {
        if v.is_empty() {
            return;
        }
        self.key(field, WT_LEN);
        write_varint(&mut self.buf, v.len() as u64);
        self.buf.extend_from_slice(v);
    }

    /// Вложенное сообщение (message / repeated message элементы).
    pub fn message(&mut self, field: u32, m: &dyn Message) {
        let body = m.encode();
        self.key(field, WT_LEN);
        write_varint(&mut self.buf, body.len() as u64);
        self.buf.extend_from_slice(&body);
    }

    /// map<string,string>: элемент карты = вложенка {1:key,2:value}.
    pub fn str_map(&mut self, field: u32, m: &BTreeMap<String, String>) {
        for (k, v) in m {
            let mut e = Writer::new();
            e.string(1, k);
            e.string(2, v);
            let body = e.into_vec();
            self.key(field, WT_LEN);
            write_varint(&mut self.buf, body.len() as u64);
            self.buf.extend_from_slice(&body);
        }
    }
}

fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

/// Читатель полей protobuf.
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn byte(&mut self) -> Result<u8, PbErr> {
        let b = *self.data.get(self.pos).ok_or(PbErr::Truncated)?;
        self.pos += 1;
        Ok(b)
    }

    pub fn varint(&mut self) -> Result<u64, PbErr> {
        let mut out: u64 = 0;
        let mut shift = 0;
        loop {
            let b = self.byte()?;
            out |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(out);
            }
            shift += 7;
            if shift >= 64 {
                return Err(PbErr::VarintTooLong);
            }
        }
    }

    fn tagged(&mut self) -> Option<Result<(u32, u8), PbErr>> {
        if self.pos >= self.data.len() {
            return None;
        }
        Some((|| {
            let key = self.varint()?;
            Ok((key as u32 >> 3, (key & 7) as u8))
        })())
    }

    /// Пропустить поле неизвестного номера любого wire-типа.
    fn skip(&mut self, wt: u8) -> Result<(), PbErr> {
        match wt {
            0 => {
                self.varint()?;
                Ok(())
            }
            1 => {
                if self.pos + 8 > self.data.len() {
                    return Err(PbErr::Truncated);
                }
                self.pos += 8;
                Ok(())
            }
            2 => {
                let n = self.varint()? as usize;
                if self.pos + n > self.data.len() {
                    return Err(PbErr::Truncated);
                }
                self.pos += n;
                Ok(())
            }
            5 => {
                if self.pos + 4 > self.data.len() {
                    return Err(PbErr::Truncated);
                }
                self.pos += 4;
                Ok(())
            }
            _ => Err(PbErr::BadWireType),
        }
    }

    pub fn value(&mut self, expect: u8) -> Result<Value, PbErr> {
        let (_, wt) = self.next_field()?;
        if wt != expect {
            return Err(PbErr::BadWireType);
        }
        match wt {
            WT_VARINT => Ok(Value::Varint(self.varint()? as i64)),
            _ => Ok(Value::Bytes(self.length_delimited()?)),
        }
    }

    /// Возвращает (номер, wire-type); читатель позиционирован на значении.
    pub fn next_field(&mut self) -> Result<(u32, u8), PbErr> {
        match self.tagged() {
            None => Err(PbErr::Truncated),
            Some(r) => r,
        }
    }

    pub fn take_field(&mut self) -> Result<Option<(u32, u8, Value)>, PbErr> {
        match self.tagged() {
            None => Ok(None),
            Some(Err(e)) => Err(e),
            Some(Ok((f, wt))) => {
                let v = match wt {
                    WT_VARINT => Value::Varint(self.varint()? as i64),
                    1 => {
                        let mut b = [0u8; 8];
                        for x in b.iter_mut() {
                            *x = self.byte()?;
                        }
                        Value::Bytes(b.to_vec())
                    }
                    2 => Value::Bytes(self.length_delimited()?),
                    5 => {
                        let mut b = [0u8; 4];
                        for x in b.iter_mut() {
                            *x = self.byte()?;
                        }
                        Value::Bytes(b.to_vec())
                    }
                    _ => return Err(PbErr::BadWireType),
                };
                Ok(Some((f, wt, v)))
            }
        }
    }

    pub fn length_delimited(&mut self) -> Result<Vec<u8>, PbErr> {
        let n = self.varint()? as usize;
        if self.pos + n > self.data.len() {
            return Err(PbErr::Truncated);
        }
        let out = self.data[self.pos..self.pos + n].to_vec();
        self.pos += n;
        Ok(out)
    }

    pub fn skip_field(&mut self, wt: u8) -> Result<(), PbErr> {
        self.skip(wt)
    }

    pub fn eof(&self) -> bool {
        self.pos >= self.data.len()
    }
}

/// Обобщённое свойство protobuf-сообщения.
pub trait Message {
    fn encode(&self) -> Vec<u8>;
    fn decode(data: &[u8]) -> Result<Self, PbErr>
    where
        Self: Sized;
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TaskStatus {
    #[default]
    Unspecified = 0,
    Received = 1,
    Validating = 2,
    Evaluating = 3,
    Accepted = 4,
    Rejected = 5,
    Forwarded = 6,
    Running = 7,
    WaitingSubtasks = 8,
    Aggregating = 9,
    Completed = 10,
    Failed = 11,
    Timeout = 12,
    Canceled = 13,
}

impl TaskStatus {
    pub fn from_code(v: i64) -> Self {
        match v {
            1 => Self::Received,
            2 => Self::Validating,
            3 => Self::Evaluating,
            4 => Self::Accepted,
            5 => Self::Rejected,
            6 => Self::Forwarded,
            7 => Self::Running,
            8 => Self::WaitingSubtasks,
            9 => Self::Aggregating,
            10 => Self::Completed,
            11 => Self::Failed,
            12 => Self::Timeout,
            13 => Self::Canceled,
            _ => Self::Unspecified,
        }
    }
    pub fn code(self) -> i64 {
        self as i64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AckStatus {
    #[default]
    Unspecified = 0,
    Queued = 1,
    Forwarded = 2,
    Rejected = 3,
    Duplicate = 4,
}

impl AckStatus {
    pub fn from_code(v: i64) -> Self {
        match v {
            1 => Self::Queued,
            2 => Self::Forwarded,
            3 => Self::Rejected,
            4 => Self::Duplicate,
            _ => Self::Unspecified,
        }
    }
    pub fn code(self) -> i64 {
        self as i64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TrustLevel {
    #[default]
    Unspecified = 0,
    Trusted = 1,
    Known = 2,
    Limited = 3,
    Untrusted = 4,
    Blocked = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TaskErrorClass {
    #[default]
    Unspecified = 0,
    Validation = 1,
    Security = 2,
    Execution = 3,
    NoWorker = 4,
    Network = 5,
    Timeout = 6,
    Limits = 7,
    Canceled = 8,
}

impl TaskErrorClass {
    pub fn from_code(v: i64) -> Self {
        match v {
            1 => Self::Validation,
            2 => Self::Security,
            3 => Self::Execution,
            4 => Self::NoWorker,
            5 => Self::Network,
            6 => Self::Timeout,
            7 => Self::Limits,
            8 => Self::Canceled,
            _ => Self::Unspecified,
        }
    }
    pub fn code(self) -> i64 {
        self as i64
    }
}

// ---------------------------------------------------------------------------
// Сообщения
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaskConstraints {
    pub max_duration_seconds: i32,
    pub allow_network_tools: bool,
    pub allow_shell: bool,
    pub allow_delegation: bool,
    pub allow_subtasks: bool,
}

impl Message for TaskConstraints {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.varint(1, self.max_duration_seconds as i64);
        w.boolean(2, self.allow_network_tools);
        w.boolean(3, self.allow_shell);
        w.boolean(4, self.allow_delegation);
        w.boolean(5, self.allow_subtasks);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Varint(x)) => m.max_duration_seconds = x as i32,
                (2, Value::Varint(x)) => m.allow_network_tools = x != 0,
                (3, Value::Varint(x)) => m.allow_shell = x != 0,
                (4, Value::Varint(x)) => m.allow_delegation = x != 0,
                (5, Value::Varint(x)) => m.allow_subtasks = x != 0,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ArtifactRef {
    pub name: String,
    pub hash: String,
    pub size: i64,
    pub media_type: String,
}

impl Message for ArtifactRef {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.name);
        w.string(2, &self.hash);
        w.varint(3, self.size);
        w.string(4, &self.media_type);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.name = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Bytes(b)) => m.hash = String::from_utf8_lossy(&b).into_owned(),
                (3, Value::Varint(x)) => m.size = x,
                (4, Value::Bytes(b)) => m.media_type = String::from_utf8_lossy(&b).into_owned(),
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaskPayload {
    pub instruction: String,
    pub context_digest: String,
    pub attachments: Vec<ArtifactRef>,
    pub labels: BTreeMap<String, String>,
}

impl Message for TaskPayload {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.instruction);
        w.string(2, &self.context_digest);
        for a in &self.attachments {
            w.message(3, a);
        }
        w.str_map(4, &self.labels);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.instruction = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Bytes(b)) => m.context_digest = String::from_utf8_lossy(&b).into_owned(),
                (3, Value::Bytes(b)) => m.attachments.push(ArtifactRef::decode(&b)?),
                (4, Value::Bytes(b)) => {
                    let mut er = Reader::new(&b);
                    let mut k = String::new();
                    let mut val = String::new();
                    while let Some((ef, _, ev)) = er.take_field()? {
                        if let Value::Bytes(s) = ev {
                            match ef {
                                1 => k = String::from_utf8_lossy(&s).into_owned(),
                                2 => val = String::from_utf8_lossy(&s).into_owned(),
                                _ => {}
                            }
                        }
                    }
                    m.labels.insert(k, val);
                }
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaskEnvelope {
    pub task_id: String,
    pub parent_task_id: String,
    pub origin_peer_id: String,
    pub sender_peer_id: String,
    pub created_at: i64,
    pub ttl: i32,
    pub priority: i32,
    pub required_skills: Vec<String>,
    pub payload: Option<TaskPayload>,
    pub constraints: Option<TaskConstraints>,
    pub context_digest: String,
    pub route_stack: Vec<String>,
    pub signature: Vec<u8>,
    pub signature_scheme: String,
    pub origin_signature: Vec<u8>,
}

impl TaskEnvelope {
    pub fn payload(&self) -> &TaskPayload {
        static EMPTY: TaskPayload = TaskPayload {
            instruction: String::new(),
            context_digest: String::new(),
            attachments: Vec::new(),
            labels: BTreeMap::new(),
        };
        self.payload.as_ref().unwrap_or(&EMPTY)
    }
    pub fn constraints(&self) -> &TaskConstraints {
        static EMPTY: TaskConstraints = TaskConstraints {
            max_duration_seconds: 0,
            allow_network_tools: false,
            allow_shell: false,
            allow_delegation: false,
            allow_subtasks: false,
        };
        self.constraints.as_ref().unwrap_or(&EMPTY)
    }
}

impl Message for TaskEnvelope {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.task_id);
        w.string(2, &self.parent_task_id);
        w.string(3, &self.origin_peer_id);
        w.string(4, &self.sender_peer_id);
        w.varint(5, self.created_at);
        w.varint(6, self.ttl as i64);
        w.varint(7, self.priority as i64);
        for s in &self.required_skills {
            w.string(8, s);
        }
        if let Some(p) = &self.payload {
            w.message(9, p);
        }
        if let Some(c) = &self.constraints {
            w.message(10, c);
        }
        w.string(11, &self.context_digest);
        for s in &self.route_stack {
            w.string(12, s);
        }
        w.bytes(13, &self.signature);
        w.string(14, &self.signature_scheme);
        w.bytes(15, &self.origin_signature);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.task_id = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Bytes(b)) => m.parent_task_id = String::from_utf8_lossy(&b).into_owned(),
                (3, Value::Bytes(b)) => m.origin_peer_id = String::from_utf8_lossy(&b).into_owned(),
                (4, Value::Bytes(b)) => m.sender_peer_id = String::from_utf8_lossy(&b).into_owned(),
                (5, Value::Varint(x)) => m.created_at = x,
                (6, Value::Varint(x)) => m.ttl = x as i32,
                (7, Value::Varint(x)) => m.priority = x as i32,
                (8, Value::Bytes(b)) => m.required_skills.push(String::from_utf8_lossy(&b).into_owned()),
                (9, Value::Bytes(b)) => m.payload = Some(TaskPayload::decode(&b)?),
                (10, Value::Bytes(b)) => m.constraints = Some(TaskConstraints::decode(&b)?),
                (11, Value::Bytes(b)) => m.context_digest = String::from_utf8_lossy(&b).into_owned(),
                (12, Value::Bytes(b)) => m.route_stack.push(String::from_utf8_lossy(&b).into_owned()),
                (13, Value::Bytes(b)) => m.signature = b,
                (14, Value::Bytes(b)) => m.signature_scheme = String::from_utf8_lossy(&b).into_owned(),
                (15, Value::Bytes(b)) => m.origin_signature = b,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaskAck {
    pub task_id: String,
    pub status: AckStatus,
    pub reason: String,
    pub accepted_by: String,
    pub timestamp: i64,
    pub signature: Vec<u8>,
}

impl Message for TaskAck {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.task_id);
        w.varint(2, self.status.code());
        w.string(3, &self.reason);
        w.string(4, &self.accepted_by);
        w.varint(5, self.timestamp);
        w.bytes(6, &self.signature);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.task_id = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Varint(x)) => m.status = AckStatus::from_code(x),
                (3, Value::Bytes(b)) => m.reason = String::from_utf8_lossy(&b).into_owned(),
                (4, Value::Bytes(b)) => m.accepted_by = String::from_utf8_lossy(&b).into_owned(),
                (5, Value::Varint(x)) => m.timestamp = x,
                (6, Value::Bytes(b)) => m.signature = b,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaskResult {
    pub task_id: String,
    pub worker_peer_id: String,
    pub status: TaskStatus,
    pub text: String,
    pub result_digest: Vec<u8>,
    pub error_message: String,
    pub started_at: i64,
    pub finished_at: i64,
    pub artifacts: Vec<ArtifactRef>,
    pub sender_peer_id: String,
    pub signature: Vec<u8>,
    pub signature_scheme: String,
    pub route_stack: Vec<String>,
    pub worker_signature: Vec<u8>,
    pub error_class: TaskErrorClass,
    pub aggregated: bool,
}

impl Message for TaskResult {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.task_id);
        w.string(2, &self.worker_peer_id);
        w.varint(3, self.status.code());
        w.string(4, &self.text);
        w.bytes(5, &self.result_digest);
        w.string(6, &self.error_message);
        w.varint(7, self.started_at);
        w.varint(8, self.finished_at);
        for a in &self.artifacts {
            w.message(9, a);
        }
        w.string(10, &self.sender_peer_id);
        w.bytes(11, &self.signature);
        w.string(12, &self.signature_scheme);
        for s in &self.route_stack {
            w.string(13, s);
        }
        w.bytes(14, &self.worker_signature);
        w.varint(15, self.error_class.code());
        w.boolean(16, self.aggregated);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.task_id = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Bytes(b)) => m.worker_peer_id = String::from_utf8_lossy(&b).into_owned(),
                (3, Value::Varint(x)) => m.status = TaskStatus::from_code(x),
                (4, Value::Bytes(b)) => m.text = String::from_utf8_lossy(&b).into_owned(),
                (5, Value::Bytes(b)) => m.result_digest = b,
                (6, Value::Bytes(b)) => m.error_message = String::from_utf8_lossy(&b).into_owned(),
                (7, Value::Varint(x)) => m.started_at = x,
                (8, Value::Varint(x)) => m.finished_at = x,
                (9, Value::Bytes(b)) => m.artifacts.push(ArtifactRef::decode(&b)?),
                (10, Value::Bytes(b)) => m.sender_peer_id = String::from_utf8_lossy(&b).into_owned(),
                (11, Value::Bytes(b)) => m.signature = b,
                (12, Value::Bytes(b)) => m.signature_scheme = String::from_utf8_lossy(&b).into_owned(),
                (13, Value::Bytes(b)) => m.route_stack.push(String::from_utf8_lossy(&b).into_owned()),
                (14, Value::Bytes(b)) => m.worker_signature = b,
                (15, Value::Varint(x)) => m.error_class = TaskErrorClass::from_code(x),
                (16, Value::Varint(x)) => m.aggregated = x != 0,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResultAck {
    pub task_id: String,
    pub accepted: bool,
    pub reason: String,
}

impl Message for ResultAck {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.task_id);
        w.boolean(2, self.accepted);
        w.string(3, &self.reason);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.task_id = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Varint(x)) => m.accepted = x != 0,
                (3, Value::Bytes(b)) => m.reason = String::from_utf8_lossy(&b).into_owned(),
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SkillDescriptor {
    pub name: String,
    pub version: i64,
    pub updated_at: i64,
    pub digest: String,
    pub description: String,
    pub models: Vec<String>,
    pub attributes: BTreeMap<String, String>,
}

impl Message for SkillDescriptor {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.name);
        w.varint(2, self.version);
        w.varint(3, self.updated_at);
        w.string(4, &self.digest);
        w.string(5, &self.description);
        for m in &self.models {
            w.string(6, m);
        }
        w.str_map(7, &self.attributes);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.name = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Varint(x)) => m.version = x,
                (3, Value::Varint(x)) => m.updated_at = x,
                (4, Value::Bytes(b)) => m.digest = String::from_utf8_lossy(&b).into_owned(),
                (5, Value::Bytes(b)) => m.description = String::from_utf8_lossy(&b).into_owned(),
                (6, Value::Bytes(b)) => m.models.push(String::from_utf8_lossy(&b).into_owned()),
                (7, Value::Bytes(b)) => {
                    let mut er = Reader::new(&b);
                    let mut k = String::new();
                    let mut val = String::new();
                    while let Some((ef, _, ev)) = er.take_field()? {
                        if let Value::Bytes(s) = ev {
                            match ef {
                                1 => k = String::from_utf8_lossy(&s).into_owned(),
                                2 => val = String::from_utf8_lossy(&s).into_owned(),
                                _ => {}
                            }
                        }
                    }
                    m.attributes.insert(k, val);
                }
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Capabilities {
    pub peer_id: String,
    pub node_name: String,
    pub version: String,
    pub skills: Vec<String>,
    pub models: Vec<String>,
    pub max_parallel_tasks: i32,
    pub running_tasks: i32,
    pub load: f64,
    pub accept_external_tasks: bool,
    pub allow_shell: bool,
    pub relay_capable: bool,
    pub resource_class: String,
    pub listen_addrs: Vec<String>,
    pub timestamp: i64,
    pub signature: Vec<u8>,
    pub skills_version: i64,
    pub skill_docs: Vec<SkillDescriptor>,
}

impl Message for Capabilities {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.peer_id);
        w.string(2, &self.node_name);
        w.string(3, &self.version);
        for s in &self.skills {
            w.string(4, s);
        }
        for s in &self.models {
            w.string(5, s);
        }
        w.varint(6, self.max_parallel_tasks as i64);
        w.varint(7, self.running_tasks as i64);
        w.double(8, self.load);
        w.boolean(9, self.accept_external_tasks);
        w.boolean(10, self.allow_shell);
        w.boolean(11, self.relay_capable);
        w.string(12, &self.resource_class);
        for s in &self.listen_addrs {
            w.string(13, s);
        }
        w.varint(14, self.timestamp);
        w.bytes(15, &self.signature);
        w.varint(16, self.skills_version);
        for d in &self.skill_docs {
            w.message(17, d);
        }
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, wt, v)) = r.take_field()? {
            match (f, &v) {
                (1, Value::Bytes(b)) => m.peer_id = String::from_utf8_lossy(b).into_owned(),
                (2, Value::Bytes(b)) => m.node_name = String::from_utf8_lossy(b).into_owned(),
                (3, Value::Bytes(b)) => m.version = String::from_utf8_lossy(b).into_owned(),
                (4, Value::Bytes(b)) => m.skills.push(String::from_utf8_lossy(b).into_owned()),
                (5, Value::Bytes(b)) => m.models.push(String::from_utf8_lossy(b).into_owned()),
                (6, Value::Varint(x)) => m.max_parallel_tasks = *x as i32,
                (7, Value::Varint(x)) => m.running_tasks = *x as i32,
                (8, _) if wt == 1 => {
                    m.load = f64::from_le_bytes(v.bytes()?.try_into().map_err(|_| PbErr::Truncated)?)
                }
                (9, Value::Varint(x)) => m.accept_external_tasks = *x != 0,
                (10, Value::Varint(x)) => m.allow_shell = *x != 0,
                (11, Value::Varint(x)) => m.relay_capable = *x != 0,
                (12, Value::Bytes(b)) => m.resource_class = String::from_utf8_lossy(b).into_owned(),
                (13, Value::Bytes(b)) => m.listen_addrs.push(String::from_utf8_lossy(b).into_owned()),
                (14, Value::Varint(x)) => m.timestamp = *x,
                (15, Value::Bytes(b)) => m.signature = b.clone(),
                (16, Value::Varint(x)) => m.skills_version = *x,
                (17, Value::Bytes(b)) => m.skill_docs.push(SkillDescriptor::decode(b)?),
                _ => {
                    let _ = wt;
                }
            }
        }
        Ok(m)
    }
}

impl Value {
    fn bytes(self) -> Result<Vec<u8>, PbErr> {
        match self {
            Value::Bytes(b) => Ok(b),
            _ => Err(PbErr::BadWireType),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SkillVersion {
    pub name: String,
    pub version: i64,
    pub digest: String,
}

impl Message for SkillVersion {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.name);
        w.varint(2, self.version);
        w.string(3, &self.digest);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.name = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Varint(x)) => m.version = x,
                (3, Value::Bytes(b)) => m.digest = String::from_utf8_lossy(&b).into_owned(),
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SkillsSyncRequest {
    pub known: Vec<SkillVersion>,
    pub full: bool,
}

impl Message for SkillsSyncRequest {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        for k in &self.known {
            w.message(1, k);
        }
        w.boolean(2, self.full);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.known.push(SkillVersion::decode(&b)?),
                (2, Value::Varint(x)) => m.full = x != 0,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SkillsSyncResponse {
    pub skills: Vec<SkillDescriptor>,
    pub skills_version: i64,
    pub peer_id: String,
    pub signature: Vec<u8>,
    pub signature_scheme: String,
    pub reason: String,
}

impl Message for SkillsSyncResponse {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        for s in &self.skills {
            w.message(1, s);
        }
        w.varint(2, self.skills_version);
        w.string(3, &self.peer_id);
        w.bytes(4, &self.signature);
        w.string(5, &self.signature_scheme);
        w.string(6, &self.reason);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.skills.push(SkillDescriptor::decode(&b)?),
                (2, Value::Varint(x)) => m.skills_version = x,
                (3, Value::Bytes(b)) => m.peer_id = String::from_utf8_lossy(&b).into_owned(),
                (4, Value::Bytes(b)) => m.signature = b,
                (5, Value::Bytes(b)) => m.signature_scheme = String::from_utf8_lossy(&b).into_owned(),
                (6, Value::Bytes(b)) => m.reason = String::from_utf8_lossy(&b).into_owned(),
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct KeyRebind {
    pub old_peer_id: String,
    pub new_peer_id: String,
    pub new_pubkey: Vec<u8>,
    pub sequence: i64,
    pub issued_at: i64,
    pub reason: String,
    pub old_signature: Vec<u8>,
    pub new_signature: Vec<u8>,
    pub signature_scheme: String,
}

impl Message for KeyRebind {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.old_peer_id);
        w.string(2, &self.new_peer_id);
        w.bytes(3, &self.new_pubkey);
        w.varint(4, self.sequence);
        w.varint(5, self.issued_at);
        w.string(6, &self.reason);
        w.bytes(7, &self.old_signature);
        w.bytes(8, &self.new_signature);
        w.string(9, &self.signature_scheme);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.old_peer_id = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Bytes(b)) => m.new_peer_id = String::from_utf8_lossy(&b).into_owned(),
                (3, Value::Bytes(b)) => m.new_pubkey = b,
                (4, Value::Varint(x)) => m.sequence = x,
                (5, Value::Varint(x)) => m.issued_at = x,
                (6, Value::Bytes(b)) => m.reason = String::from_utf8_lossy(&b).into_owned(),
                (7, Value::Bytes(b)) => m.old_signature = b,
                (8, Value::Bytes(b)) => m.new_signature = b,
                (9, Value::Bytes(b)) => m.signature_scheme = String::from_utf8_lossy(&b).into_owned(),
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PeerState {
    pub peer_id: String,
    pub timestamp: i64,
    pub skills: Vec<String>,
    pub load: f64,
    pub max_parallel_tasks: i32,
    pub version: String,
    pub status: String,
    pub addrs: Vec<String>,
    pub skills_version: i64,
}

impl Message for PeerState {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.peer_id);
        w.varint(2, self.timestamp);
        for s in &self.skills {
            w.string(3, s);
        }
        w.double(4, self.load);
        w.varint(5, self.max_parallel_tasks as i64);
        w.string(6, &self.version);
        w.string(7, &self.status);
        for s in &self.addrs {
            w.string(8, s);
        }
        w.varint(9, self.skills_version);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, wt, v)) = r.take_field()? {
            match (f, &v) {
                (1, Value::Bytes(b)) => m.peer_id = String::from_utf8_lossy(b).into_owned(),
                (2, Value::Varint(x)) => m.timestamp = *x,
                (3, Value::Bytes(b)) => m.skills.push(String::from_utf8_lossy(b).into_owned()),
                (4, _) if wt == 1 => {
                    m.load = f64::from_le_bytes(v.clone().bytes()?.try_into().map_err(|_| PbErr::Truncated)?)
                }
                (5, Value::Varint(x)) => m.max_parallel_tasks = *x as i32,
                (6, Value::Bytes(b)) => m.version = String::from_utf8_lossy(b).into_owned(),
                (7, Value::Bytes(b)) => m.status = String::from_utf8_lossy(b).into_owned(),
                (8, Value::Bytes(b)) => m.addrs.push(String::from_utf8_lossy(b).into_owned()),
                (9, Value::Varint(x)) => m.skills_version = *x,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MembershipGossip {
    pub states: Vec<PeerState>,
    pub from_peer_id: String,
    pub rebinds: Vec<KeyRebind>,
    pub revocations: Vec<KeyRebind>,
}

impl Message for MembershipGossip {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        for s in &self.states {
            w.message(1, s);
        }
        w.string(2, &self.from_peer_id);
        for r in &self.rebinds {
            w.message(3, r);
        }
        for r in &self.revocations {
            w.message(4, r);
        }
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.states.push(PeerState::decode(&b)?),
                (2, Value::Bytes(b)) => m.from_peer_id = String::from_utf8_lossy(&b).into_owned(),
                (3, Value::Bytes(b)) => m.rebinds.push(KeyRebind::decode(&b)?),
                (4, Value::Bytes(b)) => m.revocations.push(KeyRebind::decode(&b)?),
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SubtaskSpec {
    pub task_id: String,
    pub required_skills: Vec<String>,
    pub instruction: String,
    pub ttl: i32,
    pub priority: i32,
}

impl Message for SubtaskSpec {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.task_id);
        for s in &self.required_skills {
            w.string(2, s);
        }
        w.string(3, &self.instruction);
        w.varint(4, self.ttl as i64);
        w.varint(5, self.priority as i64);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.task_id = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Bytes(b)) => m.required_skills.push(String::from_utf8_lossy(&b).into_owned()),
                (3, Value::Bytes(b)) => m.instruction = String::from_utf8_lossy(&b).into_owned(),
                (4, Value::Varint(x)) => m.ttl = x as i32,
                (5, Value::Varint(x)) => m.priority = x as i32,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PeerRecord {
    pub peer_id: String,
    pub addrs: Vec<String>,
    pub skills: Vec<String>,
    pub seen_at: i64,
}

impl Message for PeerRecord {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.peer_id);
        for s in &self.addrs {
            w.string(2, s);
        }
        for s in &self.skills {
            w.string(3, s);
        }
        w.varint(4, self.seen_at);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.peer_id = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Bytes(b)) => m.addrs.push(String::from_utf8_lossy(&b).into_owned()),
                (3, Value::Bytes(b)) => m.skills.push(String::from_utf8_lossy(&b).into_owned()),
                (4, Value::Varint(x)) => m.seen_at = x,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CancelRequest {
    pub task_id: String,
    pub origin_peer_id: String,
    pub sender_peer_id: String,
    pub reason: String,
    pub signature: Vec<u8>,
}

impl Message for CancelRequest {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.task_id);
        w.string(2, &self.origin_peer_id);
        w.string(3, &self.sender_peer_id);
        w.string(4, &self.reason);
        w.bytes(5, &self.signature);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.task_id = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Bytes(b)) => m.origin_peer_id = String::from_utf8_lossy(&b).into_owned(),
                (3, Value::Bytes(b)) => m.sender_peer_id = String::from_utf8_lossy(&b).into_owned(),
                (4, Value::Bytes(b)) => m.reason = String::from_utf8_lossy(&b).into_owned(),
                (5, Value::Bytes(b)) => m.signature = b,
                _ => {}
            }
        }
        Ok(m)
    }
}

// --- RPC oneof'ы ----------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PingRequest {
    pub nonce: i64,
}
impl Message for PingRequest {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.varint(1, self.nonce);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            if let (1, Value::Varint(x)) = (f, v) {
                m.nonce = x;
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PingResponse {
    pub nonce: i64,
    pub peer_time: i64,
}
impl Message for PingResponse {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.varint(1, self.nonce);
        w.varint(2, self.peer_time);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Varint(x)) => m.nonce = x,
                (2, Value::Varint(x)) => m.peer_time = x,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CapabilitiesRequest {}
impl Message for CapabilitiesRequest {
    fn encode(&self) -> Vec<u8> {
        Vec::new()
    }
    fn decode(_: &[u8]) -> Result<Self, PbErr> {
        Ok(Self {})
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CapabilitiesResponse {
    pub capabilities: Option<Capabilities>,
}
impl Message for CapabilitiesResponse {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        if let Some(c) = &self.capabilities {
            w.message(1, c);
        }
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            if let (1, Value::Bytes(b)) = (f, v) {
                m.capabilities = Some(Capabilities::decode(&b)?);
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PeerExchangeRequest {
    pub count: u32,
}
impl Message for PeerExchangeRequest {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.varint(1, self.count as i64);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            if let (1, Value::Varint(x)) = (f, v) {
                m.count = x as u32;
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PeerExchangeResponse {
    pub peers: Vec<PeerRecord>,
}
impl Message for PeerExchangeResponse {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        for p in &self.peers {
            w.message(1, p);
        }
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            if let (1, Value::Bytes(b)) = (f, v) {
                m.peers.push(PeerRecord::decode(&b)?);
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CancelResponse {
    pub accepted: bool,
    pub reason: String,
}
impl Message for CancelResponse {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.boolean(1, self.accepted);
        w.string(2, &self.reason);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Varint(x)) => m.accepted = x != 0,
                (2, Value::Bytes(b)) => m.reason = String::from_utf8_lossy(&b).into_owned(),
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SkillLookupRequest {
    pub skills: Vec<String>,
    pub full_refresh: bool,
    pub relay_budget: i32,
    pub visited: Vec<String>,
}
impl Message for SkillLookupRequest {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        for s in &self.skills {
            w.string(1, s);
        }
        w.boolean(2, self.full_refresh);
        w.varint(3, self.relay_budget as i64);
        for s in &self.visited {
            w.string(4, s);
        }
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.skills.push(String::from_utf8_lossy(&b).into_owned()),
                (2, Value::Varint(x)) => m.full_refresh = x != 0,
                (3, Value::Varint(x)) => m.relay_budget = x as i32,
                (4, Value::Bytes(b)) => m.visited.push(String::from_utf8_lossy(&b).into_owned()),
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SkillLookupResponse {
    pub peers: Vec<PeerRecord>,
    pub partial: bool,
    pub responder_refreshed: bool,
}
impl Message for SkillLookupResponse {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        for p in &self.peers {
            w.message(1, p);
        }
        w.boolean(2, self.partial);
        w.boolean(3, self.responder_refreshed);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.peers.push(PeerRecord::decode(&b)?),
                (2, Value::Varint(x)) => m.partial = x != 0,
                (3, Value::Varint(x)) => m.responder_refreshed = x != 0,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RebindRequest {
    pub rebind: Option<KeyRebind>,
}
impl Message for RebindRequest {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        if let Some(r) = &self.rebind {
            w.message(1, r);
        }
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            if let (1, Value::Bytes(b)) = (f, v) {
                m.rebind = Some(KeyRebind::decode(&b)?);
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RebindResponse {
    pub accepted: bool,
    pub reason: String,
}
impl Message for RebindResponse {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.boolean(1, self.accepted);
        w.string(2, &self.reason);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Varint(x)) => m.accepted = x != 0,
                (2, Value::Bytes(b)) => m.reason = String::from_utf8_lossy(&b).into_owned(),
                _ => {}
            }
        }
        Ok(m)
    }
}

/// Один из вариантов RpcRequest (proto oneof `kind`).
#[derive(Debug, Clone, PartialEq)]
pub enum RpcRequestKind {
    Ping(PingRequest),
    Capabilities(CapabilitiesRequest),
    PeerExchange(PeerExchangeRequest),
    Cancel(CancelRequest),
    SkillLookup(SkillLookupRequest),
    SkillsSync(SkillsSyncRequest),
    Rebind(RebindRequest),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RpcRequest(pub RpcRequestKind);
#[derive(Debug, Clone, PartialEq)]
pub struct RpcResponse(pub RpcResponseKind);

#[derive(Debug, Clone, PartialEq)]
pub enum RpcResponseKind {
    Ping(PingResponse),
    Capabilities(CapabilitiesResponse),
    PeerExchange(PeerExchangeResponse),
    Cancel(CancelResponse),
    SkillLookup(SkillLookupResponse),
    SkillsSync(SkillsSyncResponse),
    Rebind(RebindResponse),
}

// RpcRequest/RpcResponse используют один и тот же набор номеров полей oneof,
// но разные перечисления вариантов — реализация ниже развернута вручную.

impl Message for RpcRequest {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        match &self.0 {
            RpcRequestKind::Ping(m) => w.message(1, m),
            RpcRequestKind::Capabilities(m) => w.message(2, m),
            RpcRequestKind::PeerExchange(m) => w.message(3, m),
            RpcRequestKind::Cancel(m) => w.message(4, m),
            RpcRequestKind::SkillLookup(m) => w.message(5, m),
            RpcRequestKind::SkillsSync(m) => w.message(6, m),
            RpcRequestKind::Rebind(m) => w.message(7, m),
        }
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        while let Some((f, _, v)) = r.take_field()? {
            if let Value::Bytes(b) = v {
                return Ok(RpcRequest(match f {
                    1 => RpcRequestKind::Ping(PingRequest::decode(&b)?),
                    2 => RpcRequestKind::Capabilities(CapabilitiesRequest::decode(&b)?),
                    3 => RpcRequestKind::PeerExchange(PeerExchangeRequest::decode(&b)?),
                    4 => RpcRequestKind::Cancel(CancelRequest::decode(&b)?),
                    5 => RpcRequestKind::SkillLookup(SkillLookupRequest::decode(&b)?),
                    6 => RpcRequestKind::SkillsSync(SkillsSyncRequest::decode(&b)?),
                    7 => RpcRequestKind::Rebind(RebindRequest::decode(&b)?),
                    _ => return Err(PbErr::BadWireType),
                }));
            }
        }
        Err(PbErr::Truncated)
    }
}

impl Message for RpcResponse {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        match &self.0 {
            RpcResponseKind::Ping(m) => w.message(1, m),
            RpcResponseKind::Capabilities(m) => w.message(2, m),
            RpcResponseKind::PeerExchange(m) => w.message(3, m),
            RpcResponseKind::Cancel(m) => w.message(4, m),
            RpcResponseKind::SkillLookup(m) => w.message(5, m),
            RpcResponseKind::SkillsSync(m) => w.message(6, m),
            RpcResponseKind::Rebind(m) => w.message(7, m),
        }
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        while let Some((f, _, v)) = r.take_field()? {
            if let Value::Bytes(b) = v {
                return Ok(RpcResponse(match f {
                    1 => RpcResponseKind::Ping(PingResponse::decode(&b)?),
                    2 => RpcResponseKind::Capabilities(CapabilitiesResponse::decode(&b)?),
                    3 => RpcResponseKind::PeerExchange(PeerExchangeResponse::decode(&b)?),
                    4 => RpcResponseKind::Cancel(CancelResponse::decode(&b)?),
                    5 => RpcResponseKind::SkillLookup(SkillLookupResponse::decode(&b)?),
                    6 => RpcResponseKind::SkillsSync(SkillsSyncResponse::decode(&b)?),
                    7 => RpcResponseKind::Rebind(RebindResponse::decode(&b)?),
                    _ => return Err(PbErr::BadWireType),
                }));
            }
        }
        Err(PbErr::Truncated)
    }
}

// Search topic messages

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchRequest {
    pub request_id: String,
    pub requester_peer_id: String,
    pub skills: Vec<String>,
    pub issued_at: i64,
    pub signature_scheme: String,
    pub signature: Vec<u8>,
}
impl Message for SearchRequest {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.request_id);
        w.string(2, &self.requester_peer_id);
        for s in &self.skills {
            w.string(3, s);
        }
        w.varint(4, self.issued_at);
        w.string(5, &self.signature_scheme);
        w.bytes(6, &self.signature);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.request_id = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Bytes(b)) => m.requester_peer_id = String::from_utf8_lossy(&b).into_owned(),
                (3, Value::Bytes(b)) => m.skills.push(String::from_utf8_lossy(&b).into_owned()),
                (4, Value::Varint(x)) => m.issued_at = x,
                (5, Value::Bytes(b)) => m.signature_scheme = String::from_utf8_lossy(&b).into_owned(),
                (6, Value::Bytes(b)) => m.signature = b,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchReply {
    pub request_id: String,
    pub responder_peer_id: String,
    pub peers: Vec<PeerRecord>,
    pub issued_at: i64,
    pub signature_scheme: String,
    pub signature: Vec<u8>,
}
impl Message for SearchReply {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(1, &self.request_id);
        w.string(2, &self.responder_peer_id);
        for p in &self.peers {
            w.message(3, p);
        }
        w.varint(4, self.issued_at);
        w.string(5, &self.signature_scheme);
        w.bytes(6, &self.signature);
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        let mut m = Self::default();
        while let Some((f, _, v)) = r.take_field()? {
            match (f, v) {
                (1, Value::Bytes(b)) => m.request_id = String::from_utf8_lossy(&b).into_owned(),
                (2, Value::Bytes(b)) => m.responder_peer_id = String::from_utf8_lossy(&b).into_owned(),
                (3, Value::Bytes(b)) => m.peers.push(PeerRecord::decode(&b)?),
                (4, Value::Varint(x)) => m.issued_at = x,
                (5, Value::Bytes(b)) => m.signature_scheme = String::from_utf8_lossy(&b).into_owned(),
                (6, Value::Bytes(b)) => m.signature = b,
                _ => {}
            }
        }
        Ok(m)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SearchMessageKind {
    Request(SearchRequest),
    Reply(SearchReply),
}

pub struct SearchMessage(pub SearchMessageKind);

impl Message for SearchMessage {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        match &self.0 {
            SearchMessageKind::Request(m) => w.message(1, m),
            SearchMessageKind::Reply(m) => w.message(2, m),
        }
        w.into_vec()
    }
    fn decode(d: &[u8]) -> Result<Self, PbErr> {
        let mut r = Reader::new(d);
        while let Some((f, _, v)) = r.take_field()? {
            if let Value::Bytes(b) = v {
                return Ok(SearchMessage(match f {
                    1 => SearchMessageKind::Request(SearchRequest::decode(&b)?),
                    2 => SearchMessageKind::Reply(SearchReply::decode(&b)?),
                    _ => return Err(PbErr::BadWireType),
                }));
            }
        }
        Err(PbErr::Truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip_negative() {
        let mut w = Writer::new();
        w.varint(6, -3);
        // ключ поля 6 varint = 48; значение -3 как u64 sign-extend = 2^64-3 (10 байт)
        let mut r = Reader::new(&w.buf);
        let (f, wt) = r.next_field().unwrap();
        assert_eq!((f, wt), (6, WT_VARINT));
        let v = r.varint().unwrap() as i64;
        assert_eq!(v, -3);
    }

    #[test]
    fn envelope_roundtrip() {
        let env = TaskEnvelope {
            task_id: "T-1".into(),
            origin_peer_id: "12D3Koo".into(),
            sender_peer_id: "12D3KooB".into(),
            created_at: 1_700_000_000,
            ttl: 3,
            priority: 5,
            required_skills: vec!["shell".into(), "net".into()],
            payload: Some(TaskPayload {
                instruction: "сделать".into(),
                labels: [("a".into(), "1".into())].into_iter().collect(),
                attachments: vec![ArtifactRef {
                    name: "f".into(),
                    hash: "sha256:aa".into(),
                    size: 10,
                    media_type: "text/plain".into(),
                }],
                ..Default::default()
            }),
            constraints: Some(TaskConstraints {
                max_duration_seconds: 60,
                allow_shell: true,
                ..Default::default()
            }),
            route_stack: vec!["A".into(), "B".into()],
            signature: vec![9, 8, 7],
            signature_scheme: "zeptomesh-task-v1".into(),
            origin_signature: vec![1],
            ..Default::default()
        };
        let enc = env.encode();
        let dec = TaskEnvelope::decode(&enc).unwrap();
        assert_eq!(env, dec);
    }

    #[test]
    fn caps_double_roundtrip() {
        let c = Capabilities {
            peer_id: "P".into(),
            load: 0.75,
            max_parallel_tasks: 4,
            skill_docs: vec![SkillDescriptor {
                name: "s".into(),
                version: 2,
                attributes: [("owner".into(), "ops".into())].into_iter().collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let dec = Capabilities::decode(&c.encode()).unwrap();
        assert_eq!(c, dec);
    }

    #[test]
    fn rpc_oneof_roundtrip() {
        let req = RpcRequest(RpcRequestKind::SkillLookup(SkillLookupRequest {
            skills: vec!["x".into()],
            full_refresh: true,
            relay_budget: 2,
            visited: vec!["v1".into()],
        }));
        let dec = RpcRequest::decode(&req.encode()).unwrap();
        assert_eq!(req, dec);

        let resp = RpcResponse(RpcResponseKind::Ping(PingResponse {
            nonce: 42,
            peer_time: 100,
        }));
        let dec = RpcResponse::decode(&resp.encode()).unwrap();
        assert_eq!(resp, dec);
    }

    #[test]
    fn unknown_fields_are_skipped() {
        // envelope + неизвестное поле 99 (string)
        let mut enc = TaskEnvelope {
            task_id: "T".into(),
            ..Default::default()
        }
        .encode();
        let mut extra = Writer::new();
        extra.string(99, "future");
        enc.extend_from_slice(&extra.into_vec());
        let dec = TaskEnvelope::decode(&enc).unwrap();
        assert_eq!(dec.task_id, "T");
    }
}
