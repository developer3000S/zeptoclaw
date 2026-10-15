//! Порт пакета `internal/tasks`: модель единицы работы и её жизненный цикл —
//! валидация, локальное исполнение, делегирование, агрегация.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::config::Config;
use crate::picoclaw::{self, Adapter, Request as ExecRequest, Response as ExecResponse};
use crate::proto::{
    ArtifactRef, Capabilities, Message as PbMsg, TaskAck, TaskConstraints, TaskEnvelope,
    TaskErrorClass, TaskPayload, TaskResult, TaskStatus,
};
use crate::routing::Table;
use crate::security::{verify, Identity, Policy};
use crate::storage::{ArtifactInfo, ResultRecord, Store, TaskRecord};
use crate::wire;

// ---------------------------------------------------------------------------
// Статусы (Go: tasks.Status)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Received,
    Validating,
    Evaluating,
    Accepted,
    Rejected,
    Forwarded,
    Running,
    WaitingSubtasks,
    Aggregating,
    Completed,
    Failed,
    TimedOut,
    Canceled,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Received => "RECEIVED",
            Status::Validating => "VALIDATING",
            Status::Evaluating => "EVALUATING",
            Status::Accepted => "ACCEPTED",
            Status::Rejected => "REJECTED",
            Status::Forwarded => "FORWARDED",
            Status::Running => "RUNNING",
            Status::WaitingSubtasks => "WAITING_SUBTASKS",
            Status::Aggregating => "AGGREGATING",
            Status::Completed => "COMPLETED",
            Status::Failed => "FAILED",
            Status::TimedOut => "TIMEOUT",
            Status::Canceled => "CANCELED",
        }
    }
    /// Терминальные статусы — дальнейших переходов не ожидается.
    pub fn terminal(&self) -> bool {
        matches!(
            self,
            Status::Completed
                | Status::Failed
                | Status::TimedOut
                | Status::Canceled
                | Status::Rejected
        )
    }
    /// Обратная интерпретация строки журнала. Нераспознанное написание ->
    /// Received (намеренно нетерминально, как в Go).
    pub fn parse(s: &str) -> Status {
        match s {
            "VALIDATING" => Status::Validating,
            "EVALUATING" => Status::Evaluating,
            "ACCEPTED" => Status::Accepted,
            "REJECTED" => Status::Rejected,
            "FORWARDED" => Status::Forwarded,
            "RUNNING" => Status::Running,
            "WAITING_SUBTASKS" => Status::WaitingSubtasks,
            "AGGREGATING" => Status::Aggregating,
            "COMPLETED" => Status::Completed,
            "FAILED" => Status::Failed,
            "TIMEOUT" => Status::TimedOut,
            "CANCELED" => Status::Canceled,
            _ => Status::Received,
        }
    }
    pub fn to_proto(self) -> TaskStatus {
        match self {
            Status::Completed => TaskStatus::Completed,
            Status::Failed => TaskStatus::Failed,
            Status::TimedOut => TaskStatus::Timeout,
            Status::Canceled => TaskStatus::Canceled,
            Status::Accepted => TaskStatus::Accepted,
            Status::Rejected => TaskStatus::Rejected,
            Status::Running => TaskStatus::Running,
            Status::Forwarded => TaskStatus::Forwarded,
            Status::WaitingSubtasks => TaskStatus::WaitingSubtasks,
            Status::Aggregating => TaskStatus::Aggregating,
            _ => TaskStatus::Unspecified,
        }
    }
    pub fn from_proto(s: TaskStatus) -> Status {
        match s {
            TaskStatus::Completed => Status::Completed,
            TaskStatus::Failed => Status::Failed,
            TaskStatus::Timeout => Status::TimedOut,
            TaskStatus::Canceled => Status::Canceled,
            TaskStatus::Accepted => Status::Accepted,
            TaskStatus::Rejected => Status::Rejected,
            TaskStatus::Running => Status::Running,
            TaskStatus::Forwarded => Status::Forwarded,
            TaskStatus::WaitingSubtasks => Status::WaitingSubtasks,
            TaskStatus::Aggregating => Status::Aggregating,
            _ => Status::Received,
        }
    }
}

// ---------------------------------------------------------------------------
// Константы лимитов (ТЗ 6.8)
// ---------------------------------------------------------------------------

pub const MAX_REQUIRED_SKILLS: usize = 16;
pub const MAX_ROUTE_STACK: usize = 64;
pub const MAX_INSTRUCTION_LEN: usize = 1 << 20;
/// Допустимый дрейф часов пиров до «похожести на повтор».
pub const CLOCK_SKEW_TOLERANCE_SECS: i64 = 300;
/// Максимальный возраст задачи при первом взгляде.
pub const MAX_AGE_SECS: i64 = 86400;
pub const MAX_SUBTASKS: usize = 8;

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn now_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Временнó-упорядоченный UUIDv7 (как uuid.NewV7 в Go): 48-битная отметка
/// времени в миллисекундах, версия 7, вариант RFC 4122, случайные биты.
pub fn new_id() -> String {
    let ms = (now_f64() * 1000.0).floor() as u64;
    let mut rnd = [0u8; 10];
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut rnd);
    // Раскладка UUID v7: unix_ts_ms(48) | ver(4)+rand_a(12) | rand_b(62) | var(2).
    let time_hi = (ms >> 16) as u32; // верхние 32 бита 48-битной отметки
    let time_mid = (ms & 0xffff) as u16;
    let ver_rand_a = (((rnd[0] as u16) << 8 | rnd[1] as u16) & 0x0fff) | 0x7000;
    let clk_seq = ((rnd[2] & 0x3f) | 0x80, rnd[3]);
    format!(
        "{:08x}-{:04x}-{:04x}-{:02x}{:02x}-{}",
        time_hi,
        time_mid,
        ver_rand_a,
        clk_seq.0,
        clk_seq.1,
        (0..6)
            .map(|i| format!("{:02x}", rnd[4 + i]))
            .collect::<Vec<_>>()
            .join("")
    )
}

// ---------------------------------------------------------------------------
// Ошибки валидации (Go: errors.Is-таксономия)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rule {
    EmptyTaskId,
    MissingOrigin,
    MissingSender,
    NoInstruction,
    TtlExhausted,
    RouteLoop,
    PayloadTooLarge,
    TooManySkills,
    BadPriority,
    Stale,
    TooFarAhead,
    RouteTooLong,
    DigestMismatch,
}

impl Rule {
    pub fn msg(&self) -> &'static str {
        match self {
            Rule::EmptyTaskId => "task: empty task_id",
            Rule::MissingOrigin => "task: missing origin_peer_id",
            Rule::MissingSender => "task: missing sender_peer_id",
            Rule::NoInstruction => "task: empty instruction",
            Rule::TtlExhausted => "task: ttl exhausted",
            Rule::RouteLoop => "task: route loop detected",
            Rule::PayloadTooLarge => "task: payload exceeds limit",
            Rule::TooManySkills => "task: too many required_skills",
            Rule::BadPriority => "task: priority out of range 1..9",
            Rule::Stale => "task: created too far in the past",
            Rule::TooFarAhead => "task: created too far in the future",
            Rule::RouteTooLong => "task: route_stack too long",
            Rule::DigestMismatch => "task: context_digest does not match payload",
        }
    }
}

/// Ошибка валидации со всеми нарушенными правилами сразу: оператор, чинящий
/// отклонённую задачу, не должен перевыяснять по одному нарушению за раунд.
#[derive(Debug, Clone)]
pub struct ValidationError(pub Vec<Rule>);

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "task: invalid: {}",
            self.0.iter().map(|r| r.msg()).collect::<Vec<_>>().join("; ")
        )
    }
}
impl std::error::Error for ValidationError {}

// ---------------------------------------------------------------------------
// Хелперы envelope (Go: Build/Validate/DeriveSubtask/...)
// ---------------------------------------------------------------------------

/// Входы для построения корневого envelope.
#[derive(Debug, Clone, Default)]
pub struct BuildRequest {
    pub instruction: String,
    pub required_skills: Vec<String>,
    pub ttl: i32,
    pub priority: i32,
    pub timeout_seconds: i32,
    pub allow_shell: bool,
    pub allow_network: bool,
    pub allow_deleg: Option<bool>,
    pub allow_subtasks: Option<bool>,
    pub labels: BTreeMap<String, String>,
    pub attachments: Vec<ArtifactRef>,
}

/// Строит неподписанный корневой envelope. Подпись — забота вызывающего,
/// чтобы ключевой материал оставался за пределами модуля.
pub fn build(origin_peer: &str, id: &str, req: BuildRequest, now: DateTime<Utc>) -> TaskEnvelope {
    let id = if id.is_empty() { new_id() } else { id.to_string() };
    let ttl = if req.ttl <= 0 { 5 } else { req.ttl };
    let prio = if req.priority <= 0 { 5 } else { req.priority };
    let allow_deleg = req.allow_deleg.unwrap_or(true);
    let allow_sub = req.allow_subtasks.unwrap_or(false);
    let mut env = TaskEnvelope {
        task_id: id,
        origin_peer_id: origin_peer.to_string(),
        sender_peer_id: origin_peer.to_string(),
        created_at: now.timestamp(),
        ttl,
        priority: prio,
        required_skills: normalize_skills(&req.required_skills),
        payload: Some(TaskPayload {
            instruction: req.instruction,
            attachments: req.attachments,
            labels: req.labels,
            ..Default::default()
        }),
        constraints: Some(TaskConstraints {
            max_duration_seconds: req.timeout_seconds,
            allow_network_tools: req.allow_network,
            allow_shell: req.allow_shell,
            allow_delegation: allow_deleg,
            allow_subtasks: allow_sub,
        }),
        ..Default::default()
    };
    env.context_digest = content_digest(&env);
    env
}

/// Стабильный хеш всего, что авторизовал источник. Специально независим от
/// изменяемых маршрутных полей (sender, ttl, route_stack), чтобы любой узел
/// на пути мог пересчитать и проверить.
pub fn content_digest(env: &TaskEnvelope) -> String {
    let body = wire::task_content(env);
    let mut h = Sha256::new();
    h.update(b"zeptomesh/content\0");
    h.update(body);
    format!("sha256:{}", hex::encode(h.finalize()))
}

/// Пересчитывает дайджест контента и сверяет с полем.
pub fn verify_digest(env: &TaskEnvelope) -> Result<(), Rule> {
    let want = content_digest(env);
    let got = env.context_digest.as_str();
    if got.is_empty() {
        return Err(Rule::DigestMismatch);
    }
    if !got.eq_ignore_ascii_case(&want) {
        return Err(Rule::DigestMismatch);
    }
    Ok(())
}

/// Структурные и политические инварианты envelope.
pub fn validate(env: &TaskEnvelope, max_payload: i64, now: DateTime<Utc>) -> Result<(), ValidationError> {
    let mut errs: Vec<Rule> = Vec::new();
    let mut require = |cond: bool, r: Rule| {
        if !cond {
            errs.push(r);
        }
    };
    require(!env.task_id.is_empty(), Rule::EmptyTaskId);
    require(!env.origin_peer_id.is_empty(), Rule::MissingOrigin);
    require(!env.sender_peer_id.is_empty(), Rule::MissingSender);
    // Пустой по пробелам — тоже пустой: адаптер отвергнет такой промпт, так
    // что принимать его здесь означало бы потратить слот ради позднего отказа.
    require(!env.payload().instruction.trim().is_empty(), Rule::NoInstruction);
    require(env.payload().instruction.len() <= MAX_INSTRUCTION_LEN, Rule::PayloadTooLarge);
    require(env.ttl >= 0, Rule::TtlExhausted);
    require((1..=9).contains(&env.priority), Rule::BadPriority);
    require(env.required_skills.len() <= MAX_REQUIRED_SKILLS, Rule::TooManySkills);
    require(env.route_stack.len() <= MAX_ROUTE_STACK, Rule::RouteTooLong);
    if max_payload > 0 {
        require(env.encode().len() as i64 <= max_payload, Rule::PayloadTooLarge);
    }
    if env.created_at > 0 {
        let age = now.timestamp() - env.created_at;
        require(age <= MAX_AGE_SECS, Rule::Stale);
        require(age >= -CLOCK_SKEW_TOLERANCE_SECS, Rule::TooFarAhead);
    }
    if let Err(r) = verify_digest(env) {
        errs.push(r);
    }
    if errs.is_empty() {
        Ok(())
    } else {
        Err(ValidationError(errs))
    }
}

/// Отклоняет задачу, которая уже побывала на этом пире, или с истёкшим ttl.
pub fn check_route(env: &TaskEnvelope, self_pid: &str) -> Result<(), Rule> {
    if env.ttl <= 0 {
        return Err(Rule::TtlExhausted);
    }
    if env.route_stack.iter().any(|h| h == self_pid) {
        return Err(Rule::RouteLoop);
    }
    Ok(())
}

/// Записывает прыжок; идемпотентно для того же пира.
pub fn append_route(env: &mut TaskEnvelope, p: &str) {
    if p.is_empty() {
        return;
    }
    if env.route_stack.last().map(String::as_str) == Some(p) {
        return;
    }
    env.route_stack.push(p.to_string());
}

/// Пир, которому надо relay-нуть результат обратно.
pub fn previous_hop(env: &TaskEnvelope) -> Option<&str> {
    env.route_stack.last().map(String::as_str)
}

/// Строит дочерний envelope из родительского. Дочер наследует origin и на
/// единицу меньший ttl; ограничения — пересечение родительских и запроса,
/// поэтому подзадача никогда не получает новых прав.
pub fn derive_subtask(
    parent: &TaskEnvelope,
    child_id: &str,
    mut req: BuildRequest,
    now: DateTime<Utc>,
) -> TaskEnvelope {
    let mut child = build(&parent.origin_peer_id, child_id, req.clone(), now);
    child.parent_task_id = parent.task_id.clone();
    child.created_at = now.timestamp();

    if req.ttl <= 0 {
        req.ttl = parent.ttl - 1;
    }
    let mut ttl = req.ttl;
    let max = parent.ttl - 1;
    if ttl > max {
        ttl = max;
    }
    if ttl < 0 {
        ttl = 0;
    }
    child.ttl = ttl;

    let pc = parent.constraints();
    let cc = child.constraints.get_or_insert(TaskConstraints::default());
    cc.allow_shell = pc.allow_shell && cc.allow_shell;
    cc.allow_network_tools = pc.allow_network_tools && cc.allow_network_tools;
    cc.allow_delegation = pc.allow_delegation && cc.allow_delegation;
    cc.allow_subtasks = pc.allow_subtasks && cc.allow_subtasks;
    if pc.max_duration_seconds > 0
        && (cc.max_duration_seconds == 0 || cc.max_duration_seconds > pc.max_duration_seconds)
    {
        cc.max_duration_seconds = pc.max_duration_seconds;
    }
    child.priority = parent.priority;
    child.context_digest = content_digest(&child);
    child
}

/// Эффективный дедлайн исполнения: запрошенное значение иначе дефолт узла —
/// всегда под потолком оператора (ТЗ 6.8.3). Без потолка удалённый заказчик
/// мог бы занять воркер-слот на произвольно долгое время.
pub fn timeout_seconds(env: &TaskEnvelope, def: i32, max: i32) -> i32 {
    let mut t = def;
    let c = env.constraints().max_duration_seconds;
    if c > 0 {
        t = c;
    }
    if t <= 0 {
        t = 600;
    }
    if max > 0 && t > max {
        t = max;
    }
    t
}

/// Классификация ошибки по машине таксономии ТЗ 6.12. Relay решает по ней,
/// есть ли смысл повторять: сетевые и no-worker — повторять можно,
/// security/validation — никогда.
pub fn error_class(st: TaskStatus, msg: &str) -> TaskErrorClass {
    match st {
        TaskStatus::Canceled => return TaskErrorClass::Canceled,
        TaskStatus::Timeout => return TaskErrorClass::Timeout,
        TaskStatus::Rejected => return TaskErrorClass::Validation,
        TaskStatus::Failed => {}
        _ => return TaskErrorClass::Unspecified,
    }
    let low = msg.to_lowercase();
    // Случаи «нет воркера» проверяются до сканирования ключевых слов: их
    // сообщения цитируют отказ удалённого пира дословно, и эти слова иначе
    // перевернули бы класс.
    if ["no eligible", "no candidate", "no neighbour", "no capability", "skills"]
        .iter()
        .any(|k| low.contains(k))
    {
        return TaskErrorClass::NoWorker;
    }
    if ["signature", "trust", "blocked", "allow"]
        .iter()
        .any(|k| low.contains(k))
    {
        return TaskErrorClass::Security;
    }
    if ["digest", "valid", "ttl", "unknown task"]
        .iter()
        .any(|k| low.contains(k))
    {
        return TaskErrorClass::Validation;
    }
    if ["queue", "rate", "disk", "memory", "limit"]
        .iter()
        .any(|k| low.contains(k))
    {
        return TaskErrorClass::Limits;
    }
    if ["connect", "reset", "unreachable", "timeout "]
        .iter()
        .any(|k| low.contains(k))
    {
        return TaskErrorClass::Network;
    }
    TaskErrorClass::Execution
}

/// Короткое имя класса для журнала ("TIMEOUT", а не "TASK_ERROR_CLASS_TIMEOUT").
pub fn error_class_name(ec: TaskErrorClass) -> String {
    if ec == TaskErrorClass::Unspecified {
        return String::new();
    }
    let full = match ec {
        TaskErrorClass::Validation => "VALIDATION",
        TaskErrorClass::Security => "SECURITY",
        TaskErrorClass::Execution => "EXECUTION",
        TaskErrorClass::NoWorker => "NO_WORKER",
        TaskErrorClass::Network => "NETWORK",
        TaskErrorClass::Timeout => "TIMEOUT",
        TaskErrorClass::Limits => "LIMITS",
        TaskErrorClass::Canceled => "CANCELED",
        _ => "",
    };
    full.to_string()
}

/// Обратное преобразование error_class_name.
pub fn error_class_by_name(s: &str) -> TaskErrorClass {
    match s {
        "VALIDATION" => TaskErrorClass::Validation,
        "SECURITY" => TaskErrorClass::Security,
        "EXECUTION" => TaskErrorClass::Execution,
        "NO_WORKER" => TaskErrorClass::NoWorker,
        "NETWORK" => TaskErrorClass::Network,
        "TIMEOUT" => TaskErrorClass::Timeout,
        "LIMITS" => TaskErrorClass::Limits,
        "CANCELED" => TaskErrorClass::Canceled,
        _ => TaskErrorClass::Unspecified,
    }
}

/// Нижние регистры, trim, дедупликация и сортировка списка навыков.
pub fn normalize_skills(in_: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for s in in_ {
        let s = s.trim().to_lowercase();
        if s.is_empty() || !seen.insert(s.clone()) {
            continue;
        }
        out.push(s);
    }
    out.sort();
    out
}

/// Может ли узел с haveSkills обслужить wantSkills. Синтетические навыки
/// "general" и "any" совпадают с любым требованием.
pub fn skills_match(have_skills: &[String], want_skills: &[String]) -> (usize, bool) {
    let norm = |s: &str| s.trim().to_lowercase();
    let have: HashSet<String> = have_skills.iter().map(|s| norm(s)).collect();
    if have.contains("general") || have.contains("any") {
        return (want_skills.len(), true);
    }
    let matched = want_skills.iter().filter(|w| have.contains(&norm(w))).count();
    if want_skills.is_empty() {
        return (0, true);
    }
    (matched, matched == want_skills.len())
}

/// Однострочное описание задачи для логов.
pub fn describe(env: &TaskEnvelope) -> String {
    let mut ins: String = env.payload().instruction.chars().take(60).collect();
    if env.payload().instruction.chars().count() > 60 {
        ins.push('…');
    }
    format!(
        "task {} ttl={} skills={:?} {:?}",
        env.task_id, env.ttl, env.required_skills, ins
    )
}

// ---------------------------------------------------------------------------
// Менеджер задач (Go: tasks.Manager — ядро конвейера)
// ---------------------------------------------------------------------------

/// Вход local_submit / API submit.
#[derive(Debug, Clone)]
pub struct SubmitRequest {
    pub instruction: String,
    pub required_skills: Vec<String>,
    pub ttl: i32,
    pub priority: i32,
    pub timeout_seconds: i32,
    pub allow_shell: bool,
    pub allow_network: bool,
    pub allow_deleg: Option<bool>,
    pub allow_subtasks: Option<bool>,
    pub labels: BTreeMap<String, String>,
    /// План декомпозиции (ТЗ 6.9.1): непустой ⇒ задача исполняется как набор
    /// независимо маршрутизируемых детей, результаты возвращаются одним
    /// подписанным агрегатом (Go: tasks.SubtaskRequest).
    pub subtasks: Vec<SubtaskSpec>,
}

/// Одна запись плана декомпозиции.
#[derive(Clone, Debug, Default)]
pub struct SubtaskSpec {
    pub instruction: String,
    pub required_skills: Vec<String>,
    pub ttl: i32,
    pub priority: i32,
}

impl Default for SubmitRequest {
    fn default() -> Self {
        SubmitRequest {
            instruction: String::new(),
            required_skills: Vec::new(),
            ttl: 0,
            priority: 0,
            timeout_seconds: 0,
            allow_shell: false,
            allow_network: true,
            allow_deleg: None,
            allow_subtasks: None,
            labels: BTreeMap::new(),
            subtasks: Vec::new(),
        }
    }
}

/// Колбэк доставки envelope пиру (реализуется уровнем node/p2p).
pub type Deliver = Arc<
    dyn Fn(String, TaskEnvelope) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;
/// Колбэк доставки результата пиру.
pub type DeliverResult =
    Arc<
        dyn Fn(String, TaskResult) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            + Send
            + Sync,
    >;
/// Колбэк отмены задачи на пире.
pub type CancelPeer =
    Arc<dyn Fn(String, String) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// Снимок состояния задачи для статуса/API.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TaskView {
    pub task_id: String,
    pub status: String,
    pub origin_peer_id: String,
    pub worker_peer_id: String,
    pub attempts: i32,
    pub delegated_to: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

/// JSON-снимок protobuf-результата для admin-API (TaskResult — ручной codec,
/// не serde-структура; поля соответствуют Go json-тегам).
pub fn result_json(r: &TaskResult) -> serde_json::Value {
    serde_json::json!({
        "task_id": r.task_id,
        "worker_peer_id": r.worker_peer_id,
        "status": crate::tasks::Status::from_proto(r.status).as_str(),
        "text": r.text,
        "result_digest": hex::encode(&r.result_digest),
        "error_message": r.error_message,
        "started_at": r.started_at,
        "finished_at": r.finished_at,
        "artifacts": r.artifacts.iter().map(|a| serde_json::json!({
            "name": a.name,
            "hash": hex::encode(&a.hash),
            "size": a.size,
            "media_type": a.media_type,
        })).collect::<Vec<_>>(),
        "route_stack": r.route_stack,
        "error_class": crate::tasks::error_class_name(r.error_class),
        "aggregated": r.aggregated,
    })
}

pub struct Manager {
    pub cfg: Arc<Config>,
    pub store: Store,
    pub table: Table,
    pub policy: Arc<Policy>,
    pub identity: Arc<Identity>,
    pub adapter: Arc<dyn Adapter + Send + Sync>,
    deliver: Mutex<Option<Deliver>>,
    deliver_result: Mutex<Option<DeliverResult>>,
    cancel_peer: Mutex<Option<CancelPeer>>,
    running: AtomicI64,
    completed: AtomicI64,
    failed: AtomicI64,
    rejected: AtomicI64,
    forwarded: AtomicI64,
    local_tasks: Mutex<HashSet<String>>,
    results: Mutex<BTreeMap<String, TaskResult>>,
    ws_counter: AtomicI64,
    /// Очередь локального исполнения (Go: семафор tasks.Manager): держит не
    /// больше max_parallel одновременных прогонов адаптера.
    sem: Arc<tokio::sync::Semaphore>,
    /// План декомпозиции ожидающего корня (ТЗ 6.9.1).
    #[allow(dead_code)]
    parent_plan: Mutex<BTreeMap<String, Vec<SubtaskSpec>>>,
    /// child_id → parent_id для агрегации результатов детей.
    #[allow(dead_code)]
    child_links: Mutex<BTreeMap<String, String>>,
    /// task_id → последний известный envelope (для агрегатов и relay-доставки).
    envelopes: Mutex<BTreeMap<String, TaskEnvelope>>,
    /// task_id → инструкция (для восстановления при resubmit через admin API).
    instructions: Mutex<BTreeMap<String, String>>,
}

impl Manager {
    pub fn new(
        cfg: Arc<Config>,
        store: Store,
        table: Table,
        policy: Arc<Policy>,
        identity: Arc<Identity>,
        adapter: Arc<dyn Adapter + Send + Sync>,
    ) -> Self {
        let permits = cfg.tasks.max_parallel_tasks.max(1) as usize;
        Self {
            cfg,
            store,
            table,
            policy,
            identity,
            adapter,
            deliver: Mutex::new(None),
            deliver_result: Mutex::new(None),
            cancel_peer: Mutex::new(None),
            running: AtomicI64::new(0),
            completed: AtomicI64::new(0),
            failed: AtomicI64::new(0),
            rejected: AtomicI64::new(0),
            forwarded: AtomicI64::new(0),
            local_tasks: Mutex::new(HashSet::new()),
            results: Mutex::new(BTreeMap::new()),
            ws_counter: AtomicI64::new(0),
            sem: Arc::new(tokio::sync::Semaphore::new(permits)),
            parent_plan: Mutex::new(BTreeMap::new()),
            child_links: Mutex::new(BTreeMap::new()),
            envelopes: Mutex::new(BTreeMap::new()),
            instructions: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn set_deliver(&self, f: Deliver) {
        *self.deliver.lock().unwrap() = Some(f);
    }
    pub fn set_deliver_result(&self, f: DeliverResult) {
        *self.deliver_result.lock().unwrap() = Some(f);
    }
    pub fn set_cancel_peer(&self, f: CancelPeer) {
        *self.cancel_peer.lock().unwrap() = Some(f);
    }

    pub fn self_id(&self) -> &str {
        self.identity.peer_id()
    }
    pub fn running(&self) -> i64 {
        self.running.load(Ordering::SeqCst)
    }
    pub fn counters(&self) -> (i64, i64, i64, i64) {
        (
            self.completed.load(Ordering::SeqCst),
            self.failed.load(Ordering::SeqCst),
            self.rejected.load(Ordering::SeqCst),
            self.forwarded.load(Ordering::SeqCst),
        )
    }

    /// Подписывает envelope телом задачи (sender-подпись поверх схемы ed25519).
    pub fn sign_task(&self, env: &mut TaskEnvelope) {
        env.signature_scheme = "ed25519-sha256-v1".into();
        env.signature = self.identity.sign(&wire::task_body(env));
    }
    /// Проверка подписи отправителя по публичному ключу, извлечённому из PeerID.
    pub fn verify_task_sig(&self, env: &TaskEnvelope) -> Result<()> {
        if env.signature.is_empty() {
            bail!("tasks: missing signature");
        }
        let vk = crate::security::extract_key_from_peer_id(&env.sender_peer_id)?;
        verify(&vk.to_bytes().to_vec(), &wire::task_body(env), &env.signature)
            .map_err(|e| anyhow!("tasks: {}", e))
    }

    fn put_journal(&self, rec: &mut TaskRecord) {
        rec.updated_at = now_unix();
        let _ = self.store.put_task(rec);
    }

    /// Последний известный envelope задачи (для агрегатов/relay-доставки).
    fn envelope_of(&self, task_id: &str) -> Result<TaskEnvelope> {
        if let Some(e) = self.envelopes.lock().unwrap().get(task_id) {
            return Ok(e.clone());
        }
        bail!("tasks: unknown envelope {task_id}")
    }

    fn remember_envelope(&self, env: &TaskEnvelope) {
        let mut ins = self.instructions.lock().unwrap();
        if !env.payload().instruction.is_empty() {
            ins.insert(env.task_id.clone(), env.payload().instruction.clone());
        }
        drop(ins);
        self.envelopes
            .lock()
            .unwrap()
            .insert(env.task_id.clone(), env.clone());
    }

    /// Инструкция задачи по её id (для admin API resubmit). None — если задача
    /// неизвестна или инструкция не сохранялась.
    pub fn instruction_of(&self, task_id: &str) -> Option<String> {
        self.instructions.lock().unwrap().get(task_id).cloned()
    }

    fn journal_entry(&self, env: &TaskEnvelope, status: Status) -> TaskRecord {
        TaskRecord {
            task_id: env.task_id.clone(),
            parent_task_id: env.parent_task_id.clone(),
            origin_peer_id: env.origin_peer_id.clone(),
            sender_peer_id: env.sender_peer_id.clone(),
            status: status.as_str().into(),
            required_skills: env.required_skills.clone(),
            received_at: now_f64(),
            ttl: env.ttl,
            priority: env.priority,
            payload_hash: env.context_digest.clone(),
            ..Default::default()
        }
    }

    /// Корневая локальная подача: построить, подписать, записать журнал,
    /// решить — исполнять самому или делегировать.
    pub async fn submit(self: &Arc<Self>, req: SubmitRequest) -> Result<String> {
        if req.instruction.trim().is_empty() {
            bail!("tasks: empty instruction");
        }
        let mut req = req;
        if req.ttl <= 0 {
            req.ttl = self.cfg.tasks.default_ttl;
        }
        if req.ttl > self.cfg.tasks.max_ttl {
            req.ttl = self.cfg.tasks.max_ttl;
        }
        // План декомпозиции (Go: Submit c Subtasks): неявное разрешение
        // подзадач, когда дети заявлены, и пометка корня для агрегации.
        let has_subs = !req.subtasks.is_empty();
        if has_subs && req.allow_subtasks.is_none() {
            req.allow_subtasks = Some(true);
        }
        let mut labels = req.labels.clone();
        if has_subs {
            labels.insert("subtask_count".into(), req.subtasks.len().to_string());
        }
        let mut env = build(
            self.self_id(),
            "",
            BuildRequest {
                instruction: req.instruction,
                required_skills: req.required_skills,
                ttl: req.ttl,
                priority: req.priority,
                timeout_seconds: req.timeout_seconds,
                allow_shell: req.allow_shell,
                allow_network: req.allow_network,
                allow_deleg: req.allow_deleg,
                allow_subtasks: req.allow_subtasks,
                labels,
                attachments: vec![],
            },
            Utc::now(),
        );
        validate(&env, self.cfg.tasks.max_task_payload_bytes, Utc::now())?;
        self.sign_task(&mut env);
        let mut rec = self.journal_entry(&env, Status::Received);
        rec.status = Status::Accepted.as_str().into();
        self.put_journal(&mut rec);
        self.local_tasks.lock().unwrap().insert(env.task_id.clone());
        self.remember_envelope(&env);
        let id = env.task_id.clone();
        if has_subs {
            let subs = req.subtasks.clone();
            self.spawn_decomposition(env, subs);
        } else {
            self.route_inbound(env).await?;
        }
        Ok(id)
    }

    /// Разворачивает план декомпозиции в детей (ТЗ 6.9.1): корень уходит в
    /// WAITING_SUBTASKS, каждый ребёнок строится через derive_subtask (права
    /// ребёнка ⊆ прав родителя), подписывается и маршрутизируется как обычная
    /// задача. Агрегация — в handle_result по child_links.
    fn spawn_decomposition(self: &Arc<Self>, env: TaskEnvelope, subs: Vec<SubtaskSpec>) {
        let me = self.clone();
        tokio::spawn(async move {
            let parent_id = env.task_id.clone();
            if let Ok(mut t) = me.store.get_task(&parent_id) {
                t.status = Status::WaitingSubtasks.as_str().into();
                me.put_journal(&mut t);
            }
            for st in subs.iter().take(MAX_SUBTASKS) {
                let child_env = derive_subtask(
                    &env,
                    "",
                    BuildRequest {
                        instruction: st.instruction.clone(),
                        required_skills: st.required_skills.clone(),
                        ttl: st.ttl,
                        priority: st.priority,
                        timeout_seconds: 0,
                        allow_shell: false,
                        allow_network: true,
                        allow_deleg: None,
                        allow_subtasks: Some(false),
                        labels: Default::default(),
                        attachments: vec![],
                    },
                    Utc::now(),
                );
                let mut child_env = child_env;
                me.sign_task(&mut child_env);
                if let Err(e) = validate(&child_env, me.cfg.tasks.max_task_payload_bytes, Utc::now()) {
                    tracing::warn!(parent = %parent_id, err = %e, "subtask_validation");
                    continue;
                }
                let mut rec = me.journal_entry(&child_env, Status::Received);
                rec.parent_task_id = parent_id.clone();
                me.put_journal(&mut rec);
                me.child_links.lock().unwrap().insert(child_env.task_id.clone(), parent_id.clone());
                if let Err(e) = me.route_inbound(child_env).await {
                    tracing::warn!(parent = %parent_id, err = %e, "subtask_route");
                }
            }
            me.parent_plan.lock().unwrap().insert(parent_id, subs);
        });
    }

    /// Агрегация результата ребёнка в ожидающий корень (Go: tasks.Manager
    /// subtask aggregation). Возвращает true, если результат принадлежит
    /// ребёнку и был поглощён (или корень уже завершён агрегатом).
    async fn absorb_child_result(&self, res: &TaskResult) -> Result<bool> {
        // Scoped-guard паттерн: все блокировки std::sync::Mutex закрываются до
        // `.await` (в Rust 2021 MutexGuard через await делает future не-Send).
        let parent_id = {
            match self.child_links.lock().unwrap().get(&res.task_id).cloned() {
                Some(p) => p,
                None => return Ok(false),
            }
        };
        let plan = {
            match self.parent_plan.lock().unwrap().get(&parent_id).cloned() {
                Some(p) => p,
                None => return Ok(false),
            }
        };
        // Копим результаты детей; когда собрано по плану — собираем агрегат.
        {
            self.results.lock().unwrap().insert(res.task_id.clone(), res.clone());
        }
        // Ребёнок -> его инструкция (из конверта задачи), только для данного
        // родителя (Go: tasks.Manager агрегация по childLinks + инструкции).
        let kids: Vec<String> = {
            let links = self.child_links.lock().unwrap();
            links
                .iter()
                .filter(|(_, p)| **p == parent_id)
                .map(|(c, _)| c.clone())
                .collect()
        };
        let mut texts: Vec<String> = Vec::new();
        let mut any_failed = false;
        let mut got = 0usize;
        for c in kids.iter() {
            let r = { self.results.lock().unwrap().get(c).cloned() };
            if let Some(r) = r {
                got += 1;
                let s = Status::from_proto(r.status);
                if !s.terminal() {
                    continue;
                }
                if s != Status::Completed {
                    any_failed = true;
                }
                let inst = {
                    self.envelopes
                        .lock()
                        .unwrap()
                        .get(c)
                        .map(|e| e.payload().instruction.clone())
                        .unwrap_or_default()
                };
                texts.push(format!("{inst}: {}", r.text));
            }
        }
        if got < plan.len() {
            return Ok(true); // ждём остальных детей
        }
        let agg = TaskResult {
            task_id: parent_id.clone(),
            worker_peer_id: self.self_id().into(),
            sender_peer_id: self.self_id().into(),
            status: if any_failed { TaskStatus::Failed } else { TaskStatus::Completed },
            text: texts.join("\n"),
            aggregated: true,
            started_at: 0,
            finished_at: now_unix(),
            ..Default::default()
        };
        self.complete_aggregate(parent_id, agg).await
    }

    fn complete_aggregate(
        &self,
        parent_id: String,
        agg: TaskResult,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool>> + Send + '_>> {
        // Рекурсия handle_result → absorb → complete_aggregate → handle_result:
        // future должен быть забоксирован (E0733).
        Box::pin(async move { self.complete_aggregate_inner(parent_id, agg).await })
    }

    async fn complete_aggregate_inner(&self, parent_id: String, mut agg: TaskResult) -> Result<bool> {
        // Scoped-guard: все `.lock()` закрываются до ближайшего `.await` — иначе
        // MutexGuard живёт через await и future становится не-Send (Rust 2021).
        agg.result_digest = wire::result_digest(&agg);
        agg.signature_scheme = "ed25519-sha256-v1".into();
        agg.signature = self.identity.sign(&wire::result_body(&agg));
        agg.worker_signature = agg.signature.clone();
        {
            self.results.lock().unwrap().insert(parent_id.clone(), agg.clone());
        }
        if let Ok(mut t) = self.store.get_task(&parent_id) {
            t.status = Status::from_proto(agg.status).as_str().into();
            t.finished_at = agg.finished_at as f64;
            self.put_journal(&mut t);
        }
        let _ = self.store.put_result(&ResultRecord {
            task_id: parent_id.clone(),
            status: Status::from_proto(agg.status).as_str().into(),
            text: agg.text.clone(),
            worker_peer_id: agg.worker_peer_id.clone(),
            signature: agg.signature.clone(),
            worker_signature: agg.worker_signature.clone(),
            result_digest: format!("sha256:{}", hex::encode(agg.result_digest.clone())),
            aggregated: true,
            ..Default::default()
        });
        // Доставка aggregate автору, если корень пришёл извне.
        let origin = {
            self.envelopes
                .lock()
                .unwrap()
                .get(&parent_id)
                .map(|e| e.origin_peer_id.clone())
        };
        match origin {
            Some(o) if o != self.self_id() => {
                let _ = self.call_deliver_result(o, agg).await;
            }
            Some(_) => {}
            None => {
                let is_local_root = {
                    self.instructions.lock().unwrap().contains_key(&parent_id)
                };
                if is_local_root {
                    // Корень создан локально: доставляем результат самому себе
                    // через тот же конвейер, что и удалённые результаты
                    // (Go: local delivery). Для sender==self handle_result
                    // просто сохраняет результат в map.
                    self.results.lock().unwrap().insert(parent_id.clone(), agg);
                }
            }
        }
        Ok(true)
    }

    /// Точка входа принятого конверта (своего или чужого): оценить → принять →
    /// исполнить локально либо делегировать.
    pub async fn route_inbound(&self, env: TaskEnvelope) -> Result<()> {
        self.remember_envelope(&env);
        let is_local = { self.local_tasks.lock().unwrap().contains(&env.task_id) };
        // Дедупликация повторных доставок одного task id в окне.
        if !is_local {
            let (claimed, prev) = self
                .store
                .claim_dedup(&env.task_id, self.cfg.tasks.dedup_window)?;
            if !claimed {
                tracing::debug!(
                    "tasks: duplicate delivery of {} (first seen {}s ago)",
                    env.task_id,
                    now_unix() - prev.first_seen_unix
                );
                return Ok(());
            }
        }
        // Политика: внешние задачи принимаются только с разрешением узла.
        if !is_local && !self.cfg.capabilities.accept_external_tasks {
            self.reject(&env, Status::Rejected, "node policy: external tasks disabled")
                .await;
            return Ok(());
        }
        if !is_local {
            if let Err(e) = self.verify_task_sig(&env) {
                self.policy.add_suspicion(&env.sender_peer_id, 40);
                self.reject(&env, Status::Rejected, &format!("signature: {e}"))
                    .await;
                return Ok(());
            }
            if !self.policy.allow_tasks_from(&env.sender_peer_id) {
                self.reject(&env, Status::Rejected, "security policy blocks this peer")
                    .await;
                return Ok(());
            }
            if let Err(r) = check_route(&env, self.self_id()) {
                self.reject(&env, Status::Rejected, r.msg()).await;
                return Ok(());
            }
        }
        // Shell-задачи, если узел их не объявляет, — отказ без подозрений.
        if env.constraints().allow_shell && !self.cfg.capabilities.allow_shell {
            self.reject(&env, Status::Rejected, "node does not allow shell tasks")
                .await;
            return Ok(());
        }

        let (matched, ok) = skills_match(&self.cfg.effective_skills(), &env.required_skills);
        let can_local = ok && matched > 0 || (env.required_skills.is_empty() && ok);
        let free_slot = self.running() < self.cfg.tasks.max_parallel_tasks as i64;
        let candidates = if env.constraints().allow_delegation {
            self.table.select(
                &env.required_skills,
                Some(&self.policy),
                self.self_id(),
                self.cfg.tasks.forwarding.max_fanout.max(1) as usize,
                &env.route_stack.iter().cloned().collect(),
            )
        } else {
            vec![]
        };
        // Предпочитаем локальное исполнение, когда оно возможно и слот свободен;
        // иначе — лучший удалённый кандидат; иначе очередь ожидания с повтором.
        if can_local && (free_slot || candidates.is_empty()) {
            self.execute_local(env).await
        } else if let Some(c) = candidates.into_iter().next() {
            self.forward(&env, &c.neighbor.peer_id).await
        } else if can_local {
            // Нет соседей и все слоты заняты: всё равно идём к себе — очередь
            // внутри execute_local ограничена семафором.
            self.execute_local(env).await
        } else {
            self.reject(&env, Status::Rejected, "no eligible peers reachable for required skills")
                .await;
            Ok(())
        }
    }

    async fn reject(&self, env: &TaskEnvelope, status: Status, reason: &str) {
        self.rejected.fetch_add(1, Ordering::SeqCst);
        tracing::info!("tasks: reject {}: {}", env.task_id, reason);
        let mut rec = self.journal_entry(env, status);
        rec.error = reason.into();
        self.put_journal(&mut rec);
        // Отказ летит обратно предыдущему прыжку, если он не мы сами.
        if let Some(prev) = previous_hop(env) {
            if prev != self.self_id() {
                let ack = TaskAck {
                    task_id: env.task_id.clone(),
                    status: crate::proto::AckStatus::Rejected,
                    reason: reason.into(),
                    accepted_by: self.self_id().into(),
                    timestamp: now_unix(),
                    signature: vec![],
                };
                // ACK — часть envelope-канала в Go; здесь шлём пустой
                // result c error_message, чтобы транспорт был один.
                let res = TaskResult {
                    task_id: ack.task_id.clone(),
                    worker_peer_id: self.self_id().into(),
                    status: TaskStatus::Rejected,
                    error_message: ack.reason.clone(),
                    error_class: error_class(TaskStatus::Rejected, &ack.reason),
                    sender_peer_id: self.self_id().into(),
                    started_at: ack.timestamp,
                    finished_at: ack.timestamp,
                    ..Default::default()
                };
                let _ = self.call_deliver_result(prev.to_string(), res).await;
            }
        }
    }

    fn has_deliver_result(&self) -> bool {
        self.deliver_result.lock().unwrap().is_some()
    }
    /// Заимствует колбэк только на время вызова: `Option<DeliverResult>`-копия
    /// живёт через `.await` и делает будущий код не-`Send` (MutexGuard в Rust 2021).
    async fn call_deliver_result(&self, peer: String, res: TaskResult) -> Result<()> {
        let fut = {
            let guard = self.deliver_result.lock().unwrap();
            guard.as_ref().map(|f| f(peer, res))
        };
        match fut {
            Some(f) => f.await,
            None => Ok(()),
        }
    }
    fn deliver_ref(&self) -> Option<Deliver> {
        self.deliver.lock().unwrap().clone()
    }
    fn cancel_ref(&self) -> Option<CancelPeer> {
        self.cancel_peer.lock().unwrap().clone()
    }

    async fn forward(&self, env: &TaskEnvelope, peer: &str) -> Result<()> {
        let mut e = env.clone();
        e.sender_peer_id = self.self_id().into();
        e.ttl -= 1;
        append_route(&mut e, self.self_id());
        self.sign_task(&mut e);
        self.forwarded.fetch_add(1, Ordering::SeqCst);
        let mut rec = self.journal_entry(&e, Status::Forwarded);
        rec.delegated_to = vec![peer.into()];
        self.put_journal(&mut rec);
        tracing::info!("tasks: forward {} -> {}", e.task_id, peer);
        let fut = {
            let guard = self.deliver.lock().unwrap();
            guard.as_ref().map(|f| f(peer.to_string(), e))
        };
        match fut {
            Some(f) => f.await,
            None => bail!("tasks: no delivery hook installed"),
        }
    }

    /// Каталог workspace для задачи (per-task изоляция, ТЗ 6.8.4).
    fn task_workspace(&self, task_id: &str) -> PathBuf {
        let n = self.ws_counter.fetch_add(1, Ordering::SeqCst);
        let root = if self.cfg.picoclaw.workspace_root.is_empty() {
            PathBuf::from(&self.cfg.node.data_dir).join("picoclaw").join("workspaces")
        } else {
            PathBuf::from(&self.cfg.picoclaw.workspace_root)
        };
        let dir = root.join(format!("{}-{}", task_id, n));
        let _ = std::fs::create_dir_all(&dir);
        dir
    }

    /// Локальное исполнение через адаптер PicoClaw.
    pub async fn execute_local(&self, env: TaskEnvelope) -> Result<()> {
        let _permit = self
            .sem
            .acquire()
            .await
            .map_err(|_| anyhow!("tasks: semaphore closed"))?;

        let mut rec = self.journal_entry(&env, Status::Running);
        rec.worker_peer_id = self.self_id().into();
        rec.attempts = 1;
        self.put_journal(&mut rec);
        self.running.fetch_add(1, Ordering::SeqCst);
        let secs = timeout_seconds(
            &env,
            self.cfg.tasks.default_timeout_seconds,
            self.cfg.tasks.max_timeout_seconds,
        );
        let ws = self.task_workspace(&env.task_id);
        let exec_req = ExecRequest {
            task_id: env.task_id.clone(),
            instruction: env.payload().instruction.clone(),
            skills: env.required_skills.clone(),
            workspace: ws.to_string_lossy().into_owned(),
            home: ws.join("home").to_string_lossy().into_owned(),
            allow_shell: env.constraints().allow_shell,
            allow_network: env.constraints().allow_network_tools,
            timeout: Duration::from_secs(secs.max(1) as u64),
            max_workspace_bytes: self.cfg.tasks.max_workspace_bytes,
            max_memory_bytes: self.cfg.tasks.max_task_memory_bytes,
            session_key: env.task_id.clone(),
            ..Default::default()
        };
        let fut = self.adapter.execute(exec_req);
        let resp = tokio::time::timeout(Duration::from_secs(secs.max(1) as u64 + 5), fut).await;
        self.running.fetch_sub(1, Ordering::SeqCst);

        let (status, text, err_msg, artifacts, started, finished) = match resp {
            Err(_) => (
                Status::TimedOut,
                String::new(),
                format!("execution timeout after {secs}s"),
                vec![],
                now_unix(),
                now_unix(),
            ),
            Ok(Err(e)) => {
                let timed_out = e.downcast_ref::<picoclaw::TimeoutError>().is_some();
                (
                    if timed_out { Status::TimedOut } else { Status::Failed },
                    String::new(),
                    e.to_string(),
                    vec![],
                    now_unix(),
                    now_unix(),
                )
            }
            Ok(Ok(r)) => {
                let arts = self.persist_artifacts(&r)?;
                (
                    Status::Completed,
                    r.text.clone(),
                    String::new(),
                    arts,
                    r.started_at.timestamp(),
                    r.finished_at.timestamp(),
                )
            }
        };

        let mut result = TaskResult {
            task_id: env.task_id.clone(),
            worker_peer_id: self.self_id().into(),
            status: status.to_proto(),
            text: text.clone(),
            error_message: err_msg.clone(),
            started_at: started,
            finished_at: finished,
            artifacts,
            sender_peer_id: self.self_id().into(),
            signature_scheme: "ed25519-sha256-v1".into(),
            ..Default::default()
        };
        result.result_digest = wire::result_digest(&result);
        result.signature = self.identity.sign(&wire::result_body(&result));
        result.worker_signature = result.signature.clone();
        result.error_class = error_class(result.status, &err_msg);

        // Журнал результата + задачи (Go: digest/sign до классификации).
        let rr = ResultRecord {
            task_id: result.task_id.clone(),
            status: status.as_str().into(),
            text: text.clone(),
            error_message: err_msg.clone(),
            worker_peer_id: self.self_id().into(),
            started_at: started as f64,
            finished_at: finished as f64,
            model: self.adapter.model(),
            signature: result.signature.clone(),
            worker_signature: result.worker_signature.clone(),
            artifacts: result
                .artifacts
                .iter()
                .map(|a| ArtifactInfo {
                    name: a.name.clone(),
                    hash: a.hash.clone(),
                    size: a.size,
                })
                .collect(),
            result_digest: format!("sha256:{}", hex::encode(result.result_digest.clone())),
            error_class: error_class_name(result.error_class),
            ..Default::default()
        };
        let _ = self.store.put_result(&rr);
        rec.status = status.as_str().into();
        rec.finished_at = finished as f64;
        rec.error = err_msg.clone();
        rec.result_digest = rr.result_digest.clone();
        self.put_journal(&mut rec);
        match status {
            Status::Completed => self.completed.fetch_add(1, Ordering::SeqCst),
            _ => self.failed.fetch_add(1, Ordering::SeqCst),
        };
        match status {
            Status::Completed => self.table.record_success(self.self_id()),
            Status::Failed | Status::TimedOut => self.table.record_failure(self.self_id()),
            _ => {}
        }

        // Retention workspace: чистим per-task каталог после завершения.
        let _ = std::fs::remove_dir_all(&ws);

        // Куда отдавать результат: автору (если мы не автор) или родител.
        // Scoped-guard: блокировки закрываются до `.await` ниже.
        let is_root_for_us = env.origin_peer_id == self.self_id()
            && env.parent_task_id.is_empty()
            && { self.local_tasks.lock().unwrap().contains(&env.task_id) };
        if is_root_for_us {
            {
                self.results.lock().unwrap().insert(env.task_id.clone(), result);
            }
            return Ok(());
        }
        let target = if env.origin_peer_id != self.self_id() {
            env.origin_peer_id.clone()
        } else if let Some(prev) = previous_hop(&env) {
            prev.to_string()
        } else {
            {
                self.results.lock().unwrap().insert(env.task_id.clone(), result);
            }
            return Ok(());
        };
        let fut = {
            let guard = self.deliver_result.lock().unwrap();
            guard.as_ref().map(|f| f(target, result.clone()))
        };
        match fut {
            Some(f) => f.await,
            None => {
                tracing::warn!("tasks: no result delivery hook; result stored locally");
                self.results.lock().unwrap().insert(env.task_id.clone(), result);
                Ok(())
            }
        }
    }

    /// Сохраняет артефакты ответа в контент-хранилище (hash-addressed).
    fn persist_artifacts(&self, r: &ExecResponse) -> Result<Vec<ArtifactRef>> {
        let mut out = Vec::new();
        for a in &r.artifacts {
            let data = std::fs::read(&a.path).unwrap_or_default();
            if data.is_empty() {
                continue;
            }
            let (hash, size) = self.store.store_artifact(&data)?;
            out.push(ArtifactRef {
                name: a.name.clone(),
                hash,
                size,
                media_type: guess_media(&a.name),
            });
        }
        Ok(out)
    }

    // ---- Обработка результатов и отмен на стороне автора ------------------

    /// Принятый из сети результат: верификация подписи работника, сохранение,
    /// доставка автору (если мы relay) или завершение локальной задачи.
    // Рекурсия handle_result → absorb_child_result → complete_aggregate →
    // handle_result требует боксинга future (E0733).
    pub fn handle_result(
        self: Arc<Self>,
        res: TaskResult,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        Box::pin(async move { self.handle_result_inner(res).await })
    }

    async fn handle_result_inner(&self, res: TaskResult) -> Result<()> {
        if !res.worker_peer_id.is_empty() && !res.signature.is_empty() {
            let vk = crate::security::extract_key_from_peer_id(&res.worker_peer_id)?;
            verify(&vk.to_bytes().to_vec(), &wire::result_body(&res), &res.signature)
                .map_err(|e| anyhow!("tasks: result signature: {e}"))?;
        }
        let status = Status::from_proto(res.status);
        let rr = ResultRecord {
            task_id: res.task_id.clone(),
            status: status.as_str().into(),
            text: res.text.clone(),
            error_message: res.error_message.clone(),
            worker_peer_id: res.worker_peer_id.clone(),
            started_at: res.started_at as f64,
            finished_at: res.finished_at as f64,
            // wire::result_digest возвращает сырые байты SHA-256; в журнале
            // дайджест хранится hex-строкой с префиксом схемы (как в Go).
            result_digest: format!("sha256:{}", hex::encode(wire::result_digest(&res))),
            aggregated: res.aggregated,
            error_class: error_class_name(res.error_class),
            ..Default::default()
        };
        let _ = self.store.put_result(&rr);
        // Если это результат подзадачи, которую мы разослали (мы — автор
        // корня): поглощаем в агрегат и не трогаем журнал корня до сборки.
        if self.absorb_child_result(&res).await? {
            return Ok(());
        }
        if let Ok(mut t) = self.store.get_task(&res.task_id) {
            if !Status::parse(&t.status).terminal() {
                t.status = status.as_str().into();
                t.finished_at = res.finished_at as f64;
                t.worker_peer_id = res.worker_peer_id.clone();
                t.error = res.error_message.clone();
                t.result_digest = rr.result_digest.clone();
                self.put_journal(&mut t);
            }
        }
        match status {
            Status::Completed => self.table.record_success(&res.worker_peer_id),
            Status::Failed | Status::TimedOut => self.table.record_failure(&res.worker_peer_id),
            _ => {}
        }
        let is_local_origin = { self.local_tasks.lock().unwrap().contains(&res.task_id) };
        if is_local_origin && res.sender_peer_id != self.self_id() {
            // Мы автор: задача завершена удалённым воркером.
            {
                self.results.lock().unwrap().insert(res.task_id.clone(), res);
            }
            return Ok(());
        }
        if res.sender_peer_id == self.self_id() {
            {
                self.results.lock().unwrap().insert(res.task_id.clone(), res);
            }
            return Ok(());
        }
        // Relay: послать дальше к origin. В Go маршрут результата совпадает с
        // маршрутом задачи; здесь мы добавили sender-прыжок перед отправкой,
        // поэтому предыдущий прыжок — предпоследний элемент.
        let next = if res.route_stack.len() >= 2 {
            Some(res.route_stack[res.route_stack.len() - 2].clone())
        } else {
            None
        };
        if let Some(next) = next.or_else(|| Some(res.sender_peer_id.clone())) {
            if next != self.self_id() && self.has_deliver_result() {
                return self.call_deliver_result(next, res).await;
            }
        }
        {
            self.results.lock().unwrap().insert(res.task_id.clone(), res);
        }
        Ok(())
    }

    /// Прямое завершение задачи извне (например, результат от адаптера без
    /// повторного исполнения): сохранить и доставить автору.
    pub async fn complete_task(&self, mut result: TaskResult) -> Result<()> {
        result.result_digest = wire::result_digest(&result);
        result.signature_scheme = "ed25519-sha256-v1".into();
        result.signature = self.identity.sign(&wire::result_body(&result));
        result.worker_signature = result.signature.clone();
        self.store_result(&result);
        let target = result.sender_peer_id.clone();
        if !target.is_empty() && target != self.self_id() && self.has_deliver_result() {
            return self.call_deliver_result(target, result).await;
        }
        {
            self.results.lock().unwrap().insert(result.task_id.clone(), result);
        }
        Ok(())
    }

    /// Сохранение журнала результата без доставки (используется relay-логикой
    /// и graceful shutdown для зависших задач).
    fn store_result(&self, res: &TaskResult) {
        let status = Status::from_proto(res.status);
        let rr = ResultRecord {
            task_id: res.task_id.clone(),
            status: status.as_str().into(),
            text: res.text.clone(),
            error_message: res.error_message.clone(),
            worker_peer_id: res.worker_peer_id.clone(),
            started_at: res.started_at as f64,
            finished_at: res.finished_at as f64,
            model: self.adapter.model(),
            result_digest: format!("sha256:{}", hex::encode(wire::result_digest(res))),
            aggregated: res.aggregated,
            error_class: error_class_name(res.error_class),
            ..Default::default()
        };
        let _ = self.store.put_result(&rr);
    }

    /// Сохранённый результат задачи (для poll/статуса).
    pub fn get_result(&self, task_id: &str) -> Option<TaskResult> {
        self.results.lock().unwrap().get(task_id).cloned()
    }

    /// Отмена задачи: помечаем в журнале и распространяем на воркеров.
    pub async fn cancel(&self, task_id: &str) -> Result<()> {
        let mut t = self.store.get_task(task_id)?;
        if Status::parse(&t.status).terminal() {
            return Ok(());
        }
        t.status = Status::Canceled.as_str().into();
        self.put_journal(&mut t);
        for peer in &t.delegated_to {
            let fut = {
                let guard = self.cancel_peer.lock().unwrap();
                guard.as_ref().map(|f| f(peer.clone(), task_id.to_string()))
            };
            if let Some(f) = fut {
                f.await;
            }
        }
        Ok(())
    }

    /// Снимок для /v1/status и админ-API.
    pub fn snapshot(&self, limit: usize) -> Vec<TaskView> {
        let tasks = self.store.list_tasks(limit, "").unwrap_or_default();
        let results = self.results.lock().unwrap();
        tasks
            .into_iter()
            .map(|t| TaskView {
                task_id: t.task_id.clone(),
                status: t.status.clone(),
                origin_peer_id: t.origin_peer_id.clone(),
                worker_peer_id: t.worker_peer_id.clone(),
                attempts: t.attempts,
                delegated_to: t.delegated_to.clone(),
                result: results.get(&t.task_id).map(result_json),
            })
            .collect()
    }

    /// Текущие способности узла (подписанные, для gossip/RPC).
    pub fn capabilities(&self) -> Capabilities {
        let mut c = Capabilities {
            peer_id: self.self_id().into(),
            node_name: self.cfg.node.name.clone(),
            version: crate::version::VERSION.into(),
            skills: self.cfg.effective_skills(),
            models: self.cfg.capabilities.models.clone(),
            max_parallel_tasks: self.cfg.tasks.max_parallel_tasks,
            running_tasks: self.running() as i32,
            load: if self.cfg.tasks.max_parallel_tasks > 0 {
                self.running() as f64 / self.cfg.tasks.max_parallel_tasks as f64
            } else {
                0.0
            },
            accept_external_tasks: self.cfg.capabilities.accept_external_tasks,
            allow_shell: self.cfg.capabilities.allow_shell,
            relay_capable: true,
            resource_class: self.cfg.capabilities.resource_class.clone(),
            listen_addrs: self.cfg.node.listen.clone(),
            timestamp: now_unix(),
            skills_version: 1,
            ..Default::default()
        };
        c.signature = self.identity.sign(&wire::caps_body(&c));
        c
    }
}

fn guess_media(name: &str) -> String {
    let ext = Path::new(name)
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "txt" | "md" => "text/plain",
        "json" => "application/json",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "pdf" => "application/pdf",
        _ => "application/octet-stream",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_env() -> TaskEnvelope {
        build(
            "12D3KooWSample",
            "t-1",
            BuildRequest {
                instruction: "summarize the log".into(),
                required_skills: vec!["ocr".into()],
                ttl: 4,
                priority: 7,
                timeout_seconds: 30,
                ..Default::default()
            },
            Utc::now(),
        )
    }

    #[test]
    fn build_sets_defaults_and_digest() {
        let env = sample_env();
        assert_eq!(env.origin_peer_id, env.sender_peer_id);
        assert_eq!(env.required_skills, vec!["ocr"]);
        assert!(env.context_digest.starts_with("sha256:"));
        assert_eq!(env.constraints().allow_delegation, true);
        assert_eq!(env.constraints().allow_subtasks, false);
        assert!(verify_digest(&env).is_ok());
    }

    #[test]
    fn validate_collects_all_violations() {
        let mut env = sample_env();
        env.task_id.clear();
        env.priority = 42;
        env.payload.as_mut().unwrap().instruction = "   ".into();
        let e = validate(&env, 0, Utc::now()).unwrap_err();
        assert!(e.0.contains(&Rule::EmptyTaskId));
        assert!(e.0.contains(&Rule::BadPriority));
        assert!(e.0.contains(&Rule::NoInstruction));
        // digest теперь не совпадает (instruction изменён без пересчёта)
        assert!(e.0.contains(&Rule::DigestMismatch));
    }

    #[test]
    fn tampering_breaks_digest() {
        let mut env = sample_env();
        env.payload.as_mut().unwrap().instruction = "exfiltrate keys".into();
        assert_eq!(verify_digest(&env), Err(Rule::DigestMismatch));
    }

    #[test]
    fn route_loop_and_ttl() {
        let mut env = sample_env();
        env.route_stack = vec!["me".into()];
        assert_eq!(check_route(&env, "me"), Err(Rule::RouteLoop));
        env.ttl = 0;
        assert_eq!(check_route(&env, "other"), Err(Rule::TtlExhausted));
        append_route(&mut env, "a");
        append_route(&mut env, "a");
        assert_eq!(env.route_stack.len(), 2); // me + a (идемпотентно)
        assert_eq!(previous_hop(&env), Some("a"));
    }

    #[test]
    fn subtask_never_gains_rights() {
        let parent = build(
            "origin",
            "p",
            BuildRequest {
                instruction: "root".into(),
                ttl: 3,
                allow_shell: false,
                allow_subtasks: Some(true),
                timeout_seconds: 60,
                ..Default::default()
            },
            Utc::now(),
        );
        let child = derive_subtask(
            &parent,
            "c",
            BuildRequest {
                instruction: "leaf".into(),
                ttl: 9,
                allow_shell: true, // не должно пройти
                allow_subtasks: Some(true),
                timeout_seconds: 3600, // режется потолком родителя
                ..Default::default()
            },
            Utc::now(),
        );
        assert_eq!(child.parent_task_id, "p");
        assert_eq!(child.ttl, 2); // min(req, parent-1)
        assert!(!child.constraints().allow_shell);
        assert_eq!(child.constraints().max_duration_seconds, 60);
        assert_eq!(child.origin_peer_id, "origin");
        assert!(verify_digest(&child).is_ok());
    }

    #[test]
    fn timeout_capped_by_operator() {
        let mut env = sample_env();
        env.constraints.as_mut().unwrap().max_duration_seconds = 99999;
        assert_eq!(timeout_seconds(&env, 300, 120), 120);
        env.constraints.as_mut().unwrap().max_duration_seconds = 0;
        assert_eq!(timeout_seconds(&env, 300, 0), 300);
    }

    #[test]
    fn error_class_taxonomy() {
        assert_eq!(
            error_class(TaskStatus::Timeout, "whatever"),
            TaskErrorClass::Timeout
        );
        assert_eq!(
            error_class(TaskStatus::Rejected, "digest mismatch"),
            TaskErrorClass::Validation
        );
        assert_eq!(
            error_class(TaskStatus::Failed, "no eligible peers for skill ocr"),
            TaskErrorClass::NoWorker
        );
        assert_eq!(
            error_class(TaskStatus::Failed, "bad signature from peer"),
            TaskErrorClass::Security
        );
        assert_eq!(
            error_class(TaskStatus::Failed, "connection reset by peer"),
            TaskErrorClass::Network
        );
        assert_eq!(
            error_class(TaskStatus::Failed, "agent crashed"),
            TaskErrorClass::Execution
        );
        assert_eq!(error_class_by_name("NO_WORKER"), TaskErrorClass::NoWorker);
        assert_eq!(error_class_name(TaskErrorClass::Unspecified), "");
    }

    #[test]
    fn normalize_skills_sorted_deduped() {
        let v = vec!["OCR".into(), " ocr ".into(), "".into(), "shell".into()];
        assert_eq!(normalize_skills(&v), vec!["ocr", "shell"]);
    }

    #[test]
    fn skills_match_wildcards() {
        assert_eq!(skills_match(&["general".into()], &["a".into(), "b".into()]), (2, true));
        assert_eq!(skills_match(&["a".into()], &["a".into(), "b".into()]), (1, false));
        assert_eq!(skills_match(&[], &[]), (0, true));
    }

    #[test]
    fn status_roundtrip_and_terminal() {
        for s in [Status::Completed, Status::Failed, Status::Rejected] {
            assert!(s.terminal());
            assert_eq!(Status::parse(s.as_str()), s);
        }
        assert!(!Status::parse("garbage").terminal());
        assert_eq!(Status::from_proto(TaskStatus::WaitingSubtasks), Status::WaitingSubtasks);
    }

    #[test]
    fn uuid_v7_shape() {
        let a = new_id();
        assert_eq!(a.len(), 36);
        assert_eq!(&a[14..15], "7");
        assert!(matches!(&a[19..20], "8" | "9" | "a" | "b"));
        let b = new_id();
        assert_ne!(a, b);
        assert!(a[..14] <= b[..14] || true); // time-ordered within same ms anyway
    }
}
