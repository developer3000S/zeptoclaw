//! Порт пакета `internal/triggers`: планировщик задач по cron-расписанию.
//!
//! Совместимо с Go-версией: формат записи (`id`, `name`, `schedule`, `job`),
//! семантика `max_runs`, метки `trigger`/`source` у порождённых задач,
//! ручек API соответствует docs/API.md. Cron-парсер поддерживает стандартные
//! 5-полевые выражения: `*`, числа, списки `a,b`, диапазоны `a-b`, шаг `*/n`,
//! а также алиасы `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`, `@reboot`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Result};
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};

use crate::config::{Config, JobConfig, TriggerConfig};
use crate::tasks::{Manager, SubmitRequest};

// ---------------------------------------------------------------------------
// Cron-парсер (Go: github.com/robfig/cron → собственный совместимый парсер)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Schedule {
    /// Битовы маски допустимых значений (минуты/часы — 60/24 бита).
    minute: u64,
    hour: u64,
    dom: u32,   // 1..=31
    month: u16, // 1..=12
    dow: u8,    // 0..=6 (вс=0)
}

fn parse_field(s: &str, lo: u32, hi: u32) -> Result<u64> {
    let mut mask = 0u64;
    for part in s.split(',') {
        let (range_s, step) = match part.split_once('/') {
            Some((r, st)) => (r, st.parse::<u64>().map_err(|_| anyhow::anyhow!("bad cron step {part:?}"))?),
            None => (part, 1),
        };
        if step == 0 {
            bail!("cron: zero step in {part:?}");
        }
        let (a, b) = if range_s == "*" {
            (lo as u64, hi as u64)
        } else if let Some((x, y)) = range_s.split_once('-') {
            (
                x.parse::<u64>().map_err(|_| anyhow::anyhow!("bad cron {part:?}"))?,
                y.parse::<u64>().map_err(|_| anyhow::anyhow!("bad cron {part:?}"))?,
            )
        } else {
            let v = range_s
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("bad cron {part:?}"))?;
            (v, if step > 1 { hi as u64 } else { v })
        };
        if a < lo as u64 || b > hi as u64 || a > b {
            bail!("cron: field {part:?} out of range [{lo},{hi}]");
        }
        let mut v = a;
        while v <= b {
            mask |= 1 << v;
            v += step;
        }
    }
    Ok(mask)
}

impl Schedule {
    pub fn parse(expr: &str) -> Result<Schedule> {
        let expr = expr.trim();
        let normalized = match expr {
            "@hourly" => "0 * * * *",
            "@daily" | "@midnight" => "0 0 * * *",
            "@weekly" => "0 0 * * 0",
            "@monthly" => "0 0 1 * *",
            "@yearly" | "@annually" => "0 0 1 1 *",
            "@reboot" => "* * * * *", // fires immediately once at start (Go: on boot)
            other => other,
        };
        let fields: Vec<&str> = normalized.split_whitespace().collect();
        if fields.len() != 5 {
            bail!("cron: expected 5 fields, got {:?}", expr);
        }
        let minute = parse_field(fields[0], 0, 59)?;
        let hour = parse_field(fields[1], 0, 23)?;
        let dom = parse_field(fields[2], 1, 31)? as u32;
        let month = parse_field(fields[3], 1, 12)? as u16;
        // dow: 7 == 0 (Sunday)
        let dow_mask = parse_field(fields[4], 0, 7)?;
        // Нормализация: значение 7 (воскресенье) → бит 0.
        let mut dow = ((dow_mask >> 1) & 0x3F) as u8;
        if dow_mask & (1 | 128) != 0 {
            dow |= 1 << 0;
        }
        Ok(Schedule { minute, hour, dom, month, dow })
    }

    /// Следующее срабатывание строго после `from` (UTC). Поминутный перебор
    /// с пропуском невалидных часов/дней; ограничение — ~2 года.
    pub fn next_after(&self, from: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut t = from
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
            + chrono::Duration::minutes(1);
        let deadline = t + chrono::Duration::days(366 * 2 + 1);
        while t < deadline {
            let mon = t.month() as u16;
            if self.month & (1 << mon) == 0 {
                // прыжок на начало следующего месяца
                let (y, m) = if mon == 12 {
                    (t.year() + 1, 1u32)
                } else {
                    (t.year(), mon as u32 + 1)
                };
                t = Utc.with_ymd_and_hms(y, m, 1, 0, 0, 0).single()?;
                continue;
            }
            let day = t.day();
            let wd = t.weekday().num_days_from_sunday() as u8;
            let dom_ok = day <= 31 && (self.dom >> day) & 1 != 0;
            let dow_ok = (self.dow >> wd) & 1 != 0;
            // DOM и DOW оба ограничены → OR (семантика cron); один из них * → AND.
            let dom_star = (self.dom & 0xFFFF_FFFE) == 0xFFFF_FFFE; // биты 1..=31
            let dow_star = self.dow == 0b111_1111;
            let day_ok = match (dom_star, dow_star) {
                (true, true) => true,
                (true, false) => dow_ok,
                (false, true) => dom_ok,
                (false, false) => dom_ok || dow_ok,
            };
            if !day_ok {
                t = (t + chrono::Duration::days(1)).with_hour(0).unwrap().with_minute(0).unwrap();
                continue;
            }
            let h = t.hour();
            if (self.hour >> h) & 1 == 0 {
                t = (t + chrono::Duration::hours(1)).with_minute(0).unwrap();
                continue;
            }
            let m = t.minute() as u64;
            if self.minute & (1 << m) == 0 {
                t = t + chrono::Duration::minutes(1);
                continue;
            }
            return Some(t);
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Планировщик
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriggerView {
    pub id: String,
    pub name: String,
    pub schedule: String,
    pub enabled: bool,
    pub max_runs: i32,
    pub runs: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_run: Option<DateTime<Utc>>,
    pub instruction: String,
    pub required_skills: Vec<String>,
}

struct Runtime {
    cfg: TriggerConfig,
    schedule: Schedule,
    runs: i32,
    last_run: Option<DateTime<Utc>>,
    next: DateTime<Utc>,
    enabled: bool,
}

pub struct Scheduler {
    manager: Arc<Manager>,
    inner: Mutex<Vec<Runtime>>,
    shutdown: Mutex<Option<tokio::sync::watch::Receiver<bool>>>,
}

impl Scheduler {
    pub fn new(cfg: &Config, manager: Arc<Manager>) -> Result<Arc<Self>> {
        let s = Arc::new(Self {
            manager,
            inner: Mutex::new(Vec::new()),
            shutdown: Mutex::new(None),
        });
        for t in &cfg.triggers {
            s.add(t.clone())?;
        }
        Ok(s)
    }

    pub fn set_shutdown(&self, rx: tokio::sync::watch::Receiver<bool>) {
        *self.shutdown.lock().unwrap() = Some(rx);
    }

    pub fn add(&self, tc: TriggerConfig) -> Result<String> {
        if tc.id.is_empty() {
            bail!("triggers: empty id");
        }
        if tc.schedule.is_empty() {
            bail!("triggers: {:?} has no schedule", tc.id);
        }
        let schedule = Schedule::parse(&tc.schedule)?;
        let now = Utc::now();
        let next = schedule
            .next_after(now)
            .ok_or_else(|| anyhow::anyhow!("triggers: schedule never fires"))?;
        let enabled = tc.enabled.unwrap_or(true);
        let mut g = self.inner.lock().unwrap();
        if g.iter().any(|r| r.cfg.id == tc.id) {
            bail!("triggers: id {:?} already exists", tc.id);
        }
        g.push(Runtime {
            cfg: tc,
            schedule,
            runs: 0,
            last_run: None,
            next,
            enabled,
        });
        Ok(next.to_rfc3339())
    }

    pub fn remove(&self, id: &str) -> Result<()> {
        let mut g = self.inner.lock().unwrap();
        let before = g.len();
        g.retain(|r| r.cfg.id != id);
        if g.len() == before {
            bail!("triggers: unknown id {id:?}");
        }
        Ok(())
    }

    pub fn list(&self) -> Vec<TriggerView> {
        let g = self.inner.lock().unwrap();
        g.iter()
            .map(|r| TriggerView {
                id: r.cfg.id.clone(),
                name: r.cfg.name.clone(),
                schedule: r.cfg.schedule.clone(),
                enabled: r.enabled,
                max_runs: r.cfg.max_runs,
                runs: r.runs,
                last_run: r.last_run,
                next_run: Some(r.next),
                instruction: r.cfg.job.instruction.clone(),
                required_skills: r.cfg.job.required_skills.clone(),
            })
            .collect()
    }

    fn fire_once(&self, id: &str) -> Result<String> {
        let job = {
            let g = self.inner.lock().unwrap();
            let r = g
                .iter()
                .find(|r| r.cfg.id == id)
                .ok_or_else(|| anyhow::anyhow!("triggers: unknown id {id:?}"))?;
            r.cfg.job.clone()
        };
        self.submit_job(&job, id)
    }

    fn submit_job(&self, job: &JobConfig, trigger_id: &str) -> Result<String> {
        let mut labels: BTreeMap<String, String> = job.labels.clone();
        labels.insert("trigger".into(), trigger_id.into());
        labels.insert("source".into(), "scheduler".into());
        let req = SubmitRequest {
            instruction: job.instruction.clone(),
            required_skills: job.required_skills.clone(),
            ttl: job.ttl,
            priority: job.priority,
            timeout_seconds: job.timeout_seconds,
            allow_shell: job.allow_shell,
            allow_network: job.allow_network_tools,
            allow_deleg: None,
            allow_subtasks: None,
            labels,
            subtasks: vec![],
        };
        // Спавним исполнение на runtime tokio (submit — async).
        let mgr = self.manager.clone();
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            match rt {
                Ok(rt) => rt.block_on(async move { mgr.submit(req).await }),
                Err(e) => Err(anyhow::anyhow!(e)),
            }
        });
        let id = handle.join().map_err(|_| anyhow::anyhow!("triggers: submit panicked"))??;
        Ok(id)
    }

    /// Фоновый цикл: тик раз в секунду, запуск просроченных триггеров.
    pub async fn run(self: Arc<Self>) {
        let mut rx = match self.shutdown.lock().unwrap().clone() {
            Some(rx) => rx,
            None => {
                let (tx, rx) = tokio::sync::watch::channel(false);
                std::mem::forget(tx); // «вечный» сигнал, если shutdown не задан
                rx
            }
        };
        loop {
            let due: Vec<String> = {
                let now = Utc::now();
                let mut g = self.inner.lock().unwrap();
                let mut due = Vec::new();
                for r in g.iter_mut() {
                    if !r.enabled {
                        continue;
                    }
                    if r.cfg.max_runs > 0 && r.runs >= r.cfg.max_runs {
                        r.enabled = false;
                        continue;
                    }
                    if r.next <= now {
                        due.push(r.cfg.id.clone());
                        r.runs += 1;
                        r.last_run = Some(now);
                        r.next = r.schedule.next_after(now).unwrap_or(now);
                        if r.cfg.max_runs > 0 && r.runs >= r.cfg.max_runs {
                            r.enabled = false;
                        }
                    }
                }
                due
            };
            for id in due {
                match self.fire_once(&id) {
                    Ok(task_id) => tracing::info!(trigger = %id, task = %task_id, "trigger_fired"),
                    Err(e) => tracing::warn!(trigger = %id, error = %e, "trigger_fire_failed"),
                }
            }
            let sleep = tokio::time::sleep(std::time::Duration::from_secs(1));
            tokio::pin!(sleep);
            tokio::select! {
                c = rx.changed() => {
                    if c.is_err() || *rx.borrow_and_update() {
                        break;
                    }
                }
                _ = &mut sleep => {}
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Тесты
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cron_parse_fields() {
        let s = Schedule::parse("*/15 9-17 * * 1-5").unwrap();
        assert_eq!(s.minute, (1u64 << 0) | (1 << 15) | (1 << 30) | (1 << 45));
        for h in [9u32, 12, 17] {
            assert_ne!(s.hour & (1u64 << h), 0);
        }
        assert_eq!(s.hour & (1u64 << 8), 0);
        assert_eq!(s.hour & (1u64 << 18), 0);
        // dow 1..=5
        for d in 1..=5u8 {
            assert_ne!(s.dow & (1 << d), 0);
        }
        assert_eq!(s.dow & 1, 0); // вс выключен
    }

    #[test]
    fn sunday_aliases() {
        let s = Schedule::parse("0 0 * * 0").unwrap();
        assert_ne!(s.dow & 1, 0);
        let s7 = Schedule::parse("0 0 * * 7").unwrap();
        assert_eq!(s7.dow, s.dow);
    }

    #[test]
    fn next_after_hourly() {
        let s = Schedule::parse("@hourly").unwrap();
        let base = Utc.with_ymd_and_hms(2026, 10, 16, 12, 34, 5).unwrap();
        let n = s.next_after(base).unwrap();
        assert_eq!(n, Utc.with_ymd_and_hms(2026, 10, 16, 13, 0, 0).unwrap());
    }

    #[test]
    fn next_after_daily_dow_dom_union() {
        // "0 12 * * 1" — только по понедельникам.
        let s = Schedule::parse("0 12 * * 1").unwrap();
        // Пятница 16 октября 2026 → следующий понедельник 19 октября.
        let base = Utc.with_ymd_and_hms(2026, 10, 16, 13, 0, 0).unwrap();
        let n = s.next_after(base).unwrap();
        assert_eq!(n, Utc.with_ymd_and_hms(2026, 10, 19, 12, 0, 0).unwrap());
        assert_eq!(n.weekday().to_string(), "Mon");
    }

    #[test]
    fn rejects_bad_cron() {
        assert!(Schedule::parse("61 * * * *").is_err());
        assert!(Schedule::parse("* * * *").is_err());
        assert!(Schedule::parse("@nope").is_err());
    }
}
