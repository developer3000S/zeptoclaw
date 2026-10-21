//! Обнаружение узлов mesh-сети по TCP/HTTP и фильтрация по IP-адресу.
//!
//! Два независимых механизма:
//!
//! * [`IpFilter`] — разбор и проверка IP-фильтра: одиночный адрес, CIDR
//!   (`10.0.0.0/24`) или диапазон (`10.0.0.5-10.0.0.40`). Используется поиском
//!   нод в дашборде.
//! * [`sweep`] — параллельный проход по списку `(ip, port)` с HTTP-пробой
//!   `/healthz`. Ловит узлы, у которых admin-API не пробросился в список
//!   управляемых (поднят вручную, в другом compose-проекте и т. п.).
//!
//! Диапазон IP разворачивается в список адресов заранее, поэтому обход
//! ограничен явными границами: «вся /16» без указания границ не сканируется.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

/// Максимум адресов в одном обходе (защита от слишком широких диапазонов).
pub const MAX_SWEEP_TARGETS: usize = 4096;

/// Фильтр по IP-адресу: точное совпадение, CIDR или диапазон.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IpFilter {
    /// Ровно один адрес.
    Exact(IpAddr),
    /// Сеть в нотации CIDR.
    Cidr { base: IpAddr, prefix: u8 },
    /// Инключительный диапазон `from..=to`.
    Range { from: IpAddr, to: IpAddr },
}

impl IpFilter {
    /// Разобрать пользовательский ввод. `None` — строка не является IP-фильтром
    /// (например, имя ноды: тогда поиск идёт по подстроке).
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        if let Some((net, prefix)) = s.split_once('/') {
            let base: IpAddr = net.trim().parse().ok()?;
            let prefix: u8 = prefix.trim().parse().ok()?;
            if prefix > 32 && base.is_ipv4() {
                return None;
            }
            if prefix > 128 && base.is_ipv6() {
                return None;
            }
            return Some(IpFilter::Cidr { base, prefix });
        }
        if let Some((from, to)) = s.split_once('-') {
            let from: IpAddr = from.trim().parse().ok()?;
            let to: IpAddr = to.trim().parse().ok()?;
            if from.is_ipv4() != to.is_ipv4() {
                return None;
            }
            let (lo, hi) = if from <= to { (from, to) } else { (to, from) };
            return Some(IpFilter::Range { from: lo, to: hi });
        }
        let ip: IpAddr = s.parse().ok()?;
        Some(IpFilter::Exact(ip))
    }

    /// Проверить, попадает ли адрес под фильтр.
    pub fn matches(&self, ip: IpAddr) -> bool {
        match self {
            IpFilter::Exact(want) => *want == ip,
            IpFilter::Cidr { base, prefix } => cidr_match(ip, *base, *prefix),
            IpFilter::Range { from, to } => range_match(ip, *from, *to),
        }
    }
}

fn cidr_match(ip: IpAddr, base: IpAddr, prefix: u8) -> bool {
    match (ip, base) {
        (IpAddr::V4(a), IpAddr::V4(b)) => {
            if prefix > 32 {
                return false;
            }
            let mask = if prefix == 0 { 0 } else { u32::MAX << (32 - prefix) };
            (u32::from(a) & mask) == (u32::from(b) & mask)
        }
        (IpAddr::V6(a), IpAddr::V6(b)) => {
            if prefix > 128 {
                return false;
            }
            let mask = if prefix == 0 { 0 } else { u128::MAX << (128 - prefix) };
            (u128::from(a) & mask) == (u128::from(b) & mask)
        }
        _ => false,
    }
}

fn range_match(ip: IpAddr, from: IpAddr, to: IpAddr) -> bool {
    match (ip, from, to) {
        (IpAddr::V4(a), IpAddr::V4(f), IpAddr::V4(t)) => {
            let (a, f, t) = (u32::from(a), u32::from(f), u32::from(t));
            a >= f && a <= t
        }
        (IpAddr::V6(a), IpAddr::V6(f), IpAddr::V6(t)) => {
            let (a, f, t) = (u128::from(a), u128::from(f), u128::from(t));
            a >= f && a <= t
        }
        _ => false,
    }
}

/// Развернуть адреса подсети или диапазона в список.
pub fn expand(range: &IpFilter) -> Vec<IpAddr> {
    match range {
        IpFilter::Exact(ip) => vec![*ip],
        IpFilter::Cidr { base, prefix } => match base {
            IpAddr::V4(v4) => expand_v4(*v4, *prefix),
            IpAddr::V6(v6) => expand_v6(*v6, *prefix),
        },
        IpFilter::Range { from, to } => match (from, to) {
            (IpAddr::V4(f), IpAddr::V4(t)) => {
                let (f, t) = (u32::from(*f), u32::from(*t));
                (f..=t)
                    .map(|v| IpAddr::V4(Ipv4Addr::from(v)))
                    .take(MAX_SWEEP_TARGETS)
                    .collect()
            }
            (IpAddr::V6(f), IpAddr::V6(t)) => {
                let (f, t) = (u128::from(*f), u128::from(*t));
                let mut out = Vec::new();
                let mut cur = f;
                while cur <= t && out.len() < MAX_SWEEP_TARGETS {
                    out.push(IpAddr::V6(Ipv6Addr::from(cur)));
                    if cur == t {
                        break;
                    }
                    cur += 1;
                }
                out
            }
            _ => Vec::new(),
        },
    }
}

fn expand_v4(base: Ipv4Addr, prefix: u8) -> Vec<IpAddr> {
    if prefix > 32 {
        return vec![IpAddr::V4(base)];
    }
    let mask = if prefix == 0 { 0 } else { u32::MAX << (32 - prefix) };
    let network = u32::from(base) & mask;
    let host_bits = 32 - prefix as u32;
    let size = 1u64 << host_bits;
    if size > MAX_SWEEP_TARGETS as u64 {
        // Сеть слишком широкая — отдаём только адрес сети и первые хосты.
        return (0..MAX_SWEEP_TARGETS)
            .map(|i| IpAddr::V4(Ipv4Addr::from((network + i as u32) & u32::MAX)))
            .collect();
    }
    (0..size)
        .map(|i| IpAddr::V4(Ipv4Addr::from((network as u64 + i) as u32)))
        .collect()
}

fn expand_v6(base: Ipv6Addr, prefix: u8) -> Vec<IpAddr> {
    if prefix > 128 {
        return vec![IpAddr::V6(base)];
    }
    let mask = if prefix == 0 { 0 } else { u128::MAX << (128 - prefix) };
    let network = u128::from(base) & mask;
    let host_bits = 128 - prefix as u32;
    let size = 1u64 << host_bits;
    if size > MAX_SWEEP_TARGETS as u64 {
        return (0..MAX_SWEEP_TARGETS)
            .map(|i| IpAddr::V6(Ipv6Addr::from(network + i as u128)))
            .collect();
    }
    (0..size)
        .map(|i| IpAddr::V6(Ipv6Addr::from(network + i as u128)))
        .collect()
}

/// Узел, найденный обходом сети.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FoundNode {
    /// Адрес узла.
    pub ip: String,
    /// Порт admin-API.
    pub port: u16,
    /// Готовый базовый URL.
    pub url: String,
    /// Peer ID узла (из `/healthz`), если узел ответил.
    #[serde(default)]
    pub peer_id: String,
    /// Имя узла (из `/api/v1/status`), если доступно.
    #[serde(default)]
    pub node_name: String,
}

/// Параллельно опросить `targets` и вернуть узлы, ответившие на `/healthz`.
pub async fn sweep(
    client: &reqwest::Client,
    targets: Vec<(IpAddr, u16)>,
    concurrency: usize,
    timeout: Duration,
) -> Vec<FoundNode> {
    if targets.is_empty() {
        return Vec::new();
    }
    let sem = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut futs = Vec::with_capacity(targets.len());
    for (ip, port) in targets {
        let client = client.clone();
        let sem = sem.clone();
        futs.push(async move {
            let _permit = sem.acquire_owned().await.ok()?;
            let url = format!("http://{ip}:{port}");
            let health = client
                .get(format!("{url}/healthz"))
                .timeout(timeout)
                .send()
                .await
                .ok()?;
            if !health.status().is_success() {
                return None;
            }
            let hv: serde_json::Value = health.json().await.ok()?;
            let peer_id = hv
                .get("peer_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let node_name = client
                .get(format!("{url}/api/v1/status"))
                .timeout(timeout)
                .send()
                .await
                .ok()?
                .json::<serde_json::Value>()
                .await
                .ok()
                .and_then(|v| {
                    v.get("node_name").and_then(|n| n.as_str()).map(|s| s.to_string())
                })
                .unwrap_or_default();
            Some(FoundNode {
                ip: ip.to_string(),
                port,
                url,
                peer_id,
                node_name,
            })
        });
    }
    let mut out = Vec::new();
    for f in futures::future::join_all(futs).await {
        if let Some(n) = f {
            out.push(n);
        }
    }
    out.sort_by(|a, b| (a.ip.clone(), a.port).cmp(&(b.ip.clone(), b.port)));
    out
}

use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_exact_cidr_range() {
        assert_eq!(
            IpFilter::parse("10.0.0.5"),
            Some(IpFilter::Exact("10.0.0.5".parse().unwrap()))
        );
        assert_eq!(
            IpFilter::parse("10.0.0.0/24"),
            Some(IpFilter::Cidr {
                base: "10.0.0.0".parse().unwrap(),
                prefix: 24
            })
        );
        assert_eq!(
            IpFilter::parse("10.0.0.5-10.0.0.9"),
            Some(IpFilter::Range {
                from: "10.0.0.5".parse().unwrap(),
                to: "10.0.0.9".parse().unwrap()
            })
        );
        // Перевёрнутые границы нормализуются.
        assert_eq!(
            IpFilter::parse("10.0.0.9-10.0.0.5"),
            Some(IpFilter::Range {
                from: "10.0.0.5".parse().unwrap(),
                to: "10.0.0.9".parse().unwrap()
            })
        );
        // Не IP — поиск пойдёт по подстроке.
        assert!(IpFilter::parse("zepto-0").is_none());
        assert!(IpFilter::parse("10.0.0.0/33").is_none());
    }

    #[test]
    fn matching() {
        let exact = IpFilter::parse("192.168.1.10").unwrap();
        assert!(exact.matches("192.168.1.10".parse().unwrap()));
        assert!(!exact.matches("192.168.1.11".parse().unwrap()));

        let cidr = IpFilter::parse("192.168.1.0/24").unwrap();
        assert!(cidr.matches("192.168.1.255".parse().unwrap()));
        assert!(!cidr.matches("192.168.2.1".parse().unwrap()));

        let range = IpFilter::parse("192.168.1.10-192.168.1.20").unwrap();
        assert!(range.matches("192.168.1.10".parse().unwrap()));
        assert!(range.matches("192.168.1.20".parse().unwrap()));
        assert!(!range.matches("192.168.1.21".parse().unwrap()));
        assert!(!range.matches("::1".parse().unwrap()));
    }

    #[test]
    fn expansion() {
        let cidr = IpFilter::parse("10.0.0.0/29").unwrap();
        let ips = expand(&cidr);
        assert_eq!(ips.len(), 8);
        assert_eq!(ips[0].to_string(), "10.0.0.0");
        assert_eq!(ips[7].to_string(), "10.0.0.7");

        let range = IpFilter::parse("10.0.0.1-10.0.0.3").unwrap();
        assert_eq!(expand(&range).len(), 3);

        // /16 шире лимита — обход обрезается, но не паникует.
        let wide = IpFilter::parse("10.0.0.0/16").unwrap();
        assert_eq!(expand(&wide).len(), MAX_SWEEP_TARGETS);
    }
}
