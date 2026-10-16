//! # ZeptoClaw Agent Mesh — Rust-порт
//!
//! Библиотека узлов децентрализованной P2P-сети автономных AI-агентов.
//! Порт Go-реализации (`/internal`, `/cmd`) на Rust. Модули соответствуют
//! Go-пакетам исходного проекта:
//!
//! | Rust-модуль   | Go-аналог          | Назначение                                  |
//! |---------------|--------------------|---------------------------------------------|
//! | `proto`       | `gen/zeptomesh/v1` | Protobuf-сообщения протокола mesh           |
//! | `wire`        | `internal/wire`    | Канонические энкодеры для дайджестов/подписей |
//! | `version`     | `internal/version` | Версия сборки                               |
//! | `security`    | `internal/security`| Ed25519-идентичность, подписи, PSK         |
//! | `config`      | `internal/config`  | Конфигурация узла (node.yaml)               |
//! | `storage`     | `internal/storage` | Локальное хранилище задач/пиров/артефактов  |
//! | `picoclaw`    | `internal/picoclaw`| Адаптер исполнителя: stub / cli / ollama    |
//! | `routing`     | `internal/routing` | Таблица соседей, скоринг, выбор исполнителя |
//! | `tasks`       | `internal/tasks`   | Менеджер задач: приём → исполнение → делегирование |
//! | `p2p`         | `internal/p2p`     | TCP-транспорт с length-prefixed кадрами     |
//! | `api`         | `internal/api`     | HTTP/JSON админ-API + метрики Prometheus    |
//! | `node`        | `internal/node`    | Сборка узла                                 |

pub mod api;
pub mod chat;
pub mod config;
pub mod multiformat;
pub mod node;
pub mod p2p;
pub mod picoclaw;
pub mod proto;
pub mod routing;
pub mod security;
pub mod skills;
pub mod storage;
pub mod tasks;
pub mod triggers;
pub mod version;
pub mod wire;
