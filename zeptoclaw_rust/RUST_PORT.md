# Rust-порт ZeptoClaw Agent Mesh (`zeptoclaw_rust`)

Параллельный порт Go-демона проекта на Rust. Crate: `zeptomesh` (библиотека)
+ бинарь `zeptomesh-node` (демон и админ-CLI). Цель — полная wire- и
файловая совместимость с Go-реализацией: общий protobuf-контракт, идентичные
канонические энкодеры дайджестов/подписей, один формат файла ключей, один
HTTP admin-API (CLI Go-узла работает с Rust-узлом и наоборот).

## Сборка и проверка

```bash
cd zeptoclaw_rust
cargo build --release        # target/release/zeptomesh-node
cargo test                   # 69 юнит-тестов + 1 сквозной smoke_e2e
```

Запуск узла (конфиг переиспользуется из корня репозитория):

```bash
cargo run -- genkey --out ./n0/keys/peer.key --print-id
ZEPTOMESH_DATA=./n0 ZEPTOMESH_INDEX=0 cargo run -- run -c ../configs/node.yaml
# клиентская часть CLI:
cargo run -- status -addr http://127.0.0.1:8081
cargo run -- submit -i "echo hello" 
```

Зависимости только из crates.io (`Cargo.toml`); внешнего protoc не требуется —
protobuf-кодек написан вручную (см. `proto`). Требуется Rust ≥ 1.77 (edition
2021), для тестов — Linux (в `picoclaw` используются POSIX-ограничения RLIMIT).

## Состав модулей и статус переноса

| Rust-модуль | Go-аналог | Статус | Что покрыто |
|---|---|---|---|
| `proto.rs` | `gen/zeptomesh/v1/mesh.pb.go` | ✅ готово | Ручной protobuf-кодек (varint/length-delimited) и все сообщения контракта mesh: TaskEnvelope, TaskResult, Capabilities, RPC, gossip. Бинарно совместим с Go. |
| `wire.rs` | `internal/wire` | ✅ готово | Канонические энкодеры и дайджесты — байт-в-байт как в Go (общие для digest и схем подписи). |
| `multiformat.rs` | go-libp2p / multiformats | ✅ готово | base58btc, uvarint, multihash (identity/sha256), CIDv1, libp2p-PeerID, multiaddr. Форматы выверены по исходникам библиотек; roundtrip-тесты. |
| `version.rs` | `internal/version` | ✅ готово | Штамп версии сборки. |
| `security.rs` | `internal/security` (identity, signing) | ✅ готово | Ed25519-идентичность: файл ключа `zeptomesh-ed25519-v1` + protobuf KeyInfo (байт-совместим с Go `MarshalPrivateKey`), SPKI-публикация, PeerID = base58(multihash(pubkey-proto)), подпись/верификация, извлечение ключа из PeerID, атомарная запись 0600. |
| `config.rs` | `internal/config` | ✅ готово | Полная структура YAML-конфига, Go-семантика слияния с дефолтами, `${VAR}`/`${VAR:-default}`, durations («1h30m»), env-overrides `ZEPTOMESH_*`, Validate(), derived paths. |
| `p2p.rs` | `internal/p2p` (framing) | ⚠️ частично | Length-prefixed кадры поверх tokio TCP (write_msg/read_msg, потолок 8 MiB). НЕ перенесено: libp2p-host, relay, noise/secio, connection gater, PSK-канал. |
| `storage.rs` | `internal/storage` | ⚠️ частично | JSONL-хранилище задач/пиров/событий + hash-addressed артефакты sha256. НЕ перенесено: BadgerDB (выбран JSONL как переносимый backend; при необходимости — замена на rocksdb/sled без изменения rest-слоя). |
| `routing.rs` | `internal/routing` | ✅ готово | Таблица соседей, скоринг, Select(). |
| `tasks.rs` | `internal/tasks` (manager, task) | ✅ готово | Конвейер ТЗ 6.8–6.12: build/validate/digest, route_inbound (дедуп, подпись, trust-политика, shell-политика), execute_local (семафор, workspace, таймауты, артефакты, переподпись результата), forward/TTL, handle_result с relay-возвратом, cancel, capabilities для gossip, таксономия ошибок. НЕ перенесено: decomposition.go (план/агрегация подзадач высшего уровня). |
| `picoclaw.rs` | `internal/picoclaw` | ✅ готово | Адаптеры stub / cli (RLIMIT_AS, setpgid, kill-process-group) / ollama / ws, фабрика, лимиты. |
| `api.rs` | `internal/api` | ⚠️ частично | HTTP/JSON admin-API: status/peers/tasks/get/cancel/resubmit/submit, shutdown/leave, config view, bearer-токен, Prometheus `/metrics`. Возвращают 501 endpoints неперенесённых пакетов (scan/chat/skills/triggers/brain — см. ниже). |
| `node.rs` | `internal/node` | ⚠️ частично | Сборка узла: identity → config → storage → routing → tasks → picoclaw → p2p-listener → api. Discovery ограничена static-bootstrap (многоадреса из конфига). НЕ перенесено: mDNS, DHT, search-relay pool (brain), композитный skill-источник. |
| `main.rs` | `cmd/zeptomesh-node` | ⚠️ частично | Команды: run, version, genkey, status, peers, submit, tasks, get, cancel, resubmit, leave (+ sd_notify EX_RESTART=75). НЕ перенесено: psk, reload, rotate, revoke, rebinds, skills*, triggers*, scan, candidates, backends, brain-scan, promote, share-keys, chat. |

### Не перенесено целиком (Go-пакеты без Rust-аналога)

| Go-пакет | Комментарий |
|---|---|
| `internal/discovery` (mdns, dht, gossip, searchtopic, scan) | Активное обнаружение и скан окружения; в порте пока только static bootstrap + базовый обмен пирами через gossip-кадры `p2p`/`node`. |
| `internal/brain` (catalog, sources, gossip) | Поиск/верификация LLM-эндпоинтов, шаринг кандидатов между «друзьями». |
| `internal/chat` | Чат-комнаты поверх mesh. |
| `internal/skills` | Версионированные дескрипторы навыков, дельта-синхронизация (частично заменено полем `capabilities` в `tasks`). |
| `internal/triggers` | Cron-планировщик задач. |
| `internal/telemetry`, `internal/tracing` | OpenTelemetry/OTLP, распределённые спаны. |
| `internal/logging`, `internal/metrics` | Аналог обеспечен tracing-subscriber и собственным `/metrics` в `api.rs`; полноценный отдельный prometheus-слушатель не вынесен. |
| `internal/simulator` | Модель mesh'а для нагрузочного теста ТЗ 17.3. |
| `deploy/`, `install.sh`, Makefile-цели | Инфраструктура развёртывания остаётся за Go-версией. |

## Ключевые решения порта

- **Protobuf без protoc.** `proto.rs` реализует кодек напрямую по wire-format
  spec; это исключает расхождение генераторов и даёт байтовую совместимость
  с Go (проверено тестами на эталонных векторах из Go-энкодеров).
- **Хранилище.** Вместо BadgerDB — append-only JSONL + content-addressed
  артефакты; интерфейс `storage` изолирован, замена возможна без правок
  `tasks`/`api`.
- **Транспорт.** Вместо полного libp2p — TLS-опциональный TCP с теми же
  length-prefixed кадрами и теми же идентификаторами сообщений; PeerID/
  multiformat-слой (`multiformat.rs`, `security.rs`) совместим с libp2p,
  поэтому узлы Go и Rust могут обмениваться идентичностями и подписями.
- **Совместимость конфигурации.** Rust читает тот же `configs/node.yaml`,
  что и Go, включая env-overrides — dev-кластер можно мешать (Go-узел +
  Rust-узел в одной сети) там, где не задействованы discovery/brain.

## Известные ограничения / TODO(port)

- `security.rs`: таблица observed-пилов без Mutex (заметка в коде, строка ~488).
- `node.rs`: backend-pool brain отключён (`pool = None`).
- `api.rs`: endpoints неперенесённых пакетов отвечают `501 Not Implemented`
  со списком недостающих модулей.
- Нет cross-сборки и CI-ворота для Rust-порта (Go-CI не затрагивает каталог).

## Очистка проекта (обслуживание)

Сборочные артефакты и runtime-данные не должны попадать в VCS — это
закреплено в корневом [`.gitignore`](../.gitignore) (`zeptoclaw_rust/target/`,
`zeptomesh-data/`, `*.log`, архивы). Для освобождения места на диске:

```bash
cargo clean                      # удалить zeptoclaw_rust/target (~2.3 ГБ)
rm -rf zeptoclaw_rust/zeptomesh-data   # данные dev-запусков узла (если есть)
git gc --aggressive --prune=now  # сжать объектную базу (.git ≈ 1.2 МБ)
```

История репозитория уже очищена от больших артефактов (см. UPDATE.md);
`git ls-files | grep target` возвращает пусто.

## Как читать порт вместе с Go

Контракт протокола (`api/proto/zeptomesh/v1/mesh.proto`, удалён из дерева
после переноса) полностью описан в [../docs/PROTOCOLS.md](../docs/PROTOCOLS.md) —
он же единая спецификация для Rust-реализации (`proto.rs`); статус реализации
ТЗ Go-версией (архив) — [../docs/STATUS.md](../docs/STATUS.md).
