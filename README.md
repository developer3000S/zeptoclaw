# ZeptoClaw Agent Mesh (`zeptomesh`)

Полностью децентрализованная P2P-сеть автономных AI-агентов на базе
[PicoClaw](https://github.com/sipeed/picoclaw). Узлы образуют сеть без
обязательного центрального координатора и без общей базы данных: задача может
поступить на любой узел, быть выполнена локально или рекурсивно делегирована
соседям, а результат возвращается инициатору по пройденному маршруту.

Реализовано по техническому заданию [`docs/ТЗ.md`](docs/ТЗ.md) («PicoClaw Agent
Mesh», версия 0.1).

> **Основная реализация — Rust (`zeptoclaw_rust/`).** Crate `zeptomesh` +
> бинарь `zeptomesh-node`: протокольный слой (ручной protobuf-кодек,
> канонические wire-энкодеры), идентичность Ed25519, multiformat/CID/PeerID,
> конфигурация, p2p-framing, хранилище, маршрутизация, менеджер задач,
> PicoClaw-адаптеры (stub/cli/ollama/ws), HTTP admin-API и CLI — компилируются,
> тесты зелёные. Состав, статус переноса и список непокрытых пакетов —
> [zeptoclaw_rust/RUST_PORT.md](zeptoclaw_rust/RUST_PORT.md).
>
> **Go-референс удалён из дерева.** Пакеты `internal/*`, `cmd/`, `gen/`,
> `install.sh`, `Makefile`, CI и каталог `.github/` выведены из репозитория
> после завершения порта; Go-описания в `docs/` сохранены как спецификация
> протокола и архитектуры. Статус реализации ТЗ Go-версией —
> [docs/STATUS.md](docs/STATUS.md); покрытие Rust-порта — в таблице
> `RUST_PORT.md`.

---

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Установка нод](#установка-нод)
- [Установка UI](#установка-ui)
- [Ключевые свойства](#ключевые-свойства)
- [Архитектура](#архитектура)
- [Структура репозитория](#структура-репозитория)
- [Требования](#требования)
- [Сборка и проверка](#сборка-и-проверка)
- [Конфигурация](#конфигурация)
- [Протоколы](#протоколы)
- [Безопасность](#безопасность)
- [Интеграция с PicoClaw](#интеграция-с-picoclaw)
- [RBAC для админ-API](#rbac-для-админ-api)
- [Аудит-логирование изменений и отказов](#аудит-логирование-изменений-и-отказов)
- [Метрики HTTP-запросов и инвентарь задач по статусу](#метрики-http-запросов-и-инвентарь-задач-по-статусу)
- [Документация](#документация)

---

## Быстрый старт

```bash
# Сборка и тесты Rust-демона
cd zeptoclaw_rust
cargo build --release              # бинарь target/release/zeptomesh-node
cargo test                         # юнит-тесты + сквозной smoke_e2e

# Два узла на одной машине (конфиг переиспользуется из корня репозитория)
./target/release/zeptomesh-node genkey --out ./n0/keys/peer.key --print-id
./target/release/zeptomesh-node genkey --out ./n1/keys/peer.key --print-id
ZEPTOMESH_DATA=./n0 ZEPTOMESH_INDEX=0 \
  ./target/release/zeptomesh-node run -c ../configs/node.yaml &
ZEPTOMESH_DATA=./n1 ZEPTOMESH_INDEX=1 ZEPTOMESH_MESH_PORT=4002 \
  ZEPTOMESH_API_PORT=8082 \
  ./target/release/zeptomesh-node run -c ../configs/node.yaml &

# Проверка (CLI того же бинаря)
./target/release/zeptomesh-node status -addr http://127.0.0.1:8081
./target/release/zeptomesh-node peers  -addr http://127.0.0.1:8081
./target/release/zeptomesh-node submit -addr http://127.0.0.1:8081 -i "echo hello" -w
# Если узел запущен с токеном админ-API — CLI должен знать имя env-переменной:
#   ZEPTOMESH_API_TOKEN=... zeptomesh-node status -addr ... -token-env ZEPTOMESH_API_TOKEN
# (или без -addr: ZEPTOMESH_CONFIG=<путь к node.yaml> — адрес и токен возьмутся из конфига)
```

Внимание: `zeptomesh-data/` и ключи — runtime-данные, в VCS не попадают
(см. `.gitignore`). Полные инструкции по развёртыванию —
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md); эксплуатация и восстановление —
[docs/RUNBOOK.md](docs/RUNBOOK.md).

> Команды Go-эпохи (`scan`, `brain-scan`, `candidates`, `backends`,
> `promote`, `friend-promote`, `share-keys`, `skills*`, `triggers*`,
> `rotate`, `revoke`) в Rust-порту ещё не реализованы — актуальный список
> команд и план переноса: [zeptoclaw_rust/RUST_PORT.md](zeptoclaw_rust/RUST_PORT.md).

---

## Установка нод

`install-nodes.sh` разворачивает **1..N нод** на одном сервере в двух режимах:

- **`--mode local`** — процессы через systemd user-units (нужен `cargo` +
  актуальный Rust, бинарь собирается из исходников; минимальную версию см. в
  разделе «Требования»).
- **`--mode docker`** — Docker-контейнеры (`docker compose`). Старые образы
  перед переустановкой удаляются, занятые порты автоматически смещаются на
  свободные.

```bash
# Установить 2 ноды в Docker (вариант по умолчанию)
./install-nodes.sh --nodes 2 --mode docker

# Установить 3 ноды с закрытой сетью (PSK генерируется автоматически)
./install-nodes.sh --nodes 3 --psk "/1/..." --mode docker

# Одна нода напрямую (systemd)
./install-nodes.sh --nodes 1 --mode local

# С реальным инференсом через локальный Ollama
./install-nodes.sh --nodes 2 --pico-mode ollama --ollama-url http://127.0.0.1:11434

# Справка по всем опциям
./install-nodes.sh --help
```

Раскладка портов для N нод (при совпадении с занятыми — смещается вверх): mesh
`4001+i`, admin-API `14001+i`, Prometheus `24001+i`. При установке в Docker
скрипт: удаляет старые образы `zeptomesh-node`, проверяет порты на занятость
(локальные сокеты + опубликованные порты контейнеров) и выбирает альтернативы,
поднимает первую ноду-якорь и после её готовности — остальные. Нода 0 — якорь:
её адрес (`zepto-0:<mesh-порт>`, разрешается через DNS docker-сети) передаётся
остальным нодам в `ZEPTOMESH_BOOTSTRAP`; у самой якорной ноды bootstrap пуст,
соседей она находит через gossip/DHT.

**Сходимость mesh.** Нода, стартовавшая раньше якоря, не обязана ждать его:
bootstrap-диал повторяется с backoff (2 с → 30 с), пока якорь не ответит. Адреса
соседей, полученные через gossip, нода докаливает сама (peer exchange), поэтому
mesh доходит до полного, а не остаётся звездой от якорной ноды. Соседям
объявляется **routable-адрес** ноды (в Docker это IP контейнера, а не
bind-адрес `0.0.0.0`, по которому connect ушёл бы в localhost), поэтому адреса
из `GET /api/v1/peers` пригодны для dial.

Данные нод — в `--data-dir` (по умолчанию `./zeptomesh-data`, в VCS не
попадает). `--uninstall` полностью останавливает и удаляет стек.

## Установка UI

`install-ui.sh` разворачивает веб-дашборд для наблюдения за сетью. UI
самостоятельно опрашивает admin-API нод и строит сводный граф сети (в т.ч.
обнаруживает уже запущенные `zepto-*` контейнеры/локальные ноды на типовых
портах). Режимы и требования — как у нод.

```bash
# Установить UI в Docker, ноды обнаружить автоматически
./install-ui.sh --mode docker

# Со своими нодами и токеном
./install-ui.sh --nodes "http://127.0.0.1:14001,http://127.0.0.1:14002" \
    --api-token $ZEPTOMESH_API_TOKEN

# В локальном режиме на другом порту
./install-ui.sh --mode local --port 28090 --nodes "http://127.0.0.1:14001"

# Удалить UI
./install-ui.sh --uninstall
```

UI по умолчанию доступен на `http://127.0.0.1:28090` (`--port` — сменить,
занятый порт заменяется свободным). Полное описание переменных окружения и
устройства UI — [docs/UI.md](docs/UI.md).

---

## Ключевые свойства

| Область | Реализация |
|---|---|
| Язык демона | Rust (основная реализация; исходный Go-демон выведен из дерева)
| Идентификация узла | Ed25519, самоаттестуемый libp2p `Peer ID` |
| Транспорт | Rust-порт: TCP (length-prefixed кадры); Go-референс: QUIC + TCP, Noise, relay v2 |
| Шифрование | Noise (TCP-канал); TLS 1.3 внутри QUIC; сквозное и для реле |
| Мультиплексирование | Yamux (поверх TCP) |
| NAT-преодоление | Circuit relay v2: клиент реле + опциональное служение реле с лимитами |
| Формат сообщений | Protocol Buffers v3, length-prefixed фреймы (потолок 8 MiB) |
| Обнаружение (хост) | Реестр Unix-сокетов в `<data_dir>/run` |
| Обнаружение (LAN) | mDNS (`_zeptomesh._udp`) |
| Обнаружение (WAN) | Bootstrap-узлы, Kademlia DHT (индекс навыков), Peer Exchange |
| Членство | GossipSub (эпидемическая рассылка `PeerState`) |
| Поиск исполнителя | Search-relay: расширенный опрос соседей с `full_refresh` навыков |
| Активный скан окружения | `scan` (`POST /api/v1/admin/scan`): агент сам прощупывает локальный хост, LAN (вкл. перебор подсети) и внешние источники (DHT, bootstrap, peer-exchange, search topic), находит других агентов и устанавливает с ними связь (dial + проверка подписанных capabilities → сосед в таблице); опционально медленный фоновый скан по `discovery.scan` |
| Brain (поиск LLM) | `brain` в конфиге: каталог открытых Ollama-эндпоинтов из поисковиков (Censys/Shodan/GreyNoise/ZoomEye/CriminalIP/Netlas) + пул проверенных бэкендов; **верификация автономная** — зонд `GET /api/tags`, ответивший эндпоинт получает `verified` и трафик, центрального подтверждения нет (FOA §4.4.4 игнорируется по директиве). Обмен находками и API-ключами поиска с агентами-друзьями через их admin API и общий gossip-топик |
| Хранилище | Rust-порт: append-only JSONL + content-addressed артефакты sha256; Go-референс: BadgerDB v4 |
| Делегирование | TTL, маршрут без петель, дедупликация, лимиты фан-аута и параллелизма, rate limiting |
| Декомпозиция | план подзадач (`subtasks` / `--subtask`) → дочерние конверты → один подписанный итог: секции в порядке плана, дедуп артефактов по hash, ретрай retryable-ребёна |
| Ограничения исполнителя | `max_parallel_tasks_per_peer`, порог свободного места (`statfs`), квота workspace при сборе артефактов, `RLIMIT_AS` + отдельная группа процессов для дочернего PicoClaw |
| Управление на ходу | hot-reload (`SIGHUP` / `POST /api/v1/admin/reload-config`) trust-режима и порога, allow/blocked-файлов, rate limit и уровня лога; `leave` — штатный уход с публикацией `status: "left"` |
| Подписи | `TaskEnvelope`, `TaskResult` (+ `worker_signature`), `TaskAck`, `CancelRequest`, `Capabilities`, `origin_signature` (авторство задачи, не перезаписывается ретрансляторами), `KeyRebind` (переход идентичности), `SkillsSyncResponse` |
| Навыки | Версионированные дескрипторы (name/version/digest) с epoch узла; обмен между соседями (дельта-синхронизация по RPC `skills_sync`), автообновление, если у партнёра новее; политика раскрытия, бюджет импорта, persist в `<data_dir>/skills.json` |
| Идентичность во времени | Плановая ротация ключа (заявление, подписанное обоими ключами; самодостаточно, принимается без участия оператора) и аварийный отзыв (терминальный); доверие привязано к классу идентичностей, журнал `<data_dir>/rebinds.json` |
| Доступ | Списки разрешённых/запрещённых пиров (ConnectionGater), уровни доверия, журнал безопасности (JSONL) |
| Доступ к узлу | HTTP/JSON админ-API + CLI `zeptomesh-node`, bearer-токен через env |
| Исполнитель | PicoClaw через адаптер: CLI-процесс / Pico Protocol (WebSocket `/pico/ws`) / offline-stub |

---

## Архитектура

Один узел = mesh-демон, локальный PicoClaw и node-local хранилище. Центральных
компонентов нет; `bootstrap.*` — необязательные точки входа в сеть.

```text
                     ┌──────────────────────── ZeptoClaw node ────────────────────────┐
   HTTP/JSON         │                                                                │
  (админ-API)  ────► │  internal/api          GET /healthz, /api/v1/*, POST tasks…    │
                     │        │                                                       │
                     │        ▼                                                       │
                     │  internal/tasks        Manager: приём → валидация →            │
                     │        │               выполнить | делегировать → результат    │
                     │        │                                                       │
                     │        ├────────────► internal/picoclaw  Adapter               │
                     │        │                    │  stub │ cli │ pico-ws ───────────┼──► PicoClaw
                     │        ▼                    │                                   │
                     │  internal/routing  Table: соседи, скоринг, выбор кандидатов    │
                     │        │                                                       │
                     │        ▼                                                       │
                     │  internal/p2p      Host (libp2p) + Service (task/result/rpc)   │
                     │        ▲                                                       │
                     │        │                                                       │
                     │  internal/discovery  local registry │ mDNS │ bootstrap │ DHT │
                     │        │                                  gossip (Membership)  │
                     │        ▼                                                       │
                     │  internal/security  identity │ signing │ trust │ ratelimit │   │
                     │                                   audit journal                 │
                     │  internal/storage   BadgerDB (journal, peers, dedup) +          │
                     │                     artifacts/sha256                            │
                     └───────────────────────────────────┬────────────────────────────┘
                                                         │
                            P2P: /zeptomesh/{task,result,rpc}/0.1.0,
                                 pubsub /zeptomesh/membership/0.1.0
                                                         │
                                     другие узлы mesh
```

Жизненный цикл задачи:

```text
любой узел принимает задачу
   │
   ├─ подпись/digest ✗ ──────────────────────────────► REJECTED + audit
   ├─ task_id в dedup-окне ──────────────────────────► ACK DUPLICATE (без повторного исполнения)
   ├─ есть план подзадач ────────────────────────────► WAITING_SUBTASKS: каждый пункт —
   │                                                        отдельный конверт (ttl-1,
   │                                                        пересечение прав) в этот же
   │                                                        конвейер → AGGREGATING →
   │                                                        один подписанный итог
   ├─ навыки подходят и есть слот ───────────────────► ACCEPTED → RUNNING → COMPLETED
   │                                                        └─ результат подписывается
   │                                                             (worker_signature) и едет
   │                                                             назад по route_stack
   └─ иначе ─────────────────────────────────────────► FORWARDING:
                                                          скоринг соседей → ≤ max_fanout
                                                          кандидатов, ≤ max_parallel_candidates
                                                          параллельно, ttl-1
                                                              └─ кандидатов нет → search-relay
                                                                 (соседи с full_refresh навыков)
                                                                 └─ все отказались/TTL=0 → FAILED
```

Подробнее: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Структура репозитория

```text
zeptoclaw/
├── zeptoclaw_rust/             # основная реализация демона (Rust)
│   ├── src/                    #   proto · wire · multiformat · security · config · p2p ·
│   │                           #   storage · routing · tasks · picoclaw · api · node · main
│   ├── tests/smoke_e2e.rs      #   сквозной smoke-тест (запуск узла + admin-API)
│   ├── Cargo.toml / Cargo.lock
│   ├── README.md               #   быстрый старт порта
│   └── RUST_PORT.md            #   состав, статус переноса, обслуживание/очистка
├── configs/
│   ├── node.yaml               # шаблон демона (env-параметризован; читают оба стека)
│   └── examples/
│       ├── lan.yaml            # профиль «одна локальная сеть»  (ТЗ 22.8)
│       ├── wan.yaml            # профиль «Интернет / разные хосты»
│       └── dev-node.yaml       # профиль «несколько узлов на одной машине»
├── docs/                       # ТЗ · ARCHITECTURE · PROTOCOLS · PICOCLAW-INTEGRATION ·
│                               # DEPLOYMENT · RUNBOOK · STATUS · LOADTEST · TESTREPORT · UI
├── .env.example                # переменные LLM/каналов для PicoClaw
├── install-nodes.sh            # установка 1..N нод mesh (local | docker)
├── install-ui.sh               # установка UI-дашборда (local | docker)
├── ToDo.md / UPDATE.md         # план и журнал обслуживания
├── LICENSE                     # Apache License 2.0 (полный текст)
└── .gitignore                  # target/, zeptomesh-data/, *.log, node_modules/ и пр.
```

Контракт протокола (`api/proto/zeptomesh/v1/mesh.proto`) описан в
[docs/PROTOCOLS.md](docs/PROTOCOLS.md); protobuf-кодек Rust-порта реализует
его напрямую (внешний `protoc` не требуется). Установочная инфраструктура
Go-эпохи (`Makefile`, `deploy/`, CI) удалена из дерева; вместо неё — скрипты
[`install-nodes.sh`](install-nodes.sh) (ноды) и [`install-ui.sh`](install-ui.sh)
(UI-дашборд) — см. [Установка нод](#установка-нод) и [Установка UI](#установка-ui).


---

## Требования

Для сборки Rust-демона:

- актуальный Rust, `cargo` (edition 2021). `Cargo.lock` не коммитится,
  поэтому зависимости каждый раз резолвятся в свежие версии и требуют
  свежего тулчейна: `icu_provider` v2.3+ использует edition2024, `uuid`
  требует rustc ≥ 1.89 (сборка проверена на rustc 1.99);
- доступ к crates.io (или предварительно заполненный кэш зависимостей);
- Linux для тестов `picoclaw` (используются POSIX-ограничения RLIMIT).

Внешний `protoc` **не требуется** — protobuf-кодек написан вручную
(`proto.rs`). Для Docker-режима — Docker 24+ с compose (профиль Go-эпохи,
`Dockerfile` выведен из дерева). Для выполнения задач реальным агентом —
установленный `picoclaw` (см.
[docs/PICOCLAW-INTEGRATION.md](docs/PICOCLAW-INTEGRATION.md)); без него узел
работает в режиме `picoclaw.mode: stub` — детерминированный offline-исполнитель.


---

## Сборка и проверка

```bash
cd zeptoclaw_rust

cargo build --release      # бинарь target/release/zeptomesh-node
cargo test                 # юнит-тесты + сквозной tests/smoke_e2e.rs (70 тестов)
cargo clippy               # статика (чистый вывод = ворота пройдены)
cargo fmt --check          # форматирование

# быстрая проверка бинаря
target/debug/zeptomesh-node version
target/debug/zeptomesh-node genkey --out ./n0/keys/peer.key --print-id
ZEPTOMESH_DATA=./n0 ZEPTOMESH_INDEX=0 \
  target/debug/zeptomesh-node run -c ../configs/node.yaml &
target/debug/zeptomesh-node status -addr http://127.0.0.1:8081
```

Тесты `picoclaw` используют POSIX-ограничения (RLIMIT), поэтому полный набор
прогоняется на Linux. Состав модулей, статус переноса по Go-пакетам и
известные ограничения — [zeptoclaw_rust/RUST_PORT.md](zeptoclaw_rust/RUST_PORT.md).

Покрытие тестами Go-эпохи (инвентарь, маппинг на ТЗ §17, живая приёмка
E.1–E.6) сохранено как справочник требований:
[docs/STATUS.md](docs/STATUS.md), [docs/TESTREPORT.md](docs/TESTREPORT.md).
CI-конвейер Go-версии (`.github/workflows/ci.yml`) удалён вместе с Go-деревом;
для Rust-порта CI-ворота пока нет — это пункт плана (см. TODO(port) в
`RUST_PORT.md`).


---

## Конфигурация

Формат — YAML. Загрузчик (`config.Load`) применяет `config.Default()`, затем
раскрывает в файле `${VAR}` и `${VAR:-default}` (включая вложенные default'ы),
затем накладывает YAML и переменные окружения (`ZEPTOMESH_BOOTSTRAP`
дописывается в `discovery.bootstrap`), после чего выполняется `Validate()`.
Секреты в файл конфигурации не пишутся.

Корневые секции и значимые значения по умолчанию (полный env-параметризованный
шаблон — [`configs/node.yaml`](configs/node.yaml), готовые профили —
[`configs/examples/`](configs/examples/)):

```yaml
node:
  name: zeptomesh-node
  data_dir: ./zeptomesh-data
  listen:                       # QUIC — основной, TCP — резервный
    - /ip4/0.0.0.0/tcp/4001
    - /ip4/0.0.0.0/udp/4001/quic-v1
  announce: []                  # фиксированные публичные адреса (NAT, несколько интерфейсов)
  private_network_psk: ""       # закрытая сеть libp2p; с PSK узел работает по TCP (QUIC закрытые сети не поддерживает)
  relay:                        # Circuit relay v2 (ТЗ 6.3.3.4)
    enabled: false              # пользоваться чужими реле
    advertise_as_relay: false   # служить реле (с лимитами ниже)
    static_relays: []
    limit: { max_reservations: 128, max_circuits: 16, reservation_ttl: 1h,
             conn_duration: 2m, conn_data_bytes: 131072 }

identity:
  key_file: ""                  # пусто → <data_dir>/keys/peer.key (Ed25519, 0600)

discovery:
  local_registry: true
  local_socket_dir: ""          # пусто → <data_dir>/run
  mdns: true
  mdns_service_name: _zeptomesh._udp
  bootstrap: []                 # /ip4/…/tcp/4001/p2p/12D3KooW…
  dht: true
  dht_mode: auto                # server | client | auto | auto-server
  peer_exchange: true
  bootstrap_interval: 30s
  scan:                         # активный скан окружения агентом (по умолчанию выкл)
    enabled: false              # включить медленный фоновый скан
    interval: 15m
    subnet_scan: false          # перебор локальной подсети (трафик по всей LAN)
    dial_timeout: 6s
    timeout: 45s
    max_candidates: 64
  gossip:
    enabled: true
    topic: /zeptomesh/membership/0.1.0
    heartbeat: 3s
    full_sync: 60s
    failure_timeout: 15s        # должен превышать heartbeat

neighbors: { min: 8, target: 16, max: 64 }

tasks:
  default_ttl: 5
  max_ttl: 10
  max_parallel_tasks: 4
  max_parallel_tasks_per_peer: 2       # сколько задач одного пира держится параллельно
  default_timeout_seconds: 600
  max_timeout_seconds: 3600            # потолок для ttl-таймаута задачи
  max_task_payload_bytes: 1048576
  max_artifact_bytes: 104857600
  max_workspace_bytes: 536870912       # квота на рабочие файлы задачи (ТЗ 6.8.4)
  min_free_disk_bytes: 1073741824      # порог свободного места перед стартом
  max_task_memory_bytes: 1073741824    # RLIMIT_AS дочернему процессу (unix)
  dedup_window: 15m
  retention: 168h
  forwarding:
    max_fanout: 5
    max_parallel_candidates: 3  # <= max_fanout
    max_retries: 2
    attempt_timeout: 120s
    retry_interval_seconds: 5s
    search_relay:               # расширенный поиск навыков у соседей
      enabled: true
      max_depth: 2
      fanout: 3
      request_timeout: 8s
      cache_ttl: 60s
      topic:                    # эпидемический поиск (ТЗ 6.9.5 п.5): подписанный
        enabled: false          # запрос НАВЫКОВ в общий pubsub-топик, без id/
        name: "/zeptomesh/skillsearch/0.1.0"  # инструкции задачи. По умолчанию
        request_ttl: 30s        # выключен; включается только перезапуском.
        max_answers: 8          # Подробности — configs/node.yaml.
        answer_cooldown: 10s

capabilities:
  skills: [general]
  resource_class: medium
  accept_external_tasks: true
  allow_shell: false            # опасные операции по умолчанию запрещены
  allow_network_tools: true
  max_parallel_tasks: 4
  disabled_skills: []           # вычитаются из skills

picoclaw:
  mode: stub                    # stub | binary | http
  binary: picoclaw
  workspace_root: ""            # корень рабочих каталогов; пусто → <data_dir>/picoclaw/workspaces
  timeout_seconds: 600
  max_concurrent_agents: 4
  model: ""                     # модель узла → --model (только режим binary, ТЗ 10.3)
  extra_args: []
  prompt_template: ""           # %s = инструкция; пусто — передавать как есть
  env: {}                       # например PICOCLAW_AGENTS_DEFAULTS_TEMPERATURE=0.2
  http:                         # Pico Protocol поверх WebSocket (у PicoClaw нет REST для промпта)
    base_url: http://127.0.0.1:18790   # конвертируется в ws://… адаптером
    path: /pico/ws
    health_path: /health
    token_env: PICOCLAW_MESH_PICO_TOKEN  # имя переменной с bearer-токеном
    timeout_seconds: 600
  stub:
    latency: 50ms
    echo: true

security:
  trust_mo... [truncated]
```

---

## Протоколы

> Контракт `api/proto/zeptomesh/v1/mesh.proto` и генерированный код удалены из дерева вместе с Go-пакетами;
> описание полей и схем подписи полностью сохранено ниже и в [docs/PROTOCOLS.md](docs/PROTOCOLS.md)
> (единая спецификация для обоих портов).

Сетевые протоколы узлов: `/zeptomesh/{task,result,rpc}/0.1.0`.

- `TaskEnvelope` — конверт задачи (ТЗ 6.6.1 п.1): подписанная инструкция, навыки,
  ограничения, маршрут, подпись автора (origin_signature).
- `TaskResult` — результат (ТЗ 6.6.1 п.2): подписанный ответ, артефакты,
  подпись исполнителя (worker_signature), маршрут.
- `Capabilities` — подписанное описание узла: навыки, модель, загрузка,
  ресурсный класс, адреса.
- `KeyRebind` — переход идентичности (ТЗ 11.2): подписанное заявление,
  подписанное обоими ключами.
- `SkillsSyncResponse` — ответ на запрос синхронизации навыков (ТЗ 6.3):
  подписанный дельта-обновлённый список дескрипторов.

Подробнее: [docs/PROTOCOLS.md](docs/PROTOCOLS.md).

---

## Безопасность

- Trust-режимы: open (все), limited (только из allow-файла), private (только
  из allow-файла + подпись задачи).
- Списки разрешённых/запрещённых пиров (ConnectionGater).
- Журнал безопасности (JSONL).
- Rate limiting.
- Подписи: `TaskEnvelope`, `TaskResult`, `TaskAck`, `CancelRequest`,
  `Capabilities`, `origin_signature`, `KeyRebind`, `SkillsSyncResponse`.
- Идентичность во времени: ротация ключа (заявление, подписанное обоими
  ключами) и отзыв (терминальный).

Подробнее: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#безопасность).

---

## Интеграция с PicoClaw

- PicoClaw CLI: `picoclaw agent` принимает только `-d/-m/-s/--model` и
  отказывается от неизвестных флагов.
- Pico Protocol: WebSocket `/pico/ws` (у PicoClaw нет REST для промпта).
- Offline-stub: детерминированный исполнитель для тестов.

Подробнее: [docs/PICOCLAW-INTEGRATION.md](docs/PICOCLAW-INTEGRATION.md).

---

## Документация

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) — компоненты, поток данных, маршрутизация,
  политика доверия, хранилище, отказоустойчивость.
- [PROTOCOLS.md](docs/PROTOCOLS.md) — идентификаторы протоколов, фрейминг,
  поля, канонические энкодеры, схемы подписи, ключи gossip/DHT.
- [PICOCLAW-INTEGRATION.md](docs/PICOCLAW-INTEGRATION.md) — проверенные интерфейсы
  PicoClaw, режимы адаптера, изоляция, таймауты.
- [DEPLOYMENT.md](docs/DEPLOYMENT.md) — развёртывание (LAN/WAN, PSK, bootstrap). Установка готовыми скриптами: [`install-nodes.sh`](install-nodes.sh) (ноды) и [`install-ui.sh`](install-ui.sh) (UI-дашборд).
- [RUNBOOK.md](docs/RUNBOOK.md) — эксплуатация (systemd/Docker, hot-reload, `leave`, метрики, восстановление). Справочник Go-эпохи; часть команд (`rotate`, `revoke`, hot-reload) в Rust-порту ещё не реализована.
  метрики, восстановление, бэкап ключей/БД).
- [STATUS.md](docs/STATUS.md) — статус реализации ТЗ.
- [LOADTEST.md](docs/LOADTEST.md) — нагрузочное тестирование (ТЗ 17.3). Симулятор Go-эпохи (`internal/simulator`) в Rust-порт не перенесён.
- [TESTREPORT.md](docs/TESTREPORT.md) — отчёт о тестировании Go-версии (справочник требований к тестам порта).
- [zeptoclaw_rust/RUST_PORT.md](zeptoclaw_rust/RUST_PORT.md) — Rust-порт:
  состав модулей, статус переноса Go → Rust, сборка и известные ограничения.

---

## RBAC для админ-API

Доступ к админ-API управляется ролями через именованные токены. Помимо
legacy-токена (`api.auth_token_env`, полный admin), в конфиге задаются токены
с ролями:

```yaml
api:
  tokens:
    - { name: dashboard, role: viewer,   token_env: ZEPTOMESH_UI_VIEWER_TOKEN }
    - { name: bot,       role: operator, token_env: ZEPTOMESH_OPERATOR_TOKEN }
    - { name: root,      role: admin,    token_env: ZEPTOMESH_ADMIN_TOKEN }
```

Значения токенов берутся из env; в файл конфига секреты не пишутся. Правила:

- **viewer** — только чтение (`GET`).
- **operator** — чтение + мутации задач, навыков, триггеров и сканирование
  (`/api/v1/admin/scan`). Остальные `/api/v1/admin/*` (reload, rotate-key,
  revoke, leave) недоступны.
- **admin** — всё, включая `/api/v1/admin/*`.

Если токены не сконфигурированы — поверхность остаётся loopback-first, как
до RBAC: любой вызов считается admin. При неверном токене — `401`, при
недостаточных правах — `403`.

## Аудит-логирование изменений и отказов

В журнал безопасности (JSONL в `<data_dir>/audit/security.jsonl`) пишутся
исходы админ-API-запросов:

- **`api_mutation`** — каждый не-`GET` запрос, прошедший аутентификацию и
  проверку роли; фиксируется имя токена (`actor`) и его роль (`role`).
- **`api_auth_denied`** — неверный/отсутствующий bearer-токен (`401`).
- **`api_role_denied`** — токен верный, но роль недостаточна (`403`).

События содержат `remote` (адрес клиента) и `detail` (`METHOD /path`). Сырые
значения токенов в журнал не попадают. В `AuditEvent` добавлены поля `Actor`
(имя токена) и `Role`.

## Метрики HTTP-запросов и инвентарь задач по статусу

В коллектор добавлены и зарегистрированы две метрики:

- **`zeptomesh_http_requests_total{method,path,status}`** — счётчик запросов
  админ-API. Лейбл `path` — это зарегистрированный паттерн маршрута
  (`"GET /api/v1/tasks/{id}"`), а не сырой URL, поэтому кардинальность не
  растёт от идентификаторов задач; неопознанные пути считаются как
  `unmatched`.
- **`zeptomesh_task_inventory{status}`** — гейдж: количество записей журнала
  задач по статусу. Обновляется узлом каждые 15 секунд в фоновом цикле
  `maintenance` (`publishTaskInventory`) по полному журналу; статусы,
  переставшие встречаться, обнуляются.

## Обновлен дашборд

В веб-дашборде (BFF `zeptomesh-ui`, Rust) реализовано:

- **Версия и время работы** — в шапке (`#version-indicator`) показываются
  `v{version}` и `up {аптайм}` первого reachable-узла из его `/api/v1/status`;
  аптайм форматируется («2д 3ч», «3ч 4м», «5м», «30с»).
- **Распределение статусов задач** — панель «Распределение по статусам (все
  узлы)» в разделе «Задачи»: BFF агрегирует журналы управляемых нод в
  `tasks_by_status` (сводку по статусам), фронтенд рисует pills с количеством
  на каждый статус и общий итог.
- **RBAC-роли** — `viewer` (только чтение), `operator` (задачи и прокси к
  узлам), `admin` (CRUD нод). Токены задаются через `ZEPTOMESH_UI_TOKEN`,
  `ZEPTOMESH_UI_OPERATOR_TOKEN`, `ZEPTOMESH_UI_VIEWER_TOKEN`. Без токенов
  авторизация выключена (роль `admin`).
- **Режим «только чтение»** — `ZEPTOMESH_UI_READONLY=1` запрещает любые
  мутации (CRUD нод, постановка и отмена задач) независимо от роли.
- **Экспорт и импорт реестра нод** — `GET /api/v1/nodes/export` (токены нод по
  умолчанию **не** попадают в выгрузку; `?include_tokens=true` — включить) и
  `POST /api/v1/nodes/import?mode=merge|upsert|replace`:
  - `merge` (по умолчанию) — добавить новые, существующие по URL пропустить;
  - `upsert` — добавить новые, существующие обновить;
  - `replace` — очистить реестр и записать только импортируемые записи.
- **Поиск по содержимому задач** — фильтры `GET /api/v1/tasks?status=…&q=…&skill=…&node_id=…`
  (статус, подстрока в инструкции, требуемый навык, нода).
- **Фильтрация нод по навыку** — `GET /api/v1/nodes/search?skill=…` проверяет
  реальный реестр навыков ноды (`/api/v1/skills`), а не только её теги.
- **Метрики Prometheus** — `GET /metrics` на самом BFF: число нод (всего /
  включённых / онлайн / обнаруженных), рёбра графа, инвентарь задач по
  статусам, счётчики запросов и ошибок прокси.

### Переменные окружения дашборда

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `ZEPTOMESH_UI_LISTEN` | `127.0.0.1:28090` | адрес прослушивания |
| `ZEPTOMESH_UI_DATA` | `./zeptomesh-ui-data` | каталог данных |
| `ZEPTOMESH_UI_NODES_FILE` | `<data>/nodes.json` | реестр управляемых нод |
| `ZEPTOMESH_UI_NODES` | пусто | стартовый список нод (seed) |
| `ZEPTOMESH_UI_TOKEN` | пусто | токен роли `admin` |
| `ZEPTOMESH_UI_OPERATOR_TOKEN` | пусто | токен роли `operator` |
| `ZEPTOMESH_UI_VIEWER_TOKEN` | пусто | токен роли `viewer` |
| `ZEPTOMESH_UI_API_TOKEN` | пусто | bearer для нод без своего токена |
| `ZEPTOMESH_UI_POLL_INTERVAL` | `5` | интервал опроса нод, сек |
| `ZEPTOMESH_UI_DISCOVERY_PORTS` | `8081-8085,14001-14010` | порты admin-API для обхода сети |
| `ZEPTOMESH_UI_READONLY` | `0` | `1` — запрет всех мутаций |

Полное описание переменных окружения и устройства UI — [docs/UI.md](docs/UI.md).

## Обновлена документация

Разделы README и документы приведены в соответствие реализованным изменениям:
завершённые пункты ToDo.md отмечены, статус внесён в `docs/STATUS.md`,
дашборд — в `docs/UI.md`.
