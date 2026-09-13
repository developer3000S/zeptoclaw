# ZeptoClaw Agent Mesh (`zeptomesh`)

Полностью децентрализованная P2P-сеть автономных AI-агентов на базе
[PicoClaw](https://github.com/sipeed/picoclaw). Узлы образуют сеть без
обязательного центрального координатора и без общей базы данных: задача может
поступить на любой узел, быть выполнена локально или рекурсивно делегирована
соседям, а результат возвращается инициатору по пройденному маршруту.

Реализовано по техническому заданию [`ТЗ.md`](ТЗ.md) («PicoClaw Agent Mesh»,
версия 0.1).

> **Статус: рабочий демон.** Сетевой стек, обнаружение, маршрутизация, задачи,
> административный API, CLI, тесты, `install.sh` (локально/Docker, N
> экземпляров, автостарт) — написаны, компилируются и проверяются тестами.
> Что из пунктов ТЗ сделано частично или не сделано — честный и актуальный
> отчёт: [docs/STATUS.md](docs/STATUS.md).

---

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Ключевые свойства](#ключевые-свойства)
- [Архитектура](#архитектура)
- [Структура репозитория](#структура-репозитория)
- [Требования](#требования)
- [Сборка и проверка](#сборка-и-проверка)
- [Конфигурация](#конфигурация)
- [Протоколы](#протоколы)
- [Безопасность](#безопасность)
- [Интеграция с PicoClaw](#интеграция-с-picoclaw)
- [Документация](#документация)

---

## Быстрый старт

```bash
# Вариант A — установщик: локально (systemd, автостарт) или в Docker, N узлов
./install.sh install --mode local --nodes 3      # sudo не обязателен: user-юниты
./install.sh install --mode docker --nodes 3
./install.sh status

# Вариант B — из исходников, два узла на одной машине
export PATH=$PATH:/usr/local/go/bin
go build -trimpath -o bin/ ./cmd/zeptomesh-node
bin/zeptomesh-node genkey --out ./n0/keys/peer.key   # и то же для ./n1
ZETOMESH_DATA=./n0 ZETOMESH_INDEX=0 bin/zeptomesh-node run -config configs/node.yaml &
ZETOMESH_DATA=./n1 ZETOMESH_INDEX=1 ZETOMESH_MESH_PORT=4002 \
  ZETOMESH_API_PORT=8082 ZETOMESH_PROM_LISTEN=127.0.0.1:9465 \
  bin/zeptomesh-node run -config configs/node.yaml &

# Проверка
bin/zeptomesh-node status -addr http://127.0.0.1:8081
bin/zeptomesh-node peers  -addr http://127.0.0.1:8081
bin/zeptomesh-node submit -addr http://127.0.0.1:8081 -i "hello" -w
```

Полные инструкции по развёртыванию (LAN/WAN-профили, PSK, bootstrap,
несколько экземпляров, firewall) — [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md);
эксплуатация и восстановление — [docs/RUNBOOK.md](docs/RUNBOOK.md).

---

## Ключевые свойства

| Область | Реализация |
|---|---|
| Язык сетевого слоя | Go 1.26 |
| Идентификация узла | Ed25519, самоаттестуемый libp2p `Peer ID` |
| Транспорт | QUIC (основной) + TCP (резервный) |
| Шифрование | Noise (TCP-канал); TLS 1.3 внутри QUIC; сквозное и для реле |
| Мультиплексирование | Yamux (поверх TCP) |
| NAT-преодоление | Circuit relay v2: клиент реле + опциональное служение реле с лимитами |
| Формат сообщений | Protocol Buffers v3, length-prefixed фреймы (потолок 8 MiB) |
| Обнаружение (хост) | Реестр Unix-сокетов в `<data_dir>/run` |
| Обнаружение (LAN) | mDNS (`_zeptomesh._udp`) |
| Обнаружение (WAN) | Bootstrap-узлы, Kademlia DHT (индекс навыков), Peer Exchange |
| Членство | GossipSub (эпидемическая рассылка `PeerState`) |
| Поиск исполнителя | Search-relay: расширенный опрос соседей с `full_refresh` навыков |
| Хранилище | BadgerDB v4 (node-local) + content-addressed артефакты на диске |
| Делегирование | TTL, маршрут без петель, дедупликация, лимиты фан-аута и параллелизма, rate limiting |
| Декомпозиция | план подзадач (`subtasks` / `--subtask`) → дочерние конверты → один подписанный итог: секции в порядке плана, дедуп артефактов по hash, ретрай retryable-ребёна |
| Ограничения исполнителя | `max_parallel_tasks_per_peer`, порог свободного места (`statfs`), квота workspace при сборе артефактов, `RLIMIT_AS` + отдельная группа процессов для дочернего PicoClaw |
| Управление на ходу | hot-reload (`SIGHUP` / `POST /api/v1/admin/reload-config`) trust-режима и порога, allow/blocked-файлов, rate limit и уровня лога; `leave` — штатный уход с публикацией `status: "left"` |
| Подписи | `TaskEnvelope`, `TaskResult` (+ `worker_signature`), `TaskAck`, `CancelRequest`, `Capabilities` |
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
├── api/proto/zeptomesh/v1/
│   └── mesh.proto              # контракт: TaskEnvelope, TaskResult, Capabilities, RPC
├── gen/zeptomesh/v1/
│   └── mesh.pb.go              # генерируется (protoc-gen-go), в VCS для `go build` без protoc
├── cmd/zeptomesh-node/
│   ├── main.go                 # демон: run | version | genkey | psk | reload | leave
│   └── client.go               # CLI админ-API: status|peers|capabilities|submit (--subtask)|
│                               #                   tasks|get|cancel|resubmit|reload|leave
├── internal/
│   ├── api/                    # HTTP/JSON админ-API (+ bearer-токен, /metrics)
│   ├── config/                 # YAML-конфиг, дефолты, валидация, ${VAR:-default}   + тесты
│   ├── discovery/              # local.go, mdns.go, bootstrap.go, dht.go, gossip.go
│   ├── logging/                # slog + мост для stdlib-логера libp2p
│   ├── metrics/                # Prometheus (приватный registry) + выделенный слушатель
│   ├── node/                   # сборка компонентов узла, композитный skill-источник
│   ├── p2p/                    # host.go (libp2p+relay), service.go, framing.go, gater.go + тесты
│   ├── picoclaw/               # adapter.go, cli.go, ws.go, stub.go, factory.go, limits_unix.go
│   ├── routing/                # table.go: соседи, скоринг, Select()
│   ├── security/               # identity.go, signing.go, trust.go, audit.go        + тесты
│   ├── storage/                # storage.go: BadgerDB + артефакты sha256
│   ├── tasks/                  # manager.go (исполнение/делегирование/search-relay),
│   │                           # decomposition.go (план/агрегация), task.go, disk_unix.go + тесты
│   ├── version/                # сборочный штамп
│   └── wire/                   # канонические энкодеры (общие для digest и подписей) + тесты
├── configs/
│   ├── node.yaml               # шаблон демона (env-параметризован, использует install.sh)
│   └── examples/
│       ├── lan.yaml            # профиль «одна локальная сеть»  (ТЗ 22.8)
│       └── wan.yaml            # профиль «Интернет / разные хосты»
├── deploy/docker/
│   ├── Dockerfile              # многоэтапная сборка, non-root, HEALTHCHECK
│   └── docker-compose.yml      # готовая сеть из 3 узлов
├── docs/                       # ARCHITECTURE · PROTOCOLS · PICOCLAW-INTEGRATION ·
│                               # DEPLOYMENT · RUNBOOK · STATUS
├── install.sh                  # установка: локально (systemd) или Docker, N экземпляров, автостарт
├── LICENSE                     # Apache License 2.0 (полный текст)
├── ТЗ.md                       # исходное техническое задание
├── .env.example                # переменные LLM/каналов для PicoClaw
└── go.mod / go.sum
```

`Makefile` нет — команды сборки и проверки выписаны ниже напрямую.

---

## Требования

Для сборки:

- Go ≥ 1.26 (`go.mod` объявляет `go 1.26.0`);
- доступ к прокси модулей (или предварительно заполненный модульный кэш).

Для регенерации protobuf-кода (опционально — `gen/` есть в репозитории):

- `protoc` (проверено на libprotoc 3.21.12);
- `protoc-gen-go` (проверено на v1.36.6):

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
```

Для Docker-режима — Docker 24+ с compose. Для выполнения задач реальным
агентом — установленный `picoclaw`
(см. [docs/PICOCLAW-INTEGRATION.md](docs/PICOCLAW-INTEGRATION.md)). Без него
узел работает в режиме `picoclaw.mode: stub` — детерминированный
offline-исполнитель.

---

## Сборка и проверка

```bash
export PATH=$PATH:/usr/local/go/bin

go build ./...          # все пакеты (16)
go vet ./...
gofmt -l .              # код отформатирован gofmt; пустой вывод = всё в порядке
go test ./...           # юнит-тесты internal/{wire,security,config,tasks,p2p}
go build -trimpath -o bin/ ./cmd/zeptomesh-node

# Регенерация protobuf-кода (нужны protoc и protoc-gen-go в PATH):
protoc --proto_path=api/proto --go_out=gen --go_opt=paths=source_relative \
    api/proto/zeptomesh/v1/mesh.proto
```

Покрытие тестов и его границы — таблица в [docs/STATUS.md](docs/STATUS.md#тесты-покрытие-текущее).

---

## Конфигурация

Формат — YAML. Загрузчик (`config.Load`) применяет `config.Default()`, затем
раскрывает в файле `${VAR}` и `${VAR:-default}` (включая вложенные default'ы),
затем накладывает YAML и переменные окружения (`ZETOMESH_BOOTSTRAP`
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
  private_network_psk: ""       # закрытая сеть libp2p ("приватная сеть")
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
  timeout_seconds: 600
  max_concurrent_agents: 4
  extra_args: []
  prompt_template: ""           # %s = инструкция; пусто — передавать как есть
  env: {}                       # например PICOCLAW_CONFIG=/etc/picoclaw/mesh.json
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
  trust_mode: limited           # open | limited | private
  require_task_signature: true
  drop_invalid_signatures: true
  min_trust_for_tasks: limited  # trusted | known | limited | untrusted | blocked
  allowed_peers_file: ""        # base58 peer id, по одному на строку, # = комментарий
  blocked_peers_file: ""
  max_message_bytes: 4194304
  rate_limit: { requests_per_second: 20, burst: 60 }

api:
  enabled: true
  listen: 127.0.0.1:8081
  auth_token_env: ""            # имя переменной с bearer-токеном API
  read_timeout: 15s
  write_timeout: 60s

telemetry:
  prometheus_listen: ""         # напр. 127.0.0.1:9464 — выделенный /metrics + /healthz
  log_level: info
  structured_logs: true

storage:
  engine: badger
  dir: ""                       # пусто → <data_dir>/db
  max_task_records: 100000
```

---

## Протоколы

Идентификаторы потоков (`internal/p2p/protocols.go`):

| Протокол | Обмен |
|---|---|
| `/zeptomesh/task/0.1.0` | стрим: `TaskEnvelope` → `TaskAck` |
| `/zeptomesh/result/0.1.0` | стрим: `TaskResult` → `ResultAck` |
| `/zeptomesh/rpc/0.1.0` | стрим: `RpcRequest` → `RpcResponse` (ping, capabilities, peer-exchange, cancel, skill-lookup) |
| `/zeptomesh/membership/0.1.0` | pubsub/GossipSub: `MembershipGossip` |
| `/hop/…` (libp2p circuit v2) | ретрансляция соединений, если включено реле |

Фрейм: `uvarint` длина + protobuf-байты. Жёсткий протокольный потолок — 8 MiB
(`p2p.MaxFrameBytes`), дополнительно применяется `security.max_message_bytes`.

`TaskEnvelope` содержит все поля, требуемые ТЗ: `task_id`, `parent_task_id`,
`origin_peer_id`, `sender_peer_id`, `created_at`, `ttl`, `priority`,
`required_skills`, `payload`, `context_digest`, `route_stack`, `signature`,
а также `constraints` (ограничения исполнителя) и `signature_scheme`.

Схема подписей и каноническое кодирование описаны в
[docs/PROTOCOLS.md](docs/PROTOCOLS.md); общий энкодер лежит в `internal/wire`,
чтобы digest задачи и её подпись не могли разойтись. Результат несёт
дополнительно `worker_signature` — подпись, вычисляемую исполнителем по
полям, не зависящим от маршрута; ретрансляторы её не перезаписывают, и
инициатор проверяет именно её (ТЗ 11.5).

---

## Безопасность

- **Шифрование всего трафика** обеспечивают обязательные каналы libp2p
  (Noise для TCP, TLS 1.3 для QUIC). Приложений «как есть» поверх TCP нет.
- **Криптографический Peer ID** — Ed25519; публичный ключ извлекается из самого
  идентификатора (`peer.ID.ExtractPublicKey`), поэтому подпись проверяется даже
  для незнакомых пиров.
- **Закрытая сеть** — общий `private_network_psk` (libp2p PrivateNetwork):
  чужие соединения отбрасываются до установления протоколов.
- **Подпись задач и результатов**: схема `zeptomesh-task-v1` покрывает и
  `route_stack`, поэтому записанный в конверт маршрут нельзя переставить,
  сохранив валидную подпись; `zeptomesh-result-worker-v1` защищает содержимое
  результата от подмены промежуточными узлами.
- **Списки пиров**: `security.allowed_peers_file` / `blocked_peers_file`;
  запрет (`blocked`) блокирует соединение на уровне `ConnectionGater`, а не только
  задачу. `trust_mode: private` принимает задачи исключительно из allow-списка.
- **Опасные операции по умолчанию запрещены**: `capabilities.allow_shell: false`,
  а ограничения задачи (`constraints.allow_shell`, `allow_network_tools`)
  пересекаются с политикой узла — задача может только потерять права.
  Подзадача наследует пересечение ограничений родителя (`tasks.DeriveSubtask`).
  Граница это или нет — честно описано в
  [docs/PICOCLAW-INTEGRATION.md](docs/PICOCLAW-INTEGRATION.md#6-ограничения-задачи-vs-возможности-агента).
- **Rate limiting** — по ведру на пир (`security.rate_limit`), плюс дедупликация
  `task_id` на окне `tasks.dedup_window`.
- **Журнал безопасности** — append-only JSONL в `<data_dir>/audit/security.jsonl`
  (`security.OpenAudit`); события с недопустимой подписью/временем помечаются и
  не влияют на состояние таблицы соседей.

Состав уровней доверия, режимов и порядка их применения —
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#6-политика-доверия).

---

## Интеграция с PicoClaw

Граница ответственности — интерфейс `picoclaw.Adapter`:

```go
type Adapter interface {
    Name() string
    Execute(ctx context.Context, req Request) (*Response, error)
    Healthy(ctx context.Context) error
    Capabilities(ctx context.Context) ([]string, error)
    Close() error
}
```

Три реализации (`picoclaw.New` выбирает по `picoclaw.mode`):

| `mode` | Реализация | Как работает |
|---|---|---|
| `stub` | `StubAdapter` | Детерминированный offline-ответ, пишет `result.txt` в workspace задачи. Значение по умолчанию. |
| `binary` | `CLIAdapter` | Порождает `picoclaw agent -m "<инструкция>"` с изоляцией через `PICOCLAW_HOME` / `PICOCLAW_CONFIG` / `PICOCLAW_AGENTS_DEFAULTS_WORKSPACE`; разбирает stdout (снимает баннер и префикс логотипа), `exit 0` = успех. |
| `http` | `WSAdapter` | Pico Protocol: WebSocket `/pico/ws` шлюза PicoClaw (порт 18790), bearer-токен из `token_env`, подпротокол `token.<значение>`, `message.send` → `message.create`/`typing.stop`, отдельное соединение на задачу. |

Все факты об интерфейсах PicoClaw (отсутствие `picoclaw run`, отсутствие
REST-эндпоинта для задачи, только env-переменные для config/workspace)
подтверждены чтением исходного кода — в
[docs/PICOCLAW-INTEGRATION.md](docs/PICOCLAW-INTEGRATION.md) приведены пути и
фрагменты, из которых они взяты. Выдуманных команд здесь нет.

---

## Документация

| Документ | Содержание |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Компоненты узла, поток данных, маршрутизация и скоринг, политика доверия, хранилище, отказоустойчивость |
| [docs/PROTOCOLS.md](docs/PROTOCOLS.md) | Протокольные ID, фрейминг, поля `TaskEnvelope`/`TaskResult`, канонические энкодеры, схемы подписей, gossip и DHT-клавиши |
| [docs/PICOCLAW-INTEGRATION.md](docs/PICOCLAW-INTEGRATION.md) | Проверенные интерфейсы PicoClaw, режимы адаптера, изоляция, таймауты, ограничения |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Развёртывание: install.sh (локально/Docker, N экземпляров), ручная установка, порты, PSK, bootstrap, LAN/WAN-профили |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Эксплуатация: systemd/Docker, hot-reload конфига и `leave`, метрики и алерты, диагностика отказов, восстановление, бэкап ключей и БД |
| [docs/STATUS.md](docs/STATUS.md) | Соответствие ТЗ: реализовано / частично / отклонения / не реализовано, план добивания |

---

## Лицензия

Apache License 2.0 — полный текст в [`LICENSE`](LICENSE).
