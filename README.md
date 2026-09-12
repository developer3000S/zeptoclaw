# ZeptoClaw Agent Mesh (`zeptomesh`)

Полностью децентрализованная P2P-сеть автономных AI-агентов на базе
[PicoClaw](https://github.com/sipeed/picoclaw). Узлы образуют сеть без
обязательного центрального координатора и без общей базы данных: задача может
поступить на любой узел, быть выполнена локально или рекурсивно делегирована
соседям, а результат возвращается инициатору по пройденному маршруту.

Реализовано по мотивам технического задания [`ТЗ.md`](ТЗ.md)
(«PicoClaw Agent Mesh», версия 0.1).

> **Статус: стадия разработки.** Сетевой стек, обнаружение, маршрутизация,
> безопасность, хранилище и адаптеры PicoClaw реализованы и компилируются.
> Точка входа `cmd/zeptomesh-node`, менеджер задач (`internal/tasks/manager.go`),
> административный API (`internal/api`) и контейнерная сборка **ещё не написаны**
> — запуск бинарного файла пока невозможен.
> Полная картина: [docs/STATUS.md](docs/STATUS.md).

---

## Содержание

- [Ключевые свойства](#ключевые-свойства)
- [Архитектура](#архитектура)
- [Структура репозитория](#структура-репозитория)
- [Требования](#требования)
- [Сборка и проверка](#сборка-и-проверка)
- [Конфигурация](#конфигурация)
- [Протоколы](#протоколы)
- [Безопасность](#безопасность)
- [Интеграция с PicoClaw](#интеграция-с-picoclaw)
- [Планируемый запуск (не реализован)](#планируемый-запуск-не-реализован)
- [Что реализовано и что нет](#что-реализовано-и-что-нет)
- [Документация](#документация)

---

## Ключевые свойства

| Область | Реализация |
|---|---|
| Язык сетевого слоя | Go 1.26 |
| Идентификация узла | Ed25519, самоаттестуемый libp2p `Peer ID` |
| Транспорт | QUIC (основной) + TCP (резервный) |
| Шифрование | Noise (`tls-noise` канал libp2p); для QUIC — TLS 1.3 внутри QUIC |
| Мультиплексирование | Yamux (поверх TCP) |
| Формат сообщений | Protocol Buffers v3, length-prefixed фреймы |
| Обнаружение (хост) | Реестр Unix-сокетов в `<data_dir>/run` |
| Обнаружение (LAN) | mDNS (`_zeptomesh._udp`) |
| Обнаружение (WAN) | Bootstrap-узлы, Kademlia DHT, Peer Exchange |
| Членство | GossipSub (эпидемическая рассылка `PeerState`) |
| Хранилище | BadgerDB v4 (node-local) + content-addressed артефакты на диске |
| Делегирование | TTL, маршрут без петель, дедупликация, лимиты фан-аута и параллелизма, rate limiting |
| Подписи | `TaskEnvelope`, `TaskResult`, `TaskAck`, `CancelRequest`, `Capabilities` |
| Доступ | Списки разрешённых/запрещённых пиров, уровни доверия, журнал безопасности (JSONL) |
| Исполнитель | PicoClaw через адаптер: CLI-процесс / WebSocket `/pico/ws` / offline-stub |

---

## Архитектура

Один узел = mesh-демон, локальный PicoClaw и node-local хранилище. Центральных
компонентов нет; `bootstrap.*` — необязательные точки входа в сеть.

```text
                     ┌──────────────────────── ZeptoClaw node ────────────────────────┐
   HTTP/JSON         │                                                                │
  (в планах)  ─────► │  internal/api          административный интерфейс              │
                     │        │                                                       │
                     │        ▼                                                       │
                     │  internal/tasks        TaskManager: приём → валидация →        │
                     │        │               выполнить | делегировать → агрегировать │
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
                                     другие узлы mesh (≥ 100)
```

Жизненный цикл задачи:

```text
любой узел принимает задачу
   │
   ├─ подпись/доджест ✗ ─────────────────────────────► REJECTED + audit
   ├─ task_id в dedup-окне ──────────────────────────► ACK DUPLICATE (без повторного исполнения)
   ├─ навыки подходят и есть слот ───────────────────► ACCEPTED → RUNNING → COMPLETED
   │                                                        └─ результат подписывается
   │                                                             и едет назад по route_stack
   └─ иначе ─────────────────────────────────────────► FORWARDING:
                                                          скоринг соседей → ≤ max_fanout
                                                          кандидатов, ≤ max_parallel_attempts
                                                          параллельно, ttl-1
                                                              └─ все отказались/TTL=0 → FAILED
```

Подробнее: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Структура репозитория

```text
zeptoclaw/
├── api/
│   └── proto/zeptomesh/v1/
│       └── mesh.proto            # контракт: TaskEnvelope, TaskResult, Capabilities, RPC
├── gen/
│   └── zeptomesh/v1/
│       └── mesh.pb.go            # генерируется (protoc-gen-go), в VCS для `go build` без protoc
├── internal/
│   ├── config/                   # YAML-конфиг, дефолты, валидация, подстановка ${ENV}
│   ├── discovery/                # local.go, mdns.go, bootstrap.go, dht.go, gossip.go, probe-сокет
│   ├── logging/                  # slog + мост для stdlib-логера libp2p
│   ├── metrics/                  # Prometheus-коллекторы (приватный registry)
│   ├── p2p/                      # host.go (libp2p), service.go (протоколы), framing.go, protocols.go
│   ├── picoclaw/                 # adapter.go (интерфейс), cli.go, ws.go, stub.go, factory.go
│   ├── routing/                  # table.go: соседи, скоринг, Select()
│   ├── security/                 # identity.go, signing.go, trust.go, audit.go
│   ├── storage/                  # storage.go: BadgerDB + артефакты
│   ├── tasks/                    # task.go: модель, статусы, валидация, digests
│   ├── version/                  # сборочный штамп
│   └── wire/                     # канонические энкодеры (общие для digest и подписей)
├── docs/                         # архитектура, протоколы, интеграция, статус
├── ТЗ.md                         # исходное техническое задание
├── .env.example                  # переменные LLM/каналов для PicoClaw
├── go.mod / go.sum
└── README.md
```

Каталогов `cmd/`, `configs/`, `deploy/`, `test/`, а также `Makefile` и файлов
тестов **пока нет** — они входят в незавершённые этапы плана
([docs/STATUS.md](docs/STATUS.md)).

---

## Требования

Для сборки библиотечных пакетов:

- Go ≥ 1.26 (`go.mod` объявляет `go 1.26.0`).
- Доступ к прокси модулей (или предварительно заполненный модульный кэш).

Для регенерации protobuf-кода (опционально — `gen/` есть в репозитории):

- `protoc` (проверено на libprotoc 3.21.12);
- `protoc-gen-go` (проверено на v1.36.6):

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
```

Для выполнения задач реальным агентом — установленный `picoclaw`
(см. [docs/PICOCLAW-INTEGRATION.md](docs/PICOCLAW-INTEGRATION.md)). Без него
узел работает в режиме `picoclaw.mode: stub` — детерминированный offline-исполнитель.

---

## Сборка и проверка

```bash
# Все пакеты (13 шт.) компилируются:
go build ./...

# Статическая проверка:
go vet ./...

# Форматирование (код отформатирован gofmt; пустой вывод = всё в порядке):
gofmt -l internal/ gen/

# Тесты: на момент этой версии тестовых файлов в репозитории нет,
# поэтому команда завершится без ошибок, но и без проверок:
go test ./...

# Регенерация protobuf-кода (нужны protoc и protoc-gen-go в PATH):
protoc --proto_path=api/proto --go_out=gen --go_opt=paths=source_relative \
    api/proto/zeptomesh/v1/mesh.proto
```

---

## Конфигурация

Формат — YAML. Загрузчик (`config.Load`) сначала применяет `config.Default()`,
затем распаковывает поверх файл, предварительно раскрыв `${VAR}` через
`os.ExpandEnv` — секреты не хранятся в файле конфигурации. После этого
выполняется `Validate()`, которая дополнительно выводит относительные пути из
`node.data_dir`.

Корневые секции и значимые значения по умолчанию:

```yaml
node:
  name: zeptomesh-node
  data_dir: ./zeptomesh-data
  listen:                       # QUIC — основной, TCP — резервный
    - /ip4/0.0.0.0/tcp/4001
    - /ip4/0.0.0.0/udp/4001/quic-v1
  announce: []                  # фиксированные публичные адреса (NAT, несколько интерфейсов)
  private_network_psk: ""       # /1/<base32>: изоляция mesh ("приватная сеть")

identity:
  key_file: ""                  # пусто → <data_dir>/keys/peer.key (Ed25519, 0600)

discovery:
  local_registry: true
  local_socket_dir: ""          # пусто → <data_dir>/run
  mdns: true
  mdns_service_name: _zeptomesh._udp   # домен .local добавляет библиотека
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
  default_timeout_seconds: 600
  max_task_payload_bytes: 1048576
  max_artifact_bytes: 104857600
  dedup_window: 15m
  retention: 168h
  forwarding:
    max_fanout: 5
    max_parallel_candidates: 3  # <= max_fanout
    max_retries: 2
    attempt_timeout: 120s
    retry_interval_seconds: 5s

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
  http:
    base_url: http://127.0.0.1:18800   # в ws://…/pico/ws конвертируется адаптером
    path: /api/v1/tasks
    health_path: /health
    token_env: ""               # имя переменной окружения с pico-токеном
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
  auth_token_env: ""
  read_timeout: 15s
  write_timeout: 60s

telemetry:
  prometheus_listen: ""
  log_level: info
  structured_logs: true

storage:
  engine: badger
  dir: ""                       # пусто → <data_dir>/db
  max_task_records: 100000
```

Замечания:

- `api.*` описан в модели конфигурации, но HTTP-сервер (`internal/api`) ещё не
  реализован — секция вступает в силу вместе с этим этапом.
- В `picoclaw.http` значения `base_url`/`path` соответствуют формату HTTP-адаптера
  из ТЗ; фактическая реализация поверх PicoClaw — WebSocket `/pico/ws`
  (у шлюза PicoClaw нет REST-эндпоинта для постановки задачи), см.
  [docs/PICOCLAW-INTEGRATION.md](docs/PICOCLAW-INTEGRATION.md).
- Каталог с примером `configs/config.yaml` пока отсутствует: файл появится вместе
  с `cmd/` и `Makefile`.

---

## Протоколы

Идентификаторы потоков (`internal/p2p/protocols.go`):

| Протокол | Обмен |
|---|---|
| `/zeptomesh/task/0.1.0` | стрим: `TaskEnvelope` → `TaskAck` |
| `/zeptomesh/result/0.1.0` | стрим: `TaskResult` → `ResultAck` |
| `/zeptomesh/rpc/0.1.0` | стрим: `RpcRequest` → `RpcResponse` (ping, capabilities, peer-exchange, cancel, skill-lookup) |
| `/zeptomesh/membership/0.1.0` | pubsub/GossipSub: `MembershipGossip` |

Фрейм: `uvarint` длина + protobuf-байты. Жёсткий протокольный потолок — 8 MiB
(`p2p.MaxFrameBytes`), дополнительно применяется `security.max_message_bytes`.

`TaskEnvelope` содержит все поля, требуемые ТЗ: `task_id`, `parent_task_id`,
`origin_peer_id`, `sender_peer_id`, `created_at`, `ttl`, `priority`,
`required_skills`, `payload`, `context_digest`, `route_stack`, `signature`,
а также `constraints` (ограничения исполнителя) и `signature_scheme`.

Схема подписей и каноническое кодирование описаны в
[docs/PROTOCOLS.md](docs/PROTOCOLS.md); общий энкодер лежит в `internal/wire`,
чтобы digest задачи и её подпись не могли разойтись.

---

## Безопасность

- **Шифрование всего трафика** обеспечивают обязательные каналы libp2p
  (Noise для TCP, TLS 1.3 для QUIC). Приложений «как есть» поверх TCP нет.
- **Криптографический Peer ID** — Ed25519; публичный ключ извлекается из самого
  идентификатора (`peer.ID.ExtractPublicKey`), поэтому подпись проверяется даже
  для незнакомых пиров.
- **Подпись задач и результатов**: схема `zeptomesh-task-v1` покрывает и
  `route_stack`, поэтому записанный в конверт маршрут нельзя переставить,
  сохранив валидную подпись.
- **Списки пиров**: `security.allowed_peers_file` / `blocked_peers_file`;
  запрет (`blocked`) блокирует соединение на уровне `ConnectionGater`, а не только
  задачу. `trust_mode: private` принимает задачи исключительно из allow-списка.
- **Опасные операции по умолчанию запрещены**: `capabilities.allow_shell: false`,
  а ограничения задачи (`constraints.allow_shell`, `allow_network_tools`)
  пересекаются с политикой узла — задача может только потерять права.
  Подзадача наследует пересечение ограничений родителя
  (`tasks.DeriveSubtask`).
- **Rate limiting** — по ведру на пир (`security.rate_limit`), плюс дедупликация
  `task_id` на окне `tasks.dedup_window`.
- **Журнал безопасности** — append-only JSONL в `<data_dir>/audit/security.jsonl`
  (`security.OpenAudit`); события с недопустимой подписью/временем помечаются и
  не влияют на состояние таблицы соседей.

Состав уровней доверия, режимов и порядка их применения —
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#политика-доверия).

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
| `binary` | `CLIAdapter` | Порождает `picoclaw agent -m "<инструкция>"` с изоляцией через `PICOCLAW_HOME` / `PICOCLAW_CONFIG` / `PICOCLAW_AGENTS_DEFAULTS_WORKSPACE`; разбирает stdout (снимает баннер и префикс `🦞`), `exit 0` = успех. |
| `http` | `WSAdapter` | Pico Protocol: WebSocket `/pico/ws` шлюза PicoClaw, bearer-токен, `message.send` → `message.create`/`typing.stop`. |

Все три факта об интерфейсах PicoClaw (отсутствие `picoclaw run`, отсутствие
REST-эндпоинта для задачи, только env-переменные для config/workspace)
подтверждены чтением исходного кода — в
[docs/PICOCLAW-INTEGRATION.md](docs/PICOCLAW-INTEGRATION.md) приведены пути и
фрагменты, из которых они взяты. Выдуманных команд здесь нет.

---

## Планируемый запуск (не реализован)

Раздел описывает целевой UX. **Команды ниже пока не работают**: `cmd/`,
`Makefile`, `configs/` и `deploy/` — следующие в плане этапа 2/9. Не выполняйте
их как инструкцию.

```bash
# 1. Собрать демон (этап 1–2)
make build                      # → bin/zeptomesh-node

# 2. Запустить первый узел (bootstrap/relay)
./bin/zeptomesh-node -config configs/node-a.yaml

# 3. Запустить второй узел, указав первый как bootstrap
./bin/zeptomesh-node -config configs/node-b.yaml

# 4. Проверить статус и соседей (админ API, этап 9)
curl -s localhost:8081/api/v1/status
curl -s localhost:8081/api/v1/peers

# 5. Отправить задачу
curl -s -X POST localhost:8081/api/v1/tasks -d '{
  "instruction": "…", "required_skills": ["coding"], "ttl": 5, "priority": 5
}'

# 6. Статус/результат задачи
curl -s localhost:8081/api/v1/tasks/<task_id>
```

Рабочая проверка двух узлов появится одновременно с тестами
(этап 8) в `test/` и `docs/RUNBOOK.md`.

---

## Что реализовано и что нет

| Компонент | Состояние |
|---|---|
| `api/proto` + `gen` (27 сообщений/enum, 15 типов Go) | готово, regeneration воспроизводим |
| `internal/config` | готово: загрузка, дефолты, валидация |
| `internal/wire`, `internal/security` | готово: идентичность, 5 схем подписи, trust, limiter, audit |
| `internal/storage` | готово: BadgerDB (journal/peers/dedup/meta), артефакты sha256, prune |
| `internal/p2p` | готово: host (QUIC+TCP, Noise, Yamux, DHT, PSK), фрейминг, `Service` |
| `internal/discovery` | готово: local registry + probe-сокет, mDNS, bootstrap, DHT, GossipSub |
| `internal/routing` | готово: таблица соседей, скоринг, `Select()` |
| `internal/tasks` | **модель готова** (валидация, digests, TTL, маршрут, подзадачи); `TaskManager` — нет |
| `internal/picoclaw` | готово: `stub`, `binary`, `http` (Pico WS) |
| `internal/metrics`, `internal/logging`, `internal/version` | готовы |
| `internal/node` (сборка компонентов) | нет |
| `internal/api` (админ HTTP/JSON) | нет |
| `cmd/zeptomesh-node` | нет |
| `Makefile`, `configs/`, `deploy/docker`, `test/` | нет |
| Тесты (`*_test.go`) | нет |

Полный план по этапам с критериями готовности: [docs/STATUS.md](docs/STATUS.md).

---

## Документация

| Документ | Содержание |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Компоненты узла, поток данных, маршрутизация и скоринг, политика доверия, хранилище, отказоустойчивость |
| [docs/PROTOCOLS.md](docs/PROTOCOLS.md) | Протокольные ID, фрейминг, поля `TaskEnvelope`/`TaskResult`, канонические энкодеры, схемы подписей, gossip и DHT-клавиши |
| [docs/PICOCLAW-INTEGRATION.md](docs/PICOCLAW-INTEGRATION.md) | Проверенные интерфейсы PicoClaw, режимы адаптера, изоляция, таймауты, ограничения |
| [docs/STATUS.md](docs/STATUS.md) | Статус по этапам, что не сделано, открытые вопросы, известные отклонения от ТЗ |

---

## Лицензия

В заголовках исходных файлов указан Apache License 2.0, файла `LICENSE` в
корне репозитория пока нет — см. [docs/STATUS.md](docs/STATUS.md#открытые-вопросы).
