# Архитектура ZeptoClaw Agent Mesh

Документ описывает фактическую реализацию `internal/`. Каждое утверждение
соответствует коду в репозитории; незавершённые части помечены и перечислены в
[STATUS.md](STATUS.md).

---

## 1. Модель сети

Сеть — множество равноправных узлов (пиров). Централизованного координатора,
реестра или общей БД нет. Допустимы **необязательные** bootstrap-узлы: они
помогают новому узлу войти в сеть, но при их полном отказе уже сформированная
mesh продолжает работать.

Топологии, которые система должна поддерживать, и механизм, который их
обеспечивает:

| Случай | Механизм обнаружения | Код |
|---|---|---|
| Несколько узлов на одном хосте | Реестр Unix-сокетов в `<data_dir>/run` | `discovery/local.go` |
| Узлы в одной локальной сети | mDNS (`_zeptomesh._udp`) | `discovery/mdns.go` |
| Узлы через Интернет | Bootstrap-адреса, Kademlia DHT, Peer Exchange | `discovery/bootstrap.go`, `dht.go` |
| Поддержание членства | GossipSub, эпидемическая рассылка `PeerState` | `discovery/gossip.go` |

Узел знает только часть соседей (`neighbors.min/target/max` = 8/16/64), что и
делает сеть одноранговой, а не «звездой».

### Роли узлов

Роли не закреплены протоколом — это настройки конкретного узла:

- **Relay-only** — `capabilities.accept_external_tasks: false`, `relay_capable: true`:
  маршрутизирует чужие задачи, сам не исполняет.
- **Worker** — объявляет специализированные навыки (`coding`, `research`, …) и
  ограниченное число параллельных задач.
- **Bootstrap** — стабильный публичный адрес, `dht_mode: server`, высокий fan-out;
  нужен только для входа новых участников.
- **Initiator** — любой узел с административным API: принимает задачу от оператора
  и выступает `origin_peer_id`.

---

## 2. Компоненты узла

```text
node (сборка и жизненный цикл — ещё не реализован)
├── config        YAML → модель, дефолты, валидация, ${ENV}
├── security      Identity(Ed25519) │ Signer/Verify │ Policy │ Limiter │ Audit
├── storage       BadgerDB (journal, peers, dedup, meta) + artifacts/sha256
├── p2p.Host      libp2p: QUIC+TCP │ Noise │ Yamux │ DHT(опц.) │ PSK(опц.)
├── p2p.Service   прикладные протоколы: task / result / rpc
├── discovery     local │ mdns │ bootstrap │ dht │ membership(gossip)
├── routing       Table (соседи, метрики, скоринг) + Select()
├── tasks         модель и инварианты (готово) + TaskManager (не реализован)
├── picoclaw      Adapter: stub │ binary (CLI) │ http (Pico WebSocket)
├── metrics       Prometheus (приватный registry)
└── api           административный HTTP/JSON (не реализован)
```

### Поток данных при приёме задачи

```text
stream /zeptomesh/task/0.1.0
  → p2p.Service.handleTask        readFrame + proto.Unmarshal
    → tasks.Validate              структура, лимиты, возраст, сверка context_digest
    → security.VerifyTask         подпись по sender_peer_id (ключ из peer ID)
    → security.Policy.AllowTasksFrom   trust_mode + списки
    → security.Limiter.Allow(peer)     token bucket
    → storage.ClaimDedup(task_id)      идемпотентность
    → routing.Table.Select(...)        если локально невозможно
      → p2p.Service.SendTask(候选)     параллельно ≤ max_parallel_candidates
  → TaskAck (подписан) обратно по тому же стриму
```

Возврат результата:

```text
исполнивший узел строит TaskResult(worker, статус, текст, digest) и подписывает
  → маршрутизируется назад по route_stack: каждый промежуточный узел
    пересылает его предыдущему хопу (PreviousHop) до origin_peer_id
  → origin сохраняет TaskResult и доставляет его инициатору
```

`route_stack` — причина, по которой подпись покрывает и маршрут: конверт нельзя
перехватить и переотправить по другому пути, сохранив валидную подпись. Каждый
ретранслятор дописывает себя в конец стека и **переподписывает** конверт от
своего имени.

---

## 3. Транспорт и протоколы

Создаётся в `p2p.New` (`internal/p2p/host.go`):

```go
libp2p.Identity(key)                        // Ed25519
libp2p.ListenAddrStrings(cfg.Node.Listen...) // tcp/4001 + udp/4001/quic-v1
libp2p.Security(noise.ID, noise.New)         // шифрование для TCP
libp2p.Muxer(yamux.ID, yamux.DefaultTransport)
libp2p.Transport(quic.NewTransport)          // основной
libp2p.Transport(tcp.NewTCPTransport)        // резервный
libp2p.DisableRelay()                        // circuit-relay отключён по умолчанию
```

Дополнительно: `AddrsFactory` (при заданном `node.announce`), `ConnectionGater`,
`PrivateNetwork(psk)`, `Routing(...)` с `dht.New(h, dht.Mode(...))`.

Транспорт выбирает libp2p: узел пробует QUIC, при недоступности UDP — TCP.
Оба канала зашифрованы и аутентифицированы одним и тем же ключом узла, поэтому
резервный транспорт не ослабляет модель безопасности.

Идентификаторы прикладных протоколов — в `p2p/protocols.go`:

```text
/zeptomesh/task/0.1.0     стрим: TaskEnvelope  → TaskAck
/zeptomesh/result/0.1.0   стрим: TaskResult    → ResultAck
/zeptomesh/rpc/0.1.0      стрим: RpcRequest    → RpcResponse
/zeptomesh/membership/0.1.0   pubsub (GossipSub): MembershipGossip
```

Фрейминг (`p2p/framing.go`): `uvarint` длина + protobuf. Потолок — меньшее из
`8 MiB` (константа `MaxFrameBytes`) и `security.max_message_bytes`. Обмен
закрыт по `SetDeadline`, отведённый стрим закрывается `CloseWrite()`, а при
ошибке — `Reset()` (defer в обработчике).

Подробности проводного формата — [PROTOCOLS.md](PROTOCOLS.md).

---

## 4. Обнаружение

### 4.1. Локальный реестр (`discovery/local.go`)

Каждый узел публикует `<run>/<peer_id>.json` (`LocalRecord`: `peer_id`,
`node_name`, `socket`, `addrs`, `skills`, `api_listen`, `started_at`, `pid`)
и обновляет файл с периодом `ttl/2` (heartbeat реестра). Читатель отбрасывает
записи, чей файл старше TTL.

Живость подтверждается **probe-сокетом** `<run>/<peer_id>.sock`
(`discovery.Probe`): клиент пишет строку `zeptomesh-probe`, узел отвечает одной
JSON-строкой. Правило `alive()`: если probe-сокет объявлен, но не отвечает —
запись считается мёртвой (упавший процесс оставляет файл сокета, и один mtime
был бы лживым). Ответ **обязан** содержать compact-JSON с полем
`"peer_id":"<тот же peer id>"` — иначе запись отбрасывается. Контракт на
содержимое ответа задаёт `Probe.SetStatus` (его реализует `internal/node`).

libp2p v0.49 не имеет unix-транспорта, поэтому реестр передаёт **loopback TCP/QUIC
адреса** узла, а сокет служит только для проверки живости и отладки.

### 4.2. mDNS (`discovery/mdns.go`)

Используется `libp2p/p2p/discovery/mdns`. Сервис регистрируется с именем из
`discovery.mdns_service_name` (значение по умолчанию `_zeptomesh._udp`; суффикс
`.local` добавляет библиотека). Обратный вызов `HandlePeerFound` уже содержит
`peer.AddrInfo` с валидным `ID` — доверять найденному пиру можно только после
рукопожатия Noise, которое libp2p выполняет при `Connect`.

### 4.3. Bootstrap (`discovery/bootstrap.go`)

Статический список multiaddr разбирается **на старте** с падением на ошибке:
опечатка в bootstrap-списке иначе осталась бы невидимой до момента, когда сеть
не сможет сформироваться. `DialAll` обходит узлы терпеливо (частичный отказ —
норма), `Run` повторяет попытки с интервалом `discovery.bootstrap_interval`,
пока `needMore()` сообщает, что соседей не хватает.

### 4.4. Kademlia DHT (`discovery/dht.go`)

Навыки публикуются как **provider records** для CID, производного от имени
навыка:

```go
key = CIDv1(Raw, sha256("zeptomesh/skill/v1/" + skill))
```

`ProvideSkills` объявляет все навыки узла и повторяет это каждые 12 минут;
`FindPeersBySkill` берёт **пересечение** множеств по списку навыков (задача
требует все сразу). При недоступности DHT вызывающий работает по таблице
соседей — деградация предусмотрена, ТЗ не требует DHT как обязательный компонент.

### 4.5. Gossip-членство (`discovery/gossip.go`)

`pubsub.NewGossipSub` + `Membership`. Каждое heartbeat-издание (`gossip.heartbeat`)
узел публикует свою `PeerState` (`peer_id`, `timestamp`, `skills`, `load`,
`max_parallel_tasks`, `version`, `status`, `addrs`); `PublishFull` раз в
`gossip.full_sync` рассылает до 64 известных состояний, чтобы новый узел быстро
получил широкий обзор.

Правила приёма (`ingest`), защищающие таблицу от отравления:

1. Источник сообщения — `msg.From` (аутентифицированный libp2p-пир), а не
   заявленный внутри `from_peer_id`.
2. Чужое состояние принимается только от пира, которому мы доверяем **маршрутизацию**
   (`Policy.AllowDelegationTo`); иначе любую таблицу можно было бы затравить.
3. Состояния применяются **монотонно**: более старая по `timestamp` запись
   игнорируется.
4. Временной допуск: не более 300 с в будущее и не старше `4 × failure_timeout`
   в прошлом; остальное — в журнал безопасности.
5. `status: "left"` и истечение `failure_timeout` удаляют пира из вида;
   колбэк `onExpire` сообщает об этом менеджеру и таблице соседей.

---

## 5. Маршрутизация и выбор исполнителя

Логика — `internal/routing/table.go`.

### Таблица соседей

`routing.Table` — потокобезопасное отображение `peer.ID → Neighbor`
(адреса, навыки, категория `local|lan|wan|bootstrap`, доверие, версия, load,
`max_parallel`/`running`, счётчики успехов/отказов/протокольных ошибок,
скользящее RTT, `LastSeen`, `Connected`, `Left`).

Каждое изменение отражается в `storage.PutPeer`, поэтому после рестарта таблица
восстанавливается (`Table.Restore`) — с гарантией `Connected = false`, так как
соединения перезапуск не переживают. `Upsert` **не понижает** доверие по одному
наблюдению и не переписывает метрики с нуля.

### Скоринг

Взвешенная сумма (константы в коде):

| Компонент | Вес | Источник |
|---|---|---|
| Совпадение навыков | 0.40 | `skillScore = matched/len(want)` |
| Доверие | 0.25 | trusted 1.0, known 0.75, limited 0.5, untrusted 0.2, blocked 0 |
| Свободная ёмкость | 0.20 | `(max-parallel-running)/max`; при неизвестном `max` — 0.5 |
| Задержка | 0.10 | 50 ms → 1.0, 2 s → 0.0, неизвестное RTT → 0.5 |
| История успехов | 0.05 | `successes/(successes+failures)`, при пустом счёте 0.5 |

Поправки и отсечки:

- узел не в сети (`!Connected`) получает множитель `×0.85` — набор соединения
  стоит round-trip;
- кандидаты без полного покрытия `required_skills` **исключаются** (не
  занижаются, а именно не рассматриваются);
- исключаются `self`, пиры с `Left`, явно переданные в `exclude` уже отказавшие,
  и все, кому запрещает `Policy.AllowDelegationTo`;
- сортировка по убыванию score, при равенстве — по строке peer id (детерминизм);
- срез по `fanout` (≤ `tasks.forwarding.max_fanout`).

Оценка `load` соседа приходит из gossip, а не вычисляется локально, поэтому
узлы не обязаны раскрывать друг другу содержимое задач.

### Ограничения делегирования (из ТЗ)

| Ограничение | Реализация |
|---|---|
| TTL | `ttl-1` при каждом хопе; `ttl ≤ 0` → `ErrTTLExhausted` (`tasks.CheckRoute`) |
| Нет петель | `route_stack` содержит самопроверку: повторный визит → `ErrRouteLoop` |
| Максимум параллельных задач | `tasks.max_parallel_tasks` + семафор `picoclaw.max_concurrent_agents` |
| Лимит фан-аута | `tasks.forwarding.max_fanout`, плюс `max_parallel_candidates` на параллельные попытки |
| Дедупликация `task_id` | `storage.ClaimDedup` на окне `tasks.dedup_window` (по умолчанию 15m) |
| Rate limiting | `security.Limiter`: ведро на пир (`requests_per_second`, `burst`) |
| Ограничения подзадачи | `tasks.DeriveSubtask`: пересечение прав родителя, `ttl ≤ parent.ttl-1` |

---

## 6. Политика доверия

`internal/security/trust.go`. Уровни упорядочены от привилегированного к
заблокированному: `trusted → known → limited → untrusted → blocked`;
`AtLeast` означает «не ниже запрошенного».

Режим (`security.trust_mode`):

- **open** — задачи принимаются от любого аутентифицированного пира;
- **limited** (по умолчанию) — порог `min_trust_for_tasks` (по умолчанию `limited`);
- **private** — только явный allow-список; всё остальное трактуется как `blocked`.

Разделение решений:

| Вопрос | Метод | Что блокирует |
|---|---|---|
| Соединяться ли вообще | `AllowConnection` | `blocked` (используется `ConnectionGater`) |
| Принимать ли задачу | `AllowTasksFrom` | уровень доверия/списки |
| Отдать ли работу | `AllowDelegationTo` | `blocked` **и** `untrusted` |

Операторские списки (`allowed_peers_file` / `blocked_peers_file`, base58 peer id,
`#` — комментарий) имеют приоритет над наблюдаемым значением: `blocked`
переопределяет `allow`, а запись в allow-списке поднимает доверие минимум до
`known` (в `private` — до `trusted`). `Policy.Observe` не понижает доверие по
одиночному наблюдению — это защита от временных сбоев сети, которые иначе
выглядели бы как враждебное поведение.

Подписи (`internal/security/signing.go`): схемы `zeptomesh-task-v1`,
`-result-v1`, `-ack-v1`, `-cancel-v1`, `-caps-v1`. Digest вычисляется как
`sha256("zeptomesh/" ‖ scheme ‖ 0x00 ‖ canonical_body)`; каноническое тело —
`internal/wire` (length-prefixed, поле-за-полем), поэтому байты digest задачи и
её подписи не могут разойтись: и то, и другое кодируется одним энкодером.

Проверка ключа идёт через `peer.ID.ExtractPublicKey()` (Ed25519 самоаттестуем),
с фолбэком на lookup по peerstore для не-Ed25519 ключей.

Журнал (`security/audit.go`) — append-only JSONL
(`<data_dir>/audit/security.jsonl`, 0600). Событие не может «уронить» вызывающий
код: незаписываемый журнал уходит в slog, и наоборот, запись журнала не влияет
на состояние таблицы соседей.

---

## 7. Хранилище

`internal/storage` — BadgerDB v4, **только node-local**. Прикладные ключи
(префиксы в коде):

| Префикс | Значение |
|---|---|
| `t:<task_id>` | `TaskRecord` — журнал: статус, timestamps, `worker_peer_id`, `delegated_to`, попытки |
| `r:<task_id>` | `ResultRecord` — итог: текст, ошибка, артефакты, подпись, digest |
| `p:<peer_id>` | `PeerRecord` — восстановимая таблица соседей |
| `d:<task_id>` | `DedupRecord` — идемпотентность доставки |
| `s:<skill>/<peer>` | вторичный индекс навыков |
| `c:<parent>/<child>` | рёбра «родитель → подзадачи» |
| `o:<child>` | обратное ребро к родителю |
| `m:<key>` | произвольные значения узла |

Артефакты хранятся отдельно от LSM-дерева, content-addressed:
`<data_dir>/artifacts/sha256/<первые 2 hex>/<64 hex>`, запись через tmp+rename.
В межузловых сообщениях передаётся **только** ссылка `sha256:<hex>` + размер:
большие тела не идут через роутинг-слой, что ограничивает и стоимость, и
поверхность отказа.

`Store.Prune` удаляет завершённые записи старше `tasks.retention` (7 дней) и
просроченные dedup-ключи. Незавершённые задачи не удаляются — их судьбу решает
механизм таймаутов менеджера.

---

## 8. Отказоустойчивость

Как система ведёт себя при сбоях (механизмы, уже присутствующие в коде):

| Отказ | Реакция |
|---|---|
| Падение узла-исполнителя | gossip перестаёт получать его `PeerState` → по истечении `failure_timeout` пир выпадает из вида; `onExpire` + `Table.PruneStale` удаляют его из таблицы, задачи перемаршрутизируются |
| Обрыв канала к соседу | `Connected=false`, штраф ×0.85 в скоринге; RTT-метрика больше не обновляется |
| Задача «зависла» у соседа | `forwarding.attempt_timeout` + `max_retries` + `retry_interval_seconds` |
| Двойная доставка одного `task_id` | `ClaimDedup` → `ACK DUPLICATE`, повторного исполнения нет |
| Цикл маршрута | `ErrRouteLoop` по `route_stack` |
| Удалённый узел с мусорным трафиком | `Limiter`, лимиты фрейма, `ProtoErrs`-счётчик (основание для понижения доверия) |
| Рестарт узла | ключ и таблица соседей восстанавливаются с диска; соединения — нет |
| Отказ всех bootstrap-узлов | не влияет: mesh живёт на gossip + DHT + PEX |
| Отказ DHT | деградация к таблице соседей и ручным спискам |

Возврат результата устойчив к перезапуску промежуточного узла лишь частично:
`route_stack` перезаписывается отправителем, поэтому для надёжного
восстановления результата нужен `parent_task_id`-ориентированный relay (см.
«Дальнейшее развитие» в STATUS.md).

---

## 9. Наблюдаемость

- `internal/metrics` — Prometheus-колекторы на **приватном** registry
  (`metrics.New("")`), чтобы один тестовый бинарь мог поднять несколько узлов.
  Покрывает: размеры таблицы соседей, счётчики задач по исходам, гистограммы
  длительности и числа хопов, счётчики обнаружения, гонки байтов, состояние
  адаптера PicoClaw, security-события по типам.
- `p2p.MetricsRecorder` реализован `metrics.Collector` и подключается через
  `CountedWriter`/`CountedReader` во фрейминге.
- `internal/logging` — `log/slog` (JSON по умолчанию) и `BridgeStdlib`, который
  направляет stdlib-логер (его используют libp2p и BadgerDB) в тот же поток.

---

## 10. Границы и допущения

- **Масштабирование до 100+ узлов** обеспечивается gossip-видом (не полный
  обход), ограничением таблицы и DHT-индексом навыков. Меземберство
  эпидемическое, не строгое: возможны короткие расхождения видов между узлами.
- **Точность `load`** зависит от добросовестности соседа: скоринг — эвристика, а
  не гарантия. Обязательную часть ограничений исполнителя задаёт локальная
  политика узла, а не заявление соседа.
- **Часы** узлы не синхронизируются; допуски (300 с в будущее, `MaxAge` 24 ч) —
  компромисс между replay-защитой и терпимостью к дрейфу.
- **PSK** — симметричная секретная сеть: отсекает чужие узлы, но не заменяет
  per-peer доверие (все внутри PSK имеют один ключ).
- **Circuit relay выключён** (`libp2p.DisableRelay()`): узлы за строгим NAT без
  прямого пути друг к другу не соединятся. Для продакшена с таким NAT нужны
  публичные адреса/Port-Mapping или явно включённый relay.
