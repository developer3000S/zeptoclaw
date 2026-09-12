# Протоколы ZeptoClaw Agent Mesh

Источник контракта — [`api/proto/zeptomesh/v1/mesh.proto`](../api/proto/zeptomesh/v1/mesh.proto)
(27 объявлений `message`/`enum`). Генерируемый код — `gen/zeptomesh/v1/mesh.pb.go`
(Go-пакет `zeptomeshv1`), сгенерирован `protoc 3.21.12` + `protoc-gen-go v1.36.6`.

Правила совместимости: переиспользованные номера полей не меняются,
несовместимое изменение требует нового суффикса версии в идентификаторе
протокола (`/zeptomesh/task/0.2.0`). `internal/version.ProtocolVersion`
дублирует текущую версию для диагностики.

---

## 1. Транспортные примитивы

### 1.1. Идентификаторы протоколов

| ID | Тип | Обмен | Реализация |
|---|---|---|---|
| `/zeptomesh/task/0.1.0` | stream | `TaskEnvelope` → `TaskAck` | `p2p.Service.handleTask` / `SendTask` |
| `/zeptomesh/result/0.1.0` | stream | `TaskResult` → `ResultAck` | `handleResult` / `SendResult` |
| `/zeptomesh/rpc/0.1.0` | stream | `RpcRequest` → `RpcResponse` | `handleRPC` / `RPC` |
| `/zeptomesh/membership/0.1.0` | pubsub | `MembershipGossip` | `discovery.Membership` |

### 1.2. Фрейм

```text
┌────────────────┬──────────────────────────────┐
│ uvarint(len)   │ protobuf-байты сообщения     │
└────────────────┴──────────────────────────────┘
```

Код: `internal/p2p/framing.go`.

- `MaxFrameBytes = 8 << 20` — жёсткий протокольный потолок, не настраивается.
- `security.max_message_bytes` (по умолчанию 4 MiB) — настраиваемый потолок,
  применяется как `limit` при чтении.
- Фрейм нулевой длины читается как «пустое сообщение» и трактуется вызывающим
  кодом как `io.EOF`.
- Заголовок читается побайтово (`byteReader`), чтобы не буферизовать поток.

### 1.3. Семантика стрима «запрос–ответ»

Приём (`Service.inbound`-логика в `handleTask`/`handleResult`/`handleRPC`):

1. `st.Reset()` по `defer` — поток не остаётся открытым при любой ветке.
2. Один запрос → один ответ → `CloseWrite()`.
3. Дедлайн `streamTimeout` (по умолчанию 30 с, `SetStreamTimeout`).
4. Ошибка обработчика логируется и ответ **не** отправляется: инициатор
   дождётся дедлайна и учтёт это как отказ (ретрансляция по `max_retries`).

Исходящий вызов (`Service.exchange`) сначала гарантирует соединение
(`host.Connect`), затем открывает поток и ставит `SetDeadline` на весь обмен.

---

## 2. Структуры данных

### 2.1. `TaskEnvelope` — единица работы

Поля, обязательные по ТЗ, и их назначение:

| Поле | Тип | Назначение |
|---|---|---|
| `task_id` | string | глобально уникальный id (UUIDv7, `tasks.NewID`) |
| `parent_task_id` | string | пусто для корневой задачи |
| `origin_peer_id` | string | b58str узла, впустившего задачу в сеть |
| `sender_peer_id` | string | b58str узла, отправившего **этот** конверт |
| `created_at` | int64 | unix-секунды, устанавливает origin |
| `ttl` | int32 | оставшиеся хопы делегирования |
| `priority` | int32 | 1 (низкий) … 9 (высокий) |
| `required_skills` | repeated string | требования к исполнителю |
| `payload` | `TaskPayload` | инструкция, digest контекста, вложения, метки |
| `context_digest` | string | `sha256:<hex>` по каноническому содержимому |
| `route_stack` | repeated string | посещённые узлы, для возврата результата |
| `signature` | bytes | ed25519 по `wire.TaskBody` |
| `signature_scheme` | string | `zeptomesh-task-v1` |
| `constraints` | `TaskConstraints` | разрешения исполнителю (см. ниже) |

`TaskConstraints` — `max_duration_seconds`, `allow_network_tools`, `allow_shell`,
`allow_delegation`, `allow_subtasks`. **Инвариант монотонности прав:** узел
пересекает ограничения задачи со своей политикой, подзадача наследует
пересечение родителя (`tasks.DeriveSubtask`) — задача может только потерять
права, никогда не получить новые.

`TaskPayload` содержит `instruction`, `context_digest`, `attachments`
(`ArtifactRef{name, hash, size, media_type}`) и `labels`. Большие тела в
конверт не кладутся: передаётся только content-addressed ссылка.

### 2.2. `TaskAck` — немедленный вердикт

`task_id`, `status` (`ACK_STATUS_QUEUED` — принято на исполнение,
`_FORWARDED` — принято на маршрутизацию, `_REJECTED` + `reason`,
`_DUPLICATE` — сработала дедупликация), `accepted_by`, `timestamp`, `signature`.

`TaskAck` подписывается (`zeptomesh-ack-v1`), чтобы отказ нельзя было
сфабриковать от имени другого узла при разборе инцидентов.

### 2.3. `TaskResult` — подписанный итог

`task_id`, `worker_peer_id` (тот, кто реально исполнил), `status`
(`TaskStatus`), `text` (с ограничением по размеру), `result_digest`
(sha256 по тексту и артефактам), `error_message`, `started_at`, `finished_at`,
`artifacts`, `sender_peer_id`, `signature` + `signature_scheme`
(`zeptomesh-result-v1`), `route_stack` (снимок маршрута на момент завершения).

Результат движется назад по **обратному** `route_stack`; каждый промежуточный
узел подтверждает приём (`ResultAck{accepted, reason}`) и пересылает предыдущему
хобу. Промежуточный узел меняет только `sender_peer_id` и переподписывает
результат — исходный `worker_peer_id` и `signature` исполнителя сохраняются как
доказательство авторства.

### 2.4. `TaskStatus`

```text
RECEIVED VALIDATING EVALUATING ACCEPTED REJECTED FORWARDED RUNNING
WAITING_SUBTASKS AGGREGATING COMPLETED FAILED TIMEOUT CANCELED
```

Go-зеркало с предикатом «терминальности» — `tasks.Status`:
терминальные состояния `COMPLETED, FAILED, TIMEOUT, CANCELED, REJECTED`
(`Status.Terminal()`). Маппинг туда/обратно — `Status.ToProto` /
`tasks.StatusFromProto`.

### 2.5. Управляющий RPC

`RpcRequest`/`RpcResponse` — `oneof` из пяти вариантов:

| Вариант | Запрос | Ответ | Назначение |
|---|---|---|---|
| `ping` | `PingRequest{nonce}` | `PingResponse{nonce, peer_time}` | RTT и сверка часов |
| `capabilities` | `CapabilitiesRequest{}` | `CapabilitiesResponse{Capabilities}` |Self-описание узла |
| `peer_exchange` | `PeerExchangeRequest{count}` | `PeerExchangeResponse{PeerRecord[]}` | PEX: расширение вида нового узла |
| `cancel` | `CancelRequest{task_id, origin, sender, reason, signature}` | `CancelResponse{accepted, reason}` | отзыв задачи (подпись обязательна) |
| `skill_lookup` | `SkillLookupRequest{skills}` | `SkillLookupResponse{PeerRecord[]}` | точечный запрос «у кого есть навыки» |

`Capabilities` (подписывается, `zeptomesh-caps-v1`) несёт `peer_id`, `node_name`,
`version`, `skills`, `models`, `max_parallel_tasks`, `running_tasks`, `load`,
`accept_external_tasks`, `allow_shell`, `relay_capable`, `resource_class`,
`listen_addrs`, `timestamp`.

---

## 3. Каноническое кодирование и подписи

### 3.1. Почему не «сырой protobuf»

Сериализация protobuf не канонична (порядок полей в map, кодирование неизвестных
полей), поэтому подпись «по маршалу» небезопасна. Все тела кодирует
`internal/wire` length-prefixed побайтово, в фиксированном порядке полей:

```text
Digest(scheme, body) = sha256( "zeptomesh/" ‖ scheme ‖ 0x00 ‖ body )
```

Разделитель домена в начале делает невозможным коллизию digest между схемами.

Энкодеры: `wire.TaskContent`, `wire.TaskBody`, `wire.ResultBody`,
`wire.CancelBody`, `wire.CapsBody`, `wire.AckBody`.

`TaskContent` кодирует **авторскую** часть (id, parent, origin, created_at,
priority, required_skills, payload со вложениями и метками, constraints).
`TaskBody` = `TaskContent` ‖ `sender_peer_id` ‖ `ttl` ‖ `route_stack`.

Такое разделение принципиально: `context_digest` считается по `TaskContent` и
не зависит от меняющихся в пути полей, поэтому **любой** узел на маршруте может
пересчитать и проверить, что инструкцию и ограничения никто не подменил
(`tasks.VerifyDigest`). А подпись покрывает и маршрут, поэтому переставить
`route_stack`, сохранив валидную подпись, нельзя.

### 3.2. Схемы

| Схема | Покрывает | Подписант | Проверка |
|---|---|---|---|
| `zeptomesh-task-v1` | `TaskBody` | `sender_peer_id` | `security.VerifyTask` |
| `zeptomesh-result-v1` | `ResultBody` | `sender_peer_id` | `VerifyResult` |
| `zeptomesh-ack-v1` | `AckBody` | `accepted_by` | `VerifyAck` |
| `zeptomesh-cancel-v1` | `CancelBody` | `sender_peer_id` | `VerifyCancel` |
| `zeptomesh-caps-v1` | `CapsBody` | `peer_id` | `VerifyCaps` |

Пустое поле `signature_scheme` трактуется как ожидаемая схема (совместимость с
ранними сборками); неизвестная схема — ошибка, а не «пропустить».

### 3.3. Правила приёма конверта (нормативные)

`TaskManager` (этап 6) обязан применить проверки именно в этом порядке —
дешёвые и отбрасывающие раньше дорогих:

1. размер фрейма ≤ `security.max_message_bytes`;
2. `tasks.Validate(env, max_payload, now)` — структура, лимиты, возраст,
   сверка `context_digest`;
3. `security.VerifyTask` — подпись против `sender_peer_id` (ключ из id);
4. `tasks.CheckRoute(env, self)` — `ttl > 0` и отсутствие `self` в `route_stack`;
5. `Policy.AllowTasksFrom(sender)` и `Limiter.Allow(sender)`;
6. `storage.ClaimDedup(task_id, dedup_window)` — иначе `ACK DUPLICATE`;
7. пересечение `constraints` с локальной политикой;
8. решение «исполнить / делегировать» по `routing.Table.Select`.

Отрицательный исход любого шага ≥ 3 пишется в журнал безопасности и увеличивает
счётчик протокольных ошибок пира.

### 3.4. Gossip-сообщение

`MembershipGossip{ states: PeerState[], from_peer_id }`. Авторитетен
**аутентифицированный отправитель pubsub-стрима** (`msg.From`), а не
`from_peer_id`; последнее — поле для диагностики. Правила приёма
(монотонность по `timestamp`, допуск времени, приём чужих состояний только от
пира, которому разрешена маршрутизация) описаны в
[ARCHITECTURE.md §4.5](ARCHITECTURE.md#45-gossip-членство-discoverygossipgo).

`PeerState.status` принимает `active`, `draining` (узел уходит и не берёт новой
работы), `left`.

---

## 4. Ключи DHT и поиск по навыкам

```text
skill key = CIDv1( codec=raw, mh=sha256( "zeptomesh/skill/v1/" + lowercase(skill) ) )
task  key = CIDv1( codec=raw, mh=identity( sha256( "zeptomesh/task/v1/" + task_id ) ) )
```

Код — `discovery.SkillKey` / `discovery.TaskKey`. Пространство имён
`zeptomesh/skill/v1/` отделяет наши записи от прочих приложений в общей DHT.

Задача публикуется как provider record и переобъявляется каждые 12 минут. Поиск
возвращает пересечение множеств провайдеров по списку навыков.

---

## 5. Стабильность digests

`tasks.ContentDigest` = `"sha256:" + hex( sha256( "zeptomesh/content\x00" ‖ wire.TaskContent ) )`.

Изменение любого авторского поля (инструкция, метки, вложения, ограничения,
приоритет) меняет digest; изменения в пути (`sender`, `ttl`, `route_stack`) —
нет. Это единственное, что позволяет отличить «конверт передали дальше» от
«конверт подделали».

---

## 6. Открытые вопросы протокола

- **Доставка артефактов.** Сейчас передаётся только ссылка `sha256:<hex>`.
  Протокол выгрузки/скачивания (`/zeptomesh/artifact/0.1.0` с `GetValue`/CID
  проверкой) не определён — без него результат с файлами физически не забрать.
- **Подтверждение получения результата origin'ом.** `ResultAck` даёт
  подтверждение только на одном хопе; end-to-end ack не определён.
- **Отзыв (`cancel`) и его распространение.** Формат есть, семантика для уже
  запущенного PicoClaw (прерывание процесса или только отмена пересылки) — нет.
- **Агрегация подзадач.** `SubtaskSpec`/`SubtaskPlan` описаны в proto, но
  правило сведения нескольких ответов в один (большинство, все, лучший) не
  зафиксировано.
- **Делегирование доверия.** `TaskAck` подписан, но «цепочка принятия»
  (кто именно из промежуточных узлов обещал результат) в один field не
  упакована.
