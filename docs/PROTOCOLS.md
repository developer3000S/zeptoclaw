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
| `/zeptomesh/skillsearch/0.1.0` | pubsub (по умолчанию выключен) | `SearchMessage` (`SearchRequest`\|`SearchReply`) | `discovery.SearchTopic` |

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
| `origin_signature` | bytes | ed25519 по `wire.TaskContent`, ключом **инициатора**; ретрансляторами не перезаписывается |

`TaskConstraints` — `max_duration_seconds`, `allow_network_tools`, `allow_shell`,
`allow_delegation`, `allow_subtasks`. **Инвариант монотонности прав:** узел
пересекает ограничения задачи со своей политикой, подзадача наследует
пересечение родителя (`tasks.DeriveSubtask`) — задача может только потерять
права, никогда не получить новые.

`TaskPayload` содержит `instruction`, `context_digest`, `attachments`
(`ArtifactRef{name, hash, size, media_type}`) и `labels`. Большие тела в
конверт не кладутся: передаётся только content-addressed ссылка.

`labels` — единственное место, куда ложится контекст трассировки OpenTelemetry
(ТЗ 14.3): инициатор вписывает в них W3C `traceparent`/`tracestate` до
расчёта `context_digest`, каждый последующий узел читает их обратно и
продолжает тот же трейс. Отдельного поля в протоколе для этого нет
намеренно: `labels` входят в `wire.TaskContent`, то есть уже покрыты
`context_digest` и `origin_signature`, и переписать трейс в пути нельзя —
ровно как и формулировку задачи. Узел, поднятый до появления этой возможности,
просто перенесёт два лишних ключа: совместимость `ProtocolVersion` 0.1.0 не
затронута.

**Конфиденциальность содержимого.** `TaskPayload` на уровне приложения **не
шифруется**: каждый узел `route_stack` видит инструкцию по определению. Граница
конфиденциальности — Noise-канал libp2p на каждое соединение (его не читает
наблюдатель сети, но читает сам ретранслятор), граница целостности —
`origin_signature` + `context_digest` (ретранслятор не может переписать
формулировку, сохранив валидность). Если задаче требуется тайна от промежуточных
узлов, маршруту нужен исполнитель, достижимый напрямую, либо прикладное шифрование
внутри `instruction`; ни то, ни другое mesh'ом не навязывается.

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
(`zeptomesh-result-v1`), `route_stack` (снимок маршрута на момент завершения),
`worker_signature`, `error_class` (`TaskErrorClass`), `aggregated` (итог
объединения подзадач, ТЗ 6.10.5).

Результат движется назад по **обратному** `route_stack`; каждый промежуточный
узел подтверждает приём (`ResultAck{accepted, reason}`) и пересылает предыдущему
хобу. Промежуточный узел меняет только `sender_peer_id` и переподписывает
результат — исходный `worker_peer_id` и `worker_signature` исполнителя
сохраняются как доказательство авторства: инициатор проверяет именно
`worker_signature`, поэтому правка ответа промежуточным узлом обнаруживается.

`error_class` — машиночитаемая таксономия отказа (ТЗ 6.12): `VALIDATION`,
`SECURITY`, `EXECUTION`, `NO_WORKER`, `NETWORK`, `TIMEOUT`, `LIMITS`,
`CANCELED`. Она нужна ретранслятору, чтобы отличить повторимый отказ
(`NETWORK`, `NO_WORKER`) от терминального (`SECURITY`, `VALIDATION`) без разбора
человеческого текста. Класс выводится из текста при отсутствии
(`tasks.ErrorClass`), и NO_WORKER проверяется раньше совпадений по подстроке:
текст отказа цитирует удалённый узел и может содержать слово «allow», что иначе
сдвинуло бы класс на SECURITY и выключило бы ретраи.

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

`RpcRequest`/`RpcResponse` — `oneof` из семи вариантов:

| Вариант | Запрос | Ответ | Назначение |
|---|---|---|---|
| `ping` | `PingRequest{nonce}` | `PingResponse{nonce, peer_time}` | RTT и сверка часов |
| `capabilities` | `CapabilitiesRequest{}` | `CapabilitiesResponse{Capabilities}` | Self-описание узла |
| `peer_exchange` | `PeerExchangeRequest{count}` | `PeerExchangeResponse{PeerRecord[]}` | PEX: расширение вида нового узла |
| `cancel` | `CancelRequest{task_id, origin, sender, reason, signature}` | `CancelResponse{accepted, reason}` | отзыв задачи (подпись обязательна) |
| `skill_lookup` | `SkillLookupRequest{skills, full_refresh, relay_budget, visited}` | `SkillLookupResponse{PeerRecord[], partial, responder_refreshed}` | точечный запрос «у кого есть навыки» + расширение области поиска |
| `skills_sync` | `SkillsSyncRequest{known[], full}` | `SkillsSyncResponse{skills[], skills_version, peer_id, signature, reason}` | дельта-обмен версионированными описаниями навыков (§2.6) |
| `rebind` | `RebindRequest{KeyRebind}` | `RebindResponse{accepted, reason}` | прямой пуш заявления о переходе/отзыве ключа (§6.3) |

`Capabilities` (подписывается, `zeptomesh-caps-v1`) несёт `peer_id`, `node_name`,
`version`, `skills`, `models`, `max_parallel_tasks`, `running_tasks`, `load`,
`accept_external_tasks`, `allow_shell`, `relay_capable`, `resource_class`,
`listen_addrs`, `timestamp`, а также `skills_version` (поле 16) и `skill_docs`
(поле 17) — см. §2.6. Каноническое тело `wire.CapsBody` покрывает и epoch, и
дескрипторы, поэтому **проверенная** подпись сама по себе авторитетна: описания
нельзя подменить, не портив подпись.

### 2.6. Обмен навыками (`skills_version`, `SkillDescriptor`, `skills_sync`)

Два независимых множества: имена, которые узел **готов выполнять**
(`capabilities.skills`), и дескрипторы, которые он **описывает**
(`capabilities.skill_docs`). Вторые никогда не дают права исполнения:
`skills.Registry.SetSkillDoc` отказывает для имени, которого нет в advertisement,
поэтому ни API, ни импорт от соседа не могут расширить полномочия узла.

`SkillDescriptor{name, version, updated_at, digest, description, models[],
attributes{}}`; `digest` = `"sha256:" + hex(sha256(wire.SkillDescriptorBody))`,
где тело канонически **исключает** само поле digest. Правило «новее»
(`Newer`): сначала `version`, при равных версиях и разных дайджестах —
`updated_at`.

`skills_version` — **монотонный локальный epoch** (строгое `++`), а не настенные
часы: он растёт только когда реально изменилось содержимое (`Set` сравнивает
`contentKey`), поэтому не может «зависнуть» из-за расхождения времени между
узлами. Тот же epoch дублируется в `PeerState.skills_version` (поле 9), чтобы
сосед видел устаревание своего взгляда без запроса подписанных `Capabilities`.

Обмен (`manager.onSkillsSync` ↔ `skills.Registry`):

1. запрашивающий отправляет `SkillsSyncRequest{known: [{name, version, digest}],
   full}` — свой снимок того, что уже имеет;
2. отвечающий считает `SelectDelta(known, full)` и возвращает **только** то,
   чего у просящего нет или это новее, в пределах
   `capabilities.skill_exchange.max_descriptors` (64);
3. ответ подписывается ключом узла (`zeptomesh-skills-sync-v1`) и проверяется с
   привязкой к аутентифицированному отправителю: `peer_id` в теле обязан
   совпасть с ним (`VerifySkillsSync`), иначе — событие `skills_sync_bad_signature`
   и `RecordProtocolError`;
4. просящий импортирует дескрипторы в перерасчёт «только более новые» с
   проверкой дайджеста и бюджетом `import_limit` (512) на одного соседа;
   бюджет снимается `DropPeer` при уходе/истечении пира;
5. политика раскрытия `disclose_to ∈ trusted|known|any` (по умолчанию `known`).
   **Отказ тоже подписан** и различает `disclosure_policy` и
   `exchange_disabled` в поле `reason` — пустой ответ без причины означает
   «новее нечего», а не «не говорю», и unsigned-отказ не выглядит как
   нарушение протокола.

Периодическая сверка `reconcileSkills` идёт каждые
`capabilities.skill_exchange.interval` (30 с); выученные дескрипторы и часы
версий переживают рестарт при `persist: true` (`<data_dir>/skills.json`), иначе
узел переанонсировал бы то, что соседи уже имеют.

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
| `zeptomesh-caps-v1` | `CapsBody` (включая `skills_version` и `skill_docs`) | `peer_id` | `VerifyCaps` |
| `zeptomesh-task-origin-v1` | `TaskContent` (авторская часть, без маршрутных полей) | `origin_peer_id` | `verifyAuthorship` в `OnTask` |
| `zeptomesh-result-worker-v1` | `ResultBody` (без `signature`) | `worker_peer_id` | `VerifyWorkerResult` |
| `zeptomesh-skills-sync-v1` | `SkillsSyncBody` | `peer_id` в теле = аутентифицированный отправитель | `VerifySkillsSync` |
| `zeptomesh-rebind-v1` | `RebindBody` | **обоими** ключами: `old_peer_id` и `new_peer_id` | `VerifyRebind` |

Пустое поле `signature_scheme` трактуется как ожидаемая схема (совместимость с
ранними сборками); неизвестная схема — ошибка, а не «пропустить».

### 3.3. Правила приёма конверта (нормативные)

`tasks.Manager.OnTask` применяет проверки именно в этом порядке — дешёвые и
отбрасывающие раньше дорогих (реализовано; см. `internal/p2p/service_test.go`,
интеграционные сценарии ТЗ 17.2 — в работе):

1. размер фрейма ≤ `security.max_message_bytes` (`p2p/framing.go`);
2. `tasks.Validate(env, max_payload, now)` — структура, лимиты, возраст,
   сверка `context_digest`;
3. `sender_peer_id` совпадает с аутентифицированным удалённым пиром стрима;
4. `security.VerifyTask` — подпись против `sender_peer_id` (ключ из id),
   если `security.require_task_signature: true`;
5. `verifyAuthorship` — `origin_signature` против `origin_peer_id` по
   `wire.TaskContent`: некорректная подпись фатальна всегда, отсутствие —
   при `security.require_origin_signature: true` (по умолчанию). Дочерние
   конверты (`parent_task_id ≠ ""`) не проверяются: их автор — промежуточный
   планировщик, и он же их подписывает;
6. `tasks.CheckRoute(env, self)` — `ttl > 0` и отсутствие `self` в `route_stack`;
7. сверка `deadline_at` (ограничение `tasks.max_timeout_seconds`);
8. `Policy.AllowTasksFrom(sender)`, затем `Limiter.Allow(sender)` и лимит
   `tasks.max_parallel_tasks_per_peer` для этого пира;
9. `storage.ClaimDedup(task_id, dedup_window)` — иначе `ACK DUPLICATE`;
10. пересечение `constraints` с локальной политикой (`canExecute`);
11. решение «исполнить / делегировать»: `routing.Table.Select` + расширение
    области поиска search-relay; при `constraints.allow_delegation = false`
    узел без собственной мощности обязан отклонить задачу.

Отрицательный исход шагов 2–5 и 8 (`task_invalid`, `task_sender_mismatch`,
`task_bad_signature`, `task_origin_signature`, `task_untrusted`) пишется в журнал
безопасности; отказ по маршруту, дедлайну, rate limit и квоте пира — только
`ACK REJECTED` (это легальное поведение соседа, а не протокольное нарушение).

### 3.4. Gossip-сообщение

`MembershipGossip{ states: PeerState[], from_peer_id, rebinds: KeyRebind[],
revocations: KeyRebind[] }`. Авторитетен
**аутентифицированный отправитель pubsub-стрима** (`msg.From`), а не
`from_peer_id`; последнее — поле для диагностики. Правила приёма
(монотонность по `timestamp`, допуск времени, приём чужих состояний только от
пира, которому разрешена маршрутизация) описаны в
[ARCHITECTURE.md §4.5](ARCHITECTURE.md#45-gossip-членство-discoverygossipgo).

`PeerState.status` принимает `active`, `draining` (узел уходит и не берёт новой
работы), `left`. `PeerState.skills_version` (поле 9) зеркалит
`Capabilities.skills_version` — по нему сосед понимает, что его взгляд на навыки
устарел, и инициирует `skills_sync`.

Полный снимок (`PublishFull`) пакуется **пачками по 32 состояния** с паузой
`heartbeat/8` между пачками, состояния упорядочены по убыванию `timestamp`
(равные — по id). Заявления о переходе ключей ставятся в **первый** батч: они
компактны, и их не должно отсекать отказ остальных состояний того же сообщения
(реализация: `ingestRebinds` вызывается до проверки «не принято ни одного
состояния»). Собственное эхо publish отбрасывается до подсчёта — GossipSub
доставляет локально опубливанное сообщение самому хосту.


### 3.5. Тема эпидемического поиска навыков (ТЗ 6.9.5 п.5)

`SearchMessage{ request: SearchRequest | reply: SearchReply }` — общий
pubsub-топик, последний шаг лестницы расширения поиска, когда адресная
цепочка (локально → таблица → полный refresh → relay RPC) не нашла
исполнителя. По умолчанию выключен (`tasks.forwarding.search_relay.topic.enabled: false`); включение требует перезапуска — топик join'ится на старте.
Membership и тема едут на **одном** GossipSub-роутере: второй `NewGossipSub`
на том же хосте перерегистрирует стрим-протокол и молча отбирает подписки
первого, поэтому роутер создаётся узлом один раз и передаётся обеим сторонам.

`SearchRequest{ request_id, requester_peer_id, skills[], issued_at, scheme,
signature }`. **Конфиденциальность по построению:** запрос называет только
навыки — ни id задачи, ни инструкции, ни label'ов: эпидемический поиск видит
каждый подписчик, и раскрывать он должен только потребность. `request_id` — 16
случайных байт; связать ответ с задачей можно только локально (открытый канал
сам по себе ничего не выдаёт).

`SearchReply{ request_id, responder_peer_id, peers[], issued_at, scheme,
signature }` — `peers` ≤ `max_answers`, дедуп по id. Записи — **заявления**, а
не факты: потребитель прогоняет их через `Adopt` (дозвон + проверка подписанных
Capabilities самого кандидата) до маршрутизации, поэтому лже-резponder может
лишь заставить просителя потратить неудачный дозвон на пира, которого тот и без
темы не допустил бы.

Правила приёма (код — `discovery/searchtopic.go`):

1. собственное эхо отбрасывается;
2. подпись проверяется **по аутентифицированному отправителю pubsub-сообщения**,
   и заявленный `*_peer_id` обязан совпасть с ним до всякой криптографии (как в
   membership); схема — `zeptomesh-search-request-v1` /
   `zeptomesh-search-reply-v1`;
3. возраст `issued_at` в пределах `request_ttl` + допуск хода часов; запрос
   старше окна не отвечается (поздняя реплика не должна вечно порождать работу);
4. ответ засчитывается только если `request_id` совпал с открытым локальным
   запросом; нераспознанный id — `search_reply_unsolicited` в журнал
   безопасности;
5. резponder не раскрывает незнакомцу чужие записи: без task-trust к просителю
   возвращается только собственная запись резponder'а (если он сам закрывает
   навык). Blocked-пир не получает ничего (фильтр роутера + `AllowConnection`);
6. анти-шторм: `answer_cooldown` на набор навыков, `max_answers` на сбор,
   размер `peers` ограничен на стороне ответа.

Метрики: `search_topic_requests_total`, `search_topic_answers_total`,
`search_topic_rejected_total`; событие лога `search_topic_answered`, отказы —
в журнал безопасности (`search_request_*`, `search_reply_*`). Тесты:
`internal/discovery/searchtopic_test.go` (совместное существование с membership
на одном роутере, устаревший запрос, поддельный responder id, cooldown) и
интеграционные `internal/node/integration_test.go::TestIntegrationSearchTopic*`
(A находит C через B только силой темы; с выключенной темой та же задача
исполнителя не находит — негативный контроль).

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

## 6. Заявления о переходе и отзыве ключей (`KeyRebind`)

### 6.1. Почему заявление самодостаточно

Ed25519 Peer ID — самомонтирующийся: публичный ключ восстанавливается из самого
идентификатора. Поэтому `KeyRebind` подписывается **обоими** ключами
(`wire.RebindBody` = old ‖ new ‖ new_pubkey ‖ sequence ‖ issued_at ‖ reason) и
проверяется любым узлом сети, даже никогда не видевшим ни уходящий, ни новый
ключ. Для перехода нужны обе подписи (ни одна сторона не может объявить
передачу доверия в одиночку), для **отзыва** — только уходящая
(`new_peer_id` пуст): сфальсифицировать чужой отзыв нельзя, у противника нет
чужого ключа.

Отсюда асимметрия правил приёма, принципиальная для безопасности:

| Сообщение | Требуется доверие к ретранслятору? | Почему |
|---|---|---|
| `PeerState` в gossip | да (`AllowDelegationTo`) | состояние ничем не подписано и подделываемо |
| `KeyRebind` | **нет** — принимается от любого источника | сам верифицируется двумя подписями; узел, которому ещё нет доверия, может перенести переход |

### 6.2. Правила применения (`security.RebindStore.Apply`)

1. `VerifyRebind` — обе подписи (для отзыва — одна) обязательны;
2. **уже известное** заявление с `sequence ≤` сохранённого не применяется и
   ошибкой не считается (реплей по gossip — норма, а не нарушение; иначе журнал
   безопасности тонет в `rebind_rejected`);
3. откат по `sequence` назад для известного `old_peer_id` невозможен;
4. если `old_peer_id` уже **отозван**, заявление отвергается: отзыв терминален,
   иначе скомпрометированный ключ увёл бы своё доверие на свежий id;
5. глубина цепочки переходов ограничена (`TestRebindChainDepthBound`) —
   класс идентичностей не может расти бесконечно;
6. при загрузке `<data_dir>/rebinds.json` подписи **перепроверяются**:
   подправленный файл не выдаёт и не отзывает идентичности.

Окно повторной публикации — 12 ч (`StatementsSince`/`StatementsFresh`): транспортная
свежесть, а не срок годности заявления. Узел, узнавший переход позже, всё равно
его примет; окно лишь решает, что стоит рассылать самостоятельно.

### 6.3. Каналы распространения

- **gossip**: в каждом heartbeat и первым батчем full-sync (§3.4);
- **прямой пуш** по control-RPC (`RpcRequest_Rebind` → `RebindResponse{accepted,
  reason}`) всем соединённым соседям — и обязательный путь при
  `discovery.gossip.enabled: false`, где gossip-канала нет вовсе. Отправляется
  дважды: при ротации (до установки нового ключа) и при рукопожатии с новым
  соседом (`introduce` → `ShareRebinds`), чтобы свежеподнятый узел не держал
  преемника за незнакомца;
- **операция узла** `RotateKey` выдерживает порядок «построить → проверить →
  опубликовать → разослать → и только затем `Save` нового ключа»: иначе соседи
  на мгновение увидели бы незнакомый id вместо преемника, которому доверяют.
  `RevokeSelf` дополнительно переносит ключевой файл на
  `<key_file>.revoked-<случайное>` и гасит процесс так, чтобы супервизор его
  **не** поднимал (`Node.Halted()`, а не код 75, как у `leave`).

### 6.4. Что меняется в состоянии принявшего узла

- доверие и записи таблицы соседей переносятся на преемника; отзыв
  (`new_peer_id == ""`) помечает класс гашёным;
- **доверие привязано к классу идентичностей** (`ClassOf`/`ClassRevoked`):
  запрет любого члена блокирует весь класс, разрешение любого члена
  распространяется на преемника, отзыв любого члена гасит весь класс. Иначе
  блокировка обходилась бы ротацией, а плановая ротация — ломала делегирование;
- bootstrap-запись вида `/ip4/…/p2p/<старый id>` переименовывается в памяти
  (`Bootstrap.Rename`) — адрес верен, устарел только id;
- уходящий id удаляется из таблицы, выученные дескрипторы навыков переносятся;
- отзыв собственного id, **выученный** из gossip (сценарий украденного ключа),
  останавливает узел так же, как собственный `revoke`.

Проверки: `internal/security/rebind_test.go`,
`internal/discovery/gossip_rebind_test.go` (живой libp2p, включая «переход
принимает узел, не знавший новый ключ» и «заявления переживают сообщение, в
котором все `PeerState` отклонены»).

---

## 7. Открытые вопросы протокола

- **Доставка артефактов.** Сейчас передаётся только ссылка `sha256:<hex>`.
  Протокол выгрузки/скачивания (`/zeptomesh/artifact/0.1.0` с `GetValue`/CID
  проверкой) не определён — без него результат с файлами физически не забрать.
- **Подтверждение получения результата origin'ом.** `ResultAck` даёт
  подтверждение только на одном хопе; end-to-end ack не определён.
- **Отзыв (`cancel`) и его распространение.** Реализовано: `OnCancel` принимает
  запрос только от `origin_peer_id` и по валидной подписи, `applyCancelLocal`
  дёргает `cancelFn` задачи (дочерний процесс PicoClaw убивается вместе с
  группой процессов), отмена спускается по `downstream` и на детей разложенного
  плана. Не определено только подтверждение отмены: `CancelResponse.accepted`
  возвращается на каждом хопе, но end-to-end гарантии «весь тракт встал» origin
  не собирает.
- **Агрегация подзадач.** Правило сведения зафиксировано и реализовано
  (`internal/tasks/decomposition.go`): ожидаются **все** дети; итог `COMPLETED`
  только если все дети завершились; тексты склеиваются в порядке плана секциями
  `### subtask N`, артефакты дедуплицируются по hash; первая ошибка определяет
  итоговый статус и класс ошибки; `aggregated=true`, считается `result_digest`,
  итог подписывается как исполнителем (`worker_signature`) — origin получает один
  подписанный ответ, а не N. Retryable-ребёнок повторяется ровно один раз
  (`maxSubtaskRetries=1`). План передаётся в поле `subtasks` JSON-API и
  повторяемым CLI `--subtask`; `pb.SubtaskSpec` в proto объявлен, но не
  используется (план живёт в `tasks.SubtaskRequest`).
- **Делегирование доверия.** `TaskAck` подписан, но «цепочка принятия»
  (кто именно из промежуточных узлов обещал результат) в один field не
  упакована.
