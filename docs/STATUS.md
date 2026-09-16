# Статус реализации: соответствие ТЗ

Документ — официальный отчёт о состоянии проекта относительно
[`../ТЗ.md`](../ТЗ.md) («PicoClaw Agent Mesh», в. 0.1). Каждое утверждение
проверено чтением кода на момент редактирования документа. Разделы ТЗ указаны в
формате `[№]`.

Смежные документы: [ARCHITECTURE.md](ARCHITECTURE.md),
[PROTOCOLS.md](PROTOCOLS.md), [PICOCLAW-INTEGRATION.md](PICOCLAW-INTEGRATION.md),
[DEPLOYMENT.md](DEPLOYMENT.md), [RUNBOOK.md](RUNBOOK.md).

---

## 1. Реализовано

### Транспорт и идентичность

| Требование ТЗ | Реализация |
|---|---|
| [7.1] QUIC основной + TCP резервный, Noise, Yamux, Ed25519 | `internal/p2p/host.go` (`New`: `libp2p.Security(noise.ID…)`, `Muxer(yamux…)`, `Transport(quic…)`, `Transport(tcp…)`); опции подключаются условно |
| [6.1] Стабильный Peer ID, ключ `0600`, каталог `0700` | `internal/security/identity.go`, `cmd/zeptomesh-node/main.go` (`genKey`: `os.Chmod(0o600)`, `os.Chmod(dir,0o700)`) |
| [11.1] Приватный ключ не покидает узел; PSK не светится в логах | подпись по p2p-ключу; `redact()` в `main.go` |
| [7.1] Приватная сеть (PSK) | `node.private_network_psk`, `decodePSK`, `libp2p.PrivateNetwork`. Ограничение go-libp2p: QUIC-транспорт не поддерживает закрытые сети и с PSK отказывается строиться, поэтому узел под PSK работает TCP-only — `tcpOnlyAddrs` отбрасывает quic-v1/webtransport из `listen`/`announce` (warn `quic_disabled_private_network`) вместо падения на старте (найдено живой приёмкой E.6, см. TESTREPORT §6.2) |
| [6.3.3.4] Circuit relay v2: опциональность, лимиты реле, приоритет прямого соединения | `host.go`: `EnableRelayService` с `relayServiceOptions` (ёмкости и лимиты из `node.relay.limit`), `EnableAutoRelayWith{StaticRelays,PeerSource}`; `DisableRelay()` при выключенной опции; ретрансляция включается libp2p только когда прямого пути нет |
| [7.5] Protobuf поверх libp2p-потоков, length-prefixed фреймы, потолок кадра | `api/proto/zeptomesh/v1/mesh.proto`, `internal/p2p/framing.go` (`MaxFrameBytes = 8 MiB`), `security.max_message_bytes` |

### Обнаружение [6.3]

- [6.3.1] Реестр Unix-сокетов узлов на одном хосте: `internal/discovery/local.go`
  (`LocalRegistry`, heartbeat, проверка живости через probe-сокет).
- [6.3.2] mDNS: `internal/discovery/mdns.go` (обёртка `mdns.NewMdnsService`,
  сервис `_zeptomesh._udp`), найденный пир обходится `onDiscovered` →
  `Connect` → верификация подписанных `Capabilities` перед доверием к навыкам.
- [6.3.3.1] Bootstrap-список с ретраями и меткой здоровья:
  `internal/discovery/bootstrap.go`.
- [6.3.3.2] Peer exchange: `RpcRequest_PeerExchange` (`OnRPC` в
  `internal/tasks/manager.go`) + периодический опрос `requestPeerExchange`
  (`internal/node/node.go`).
- [6.3.3.3] Kademlia DHT-индекс навыков: `internal/discovery/dht.go`
  (`ProvideSkills`, `FindPeersBySkill`, переанонс по таймеру).
- [6.3.1 п.4] Gossip-членство (эпидемическая рассылка `PeerState`):
  `internal/discovery/gossip.go` (GossipSub, анти-поизонинг-правила приёма,
  `expireLoop` по `failure_timeout`). Полный синхронный снимок `PublishFull`
  пакуется батчами по 32 состояния с паузой `heartbeat/8` между ними — снимок
  большого состава не превращается в один всплеск трафика (мера против
  self-poisoning на принимающей стороне). Заявления о переходе ключей
  (`rebinds`/`revocations`) ставятся в **первый** батч: они переживают отказ
  остальных состояний того же сообщения, а эхо собственного publish
  отбрасывается до подсчёта.
- [6.3 п.5] Управление включением слоёв: `discovery.gossip.enabled` теперь
  действительно gates конструирование `Membership` (был декоративным), а
  внутренняя опция `node.Options.SkipDiscovery` отключает всю сетевую фазу
  обнаружения — узел остаётся достижимым только по явно набранным адресам
  (отладка, air-gapped кластер, детерминированные тесты).

### Маршрутизация и задачи

- [6.4] Таблица соседей с эмпирикой: `internal/routing/table.go`
  (`Table.Upsert/Select/PruneStale/Sample/Restore`, веса скоринга
  `wSkill .40 / wTrust .25 / wCapacity .20 / wLatency .10 / wSuccess .05`).
- [6.4.3] Категории близости `CatLocal/CatLAN/CatWAN`
  (`internal/node/node.go:classify`).
- [6.6] Формат задачи: `TaskEnvelope` со всеми полями ТЗ
  (`task_id, parent_task_id, origin_peer_id, sender_peer_id, ttl, priority,
  required_skills, payload, context_digest, route_stack, signature`);
  канонические энкодеры digest/подписей — `internal/wire/canonical.go`.
- [6.6.1 п.4] **Запланированные триггеры — четвёртый источник задач**
  (`internal/triggers`): cron-выражение + задача, которые узел подставляет себе
  сам, как если бы оператор вызвал `submit`. Cron-диалект — узкое
  общепринятое подмножество (пять полей, `* a-b a,b */n`, имена
  `jan..dec`/`sun..sat`, воскресенье `0|7`, классическое ИЛИ при
  ограниченных обоих day-полях), собственный парсер без сторонних библиотек:
  выражение, которое узел не понимает, отклоняется при создании, а не молча
  никогда не срабатывает (`ParseSchedule` → `ErrBadSchedule`).
  Разрешение — минутное; **пропущенные во время простоя запуски не
  доигрываются** (`LastFire` персистится вместе со списком в одном meta-ключе —
  рестарт не удваивает и не откатывает расписание). Два семейства: список
  `triggers:` в `node.yaml` (горячая перезагрузка: `SetConfigTriggers` —
  синхронизированный живой владелец, группа валидируется целиком, неизменённые
  записи сохраняют объект и бухгалтерию, поэтому перезагрузка конфига не
  приводит к двойному запуску) и хранимые через API/CLI (в BadgerDB,
  переживают рестарт). Конфигурационные id защишены от затенения: `POST
  /api/v1/triggers` с id из YAML отклоняется. Тело запроса описывает
  расписание, но не его историю — `run_count/last_fire/last_error` из JSON
  отбрасываются, иначе клиент мог бы беззвучно «разоружить» задачу. Задача
  помечается `labels{trigger: <id>, source: trigger}` и уходит в обычный
  конвейер (маршрутизация по `required_skills`, права — пересечение с
  возможностями узла). Паника в submit записывается как ошибка запуска, а не
  как успех, и не роняет демон. Метрики/статус: поле `triggers` в
  `GET /api/v1/status`, `GET|POST /api/v1/triggers`,
  `DELETE /api/v1/triggers/{id}`, CLI `triggers` / `trigger-add` / `trigger-rm`.
- [6.6.5] Дедупликация: `storage.ClaimDedup` (окно `tasks.dedup_window`),
  `ACK DUPLICATE` без повторного исполнения. Притязание атомарно (одна
  транзакция) и ограничено окном: повтор внутри окна не продлевает его и не
  даёт исполнить задачу второй раз, а по истечении окна тот же id снова
  заявляем — надиктованный от отправителя повтор доставки не подавляется
  навсегда. `resubmit` обходит дедуп иначе: он создаёт задачу с новым id.
- [6.7] Жизненный цикл и статусы: enum в `mesh.proto`, журнал задач
  `internal/storage` (`TaskRecord`), `TaskStatus` в `internal/tasks/task.go`.
- [6.8.1/6.8.2] Изоляция исполнения: `prepareSandbox` (`manager.go`) — на
  задачу создаются `tasks/<id>/{home,workspace}`, собственный
  `config.json` с `PICOCLAW_*`-окружением.
- [6.8.3] Таймаут задачи с настраиваемым максимумом:
  `tasks.TimeoutSeconds(env, def, max)` + `tasks.max_timeout_seconds`.
- [6.9] Делегирование: `manager.routeOrigin → forward` (fanout, параллельные
  попытки, TTL-1, цикл-защита `CheckRoute`).
- [6.9.5] Поиск исполнителя с расширением области видимости: search-relay
  (`manager.searchRelay`, `onSkillLookup`): при пустом локальном выборе узел
  рассылает `SkillLookupRequest` соседям с `full_refresh=true` — обязанность
  соседа перезагрузить полный набор навыков (DHT/gossip/table) и прогнать
  собственный `Select`; дальше `relay_budget`, `visited` (антицикл), `partial`.
  См. также `searchRelay`-метрики (`internal/metrics`).
- [6.9.5 шаг 5] Тематический P2P-топик поиска (`discovery.SearchTopic`,
  ТЗ 6.9.5 п.5): финальный, эпидемический шаг лестницы — подписанный запрос
  **только навыков** (ни id задачи, ни инструкции — эпидемический канал видит
  каждый подписчик) публикуется в `/zeptomesh/skillsearch/0.1.0`; узел,
  закрывающий навык, отвечает подписанным списком кандидатов (≤ `max_answers`,
  дедуп). Принимается строго по аутентифицированному pubsub-отправителю:
  заявленный id == sender до криптографии, `issued_at` в окне `request_ttl`,
  ответ годен только на открытый локальный `request_id`. Ответы — заявления:
  перед маршрутизацией каждый кандидат проходит `Adopt` (дозвон + проверка
  подписанных Capabilities). Незнакомому просителю резponder раскрывает только
  себя (чужие записи — только при task-trust), blocked — не получает ничего.
  Анти-шторм: `answer_cooldown` на набор навыков. Router GossipSub один на
  хост и разделяется с membership (второй `NewGossipSub` молча отбирает
  стрим-протокол первого — проверено тестом). По умолчанию **выключено**,
  включается только перезапуском. Шаг в `searchRelay` выполняется даже при
  `search_relay.enabled: false` — тема существует ровно для случая, когда
  адресно спросить некого. Тесты: `internal/discovery/searchtopic_test.go`
  (+config `searchtopic_test.go`), интеграция
  `TestIntegrationSearchTopicFindsUnacquaintedWorker` (A находит C через B
  только темой) и `...StaysSilentWhenDisabled` (негативный контроль).
- [6.10] Возврат результата по входящему тракту: `OnResult` (origin — приём,
  промежуточный хоп — ретрансляция вверх с собственной подписью).
  [11.5] Цепочка подписей: `worker_signature` (`security.SignWorkerResult` /
  `VerifyWorkerResult`) вычисляется исполнителем по каноническому
  `wire.ResultContent` и ретрансляторами не перезаписывается — инициатор
  проверяет именно её, подмена результата промежуточным узлом обнаруживается.
- [6.11] Отмена: подписанный `CancelRequest` (`SchemeCancel`), приём только от
  инициатора (`OnCancel`), остановка локального процесса через `cancelFn`.
- [6.12] Классификация ошибок: `TaskErrorClass` в протоколе и
  `tasks.ErrorClass(status, msg)` (validation/security/execution/no-worker/
  network/timeout/limits/canceled).
- [6.12.1] Ретраи без повторного вопроса отказавшему: `routeOrigin` ведёт
  множество исключений и передаёт его в `forward`, который читает его в
  `Table.Select` и пополняет каждым кандидатом, отказавшим, недоступным или уже
  держащим задачу. Без этого выбор детерминирован, а `RecordFailure` сдвигает
  скоринг всего на 0.0225 — «повтор» оставался вторым вопросом тому же узлу.
  Ретранслятор (`OnTask`) делает одну попытку и множество не ведёт: повтор —
  дело upstream, у которого виден весь маршрут.
- [6.9.3] TTL уменьшается на каждом хопе, `ttl<=0` запрет делегирования
  (`forward` в `manager.go`); `constraints.allow_delegation=false` исполняется тем
  же `forward` — узел без собственной мощности обязан отклонить задачу, а не
  пересылать её вопреки запрету инициатора.
- [6.9.1] Декомпозиция на подзадачи: план приходит с задачей (`SubmitRequest.Subtasks`
  / JSON `subtasks` / повторяемый CLI `--subtask`), каждый пункт порождает дочерний
  конверт через `tasks.DeriveSubtask` и уходит в обычный конвейер маршрутизации
  (`internal/tasks/decomposition.go`); `parent_task_id` связывает детей в журнале
  (`storage.LinkChild/ChildrenOf`), статус `WAITING_SUBTASKS` выставляется родителю.
- [6.10.5] Агрегация результатов подзадач: `finishFanout/aggregateResult` ждут все
  детей (или их ретраи), сводят ответы в порядок плана (секции `### subtask N`),
  дедуплицируют артефакты по hash, наследуют первую ошибку, выставляют
  `aggregated=true`, считают `result_digest`, подписывают итог как исполнитель
  (`worker_signature`) и отдают единственному origin-waiter. Один ретрай ребёнка
  при retryable-классе ошибки (`maxSubtaskRetries=1`).
- [6.8.4] Исполнение лимитов ресурсов: `max_parallel_tasks_per_peer` — счёт
  активных задач одного пира в `OnTask` до dedup-притязания;
  `min_free_disk_bytes` — `statfs` по каталогу задач перед `prepareSandbox`
  (`internal/tasks/disk_unix.go`); `max_workspace_bytes` — кумулятивная квота при
  сборе артефактов (`collectArtifacts`, останов обхода `SkipAll`);
  `max_task_memory_bytes` — `RLIMIT_AS` через `ulimit -v` в shell-обёртке запуска
  (`internal/picoclaw/limits_unix.go`), плюс `Setpgid`/`Pdeathsig=SIGKILL` и
  убийство группы процессов при отмене/таймауте.
- [10.3] Модель — свойство узла: `picoclaw.model` передаётся локальному агенту
  как `--model` (режим `binary`). Значение видно в `GET /api/v1/status`
  (`adapter.model`) и пишется в локальный журнал задачи (`ResultRecord.model`):
  приоритет у `model_name`, раскрытого самим агентом, иначе — запрошенной узлом
  модели. В wire это не уходит (см. §2): `TaskResult` поля модели не имеет.
  Прочие модельные параметры в mesh-конфиг не вынесены сознательно:
  `picoclaw agent` принимает только `-d/-m/-s/--model` (`Args: cobra.NoArgs`,
  неизвестный флаг = сорванная задача), а температуру и `max_tokens` PicoClaw
  читает из `PICOCLAW_AGENTS_DEFAULTS_*` — для этого уже есть `picoclaw.env`.
- [6.4.3] RTT соседей измеряется регулярно: `measureRTT` в maintenance-цикле
  (`internal/node/node.go`) пингует до 8 connected-соседей каждые 15 с и
  складывает выборку в скользящее среднее `Table.RecordRTT` — компонент
  `latency_score` в скоринге перестаёт быть нейтральным.
- [6.5.3] Исключение молчащего соседа подтверждено адресным опросом
  (`reapSuspects`/`probePeer` в `internal/node/node.go`, `Table.Suspects/Touch`
  в `internal/routing`): раньше запись вычёркивалась по одному лишь
  `failure_timeout×3` молчания — а gossip best-effort, так что живые, но
  замолчавшие узлы (задержка pubsub-батча, занятой вход) пропадали из вида
  маршрутизации, и это выглядело как «no eligible peers reachable» при
  исправном исполнителе. Теперь молчание делает соседа лишь *подозреваемым*:
  обход (не более 8 опросов за проход, старые первыми, timeout 2 с на опрос)
  пингует его по существующему соединению; ответивший остаётся с обновлённым
  `LastSeen`, не ответивший удаляется (MarkLeft в membership, сброс выученных
  навыков). Опрос отвечает на вопрос «достижима ли идентичность вовсе», а не
  «есть ли у пира мощность» — второго сообщения в протоколе mesh нет, и это
  честная граница проверки.
- [6.3.3.2] Ответ PeerExchange фильтруется политикой (`AllowConnection`) и
  исключает уже ушедших пиров — узел не помогает заблокированному пиру
  находить сеть.
- Подключение, принятое узлом (входящее соединение), дополняется запросом
  подписанных навыков пира (`Node.introduce` ← `ConnectedF`): без этого
  пассивная сторона не знала ни навыков, ни уровня доверения соседа и не могла
  делегировать работу обратно по тому же пути, которым задача пришла.
- Удаление пира во время задачи больше не ждёт таймаута: `OnPeerDisconnected`
  (`manager.go`) помечает TIMEOUT все задачи, чей принявший downstream пропал,
  и отдаёт результат по назначению (origin / fanout / relay вверх).

### Безопасность и доверие [11]

- `internal/security/trust.go`: пять уровней доверия, три режима
  (`open|limited|private`), allow/deny-файлы, `Observe` без понижающих решений
  (наблюдение может только повысить), `AllowConnection` (используется
  `ConnectionGater`), `AllowTasksFrom`, `AllowDelegationTo`.
- `internal/p2p/gater.go`: `ConnectionGater` блокирует соединения с
  заблокированными пирами на уровне транспорта (не только задач).
- `internal/security/audit.go`: ведра rate-limit (`Limiter`) + журнал
  безопасности JSONL (`security.Audit`, `<data_dir>/audit/security.jsonl`).
- [11.4] Опасные операции по умолчанию выключены: `allow_shell: false` в
  дефолтах, пересечение прав (`canExecute`, `DeriveSubtask`).
- [11.2 п.3–4] **Ротация и отзыв ключей** (`internal/security/rebind.go`,
  `SignRebindOld`/`SignRebindNew`/`VerifyRebind` в `signing.go`, схема
  `SchemeRebind`): заявление `KeyRebind` подписывается **обоими** ключами —
  уходящим и новым, — а публичные ключи восстанавливаются из самих Ed25519
  Peer ID. Поэтому заявление самодостаточно: его принимает любой узел, никогда
  не видевший новый ключ, и плановая смена ключа не требует правки списков на
  соседях. Отзыв — то же заявление без преемника, подписанный только уходящим
  ключом (сфальсифицировать чужой отзыв нельзя).
  - `RebindStore` — журнал `<data_dir>/rebinds.json`: `Apply` верифицирует
    подпись и требует строго возрастающей `sequence` (реплей старше не
    применяется и ошибкой не считается), `load` при старте **перепроверяет
    подписи** хранимых заявлений — подправленный файл не выдаёт и не отзывает
    идентичности.
  - **Отзыв терминален**: ключ, объявивший отзыв, не может ни во что «перейти»
    (`Apply` отказывает), иначе скомпрометированный узел увёл бы своё доверие на
    свежий идентификатор.
  - **Доверие привязано к классу идентичностей, а не к текущему ключу**
    (`ClassOf`/`ClassRevoked`, `TrustOf` в `trust.go`): запрет любого члена
    класса блокирует весь класс (иначе блокада обходилась ротацией), разрешение
    любого члена распространяется на преемника (иначе ротация понижала бы
    доверие и ломала делегирование), отзыв любого члена гасит весь класс.
  - Перенос состояния при переходе: запись таблицы соседей (навыки, метрики,
    категория), выученные дескрипторы навыков; bootstrap-запись вида
    `/p2p/<старый id>` переименовывается в памяти (`Bootstrap.Rename`), уходящий
    id удаляется из таблицы.
  - Распространение: gossip несёт заявления в каждом heartbeat и в full-sync
    (первым батчем), плюс прямой пуш по control-RPC (`RpcRequest_Rebind`) всем
    соединённым соседям — единственный канал при
    `discovery.gossip.enabled: false`. Принятие из gossip не требует маршрутного
    доверия к ретранслятору, в отличие от `PeerState` (обоснование — в
    `ingestRebinds`, PROTOCOLS §6).
  - Операции узла: `Node.RotateKey` (заявление → публика → прямой пуш → и
    только затем запись нового ключа, чтобы соседи не увидели незнакомца),
    `Node.RevokeSelf` (рассылка + перенос ключевого файла на
    `<key_file>.revoked-<случайное>` + остановка), `acceptRebind`,
    `PublishRebind`, `ShareRebinds`, `Node.Halted()` — канал, по которому `run`
    завершается **без** сигнала перезапуска (в отличие от `leave`, код 75), а
    повторный старт отклоняется локальным стражем.
  - API/CLI: `POST /api/v1/admin/rotate-key`, `POST /api/v1/admin/revoke`,
    `GET /api/v1/rebinds`; `zeptomesh-node rotate|revoke|rebinds`.
    Метрика `zeptomesh_identity_rebinds_applied_total`.
- [6.6.4 усиление] **`origin_signature`**: конверт несёт вторую подпись — по
  каноническому `wire.TaskContent`, ключом **инициатора**. Ретрансляторы
  перезаписывают `sender_peer_id`/`ttl`/`route_stack` и свою `signature`, но
  авторскую не трогают, поэтому итоговый исполнитель может проверить подлинность
  формулировки задачи, а не только подпись последнего отправителя. Проверяется в
  `OnTask` (`verifyAuthorship`, событие `task_origin_signature`): некорректная
  подпись фатальна всегда, отсутствие — при
  `security.require_origin_signature: true` (значение по умолчанию). Дочерние
  конверты декомпозиции подписываются тем же механизмом (планировщик и есть
  инициатор).

### Обмен навыками (дополнение к ТЗ по запросу)

Узлы обмениваются **версионированными описаниями** навыков и подтягивают их
друг у друга, если у партнёра версия новее. Реестр —
`internal/skills/registry.go`; в нём два независимых множества: имена, которые
узел *готов выполнять* (из `capabilities.skills`, минус `disabled_skills`), и
дескрипторы, которые он *описывает* (`capabilities.skill_docs`). Вторые никогда
не дают права исполнения — `SetSkillDoc` отказывает, если имени нет в
advertisement, поэтому описание скила не может расширить полномочия узла ни
через API, ни через импорт от соседа.

- Дескриптор: `name/version/updated_at/description/models/attributes` +
  `digest` (`sha256:<hex>`) по каноническому `wire.SkillDescriptorBody`,
  исключающему само поле digest. `Newer`: по `version`, при равных версиях и
  разных дайджестах — по `updated_at`.
- Epoch узла (`skills_version`, поле 16 в `Capabilities` и 9 в `PeerState`) —
  монотонный локальный счётчик (`pure ++`), не смешанный с настенными часами:
  он меняется только при реальном изменении содержимого (`Set` сравнивает
  `contentKey`, а не версию из вызова), а значит не может «зависнуть» из-за
  расхождения времени между узлами.
- Каналы раскрытия: дескрипторы едут в подписанных `Capabilities`
  (`wire.CapsBody` покрывает epoch и дескрипторы — проверенный ответ
  авторитетён сам по себе), epoch дополнительно приходит в gossip-состояниях;
  дельта-синхронизация — RPC `skills_sync` (`SkillsSyncRequest{known,full}` →
  `SkillsSyncResponse`), где запрошенный узел сам считает `SelectDelta` по
  присланным версиям.
- Ответ подписывается ключом узла (`SchemeSkillsSync`) и проверяется с
  привязкой к аутентифицированному отправителю: `peer_id` в теле должен совпасть
  с ним (`VerifySkillsSync`), иначе — событие `skills_sync_bad_signature` и
  `RecordProtocolError`. Отказ («нечего» / «не скажу») тоже подписан: пустой
  ответ различает `disclosure_policy` и `exchange_disabled`, и unsigned-отказ
  не выглядит как нарушение протокола.
- Политика раскрытия `capabilities.skill_exchange.disclose_to`:
  `trusted|known|any` (по умолчанию `known`) — аннотации к скилам считаются
  метаданными оператора, а не сетевыми данными.
- Бюджет и цикл: `max_descriptors` (64) на один ответ, `import_limit` (512)
  дескрипторов на одного соседа (снимается `DropPeer` при уходе/истечении),
  периодическая сверка `reconcileSkills` каждые `interval` (30 s).
- Хранение: `<data_dir>/skills.json` при `persist: true` — часы версий
  переживают рестарт, поэтому узел не переанонсирует то, что соседи уже имеют.
- Наблюдение: `zeptomesh_skills_version` (gauge),
  `zeptomesh_skills_synced_total`, `zeptomesh_skills_descriptors_imported_total`,
  `zeptomesh_skills_sync_refused_total`; `GET /api/v1/skills` (свои + выученный
  обзор по каждому соседу), `POST /api/v1/skills`, `DELETE
  /api/v1/skills/{name}`, `POST /api/v1/skills/sync`; CLI
  `skills|skill-set|skill-rm|skills-sync`.

### Хранилище [9]

`internal/storage/storage.go` — BadgerDB v4: задачи (`t:`), результаты (`r:`),
пиры (`p:`), дедуп-притязания (`d:`), вторичный индекс навыков (`s:<skill>/<peer>`,
нормализованный и перестраиваемый при каждом `PutPeer`), связи parent/child
(`c:<parent>/<child>`, `o:<child>`), meta (`m:`), `Prune(keep)`,
content-addressed артефакты sha256 (`StoreArtifact/LoadArtifact/ArtifactPath`).

### Наблюдаемость [14]

- [14.1] Логи: `log/slog`, JSON-хендлер, обязательный контекст. Имена полей
  заданы политикой в `internal/logging`: `ts` (вместо slog-овского `time`),
  `level`, `event` (текст записи — в этом коде он всегда машинное имя события),
  `message` (повторяет `event`, пока вызывающий код не принёс собственный текст) и
  `component` на каждой записи. `peer_id` добавляет узел, `task_id` присутствует в
  каждой записи, которая касается конкретной задачи: `task_delegated`,
  `task_executing`, `task_failed_peer_gone`, `task_sign_for_hop_failed`,
  `delegation_rejected`/`delegation_no_answer`, подписи ак (`ack_sign_failed`),
  поиска исполнителя (`search_relay_widened_view`),
  всего журнала декомпозиции (`subtask_retry`, `subtask_retry_failed`,
  `subtask_missing`, `subtask_orphaned`) и транспортных отказов `task`/`result`
  (`inbound_task_handler`, `inbound_result_write` и т.д.). `component` проставляют
  `rebinds`, `skills`, `api`, `transport`, `routing`, `discovery`, `tasks`,
  `picoclaw`.
  `logging.Component(logger, …)` — единственный способ переименовать компонент:
  обработчик `boundAttrs` держит связанные атрибуты сам, поэтому `With` не
  дописывает второй `component`, а одноимённый аргумент вызывающего кода заменяет
  связанное значение, а не удваивает его. До этой политики живой вывод узла содержал
  записи с повторённым ключом (`host_started` — два `peer_id`, `node_started` — два
  `node_name`): потребитель, читающий JSON в словарь, оставляет последний экземпляр
  молча, строгий — запись отклоняет.
  Мост stdlib→slog (`logging.BridgeStdlib`) идёт через ту же политику.
- Метрики: `internal/metrics` — полный набор обязательных метрик ТЗ 14.2.1
  (+ дедуп/таймауты/security/relay); приватный реестр (безопасно N узлов в
  одном процессе).
- [14.2] Выделенный Prometheus-слушатель: `telemetry.prometheus_listen` +
  `metrics.Collector.ServeMetrics` (`internal/metrics/serve.go`), `/healthz`
  на том же порту; поднимается в `cmd/zeptomesh-node/main.go`.
- [14.3] OpenTelemetry (рекомендация ТЗ — реализована): конвейер задач несёт
  спаны `task.submit` → `task.receive` → `task.forward`/`task.execute` (+
  `task.plan` для декомпозиции), каждый с атрибутами `zeptomesh.task_id`,
  `zeptomesh.origin_peer_id`, `zeptomesh.required_skills` — то есть трейс
  склеивается с журналом и метриками по сквозному `task_id`, который ТЗ делает
  обязательным. Пакеты разделены по стоимости зависимости: `internal/tracing` —
  только API (уже был в графе через libp2p), `internal/telemetry` — SDK +
  OTLP/HTTP-экспортёр, единственное место, где они линкуются; `internal/config`
  SDK не импортирует. По умолчанию `telemetry.tracing.enabled: false`:
  Setup ничего не конструирует, глобальные провайдеры остаются no-op'ами API,
  и узел, который о трейсинге не упоминал, не платит ни за экспорт, ни за
  гонки. Перенос контекста через hop — в `payload.labels` конверта: labels
  входят в `wire.TaskContent`, значит покрыты digest и авторской подписью, и
  промежуточный узел не может переписать трейс сильнее, чем формулировку
  задачи; предшествующий узел без этой функции просто пронесёт два лишних
  ключа (совместимость сохранена, `ProtocolVersion` не менялся). Traceparent
  вписывает только автор (в `Submit` и при инъекции подзадач — до пересчёта
  digest), читают все (`Extract` на приёме), поэтому заголовок не появляется
  там, где его не подписывали. Секция перечитывается только рестартом (SDK
  ставит глобальные провайдеры однажды) — `Diff` честно относит
  `telemetry.tracing` к `requires_restart`.
  Проверено: `internal/tasks/tracing_test.go` (рекордер SDK: submit+execute в
  одном трейсе на одном узле, атрибуты, невозможность переписать labels без
  потери digest) и `TestIntegrationTraceContinuesAcrossNodes` в
  `internal/node/integration_test.go` (живой libp2p: задача с А исполнена Б,
  три спана одного трейса, родитель `receive` — спан `submit` с А); негативный
  контроль прогонялся выключением инъекции — тест падает на разрыве трейса.
  Экспорт в живой collector (Jaeger/Tempo) не проверялся: collector на этой
  машине не поднят, трафик экспорта наблюдался только против закрытого порта
  (`internal/telemetry/telemetry_test.go` — ограниченный shutdown).

### Управление [12/13]

- HTTP/JSON админ-API (`internal/api/admin.go`): `GET /healthz`,
  `GET /api/v1/{status,peers,capabilities,tasks,tasks/{id},config,rebinds}`,
  `POST /api/v1/tasks` (в т.ч. план декомпозиции в поле `subtasks`),
  `POST /api/v1/tasks/{id}/cancel`, `DELETE /api/v1/tasks/{id}` (тот же обработчик —
  форма, ожидаемая ТЗ), `POST /api/v1/tasks/{id}/resubmit`,
  `GET/POST /api/v1/skills`, `DELETE /api/v1/skills/{name}`,
  `POST /api/v1/skills/sync`, `POST /api/v1/admin/reload-config`,
  `POST /api/v1/admin/rotate-key`, `POST /api/v1/admin/revoke`,
  `POST /api/v1/admin/leave`, `GET /metrics`; bearer-токен через
  `api.auth_token_env`.
- [13.1.1] Горячая перезагрузка конфигурации: `Node.ReloadConfig` перечитывает файл,
  с которым узел запущен, и применяет только те поля, у которых есть
  синхронизированный владелец (posture trust-политики, имя/содержимое allow/deny-
  файлов, rate limit, уровень лога). `Config.Diff` честно делит изменения на `hot` и
  `requires_restart`; сам `Config` в рантайме не мутируется, поэтому читатели
  без блокировок видят согласованный снимок. Тот же путь доступен как `SIGHUP`
  (`ExecReload` в systemd-юните) и как CLI `zeptomesh-node reload`.
- [13.1.1] `leave`: узел публикует в gossip `PeerState{status:"left"}` и завершает
  процесс с кодом 75 — `Restart=on-failure`/`systemd-notify` convention, так что
  супервизор поднимает его с новым конфигом; CLI `zeptomesh-node leave`.
- CLI (`cmd/zeptomesh-node`): `run`, `version`, `genkey`, `psk`, `status`,
  `peers`, `capabilities`, `submit` (в т.ч. `--subtask`), `tasks`, `get`, `cancel`,
  `resubmit`, `reload`, `leave`, `skills`, `skill-set`, `skill-rm`, `skills-sync`,
  `rotate`, `revoke`, `rebinds`.
- Конфигурация [12.1]: `internal/config` — дефолты, YAML-слияние,
  `${VAR:-default}` (+вложенные ссылки, `config_test.go`), `Validate()` с
  выводом относительных путей и `applyEnvOverrides` (`ZETOMESH_BOOTSTRAP`).

### Сборка и развёртывание [15/22]

- `Makefile` — единая точка сборки/проверки/запуска: `all` (fmt-check+vet+build+
  test), `build`/`build-race`/`cross` (linux/amd64+arm64, ТЗ 15.1), `deps`,
  `tidy`, `toolchain`, `proto`/`proto-check`, `fmt`/`fmt-check`, `vet`, `lint`,
  `static-check`, `ci` (статика + сборка + тесты под `-race`), `test` /
  `test-unit` / `test-race` / `test-cover` / `test-integration` / `test-load` /
  `bench`, `run-1`, `dev-cluster` / `dev-status` / `dev-rotate` /
  `dev-cluster-down`, `install-local` / `install-docker` / `docker-build`,
  `clean`, `help`. Мишень `test-integration` (`-tags=integration`) расписана
  живыми сценариями (см. таблицу тестов ниже); `test-load` (`-tags=load`) и
  `bench` объявлены; у `test-load` цель есть и прогнана (`internal/simulator`,
  см. §2 и [LOADTEST.md](LOADTEST.md)), у `bench` файлов-мишеней пока нет (см. §5).
- `scripts/dev-cluster.sh` — локальный кластер из N узлов на одном хосте
  (конфиг `configs/examples/dev-node.yaml`, unix-socket реестр вместо mDNS,
  порты `4101+i`/`8101+i`/`9564+i`): `up|down|status|logs|peers|skills|sync|
  rebinds|rotate|submit|down-and-clear`.
- `deploy/docker/Dockerfile` — многоэтапная сборка (golang→debian-slim,
  non-root, HEALTHCHECK `/healthz`, EXPOSE 4001/tcp+udp, 8081, 9464).
- `deploy/docker/docker-compose.yml` — сеть из 3 узлов (порты base+i,
  bootstrap на zepto-0 через dns4).
- `install.sh` — установка: локально (systemd system/user-юниты, шаблон
  `zeptomesh@<i>`, автостарт) или в Docker (сборка образа + генерация
  compose-стека ровно на N экземпляров, `up -d`); выбор N экземпляров
  (`--nodes`), `status/start/stop/uninstall`; peer ID и bootstrap-адрес
  первого узла прописываются остальным.
- Примеры конфигурации [22.8]: `configs/examples/lan.yaml`,
  `configs/examples/wan.yaml`.
- [15.1] **arm64 проверена без чужеродного рантайма на хосте**: `make cross`
  собирает linux/amd64 и linux/arm64 (оба ELF подтверждены `file`), а полный
  прогон тестов выполняется под эмуляцией — `apt-get download qemu-user-static`
  + `dpkg-deb -x` в каталог вне дерева проекта, затем
  `GOARCH=arm64 CGO_ENABLED=0 go test -exec=…/qemu-aarch64-static ./...` →
  exit 0 для всего набора и отдельно для `-tags=integration ./internal/node/
  ./internal/security/` (живой libp2p на эмулированном arm64, 40 c). Ограничение
  честно: `-race` под этим способом недоступен (CGO=0, arm64 C-тулчейна на
  машине нет), а эмуляция проверяет корректность кода на arch64, но не
  производительность и не гонки.
- [15.1 conveyor] **CI-конвейер настроен** — `.github/workflows/ci.yml`
  (GitHub Actions, push/PR в main): 7 задач — статика (`fmt-check`, `vet`
  c build-тегами, `proto-check` с protoc 3.21.12 из заголовка gen/), сборка +
  `make cross` с проверкой ELF, юнит-набор под `-race` с покрытием,
  интеграционные сценарии под `-race` + стресс `-count=2`, живая приёмка
  `scripts/acceptance.sh` E.1–E.6 с артефактом протокола, **родная arm64**
  (раннер `ubuntu-24.04-arm`: build + полный тестовый набор — закрывает зазор
  «arm64 hardware» из TESTREPORT §7; `-race` на arm64 остаётся эмуляцией),
  сборка Docker-образа. Гейты повторяют раздел 5 TESTREPORT один-в-один.
- [15.4]/[22 п.7] **Ansible** — `deploy/ansible/`: роль `zeptomesh` закрывает
  шесть обязанностей ТЗ по отдельности (теги `docker`, `dirs`, `config`, `run`,
  `health`, `peerids`) плюс шаг 0 `preflight` (`tags: always`), который ловит
  ошибки входных данных до любых изменений на хосте. `deploy.yml` разворачивает
  (Docker → каталоги → конфиг → образ → контейнер → здоровье → сбор Peer ID),
  `collect_peerids.yml` только опрашивает уже работающие узлы и ничего не
  меняет (`zeptomesh_stage=collect`). Конфигурация доставляется env-файлами
  (`instance.env.j2`), а не правкой образа; тег образа закреплён (`0.1.0`,
  `latest` запрещён документально), PSK и API-токены — только из
  `ansible-vault` (`.vault.example.yml`). Шаг 6 собирает начальные адреса через
  `GET /api/v1/status` (поле `addrs`) и складывает их в `mesh_nodes.bootstrap.txt`
  — готовое значение `ZETOMESH_BOOTSTRAP` для второй волны; `zeptomesh_auto_bootstrap`
  подставляет точки входа остальным узлам сам. Проверено на этой машине:
  `ansible-playbook --syntax-check` обоих плейбуков и `ansible-lint
  --profile production` → «0 failure(s), 0 warning(s) on 15 files»; живой
  прогон против множества хостов не выполнялся (в наличии один локальный
  хост) — см. §4.
- `LICENSE` — полный текст Apache-2.0 (совпадает с
  `https://www.apache.org/licenses/LICENSE-2.0.txt`).

### Тесты (покрытие текущее)

| Файл | Что проверяет |
|---|---|
| `internal/wire/canonical_test.go` | стабильность канонического кодирования, domain separation, независимость content-digest от полей маршрута |
| `internal/security/security_test.go` | генерация/загрузка идентичности, подписи task/result/caps/ack, отказ неподписанным и подписанным не тем узлом, trust-политика, limiter (в т.ч. `Wait`: ждёт токен, уважает дедлайн и не тратит его при отмене), audit JSONL |
| `internal/p2p/service_test.go` | in-process libp2p: ответы RPC/задач/результатов на малых и больших фреймах (регрессия на premature stream reset), отказ oversized-кадра и здоровье сервера после него, нотификаторы соединений |
| `internal/p2p/host_psk_test.go` | регрессия на дефект, найденный живой приёмкой E.6: узел с PSK не стартовал вообще (QUIC-транспорт go-libp2p отказывается строиться под `PrivateNetwork`). Теперь: `tcpOnlyAddrs` отбрасывает quic/quic-v1/webtransport из listen/announce (остальное сохраняет, невалидное — на совести libp2p); под PSK хост поднимается TCP-only; два пира с одной PSK соединяются, с разными — нет (живой handshake) |
| `internal/config/config_test.go` | expandEnv (вложенные default'ы), `ZETOMESH_BOOTSTRAP`, раскрытие поставляемого шаблона, `picoclaw.model` — литерал и `${ZETOMESH_PICO_MODEL:-}` (пустая переменная обязана схлопываться в «нет мнения») |
| `internal/picoclaw/cli_test.go` | аргументы CLI-адаптера против фиктивного исполняемого файла, печатающего собственный argv: модель узла как ровно один `--model` (второй сделал бы выбор CLI неопределённым), `extra_args` хвостом после модели, переопределение `Request.Model` заменяет default, отсутствие `--model` при пустой/пробельной конфигурации, порядок относительно `-s`, шаблон промпта вместе с моделью, `Model()`/`ModelOf()` (stub его не реализует → mesh молчит) |
| `internal/tasks/search_relay_test.go` | навыки-покрытие, дедуп PeerRecord, wire-roundtrip SkillLookup (full_refresh/relay_budget/visited) |
| `internal/tasks/decomposition_test.go` | декомпозиция/агрегация сквозняком на живом узле: план → дети через обычный конвейер → один подписанный итог с digest'ом и порядком плана; журнал parent/child; ретрай retryable-ребёна ровно один раз и оседание non-retryable без ретрая; отказ заведомо неверных планов (размер, пустая инструкция, `allow_subtasks=false`, исчерпанный TTL) |
| `internal/skills/registry_test.go` | реестр навыков: монотонность epoch при реальном изменении содержимого, согласованные дайджесты дескрипторов, правило `Newer`, импорт только более новых с проверкой дайджеста, `SelectDelta`, помесячный `import_limit` на узла, часы версий в `skills.json`, разделение «описать» и «уметь» (`DropDoc` не снимает имени), обзор по соседям |
| `internal/security/rebind_test.go` | заявления о переходе/отзыве: двойная подпись и отказ при порче/подписи третьим ключом, отзыв достаточно старого ключа, журнал (`Apply`: реплей не ошибка, откат по `sequence` запрещён, повторный пересмотр подписи при загрузке), терминальность отзыва и ограничение глубины цепочки, наследование доверия по классу, приоритет операторского запрета над ротацией, окно актуальности заявлений |
| `internal/security/skills_sync_test.go` | подпись `skills_sync` (roundtrip, отказ при несовпадении `peer_id` с аутентификатором, отказ unsigned), `origin_signature`: выживает после переписывания полей маршрута ретранслятором, отвергается при подмене содержимого и при подписи чужим ключом |
| `internal/discovery/gossip_rebind_test.go` | доставка заявлений по живому libp2p: переход принимает узел, никогда не знавший новый ключ (ретранслятор без маршрутного доверия), заявления переживают сообщение, в котором все `PeerState` отклонены, отбрасывание поддельных/устаревших/«из будущего», отзыв отдельным полем |
| `internal/routing/table_test.go` | доверие не хранится в таблице соседей: частичные наблюдения (gossip/реестр/обновление навыков) не повышают незнакомца до `trusted`, скор читает политику, планка оператора видна мгновенно без записи, `Restore` не воскрешает статус (персистентное состояние не подменяет состояние процесса), но сохраняет выученные навыки/epoch; [6.5.3] `Suspects` перечисляет молчащих соседей, не удаляя их, и возвращает старых первыми (бюджет обхода); `Touch` обновляет наблюдение и снимает статус suspect, но не создаёт запись о незнакомом peer'е — ответ опроса не воскрешает исключённого пира |
| `internal/tasks/errors_test.go` | `ErrorClass`: NO_WORKER имеет приоритет над цитатой из удалённого отказа (иначе класс смещается на SECURITY и ретраи выключаются), `ErrorResult` несёт причину, класс переживает round-trip через журнал (в т.ч. записи старого формата без поля) |
| `internal/tasks/result_binding_test.go` | результат от пира, которому задача не передавалась, отвергается (подпись доказывает авторство, а не то, что его просили); ожидаемый пир никогда не отвергается по привязке — отказ формулируется как результат маршрутизации |
| `internal/logging/logging_test.go` | политика полей записи [14.1]: обязательные `ts/level/event/message/component` и отсутствие slog-овских `time`/`msg`; связанный ключ не удваивается, а одноимённый аргумент вызывающего кода его заменяет (`peer_id`, `node_name`, `message`); `Component` переопределяет дефолт одним полем; мост stdlib даёт ту же схему; фильтр уровней переживает обёртку; 32 горутины пишут один `component` на запись |
| `internal/tasks/validation_test.go` | валидация задач [ТЗ 6.6/17.1] прямо по каждой причине отказа: пустые id/origin/sender, пустая (в т.ч. пробельная) инструкция, лимиты инструкции и `max_task_payload_bytes`, приоритет вне 1..9, потолок `required_skills` и `route_stack`, ttl, «слишком старая» и «из будущего» по времени, подделка digest, отличие «digest отсутствует» от «digest не тот», перечисление всех нарушений в одной ошибке (и её оборачиваемость для `errors.Is`), цикл и исчерпанный ttl в `CheckRoute`, однократность записи хопов в `AppendRoute`, приемлемость собственного вывода `Build` для `Validate`, круговая таблица `SkillsMatch` (general/any, регистр и пробелы, частичное покрытие), round-trip через protobuf |
| `internal/storage/storage_test.go` | локальное хранилище [9] и дедуп [6.6.5]: `PutPeer` перестраивает вторичный индекс навыков одного пира (снятый навык перестаёт его называть, чужие пиры не задеваются), нормализация имён совпадает с `tasks.SkillsMatch`, пустое имя не создаёт ключ, легаси-ключи верbatim-регистра перестраиваются первым же апсейтом, испорченная предыдущая запись не клинит запись; `ClaimDedup` — первая доставка исполняется, повтор в окне нет, просрочка окна вновь заявляема, `Count` считает притязания, а не доставки; round-trip задачи/результата (в т.ч. поля `model`), порядок и лимит `ListTasks` (срезает старые), обе стороны `LinkChild`, content-addressing артефактов (идемпотентность, отказ чужому алгоритму, `..`-хеши не выходят за корень), `Prune` (уходит только законченное, вместе с результатом), `MarkSeen`, `Stats`, переживание рестарта |
| `internal/triggers/cron_test.go` | парсер cron [6.6.1 п.4]: принимает ходовые формы (звёздочки, списки, шаги, диапазоны, `5/20` как «от 5 до конца поля», имена месяцев и дней, воскресение как `0` и `7`, `*/15 8,9`); комбинация двух day-полей по классическому правилу (один ограниченный решает сам, оба ограниченных — ИЛИ); `Matches` оперирует минутами и игнорирует секунды; `Next` строго после данной минуты, через год и через 29 февраля 2028, а `0 0 30 2 *` («30 февраля») отдаёт «никогда», а не вечный цикл; ~15 malformed-выражений отклоняются с `ErrBadSchedule`, называя поле и всё выражение; нормализация пробелов в `String()` |
| `internal/triggers/scheduler_test.go` | планировщик [6.6.1 п.4] на встроенных часах и in-memory Saver: не более одного запуска за минуту даже при двух проходах; пропущенные минуты пропускаются, а не доигрываются; бухгалтерия (`LastFire`/`RunCount`/`LastTaskID`/`LastError`) переживает пересоздание планировщика; выключенный и исчерпанный по `max_runs` не стреляют вообще; сбой submit записывается и не мешает следующему запуску; правка расписания обнуляет счёт, правка job — нет; конфиг-расписания не пишутся в хранилище и перекрывают хранимую запись с тем же id; неизменённый конфиг-триггер сохраняет живой объект (регрессия на двойной запуск «* * * * *» после перезагрузки конфига), правка — вступает в силу; дубликат id отклоняется целиком; отклонённая перезагрузка оставляет старые расписания живыми; битое выражение видно через `View.BadSchedule` вместо тишины; `NextFire` слушается выключателя, лимита и «будущего» `LastFire`; `Tick` под 8 горутинами стреляет один раз; паника submit превращается в ошибку запуска; fired-job несёт `labels{trigger}`; фасад `Views/Add/Delete` для API (id из конфига нельзя ни затенить, ни удалить); `untilBoundary` всегда положителен. Всё под `-race` |
| `internal/api/triggers_test.go` | HTTP-контракт [6.6.1 п.4]: `GET /api/v1/triggers` показывает конфиг-расписание с `next_fire`; `POST` создаёт хранимый триггер, отклоняет 422 затенение id из YAML и битое выражение, мусор в теле даёт 400, причём отклонённый триггер не попадает в хранилище; `DELETE` удаляет хранимый, второй вызов и удаление конфиг-id дают 404; хранимый триггер переживает пересоздание планировщика; тело запроса не может подделать бухгалтерию — `run_count`/`last_fire`/`last_error` из JSON отбрасываются, расписание остаётся боевым |
| `internal/config/searchtopic_test.go` | секция `search_relay.topic`: по умолчанию выключена, полное чтение блока, отказ при имени без `/`, нулевом `request_ttl`/`max_answers`, отрицательном `answer_cooldown`; выключенная секция не валидируется; включение темы — только `requires_restart` |
| `internal/discovery/searchtopic_test.go` | тема поиска на живом libp2p: совместное существование с membership на ОДНОМ GossipSub-роутере (иначе второй join молча убивает подписки первого — проверяется доставкой состояний), подпись/приём запроса и ответа, скип собственного эха (заявка не отвечается самому себе), игнорирование устаревшего запроса, ОТВЕРГНУТЫЙ ответ с чужим `responder_peer_id` не попадает в результат, `answer_cooldown` глушит второй ответ на тот же набор навыков, гейты конструктора (отключённая тема/битое имя не join'ятся) |
| `internal/tasks/journal_status_test.go` | регрессия на гонку быстрой записи результата с пост-ack бухгалтерией: `ParseStatus` — точная инверсия `String` (неизвестное написание не терминально), статус журнала не воскресает из `COMPLETED` поздним `updateStatus`/`FORWARDED` (страж `Terminal()` + `journalMu` сериализуют read-modify-write) |
| `internal/tasks/tracing_test.go` | [14.3] на in-memory рекордере SDK: `task.submit` и `task.execute` одного трейса на одном узле, родитель `execute` — спан `submit`, сквозной `zeptomesh.task_id` и `worker_peer_id` в атрибутах; traceparent переживает пересчёт digest и `Validate` (то есть конверт с ним подписываем и отправляем), `Extract` на принимающей стороне восстанавливает trace/span id отправителя, инъекция не трогает карту labels вызывающего; переписанный ретранслятором `traceparent` ломает `VerifyDigest` — безопасность переноса контекста в labels доказана напрямую |
| `internal/telemetry/telemetry_test.go` | [14.3] граница стоимости: `enabled: false` ничего не конструирует и не подменяет глобальные провайдеры, shutdown возвратен и безвреден; битые `endpoint` (схема, путь, внутренний пробел, висячее `:`) и `sample_ratio` вне 0..1 отклоняются с именованной ошибкой до первого экспорта; включённый путь ставит SDK-провайдер, а flush против недостижимого collector завершается в пределах лимита (узел не висит на остановке) |
| `internal/node/integration_test.go` | **ТЗ 17.2 + 17.4, живой libp2p** (тег `integration`, `make test-integration`): 17.2.1–2 обнаружение двух узлов; 17.2.2–4 mesh из трёх узлов, задача с А исполняется Б, результат подписан Б и вернулся А; причина отказа, когда исполнителя нет; 17.2.6 делегирование через промежуточный узел (ручная топология, `route_stack` без исполнителя); 17.2.5 исполнитель умирает с задачей в руках → `TIMEOUT` с названной причиной, а не ожидание TTL; 17.2.7 повторная доставка того же id не исполняется дважды; 17.2.8 истёкший TTL останавливается на входе и не распространяется; 17.4.1–2 unsigned-конверт и конверт с переписанным содержимым (digest пересчитан, транспортная подпись наведена) отвергаются по авторской подписи; 17.4.3 приём задач подчиняётся порогу `min_trust_for_tasks` и немедленно отпускается после решения оператора; 17.4.4 заблокированный узел не получает соединения и потока (оба направления); 17.4.6 превышение rate-limit даёт отказ с причиной, принятые задачи при этом исполняются; 17.4.5 каждое ребро шифровано (noise/tls) и ни один канал узла, не участвующего в задаче, не раскрывает её содержимое (protobuf-байты gossip-состояния, capabilities, ответов peer-exchange/skill-lookup/skills-sync, журнал, статус и метрики); [6.12.1] постоянный отказ верхнего кандидата (`peer not trusted for tasks`) не приводит к повторному вопросу ему же: ровно одна попытка отвергнута, ровно одна делегация успешна, задача исполнена вторым соседом; **[6.6.1 п.4] planned trigger от начала до конца**: cron из `node.yaml` проходит живой планировщик → submit → маршрутизацию, результат подписан соседом и вернулся инициатору, запуск виден в `Views`, конфиг-запись не утекает в хранилище и не удаляется через API, а хранимый через API триггер показан в том же списке; **[14.3] сквозной трейс через реальный hop**: задача, подставленная на А и исполненная Б, даёт `task.submit`+`task.receive`+`task.execute` в одном трейсе, родитель `receive` — спан `submit` с другого узла, `zeptomesh.task_id` на всех трёх; **[6.9.5 шаг 5] эпидемическая тема поиска**: в трёхузловой ручной топологии (A—B—C, A не знает C, адресный relay выключен) задача с навыком `research` находит C только силой темы (метрики `search_topic_requests/answers`>0, A не отвечает сам себе), а с выключенной темой та же задача исполнителя НЕ находит — негативный контроль |

---

## 2. Реализовано частично

| Пункт ТЗ | Что есть | Чего не хватает |
|---|---|---|
| [6.9.1] Планирование разбиения | исполнение плана готово (§1): дочерние конверты, маршрутизация, агрегация | сам план по-прежнему приходит извне (API/CLI/журнал); автоматического «разбей инструкцию на N подзадач» по метаданным навыков нет — это функция PicoClaw/агента, а не mesh'а; `pb.SubtaskSpec` в proto не используется (план живёт в `tasks.SubtaskRequest`) |
| [6.5] SWIM-подобный прямой опрос | детектирование отказов по `failure_timeout` + конн-нотификер + быстрый `OnPeerDisconnected` для задач; перед исключением молчащего соседа узел пингует его адресно и оставляет, если тот отвечает (§1, [6.5.3]) | опрос проверяет достижимость идентичности, а не готовность исполнять: отдельного «ты свободен для работы?» сообщения в протоколе нет (сознательная граница — см. §1) |
| [14.3] OpenTelemetry | реализовано, см. §1: спаны конвейера задач, контекст в подписанных labels, OTLP/HTTP-экспортёр, по умолчанию выключено | живого экспорта в collector не проверялось (collector не поднят); сквозной трейс подтверждён только на двух узлах в одном процессе |
| [15.4] Массовое развёртывание | Ansible-роль `deploy/ansible` — все шесть шагов ТЗ, `--syntax-check` и `ansible-lint --profile production` чисты (см. §1) | живой многохостный прогон не выполнялся: на этой машине один локальный хост, развёртывание проверялось статически и на `--check`-режиме инвентаря |
| [17.3]/[22 п.6] Нагрузочное тестирование | симулятор membership/routing (`internal/simulator`, детерминированная модель на тех же `routing.Table.Select`, gossipsub-параметрах и wire-размерах): прогон 100/500/1000 узлов (`make test-load`, 73 мин) и отчёт [LOADTEST.md](LOADTEST.md); **вердикт по ТЗ 16.4 — НЕ выполнен**: служебный трафик в покое 678.6 / 8388.8 Кбит/с на узел при нормативе ≤ 100, доминирует `PublishFull` (85–86 % при 500). Быстрые инварианты модели идут в `make test` | это **модель, а не живой кластер**: нет TCP/TLS-рукопожатий, channel-задержек, пропускной способности, backpressure, CPU/ памяти/GC реального узла и криптоисполнения; полный прогон на 1000 узлов не дошёл до конца за 60 мин (покой — отдельный размер 40:1:1, экстраполяция помечена как оценка); аналитическая замена ретрансляции full-sync расходится с точным обходом до +58 % при 300 узлах (замерено, см. «Границы» в отчёте) — на вердикт в разах не влияет, на наклон влияет |
| [6.8.4] Ограничения ресурсов | `max_parallel_tasks_per_peer` (счёт задач пира в `OnTask`), `min_free_disk_bytes` (`statfs` перед `prepareSandbox`), `max_workspace_bytes` (квота при сборе артефактов), `max_task_memory_bytes` (`RLIMIT_AS` + группа процессов с `Pdeathsig`), `allow_shell`/`allow_network_tools` — узел как исполнитель не принимает задачу, требующую больше, чем он разрешает (`canExecute`), и передаёт права в `picoclaw.Request` | «отключение сети/оболочки» остаётся административным ограничением, а не техническим: дочерний процесс PicoClaw не помещается в network namespace и не теряет привилегий — для жёсткой границы нужен контейнер/песочница на узле; квота workspace считается при сборе результатов, а не удерживается файловым лимитом ядра; `ulimit -v` — best-effort (оболочка может не поддержать, тогда ограничение не применяется) |
| [10.3/10.4] PicoClaw | три режима адаптера (см. PICOCLAW-INTEGRATION.md); модель как свойство узла: `picoclaw.model` → `--model` в режиме `binary`, раскрытие модели в статусе и локальном журнале (§1) | выбора модели **задачей** нет: `TaskEnvelope`/`TaskResult` поля модели не несут, а добавить их — несовместимое изменение протокола (`ProtocolVersion` 0.1.0 → 0.2.0), поэтому `Request.Model` остаётся внутренним крюком. `capabilities.models` объявляется пирам, но не участвует в маршрутизации: «у кого стоит llm-a» mesh не знает. Режим `http` модель не выбирает вовсе (у Pico Protocol нет проверенного поля запроса), только читает `model_name` в ответе; мультиагентность сведена к очереди одного адаптера |

---

## 3. Известные отклонения от ТЗ

1. **HTTP/JSON вместо gRPC** ([5], [13.1]). ТЗ допускает выбор
   «gRPC или HTTP/JSON»; выбран JSON ради отладки и отсутствия
   grpc-рантайма. Межузловой обмен — protobuf поверх libp2p-потоков.
2. **BadgerDB вместо SQLite** ([6.x хранилище]). Локальный движок выбран
   BadgerDB v4; SQLite не используется (в ТЗ допустимы оба).
3. **Префикс путей админ-API `/api/v1/...`** вместо голых `/status`,
   `/peers`… ([13.1.1]); `health` назван `healthz`.
4. **Отмена задачи**: `DELETE /api/v1/tasks/{id}` реализована в форме ТЗ, а
   `POST /api/v1/tasks/{id}/cancel` — равнозначный псевдоним (удобен из CLI и
   для клиентов, которым недоступен DELETE). Обе формы ведут в один обработчик,
   он идемпотентен.
5. **Имя пространства метрик `zeptomesh_*`** вместо `picomesh_*`
   ([14.2.1]); набор метрик покрыт полностью, имена — с префиксом проекта
   (`zeptomesh_peers_total` ≡ `picomesh_peers_total` и т.д.).
6. **Имена протоколов** `/zeptomesh/*` ([7.4]).
7. **PSK как «закрытая сеть»** вместо WireGuard/Tailscale ([5]): транспортная
   изоляция достигается симметричным ключом libp2p; административная граница
   остаётся за trust-политикой.
8. **`trusted/untrusted` как категории таблицы не заведены**: доверие живёт в
   `security.Policy`, в таблице — только `category` близости; `CatBootstrap`
   определён и не присваивается (bootstrap-узлы попадают в CatWAN).
9. **Подпись задачи переподписывается каждым хопом** (sender-схема), инициатор
   проверяет последнего отправителя + цепочку маршрута; это осознанное
   упрощение (см. [6.6.4] в ARCHITECTURE), подписанная цепочка результатов
   при этом ведётся через `worker_signature`. Сильнее, чем было при формулировке
   ТЗ: с появлением `origin_signature` ([6.6.4 усиление], §1) авторство и
   неизменность содержимого дополнительно заверяет ключ инициатора, и ретранслятор
   не может переписать формулировку, сохранив валидность.

---

## 4. Не реализовано

| Пункт ТЗ | Комментарий |
|---|---|
| — | открытых пунктов нет; остались вопросы к заказчику (§6) и работы из §5 ниже |

Пункты, закрытые в этом цикле: [19.9] отчёт о тестировании — написан
([TESTREPORT.md](TESTREPORT.md)); [20] приёмочные испытания по приложению E —
проведены живым harness `scripts/acceptance.sh` (E.1–E.6, 21/21 PASS на реальных
процессах, протокол в `.dev/acceptance/PROTOCOL.txt`, сводка — TESTREPORT §4).
Открытые испытания нашли и починили реальный дефект (узел с PSK не стартовал —
TESTREPORT §6.2).

Нагрузочное тестирование [17.3]/[22 п.6] переехало в §2 «Реализовано
частично»: симулятор `internal/simulator` работает, прогон 100/500/1000 сделан,
отчёт [LOADTEST.md](LOADTEST.md) написан; не хватает живого кластера такого
размера (модель — не сеть).

---

## 5. План добивания (в порядке стоимости)

1. Нагрузочный профиль **на живом кластере** (100/500/1000 узлов): модель
   прогнана и задокументирована (§2, LOADTEST.md), но живая сеть такого
   размера не поднималась — ни TCP/ channel/ backpressure-чисел, ни реального
   CPU/ памяти профиля.
2. Ретрансляция результата по `route_stack`: переживание перезапуска
   промежуточного хопа (сейчас восстановлению помогает `parent_task_id`, но не
   полный стек).
3. Модель **задачей** [10.3/10.4]: сквозной атрибут от API/CLI до маршрута и
   выбор исполнителя по модели. Требует поля в `TaskEnvelope`/`TaskResult`,
   то есть несовместимого изменения протокола (решение заказчика — пока без
   него, см. «Выполнено»); конфигурация модели узла сделана.
4. Конфигурируемое пространство имён метрик (вопрос §6.2).
5. Планирование декомпозиции самим узлом (сейчас план приходит извне) —
   относится к PicoClaw/агенту, а не к mesh.
6. Бенчмарки (`make bench` объявлена, файлов-мишеней нет): замеры маршрутизации
   и кодирования для сравнения до/после.

Выполнено из прежнего списка:

- CI-конвейер [15.1 conveyor] — `.github/workflows/ci.yml` (7 задач: статика,
  сборка+cross, unit -race, интегра -race + count=2, живая приёмка E.1–E.6,
  родная arm64, docker-образ); гейты = TESTREPORT §5. Первый прогон — после
  пуша workflow (статус: зелёный/красный публикуется бейджем в README).
- Живые приёмочные испытания [20] (приложение E) — harness `scripts/acceptance.sh`
  на реальных процессах: E.1–E.6, 21/21 PASS; протокол —
  `.dev/acceptance/PROTOCOL.txt`, сводка и честная граница (wire-подписи
  покрыты интеграционно, не из bash) — TESTREPORT §4. Побочный эффект —
  найден и исправлен реальный дефект (узел с PSK не стартовал; §1 [7.1],
  TESTREPORT §6.2).
- Отчёт о тестировании [19.9] — [TESTREPORT.md](TESTREPORT.md): инвентарь
  (28 юнит-файлов / 202 теста + 18 интеграционных + model-load), маппинг на
  17.1–17.4, гейты с кодами выхода, три найденных дефекта, список
  непроверенного.
- Тематический P2P-топик поиска (ТЗ 6.9.5 шаг 5) — `discovery.SearchTopic`
  на разделённом с membership GossipSub-роутере, подписанные request/reply,
  skills-only (конфиденциальность по построению), Adopt-верификация ответов,
  анти-шторм-ограждения; по умолчанию выключен (§1 [6.9.5 шаг 5],
  PROTOCOLS §3.5).
- OpenTelemetry (ТЗ 14.3) — спаны конвейера задач и перенос контекста в
  подписанных labels (§1 «Наблюдаемость»); живой экспорт в collector не
  проверялся — см. §2.
- Проверка arm64 (ТЗ 15.1) — кросс-сборка + полный прогон тестов под
  qemu-aarch64, отдельно интеграционных (§1 «Сборка и развёртывание»).
- Формат логов ТЗ 14.1 (§1 «Наблюдаемость»): поля названы как требует ТЗ,
  `component` есть у каждой подсистемы, `task_id` — у каждой записи о задаче.
  Работа вскрыла ещё один дефект: живые записи содержали повторённый ключ
  (`host_started` — два `peer_id`), что для читателя JSON означает либо молча
  потерянное значение, либо неразбираемую строку.
- `security.Limiter.Wait` научился ждать: раньше метод пробовал `Allow` и
  возвращал `ctx.Err()`, отказывая там, где следовало подождать, и пропуская там,
  где токен успевал восстановиться, — проверялось тестом, зависевшим от скорости
  машины. Вызовов в проде у метода не было, поведение узла не изменилось.
- Ретраи делегирования больше не переспрашивают отказавшего соседа (§1, [6.12.1]).
- Плановые триггеры [6.6.1 п.4] (§1): четвёртый источник задач — cron + задача,
  собственные парсер и планировщик (без сторонних библиотек), два семейства
  расписаний (закреплённые в `node.yaml` с горячей перезагрузкой и хранимые через
  API/CLI), статус/эндпоинты/CLI, e2e-сценарий на живом mesh.
- Средства массового развёртывания [15.4]/[22 п.7] (§1): `deploy/ansible` —
  роль со всеми шестью шагами ТЗ, `deploy.yml`/`collect_peerids.yml`, примеры
  инвентаря и vault; синтаксис и `ansible-lint --profile production` чисты.
  Оговорка: живой многохостный прогон не выполнялся (см. §2).
- Модель как конфигурация узла [10.3/10.4] (§1): `picoclaw.model`
  → `--model` в режиме `binary`, раскрытие в статусе и локальном журнале.
  Выбор модели **задачей** сознательно не делался по решению заказчика — он
  требует несовместимого изменения протокола (`ProtocolVersion` 0.1.0 → 0.2.0);
  оговорённая асимметрия: режим `http` модель не выбирает.
- Аудит процесса разработки против ТЗ (§17.1, §18–§22) и два найденных им
  дефекта. Проверка покрытия шести пунктов 17.1 нашла две дыры:
  `internal/storage` не имел тестов вовсе («работа с локальным хранилищем»),
  `tasks.Validate`/`CheckRoute` не проверялись напрямую («валидация задач»).
  Их закрытие вскрыло: (1) вторичный индекс навыков `s:<skill>/<peer>` был
  односторонним — `PutPeer` только дописывал ключи и никогда не снимал их, а
  `Prune` их не касался, то есть снятый навык навсегда оставлял пира
  «находимым» для него; плюс имена индексировались без нормализации, тогда как
  весь mesh сравнивает навыки в нижнем регистре и без пробелов, и пустое имя
  создавало мусорный ключ, совпадающий с любым префиксным запросом (§1 «Хранилище»,
  §7 ARCHITECTURE). (2) `Validate` склеивал причины в одну строку через
  `errors.New`, поэтому отказ нельзя было классифицировать через `errors.Is` —
  теперь возвращается `ValidationError`, перечисляющая все нарушения и
  разворачивающаяся в них. Попутно: пустая по смыслу (пробельная) инструкция
  отвергается на границе, а не после занятия слота исполнения. Влияние на
  живую сеть у (1) отсутствует — маршрутизация читает навыки из
  `Membership`/`Table`/реестра, а не из этого индекса; речь о негодном
  публичном API и персистентном мусоре на диске. Проверка §19.8 (описание
  конфигурационных параметров): все 123 ключа `yaml` упоминаются в
  документации/шаблонах; два ранее не описанных (`security.require_origin_signature`,
  `picoclaw.workspace_root`) добавлены в `configs/node.yaml` и README,
  недостающие переменные — в DEPLOYMENT. Остальное по §17.3/19.9/20/22 п.6–7
  не сделано и отмечено в §4.
- Интеграционные сценарии ТЗ 17.2 и спец. проверки 17.4 (§1 «Тесты»,
  `internal/node/integration_test.go` — живые libp2p-узлы в одном процессе под
  тегом `integration`). Их написание вскрыло и исправило три дефекта: машинный
  класс ошибки терялся при чтении из журнала; результат принимался от любого пира,
  знающего `task_id` (теперь — только от того, кому задача реально передавалась);
  доверие копировалось в таблицу соседей, где нулевое значение оказывалось
  максимальным уровнем, — частичная запись (gossip, реестр, обновление навыков)
  повышала незнакомца до `trusted` и закрепляла это на диске (теперь таблица
  доверия не хранит вовсе, §6 ARCHITECTURE).
- Декомпозиция/агрегация подзадач (§1, [6.9.1] и [6.10.5]), исполнение лимитов
  ресурсов (§1, [6.8.4]), RTT-пинг в maintenance-цикле (§1, [6.4.3]),
  админ-эндпоинты `reload-config`/`leave` (§1, [13.1.1]), батчинг full-sync gossip
  (§1, [6.5.3] — `PublishFull` пачками по 32 с паузой `heartbeat/8`), ротация и
  отзыв ключей (§1, [11.2 п.3–4]), обмен навыками с автообновлением (дополнение к
  ТЗ по запросу), `Makefile` и dev-кластер (§1, [15]), тесты реестра навыков,
  переходов ключей и их доставки по gossip (§1 «Тесты»).

---

## 6. Открытые вопросы к заказчику

1. Допустимо ли сохранение `worker_signature` в журнале как единственной
   длинной гарантии (цепочка hops не переносится полностью).
2. Требуется ли строго `picomesh_*` в именах метрик или достаточно
   конфигурируемого namespace.
3. Ожидается ли SQLite как обязательный движок (сейчас BadgerDB, плагин
   `storage.engine` объявлен, но не переключаем).
