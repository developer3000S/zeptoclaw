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
| [7.1] Приватная сеть (PSK) | `node.private_network_psk`, `decodePSK`, `libp2p.PrivateNetwork` |
| [6.3.3.4] Circuit relay v2: опциональность, лимиты реле, приоритет прямого соединения | `host.go`: `EnableRelayService` с `relayServiceOptions` (ёмкости и лимиты из `node.relay.limit`), `EnableAutoRelayWith{StaticRelays,PeerSource}`; `DisableRelay()` при выключенной опции; ретрансляция включается libp2p только когда прямого пути нет |
| [7.5] Protobuf поверх libp2p-потоков, length-prefixed фреймы, потолок кадра | `api/proto/zeptomesh/v1/mesh.proto`, `internal/p2p/framing.go` (`MaxFrameBytes = 8 MiB`), `security.max_message_bytes` |

### Обнаружение [6.3]

- [6.3.1] Реестр Unix-сокетов со-located-узлов: `internal/discovery/local.go`
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
  `expireLoop` по `failure_timeout`).
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
  канонические энкодерыdigest/подписи — `internal/wire/canonical.go`.
- [6.6.5] Дедупликация: `storage.ClaimDedup` (окно `tasks.dedup_window`),
  `ACK DUPLICATE` без повторного исполнения.
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
- [6.4.3] RTT соседей измеряется регулярно: `measureRTT` в maintenance-цикле
  (`internal/node/node.go`) пингует до 8 connected-соседей каждые 15 с и
  складывает выборку в скользящее среднее `Table.RecordRTT` — компонент
  `latency_score` в скоринге перестаёт быть нейтральным.
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
  (`open|limited|private`), allow/deny-файлы, `Observe` без понижающих
  демущений, `AllowConnection` (используется `ConnectionGater`),
  `AllowTasksFrom`, `AllowDelegationTo`.
- `internal/p2p/gater.go`: `ConnectionGater` блокирует соединения с
  заблокированными пирами на уровне транспорта (не только задач).
- `internal/security/audit.go`: ведра rate-limit (`Limiter`) + журнал
  безопасности JSONL (`security.Audit`, `<data_dir>/audit/security.jsonl`).
- [11.4] Опасные операции по умолчанию выключены: `allow_shell: false` в
  дефолтах, пересечение прав (`canExecute`, `DeriveSubtask`).

### Хранилище [9]

`internal/storage/storage.go` — BadgerDB v4: задачи (`t:`), результаты (`r:`),
пиры (`p:`), дедуп (`d:`), связи parent/child (`s:`, `LinkChild/ChildrenOf`),
meta/claim (`c:`), `Prune(keep)`, content-addressed артефакты sha256
(`StoreArtifact/LoadArtifact/ArtifactPath`).

### Наблюдаемость [14]

- Логи: `log/slog`, JSON-хендлер, обязательный контекст (`node_name`,
  `peer_id` в `node.New`, `component` у API), мост stdlib→slog
  (`logging.BridgeStdlib`).
- Метрики: `internal/metrics` — полный набор обязательных метрик ТЗ 14.2.1
  (+ дедуп/таймауты/security/relay); приватный реестр (безопасно N узлов в
  одном процессе).
- [14.2] Выделенный Prometheus-слушатель: `telemetry.prometheus_listen` +
  `metrics.Collector.ServeMetrics` (`internal/metrics/serve.go`), `/healthz`
  на том же порту; поднимается в `cmd/zeptomesh-node/main.go`.

### Управление [12/13]

- HTTP/JSON админ-API (`internal/api/admin.go`): `GET /healthz`,
  `GET /api/v1/{status,peers,capabilities,tasks,tasks/{id},config}`,
  `POST /api/v1/tasks` (в т.ч. план декомпозиции в поле `subtasks`),
  `POST /api/v1/tasks/{id}/cancel`, `DELETE /api/v1/tasks/{id}` (тот же обработчик —
  форма, ожидаемая ТЗ), `POST /api/v1/tasks/{id}/resubmit`,
  `POST /api/v1/admin/reload-config`, `POST /api/v1/admin/leave`, `GET /metrics`;
  bearer-токен через `api.auth_token_env`.
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
  `resubmit`, `reload`, `leave`.
- Конфигурация [12.1]: `internal/config` — дефолты, YAML-слияние,
  `${VAR:-default}` (+вложенные ссылки, `config_test.go`), `Validate()` с
  выводом относительных путей и `applyEnvOverrides` (`ZETOMESH_BOOTSTRAP`).

### Сборка и развёртывание [15/22]

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
- `LICENSE` — полный текст Apache-2.0 (совпадает с
  `https://www.apache.org/licenses/LICENSE-2.0.txt`).

### Тесты (покрытие текущее)

| Файл | Что проверяет |
|---|---|
| `internal/wire/canonical_test.go` | стабильность канонического кодирования, domain separation, независимость content-digest от полей маршрута |
| `internal/security/security_test.go` | генерация/загрузка идентичности, подписи task/result/caps/ack, отказ неподписанным и подписанным не тем узлом, trust-политика, limiter, audit JSONL |
| `internal/p2p/service_test.go` | in-process libp2p: ответы RPC/задач/результатов на малых и больших фреймах (регрессия на premature stream reset), отказ oversized-кадра и здоровье сервера после него, нотификаторы соединений |
| `internal/config/config_test.go` | expandEnv (вложенные default'ы), `ZETOMESH_BOOTSTRAP`, раскрытие поставляемого шаблона |
| `internal/tasks/search_relay_test.go` | навыки-покрытие, дедуп PeerRecord, wire-roundtrip SkillLookup (full_refresh/relay_budget/visited) |
| `internal/tasks/decomposition_test.go` | декомпозиция/агрегация сквозняком на живом узле: план → дети через обычный конвейер → один подписанный итог с digest'ом и порядком плана; журнал parent/child; ретрай retryable-ребёна ровно один раз и оседание non-retryable без ретрая; отказ заведомо неверных планов (размер, пустая инструкция, `allow_subtasks=false`, исчерпанный TTL) |

---

## 2. Реализовано частично

| Пункт ТЗ | Что есть | Чего не хватает |
|---|---|---|
| [6.9.1] Планирование разбиения | исполнение плана готово (§1): дочерние конверты, маршрутизация, агрегация | сам план по-прежнему приходит извне (API/CLI/журнал); автоматического «разбей инструкцию на N подзадач» по метаданным навыков нет — это функция PicoClaw/агента, а не mesh'а; `pb.SubtaskSpec` в proto не используется (план живёт в `tasks.SubtaskRequest`) |
| [6.5] SWIM-подобный прямой опрос | детектирование отказов по `failure_timeout` + конн-нотификер + быстрый `OnPeerDisconnected` для задач | подозрительный пир не опрашивается адресно перед исключением |
| [6.5.3] Gossip full-sync | `PublishFull` | ограничен 64 состояниями (`maxBatch`): в сети 100+ это частичная синхронизация |
| [14.1] Поля логов | `ts/level/peer_id/component` (+`msg`) | отдельного поля `event` нет (событие в `msg`); `task_id` присутствует не во всех событийных строках |
| [14.3] OpenTelemetry | сквозной `task_id` в метриках/логах/конвертах | библиотечного трейсинга нет (пункт ТЗ сформулирован как рекомендация) |
| [6.12.1] Ретраи | `max_retries`, `retry_interval`, повторный выбор кандидатов | параметр `exclude` у `Table.Select` есть, но вызывающий код (`manager.forward`) передаёт `nil` — отказавшие соседи могут выбираться снова |
| [15.1] Платформы | amd64 собирается и проверяется | arm64 не проверялась; CI-конвейера нет |
| [17] Тесты | юнит + in-process транспорт | нет интеграционных сценариев ТЗ 17.2 (2–3 узла: обнаружение, делегирование, отказ узла), нагрузочных 17.3, спец. проверок безопасности 17.4 |
| [15.4] Массовое развёртывание | `install.sh` (локально/Docker, N экземпляров) | Ansible-плейбуков нет; роль «100+ серверов» закрыта только на одном хосте |
| [6.8.4] Ограничения ресурсов | `max_parallel_tasks_per_peer` (счёт задач пира в `OnTask`), `min_free_disk_bytes` (`statfs` перед `prepareSandbox`), `max_workspace_bytes` (квота при сборе артефактов), `max_task_memory_bytes` (`RLIMIT_AS` + группа процессов с `Pdeathsig`), `allow_shell`/`allow_network_tools` — узел как исполнитель не принимает задачу, требующую больше, чем он разрешает (`canExecute`), и передаёт права в `picoclaw.Request` | «отключение сети/оболочки» остаётся административным ограничением, а не техническим: дочерний процесс PicoClaw не помещается в network namespace и не теряет привилегий — для жёсткой границы нужен контейнер/песочница на узле; квота workspace считается при сборе результатов, а не удерживается файловым лимитом ядра; `ulimit -v` — best-effort (оболочка может не поддержать, тогда ограничение не применяется) |
| [10.3/10.4] PicoClaw | три режима адаптера (см. PICOCLAW-INTEGRATION.md) | `capabilities.models` не прокидывается в `Request.Model`; мультиагентность сведена к очереди одного адаптера |

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
   при этом ведётся через `worker_signature`.

---

## 4. Не реализовано

| Пункт ТЗ | Комментарий |
|---|---|
| [11.2 п.3–4] Ротация и отзыв ключей | новых идентификаторов/томстоунов нет; смена ключа = новый Peer ID вручную |
| [6.6.1 п.4] Запланированные триггеры задач | нет cron-механизма |
| [6.9.5 шаг 5] Поиск через тематический P2P-топик | расширение поиска сделано адресным RPC fan-out'ом (search-relay), отдельного search-топика нет |
| [17.3] Нагрузочное тестирование 100/500/1000 узлов и отчёт | нет симулятора/бенчмарков |
| [19.9] Отчёт о тестировании | нет |
| [20] Приёмочные испытания по приложению E | не проводились, отчёта нет |
| [22 п.6–7] Отчёт о нагрузочном тестировании; Ansible | нет |

---

## 5. План добивания (в порядке стоимости)

1. Ретрансляция результата по `route_stack`: переживание перезапуска промежуточного
   хопа (сейчас восстановлению помогает `parent_task_id`, но не полный стек).
2. Активный опрос подозрительного пира перед исключением (SWIM-подобно) —
   детект отказа сегодня пассивный.
3. Разделение full-sync gossip (батчинг пагинацией) для 100+ узлов.
4. Интеграционные сценарии ТЗ 17.2 и 17.4 (в т.ч. два узла в одном процессе).
5. Нагрузочный профиль + отчёт [22 п.6], Ansible-роль [15.4].
6. Ротация/отзыв ключей [11.2].
7. OpenTelemetry-трейс (span на задачу, task_id → trace correlation).
8. Передача `exclude` в `Table.Select` при ретраях `forward` [6.12.1].
9. Планирование декомпозиции самим узлом (сейчас план приходит извне) —
   относится к PicoClaw/агенту, а не к mesh.

Выполнено из прежнего списка: декомпозиция/агрегация подзадач (§1, [6.9.1] и
[6.10.5]), исполнение лимитов ресурсов (§1, [6.8.4]), RTT-пинг в maintenance-цикле
(§1, [6.4.3]), админ-эндпоинты `reload-config`/`leave` (§1, [13.1.1]).

---

## 6. Открытые вопросы к заказчику

1. Допустимо ли сохранение `worker_signature` в журнале как единственной
   длинной гарантии (цепочка hops не переносится полностью).
2. Требуется ли строго `picomesh_*` в именах метрик или достаточно
   конфигурируемого namespace.
3. Ожидается ли SQLite как обязательный движок (сейчас BadgerDB, плагин
   `storage.engine` объявлен, но не переключаем).
