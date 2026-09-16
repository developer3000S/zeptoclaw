# Отчёт о тестировании (ТЗ §19.9)

Документ — отчёт о тестировании системы ZeptoClaw / ZeptoMesh Agent Mesh:
инвентарь тестов, маппинг на требования §17 ТЗ, прогоны, гейты с фактическими
кодами выхода, дефекты, найденные тестированием, и честный список того, что
осталось непроверенным. Место в общем статусе — [STATUS.md](STATUS.md);
нагрузочная симуляция вынесена в [LOADTEST.md](LOADTEST.md), живая приёмка —
в раздел 4 этого отчёта (протокол `[20]`/приложение E).

## Как воспроизвести

```bash
export PATH=/usr/local/go/bin:/root/go/bin:$PATH
make fmt-check vet proto-check      # статика + соответствие gen/ протоколу
make build cross                    # linux/amd64 + linux/arm64
make test-race                      # весь юнит-набор под -race
make test-integration               # интеграционные сценарии ТЗ 17.2/17.4 под -race
make test-load                      # симуляция 100/500/1000 узлов → load-test-output.txt
./scripts/acceptance.sh             # живые приёмочные испытания E.1–E.6 (ТЗ §20)
./scripts/acceptance.sh E.4         # один сценарий
```

Повторять интеграционный набор несколько раз (`-count=2`) — осознанная
практика этого проекта: ровно так были пойманы две настоящие гонки (раздел 6).
Юнит- и интеграционный наборы не следует запускать параллельно друг с другом:
на 4 ядрах конкуренция проявлялась как ложные задержки сетевых тестов.

## 1. Модульные тесты (ТЗ 17.1)

Требование 17.1 | Где проверяется
---|---
Валидация задач | `internal/tasks/validation_test.go` — по каждой причине отказа `Validate`/`CheckRoute`/`AppendRoute` напрямую, таблица `SkillsMatch`, round-trip через protobuf; `internal/tasks/errors_test.go` — типизация отказов (`errors.Is`), `internal/tasks/journal_status_test.go` — `ParseStatus` как точная инверсия `String`
Подпись сообщений | `internal/security/security_test.go` — roundtrip задачи/результата/capabilities, отказ unsigned, отказ чужому подписанту; `internal/security/skills_sync_test.go` — подпись `skills_sync` и `origin_signature` (переживает переписывание маршрута ретранслятором, отвергает подмену содержимого); `internal/security/rebind_test.go` — двойная подпись rebinding-цепочек, отзыв, replay/rewind; `internal/wire/canonical_test.go` — каноническое кодирование digest'ов (task/result/caps/search)
Таблица соседей | `internal/routing/table_test.go` — веса, filter по навыкам, eviction, учёт отказов; `internal/discovery/gossip_rebind_test.go` — доставка заявлений по живому libp2p; `internal/discovery/searchtopic_test.go` — приёменные правила темы поиска
Локальное хранилище | `internal/storage/storage_test.go` (20 тестов) — dedup-притязание, TTL-окно, журнал задач/результатов, retention, артефакты
Обработка ошибок | `internal/tasks/errors_test.go`; `internal/picoclaw/cli_test.go` — отказ бинарника/таймаут/битый вывод адаптера; `internal/logging/logging_test.go` — redaction секретов в логах
Дедупликация | `internal/storage` (ClaimDedup + окно) и сквозной сценарий 17.2.7 в интеграционном наборе (`TestIntegrationDuplicateDeliveryIsNotExecutedTwice`); `internal/tasks/search_relay_test.go` — `dedupRecords`: слияние partial-ответов поиска

Всего инвентарь: **28 юнит-файлов, 202 тест-функции** (счёт `^func Test`, без
учёта табличных подтестов; интеграционные и load-файлы — отдельно ниже).
Покрытие юнит-набором — **56.9 %** операторов (`go tool cover`; по пакетам:
triggers 88.1 %, storage 81.4 %, telemetry 78.6 %, skills 77.9 %, tasks 42.4 % —
остальное добирают интеграционные прогоны на живом libp2p, которым покрытие
unit-прогоном не считается).

Прочие юнит-файлы: `internal/config/config_test.go` (+ `searchtopic_test.go`) —
дефолты, разбор, валидация, Diff/restart; `internal/api/triggers_test.go` —
HTTP-слой CRUD триггеров и авторизация; `internal/skills/registry_test.go` —
реестр навыковых дескрипторов; `internal/tasks/{search_relay,result_binding,
model_journal,decomposition,tracing}_test.go` — лестница поиска, привязка
ответа к делегату, журнал модели, декомпозиция, OTel-спаны on in-memory
рекордере; `internal/telemetry/telemetry_test.go` — OTel-API-мост без SDK;
`internal/p2p/{service_test.go,host_psk_test.go}` — фрейминг/RPC поверх живых
хостов и PSK-поведение (см. раздел 6); `internal/simulator/load_fast_test.go`
(13) — инварианты модели нагрузки без тега `load`.

## 2. Интеграционные тесты (ТЗ 17.2)

`internal/node/integration_test.go`, тег `integration`, живой libp2p
(in-process), `make test-integration`. Маппинг восьми обязательных сценариев:

№ 17.2 | Сценарий | Тест
---|---|---
1 | Два узла обнаруживают друг друга локально | `TestIntegrationTwoNodesDiscoverEachOther`
2 | Три узла образуют сеть | `TestIntegrationThreeNodeMeshAndTaskRouting`
3 | Задача с узла А исполняется узлом Б | `TestIntegrationThreeNodeMeshAndTaskRouting` (+ `TestIntegrationTaskFailsWithReasonWhenNoExecutorExists` — отрицательная ветка)
4 | Результат возвращается узлу А | тот же тест: `completedFrom(a,…,b)` + проверка подписи исполнителя
5 | Узел падает во время задачи | `TestIntegrationWorkerDiesMidTask` — TIMEOUT с названной причиной, без ожидания TTL
6 | Делегирование через промежуточный узел | `TestIntegrationDelegationThroughIntermediateNode` — ручная топология A—B—C, `route_stack` без исполнителя, ретрансляция в журнале B
7 | Повторная отправка не исполняется дважды | `TestIntegrationDuplicateDeliveryIsNotExecutedTwice`
8 | Истёкший TTL не распространяется | `TestIntegrationExpiredTTLDoesNotSpread`

Сверх обязательного — те же интеграционные прогоны закрывают ТЗ 17.4, 6.12.1,
6.6.1 п.4, 14.3 и 6.9.5 п.5:

Требование | Тест
---|---
17.4.1 задача без подписи отвергается; 17.4.2 с переписанным содержимым (digest пересчитан, транспортная подпись наведена) — по авторской подписи | `TestIntegrationUnsignedAndRewrittenTasksAreRefused`
17.4.3 неизвестный узел не получает задач при `limited` (и немедленно начинает после решения оператора) | `TestIntegrationTaskAdmissionFollowsTrustPolicy`
17.4.4 заблокированный узел не может подключиться (ни соединения, ни потока, в обе стороны) | `TestIntegrationBlockedPeerCannotConnect`
17.4.5 реле не читает содержимое: ни один канал узла вне пути задачи не раскрывает instruction (protobuf-байты gossip/capabilities/PEX/skill-lookup, журнал, статус, метрики) | `TestIntegrationTaskContentStaysOffTheServicePlane`
17.4.6 превышение rate-limit → отказ с причиной, принятые задачи исполняются | `TestIntegrationRateLimitRefusesExcessTasks` (+ юнит `TestLimiter*`)
6.12.1 перманентно отвергающий кандидат не спрашивается повторно | `TestIntegrationRetryAsksAFreshPeerAfterARefusal`
6.6.1 п.4 planned trigger: cron → живой планировщик → submit → маршрут → подписанный соседом результат | `TestIntegrationScheduledTriggerInjectsATask`
узел, молча живущий без heartbeat, не вытесняется раньше failure_timeout | `TestIntegrationSilentButLivingPeerSurvivesEviction`
14.3 сквозной трейс через реальный hop (`submit→receive→execute` в одном трейсе) | `TestIntegrationTraceContinuesAcrossNodes`
6.9.5 п.5 эпидемическая тема поиска находит незнакомый узел A—B—C (позитив) | `TestIntegrationSearchTopicFindsUnacquaintedWorker`
он же, негативный контроль: с выключенной темой исполнитель не находится | `TestIntegrationSearchTopicStaysSilentWhenDisabled`

Итого интеграционный набор: **18 тестов** × `count` прогонов.

## 3. Нагрузочное тестирование (ТЗ 17.3, 22 п.6)

Модель — `internal/simulator`: тот же membership/routing-протокол, детерминированная
сеть 100/500/1000 узлов (`TestLoadSim`, тег `load`, `make test-load`). Полный
отчёт с таблицами, границами модели и погрешностью аналитической ретрансляции —
**[LOADTEST.md](LOADTEST.md)**; здесь — итог.

Из пяти проверяемых величин 17.3 измерены все пять. Вердикт по нормативу
трафика 16.4: **НЕ выполнен** — служебный приём в покое 678.6 Кбит/с/узел при
100 узлах и 8388.8 при 500 против потолка ≤100 (доминирует `PublishFull`,
85–86 %); 1000 узлов — тот же порядок (≈2,5·10³, оценка: полный прогон этого
размера не завершился за отведённое время). Членство стабильно, обнаружение —
медиана 1.3 heartbeat, маршрутизация — 2–3 heartbeat, связность графа при
отказе 10 % проседает до 0.79 и восстанавливается до 1.0.

Граница честности: это **модель**, а не живая сеть: нет TCP/TLS-рукопожатий,
задержки канала, backpressure, CPU/памяти/GC и криптоопераций; там, где метрика
неизмерима, в отчёте стоит «не измеряется данной моделью».

## 4. Живые приёмочные испытания (ТЗ §20, приложение E)

`scripts/acceptance.sh` — harness на реальных ОС-процессах `zeptomesh-node`
(свой data_dir, порты, unix-сокет-реестр; PicoClaw — stub). Полный прогон
**2026-09-16: 21/21 PASS, FAIL=0**; протокол с дословными выводами —
`.dev/acceptance/PROTOCOL.txt` (каталог в `.gitignore`, ключевые строки
продублированы здесь).

Сценарий | Проверяет | Итог
---|---|---
E.1 (4 проверки) | запуск одного узла, генерация идентичности (права 600), loopback-only слушатели, локальная задача COMPLETED | PASS
E.2 (3) | знакомство двух процессов через реестр unix-сокетов, обмен возможностями (`/api/v1/skills.peers` содержит навык соседа), различность peer id | PASS
E.3 (2) | делегирование соседу: навыка нет у инициатора — задача исполнена соседом (`worker` == id соседа), результат подписан исполнителем (`worker_signed`) | PASS
E.4 (4) | многопрыжковая цепочка n0→n1→n2 (реестр/gossip/DHT выключены, знакомство только bootstrap-списками): n0 знает ровно 1 узел; исполнитель за два ребра; `route_stack`=[n0,n1] без исполнителя; журнал n1 фиксирует ретрансляцию (`delegated_to`∋n2, n1 не исполнитель, origin цел) | PASS
E.5 (3) | SIGKILL единственного носителя навыка: задача на мёртвый навык завершается FAILED с внятной причиной («no neighbour advertises skills [summarize]»), живая часть сети продолжает исполнять | PASS
E.6 (5) | PSK-граница: узлы с разными PSK не соединяются (neighbors=0) и задача через границу не исполняется; API: 401 без bearer, 200 с токеном; секрет не попадает в логи узла | PASS

Граница честности E.6 п.1: «поддельный конверт на wire-потоке» из bash
проверяется только на уровне PSK-границы и API-авторизации — raw-клиент
libp2p-потока из shell недоступен; прямое покрытие — интеграционный
`TestIntegrationUnsignedAndRewrittenTasksAreRefused` (раздел 2), и протокол
приёмки говорит об этом явно.

## 5. Гейты: фактические результаты

Команда | Результат
---|---
`make fmt-check vet` | зелёный (gofmt-пусто, `go vet ./...`)
`make proto-check` | «gen/ is up to date with api/proto/…/mesh.proto»
`go build ./...`, `make build`, `make cross` | зелёный; linux/amd64 + linux/arm64 собраны
`go test -count=1 -race -timeout 900s ./...` | EXIT=0 (все пакеты)
`make test-integration` (race, count=1) | EXIT=0 (node+security)
`go test -count=2 -tags=integration …` (без race, стресс повторов) | EXIT=0 после исправлений раздела 6
`make test-load` (100/500) | EXIT=0; 1000 — не уложился в 60-минутный timeout цели, idle-замер снят отдельным прогоном (LOADTEST.md)
`./scripts/acceptance.sh` | 21/21 PASS, exit 0

## 6. Дефекты, найденные тестированием

Тестирование этого цикла нашло и починило три реальных дефекта продукта
(не тестов):

1. **Воскрешение статуса журнала** (найдено падающим интеграционным прогоном
   при конкурентных наборах): пост-ack бухгалтерия `forward()` успевала
   записать `FORWARDED` в уже закрытую быстрым исполнителем `COMPLETED`-запись —
   журнал противоречил хранящемуся результату. Фикс: `ParseStatus` (точная
   инверсия `String`, непонятное написание — нетерминально), `journalMu`
   (сериализация read-modify-write) и страж `Terminal()` в не-терминальных
   писателях. Регрессия: `internal/tasks/journal_status_test.go`.
2. **Узел с PSK не запускался вообще** (найдено живым E.6): `libp2p.New` под
   `PrivateNetwork` падает на построении QUIC-транспорта («QUIC doesn't support
   private networks yet», go-libp2p v0.49). Фикс в `internal/p2p/host.go`: PSK
   раскодируется до выбора транспортов; при PSK хост строится TCP-only,
   quic-v1/webtransport-адреси из `listen`/`announce` отбрасываются с warn
   `quic_disabled_private_network`; без ни одного TCP-адреса — внятная ошибка
   старта. Документировано в DEPLOYMENT/README/config. Регрессии:
   `internal/p2p/host_psk_test.go` (TCP-only под PSK; пиры с одной PSK
   соединяются, с разными — нет).
3. **Тайминг-зависимые проверки переходных состояний** (найдено `-count=2`):
   тест делегирования и приёмочный E.4 ждали из журнала транзитные
   `FORWARDED`/`DelegatedTo`, которые быстрый исполнитель закрывает терминальным
   `COMPLETED` раньше наблюдения (а `DelegatedTo` дописывается асинхронной
   бухгалтерией позже терминала). Продуктовская семантика верна (терминальная
   запись — истина), поэтому проверки переведены на осевшие инварианты
   (делегирование видно по `delegated_to`, исполнитель — не relay, origin цел;
   статус допускается обоими) — assertions не ослаблены, а сделаны
   независимыми от порядка двух корректных интерливингов. Отдельно тема поиска
   в тесте переведена с фиксированного сна на ожидание факта (probe-запрос до
   первого ответа): на загруженной машине GRAFT не влезал в 2 с.

Дефекты harness-слоя (pid двойного форка, коллизии портов от осиротевших
процессов, имена полей JSON `task.status`/`result.error`) найдены и исправлены
в `scripts/acceptance.sh`; они не касались продукта.

## 7. Что осталось непроверенным

- **Живой кластер 100/500/1000 узлов**: смоделировано, не поднималось.
  Абсолютные числа трафика/CPU на реальной сети будут другими (модель без
  channel/backpressure/crypto); вердикт 16.4 — только модельный.
- **Точный обход full-sync при 1000 узлов**: погрешность аналитической
  ретрансляции замерена до 300 узлов (+58 % байтов); наружу не экстраполирована.
- **OTel-экспорт в живой коллектор**: API-мост, спаны и injection покрыты
  in-memory рекордером и интеграционным hop-тестом; HTTP-экспорт в конкретный
  бэкенд не проверялся.
- **arm64-оборудование**: кросс-сборка зелёная; локальных прогонов на arm64-машине
  не было. С настройкой CI закрыто частично: задача `arm64` в
  `.github/workflows/ci.yml` гоняет сборку и тестовый набор на родном
  arm64-раннере (ubuntu-24.04-arm) — первый зелёный прогон конвейера будет
  тем самым подтверждением; `-race` на arm64 остаётся недосягаем (эмуляция на
  x86, см. STATUS §1 [15.1]).
- **Реальный бинарник PicoClaw**: интеграция покрыта контрактным CLI-тестом на
  стабах и fixtures (`internal/picoclaw/cli_test.go`), живым PicoClaw не
  прогонялась.
- **E.6 п.1 в raw-wire форме** (поддельный конверт, собранный вне libp2p-стека)
  — покрыт in-process на живом libp2p, из shell не повторяем.
- CI-конвейер [15.1 conveyor] настроен (`.github/workflows/ci.yml`, гейты =
  раздел 5 этого отчёта; статус прогонов — на вкладке Actions репозитория).
  `make bench` объявлена, но файлов-мишеней нет — замеры маршрутизации/
  канонизации не проводились и в конвейере не участвуют.
