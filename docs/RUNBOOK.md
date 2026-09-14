# Эксплуатационное руководство (RUNBOOK)

Документ для оператора работающей сети ZeptoClaw Agent Mesh: управление
сервисами, мониторинг, диагностика типовых отказов, резервное копирование и
восстановление (ТЗ 19.6–19.7, 22.9). Развёртывание — в
[DEPLOYMENT.md](DEPLOYMENT.md).

Все пути по умолчанию: `$DATA_DIR = /var/lib/zeptomesh` (root-установка) или
`~/.local/share/zeptomesh` (user-установка). Экземпляр с номером `i` живёт в
`$DATA_DIR/i/`.

---

## 1. Управление

### 1.1. systemd (рекомендуемый путь, `install.sh --mode local`)

Установка создаёт **шаблон юнита** `zeptomesh@<i>`: один юнит-файл на все
экземпляры, экземпляр выбирается индексом.

```bash
./install.sh status                    # список экземпляров и их состояние
./install.sh start                     # запустить все установленные
./install.sh stop                      # остановить все
./install.sh uninstall                 # убрать сервисы и автостарт (данные остаются)

# разово, без install.sh:
systemctl status  zeptomesh@0          # root-установка
systemctl --user status zeptomesh@0    # user-установка
systemctl restart zeptomesh@1
journalctl -u zeptomesh@0 -f           # логи (user: journalctl --user -u …)
```

Автостарт включается при установке (`systemctl enable --now`); при загрузке
системы systemd сам поднимает все `zeptomesh@0..N` (юниты с `WantedBy=
multi-user.target` / `default.target`, перезапуск `on-failure`, `RestartSec=5`).

**Остановка одного экземпляра без остановки mesh** сетью переносится штатно:
`Stop()` узла публикует в gossip `status: "left"`, соседи исключают его из
видимого состава; задачи, шедшие через него, будут переотправлены
(`resubmit`) или упадут по TTL (см. §4).

### 1.2. Docker (`install.sh --mode docker`)

```bash
docker compose -f $DATA_DIR/docker/docker-compose.yml ps
docker compose -f $DATA_DIR/docker/docker-compose.yml logs -f zepto-0
docker compose -f $DATA_DIR/docker/docker-compose.yml restart zepto-1
docker compose -f $DATA_DIR/docker/docker-compose.yml up -d --force-recreate zepto-2
```

Автостарт обеспечивает `restart: unless-stopped` (контейнер вернётся после
перезагрузки хоста при активном docker-демоне). Публикация портов:
`mesh 4001+i (tcp+udp)`, `API 127.0.0.1:8081+i`, `metrics 127.0.0.1:9464+i`.

### 1.3. Ручной запуск (отладка)

```bash
export $(grep -v '^#' $DATA_DIR/0/instance.env | xargs)
zeptomesh-node run -config $DATA_DIR/etc/node.yaml
```

### 1.4. CLI против живого узла

```bash
zeptomesh-node status       -addr http://127.0.0.1:8081
zeptomesh-node peers        -addr http://127.0.0.1:8081
zeptomesh-node capabilities -addr http://127.0.0.1:8081
zeptomesh-node submit  -addr … -i "текст задачи" -skills ocr -ttl 5 -priority 5 -w
zeptomesh-node submit  -addr … -i "план" -subtask 'ocr|прочитать скан' -subtask 'coding|оформить таблицу' -w
zeptomesh-node tasks   -addr …            # журнал
zeptomesh-node get     <task_id> -addr …  # статус/результат
zeptomesh-node cancel  <task_id> -addr … -reason "…"
zeptomesh-node resubmit <task_id> -addr …
zeptomesh-node skills  -addr …            # свои навыки + выученный обзор по соседям
zeptomesh-node skill-set -addr … -name ocr -desc "…" -model gpt-x -attr gpu=1,lang=ru
zeptomesh-node skill-rm  ocr -addr …      # снять описание, имя остаётся объявленным
zeptomesh-node skills-sync -addr …        # немедленно подтянуть свежие описания
zeptomesh-node reload  -addr …            # hot-reload конфига (§1.5)
zeptomesh-node leave   -addr …            # штатный уход узла (§1.5)
zeptomesh-node rotate  -addr … -reason "…" # плановая смена ключа (§4.5)
zeptomesh-node revoke  -addr … -reason "…" # аварийный отзыв идентичности (§4.6)
zeptomesh-node rebinds -addr …            # журнал принятых переходов/отзывов
# при заданном api.auth_token_env добавьте -token-env ZETOMESH_API_TOKEN
```

`-subtask '<skills>|<instruction>'` — пункт плана декомпозиции (повторяемый);
родительская задача при этом ничего не исполняет сама, а только собирает детей
в один подписанный итог (ТЗ 6.9.1/6.10.5).

`skills`/`skill-set` описывают навыки, которые узел **уже объявляет** в
`capabilities.skills`: описание не даёт права исполнения и не может расширить
полномочия узла ни через CLI, ни через импорт от соседа (§4.7).

### 1.5. Горячая перезагрузка конфигурации и штатный уход (ТЗ 13.1.1)

Часть настроек применяется без остановки узла. Три равнозначных пути:

```bash
systemctl reload zeptomesh@0            # root-установка
systemctl --user reload zeptomesh@0     # user-установка (юнит имеет ExecReload=kill -HUP)
kill -HUP <pid>                         # ручной запуск
zeptomesh-node reload  -addr http://127.0.0.1:8081
curl -X POST http://127.0.0.1:8081/api/v1/admin/reload-config
```

Ответ содержит два списка — `applied` (то, что перестроилось на ходу) и
`requires_restart` (поля, для которых нужен перезапуск). Применяются без
перезапуска: `security.trust_mode`, `security.min_trust_for_tasks`, содержимое и
имена `security.allowed_peers_file` / `blocked_peers_file` (запись из файла
добавляется к текущему состоянию политики),
`security.rate_limit.requests_per_second` (вместе с `burst`),
`telemetry.log_level`.
Остальное (адреса прослушивания, PSK, навыки, лимиты задач, включение discovery,
весь блок `picoclaw` — включая `picoclaw.model`, ТЗ 10.3)
требует `systemctl restart zeptomesh@<i>` / `docker compose restart zepto-<i>`.

Сам файл конфигурации в рантайме не мутируется: узел перечитывает его с диска и
обновляет только владельцев с корректной синхронизацией, поэтому читатели видят
согласованный снимок. Горячая перезагрузка возможна только если узел запущен с
`-config` (без пути к файлу `reload` вернёт ошибку).

«Мягкий» уход узла из сети (перед обслуживанием), чтобы соседи узнали об этом
сразу, а не по истечении `failure_timeout`:

```bash
zeptomesh-node leave   -addr http://127.0.0.1:8081
curl -X POST http://127.0.0.1:8081/api/v1/admin/leave
```

`leave` публикует в gossip `PeerState{status: "left"}`, дожимает журнал и завершает
процесс кодом **75**. Это осознанный выбор: `Restart=on-failure` (systemd-юнит из
`install.sh`) и `restart: unless-stopped` (compose) трактуют ненулевой код как
сбой и поднимают узел заново — с перечитанным конфигом. Если узел поднимать не
надо, останавливайте его штатно (`systemctl stop zeptomesh@<i>`), а не `leave`.

---

## 2. Мониторинг

### 2.1. Точки сбора

| Что | Где |
|---|---|
| health (без аутентификации) | `GET http://<api>/healthz` |
| метрики Prometheus | `GET http://<api>/metrics` (админ-API, с токеном, если задан) и выделенный `telemetry.prometheus_listen` (в shipped-конфиге — `127.0.0.1:9464`; compose публикует `9464+i` внутри контейнера на `0.0.0.0`, наружу — только loopback хоста) |
| статус/состав | `GET /api/v1/status`, `GET /api/v1/peers` |
| журнал безопасности | `$DATA_DIR/<i>/audit/security.jsonl` |
| журнал задач | `GET /api/v1/tasks?status=FAILED` (BadgerDB) |

### 2.2. Логи: схема записи (ТЗ 14.1)

Узел пишет JSON-строки (stderr; `telemetry.structured_logs: false` даёт тот же
набор полей в key=value). Поля, которые есть у **каждой** записи:

| Поле | Смысл |
|---|---|
| `ts` | время записи (RFC 3339 с наносекундами) |
| `level` | `DEBUG`/`INFO`/`WARN`/`ERROR` |
| `event` | машинное имя события — то, по чему фильтруют (`task_delegated`, `caps_bad_signature`, …) |
| `message` | текст для человека; совпадает с `event`, пока вызывающий код не принёс собственный текст |
| `component` | подсистема: `node`, `transport`, `routing`, `discovery`, `tasks`, `skills`, `rebinds`, `api`, `picoclaw` |
| `peer_id` | id этого узла (добавляется в `node.New`) |
| `node_name` | имя из конфига |

`task_id` присутствует у каждой записи, которая касается конкретной задачи, —
от делегирования и исполнения до отказов транспорта и строк декомпозиции.
Записи libp2p и BadgerDB идут через тот же формат (`logging.BridgeStdlib`), их
`event` — текст библиотеки, а не имя события.

```bash
journalctl -u zeptomesh@0 -o cat | jq -c 'select(.component=="tasks" and .event=="delegation_rejected")'
journalctl -u zeptomesh@0 -o cat | jq -r 'select(.task_id=="01a0…") | "\(.ts) \(.component) \(.event)"'
```

### 2.3. Обязательные метрики (ТЗ 14.2.1) и алерты

Пространство имён — `zeptomesh_` (отличие от ТЗ `picomesh_` осознанно, см.
[STATUS.md](STATUS.md#3-известные-отклонения-от-тз)).

| Метрика | Тип | Смысл | Порог для тревоги |
|---|---|---|---|
| `zeptomesh_peers_total` | gauge | соседей в таблице | `< neighbors.min` долго |
| `zeptomesh_peers_connected` | gauge | из них с живым соединением | `= 0` |
| `zeptomesh_tasks_received_total` | counter | принято задач (всеми источниками) | — (для rate) |
| `zeptomesh_tasks_completed_total` | counter | успешно | доля падений |
| `zeptomesh_tasks_failed_total` | counter | с ошибкой | `increase(5m) >` базовой линии |
| `zeptomesh_tasks_running` | gauge | выполняется сейчас | `= max_parallel_tasks` длительное время |
| `zeptomesh_tasks_delegated_total` | counter | делегировано | — |
| `zeptomesh_task_duration_seconds` | histogram | длительность | p95 > таймаута |
| `zeptomesh_discovery_mdns_peers_found_total` | counter | находок mDNS | 0 при ожидаемом LAN |
| `zeptomesh_dht_lookups_total` | counter | DHT-запросов | взрыв (шторм поиска) |
| `zeptomesh_network_bytes_sent_total` / `_received_total` | counter | трафик | всплеск без задач |
| `zeptomesh_picoclaw_processes_running` | gauge | одновременных агентов | `= max_concurrent_agents` |

Дополнительно: `tasks_rejected/duplicate/timeout_total`,
`forward_attempts_total{outcome=…}`, `security_events_total{event=…}`,
`gossip_published/received_total`, `search_relays_total`,
`search_relay_hits_total`, `skill_full_refreshes_total`,
`task_route_hops`, `neighbor_score{peer}`, `build_info`.

Обмен навыками и ротация идентичностей:

| Метрика | Тип | Смысл |
|---|---|---|
| `zeptomesh_skills_version` | gauge | локальный epoch описаний навыков (растёт только при реальном изменении содержимого) |
| `zeptomesh_skills_synced_total` | counter | выполненных дельта-обменов `skills_sync` |
| `zeptomesh_skills_descriptors_imported_total` | counter | принятых дескрипторов от соседей |
| `zeptomesh_skills_sync_refused_total` | counter | отказов по политике раскрытия (в т.ч. запросов от незнакомцев) |
| `zeptomesh_identity_rebinds_applied_total` | counter | применённых заявлений о переходе/отзыве ключей |

Тревоги на них: рост `skills_sync_refused_total` при ожидаемом обмене — соседи
не проходят `disclose_to`; `identity_rebinds_applied_total` вне плановой ротации —
кто-то в сети меняет ключ (свериться с `zeptomesh-node rebinds`, §4.5/§4.6).

### 2.4. Промышленные алерты (пример для Prometheus)

```yaml
groups:
- name: zeptomesh
  rules:
  - alert: ZeptoMeshNoPeers
    expr: zeptomesh_peers_connected == 0
    for: 3m
  - alert: ZeptoMeshTaskFailures
    expr: rate(zeptomesh_tasks_failed_total[5m]) >
          0.2 * rate(zeptomesh_tasks_received_total[5m])
    for: 10m
  - alert: ZeptoMeshSecuritySignature
    expr: increase(zeptomesh_security_events_total{event=~".*bad_signature.*"}[15m]) > 5
  - alert: ZeptoMeshAgentSaturated
    expr: zeptomesh_picoclaw_processes_running >= 4
    for: 15m
```

Скрейпинг: `prometheus_listen` на каждый узел (в compose — порты `9464+i`
за loopback хоста, наружу не публикуются; скрейпить либо с того же хоста,
либо через `docker exec`/ssh-туннель — наружу метрики не выставлять).

---

## 3. Диагностика типовых проблем

### 3.1. Узел не стартует

1. `journalctl -u zeptomesh@0 -n 50` / `docker logs zeptomesh-0`.
2. Частые причины:
   - **порт занят** (`bind: address already in use`) → §3.2;
   - **битый ключ** (`security: … key`) → ключ `$DATA_DIR/<i>/keys/peer.key`
     повреждён: восстановить из бэкапа (§5), иначе узел потеряет идентичность;
   - **битый конфиг** (`config: parse/invalid …`) → правка шаблона/ENV;
     проверить `ZETOMESH_*` из `instance.env`;
   - **BadgerDB залочен** (`failed to acquire lock`) → второй процесс с тем же
     `db/` (ручной запуск поверх systemd-экземпляра). Убить процесс-двойник.

### 3.2. Конфликт портов

```bash
ss -ltnup | grep -E ':(4001|4002|8081|9464)\b'
```

Решение: сдвинуть базы (`ZETOMESH_BASE_MESH_PORT`, `…_API_PORT`, `…_PROM_PORT`
в `install.sh`), либо пересоздать юниты. порты экземпляров считаются от базы,
см. DEPLOYMENT §3.

### 3.3. Задачи не делегируются

- `GET /api/v1/peers`: есть ли связный сосед с нужными `skills`?
- `zeptomesh_forward_attempts_total{outcome="no_candidates"}` растёт — кандидатов
  нет: проверьте навыки (`capabilities.skills` исполнителя),
  `accept_external_tasks`, trust (`trust_mode`/`min_trust_for_tasks`), TTL
  задачи (`ttl<=1` не делегируется дальше).
- Сработал ли search-relay: `zeptomesh_search_relays_total`,
  `skill_full_refreshes_total`; в логе — `search_relay_widened_view`.
- Битый маршрут/часы: `security_events_total{event="gossip_rejected"}`.

### 3.4. Сеть распалась на острова

Каждый остров видит только себя (`peers_total` мал, bootstrap недоступен).
Лечится: поднять точку входа (постоянный узел из `discovery.bootstrap`) и
дать `bootstrap_interval`/PEX сделать своё; вручную — `zeptomesh-node peers`
для сверки. Профильтровать: `peer_joined via=…` в логах.

### 3.5. Узел «потерял» навыки (Select их не видит)

Навыки соседа котируются только после верификации подписанных `Capabilities`.
Симптом: в логах `caps_fetch_failed`/`caps_bad_signature`. Проверить:
время на узлах (расхождение > 300 с отбрасывает gossip), версии протокола,
целостность ключа соседа (сменили ключ — прежние подписи невалидны).

### 3.6. Взрыв трафика

- `gossip_published_total` + маленький `heartbeat` → поднять heartbeat/full_sync.
- DHT: `dht_mode: client` на временных узлах, `server` только постоянным.
- Поиск-релей: `search_relay.fanout`/`max_depth` вниз.
- `network_bytes_*` на конкретном peer — кандидат на `blocked_peers_file`.

### 3.7. Задача отвечена не той моделью (ТЗ 10.3)

Модель — свойство узла, а не задачи: сосед не может её попросить, и в протоколе
её нет. Где смотреть:

```bash
curl -s -H "Authorization: Bearer $TOKEN" $API/api/v1/status | jq .adapter
# {"name":"picoclaw-cli","healthy":true,"model":"llm-a"}
curl -s -H "Authorization: Bearer $TOKEN" $API/api/v1/tasks/<id> | jq .result.model
```

- `adapter.model` пуст → `picoclaw.model` не задан, выбирает PicoClaw сам
  (обычно по `PICOCLAW_CONFIG`).
- `model` в журнале задачи отличается от `adapter.model` → это раскрытое агентом
  `model_name`: он реально ответил другой моделью (свой fallback/маршрутизация).
- `adapter.model` задан, а в логах `picoclaw_model_not_applicable` → режим
  `http`: Pico Protocol поля выбора модели не имеет, настройку надо менять в
  конфигурации шлюза.
- Изменили `picoclaw.model` и сделали `reload` → блок `picoclaw` честно числится
  в `requires_restart`; без перезапуска процесса значение не применится.

---

## 4. Восстановление после сбоев (ТЗ 19.7)

### 4.1. Потеря одного узла

1. Определить: `systemctl status zeptomesh@<i>` / `docker ps`.
2. Первая линия — перезапуск (`install.sh start` или
   `systemctl restart zeptomesh@<i>`). Состояние (ключ, таблица соседей,
   журнал) переживает рестарт: `neighbor_table_restored` в логе.
3. Если диск/хост потерян — восстановить из бэкапа (§5) на новый хост и
   запустить (DEPLOYMENT §4). **Peer ID сохранится только при восстановлении
   `keys/peer.key`**; без ключа узел становится новым соседом для сети.
4. Задачи, которые узел выполнял, истекут по таймауту у инициатора и
   переотправляются (`resubmit`), дедуп не даст исполнить дважды.

### 4.2. Деградация большинства (потеря «quorum»)

Gossip-сеть формального quorum не имеет; однако при недоступности bootstrap и
DHT-серверов новый узел не присоединится. Действия:

1. Сохранить уцелевшие узлы (не перезагружать mesh целиком).
2. Поднять bootstrap с публичным адресом (WAN-профиль, DEPLOYMENT §5).
3. Островки сливаются автоматически: bootstrap + PEX + full-sync
   (`PublishFull`, пачками по 32 состояния с паузой `heartbeat/8`; заявления о
   переходе ключей идут первым батчем и переживают отказ остальных состояний).

### 4.3. Распад mesh (все видят только себя)

Причины и проверка: разные `private_network_psk` (PSK обязателен один на сеть
— сравнить `instance.env`), неверный `bootstrap` (нет `/p2p/<peer_id>`),
фаервол не пропускает QUIC udp, `mdns: false` там, где вход был только по mDNS.

### 4.4. Потеря/повреждение БД (BadgerDB)

Признак: ошибка лога при старте в `storage.Open`. Восстановление:
`rm -rf $DATA_DIR/<i>/db` + разворачивание копии из бэкапа; журнальные записи
о задачах — воспроизводимые данные, без них узел работает (теряется история
и дедуп-окно: повторная доставка старой задачи может исполниться заново —
принять это решение осознанно).

### 4.5. Ротация ключа узла (плановая)

Смена ключа больше не требует правки списков на каждом узле: узел сам объявляет
переход, подписав его **обоими** ключами (уходящим и новым). Заявление
самодостаточно — публичные ключи восстанавливаются из самих Peer ID, — поэтому
его принимает любой узел сети, даже никогда неловавший новый ключ
([ТЗ 11.2 п.3](../ТЗ.md)).

```bash
zeptomesh-node rotate -addr http://127.0.0.1:8081 -reason "плановая ротация"
```

Порядок действий и его смысл:

1. `rotate` строит заявление `KeyRebind`, проверяет его на себе и записывает в
   локальный журнал `<data_dir>/rebinds.json`.
2. Заявление рассылается **до** установки нового ключа: напрямую по
   control-RPC всем соединённым соседям (поле `announced_to` в ответе) и
   эпидемически через gossip. Если бы сначала был записан новый ключ, соседи
   увидели бы незнакомца вместо преемника узла, которому доверяют.
3. Новый ключ атомарно пишется в `identity.key_file` (путь из конфигурации не
   меняется — правка YAML не нужна).
4. Узел перезапускается: `zeptomesh-node leave -addr …` (код выхода 75,
   см. §1.5) или `systemctl restart zeptomesh@<i>`.

Что происходит на остальной сети: сосед, принявший заявление, переносит на
преемника запись таблицы соседей (навыки, метрики, категорию) и **доверие**
уходящего id. Блокировка, снятая с старого id, действует и на новый: решение
оператора относится к узлу, а не к ключу (§4.6).

Обратная сторона того же механизма: `zeptomesh-node rebinds -addr …` показывает
журнал принятых переходов и отзывов — первый вопрос «почему этому пиру теперь
не доверяют» решается по нему.

Если у узла в `discovery.bootstrap` есть запись вида `/ip4/…/p2p/<старый id>`,
она переименовывается в памяти автоматически (адрес верный, устарел только
идентификатор); в YAML её стоит поправить при случае — без спешки, сеть уже
соединяется.

### 4.6. Компрометация ключа узла (аварийный отзыв)

Отозвать идентичность может и сам узел, и оператор. Отзыв — заявление о
переходе **без преемника**; оно подписывается только уходящим ключом, поэтому
сфальсифицировать отзыв чужого узла нельзя.

**Вариант A — узел жив и под контролем:**

```bash
zeptomesh-node revoke -addr http://127.0.0.1:8081 -reason "утечка ключа"
```

Узел рассылает отзыв (прямой пуш + gossip), убирает свой ключевой файл на
`<key_file>.revoked-<случайное>` и завершает процесс **без перезапуска**:
`run` выходит с кодом 0 и не подаёт сигнал рестарта, а последующий старт с этим
же `data_dir` отвергается локальным стражем («identity … is revoked»). Именно
поэтому отзванный узел не возвращается в сеть даже при
`restart: always` у контейнера.

**Вариант B — узел потерян, ключ у противника.** Заявление об отзыве подписывается
только уходящим ключом, а его у оператора как раз и нет: самостоятельно отозвать
чужую идентичность этим механизмом нельзя. Это обратная сторона
самодостаточности — никто не может объявить переход или отзыв от чужого имени.
Основное средство здесь — операторский блок-лист:

1. Внести старый Peer ID в `blocked_peers_file` остальных узлов и применить
   `zeptomesh-node reload -addr …` (§1.5) — блокирует и соединения
   (`ConnectionGater` проверяет `AllowConnection`), и задачи (`AllowTasksFrom`),
   без рестарта узлов.
2. Удалить узел из bootstrap-списков и из allow-списков, если он там был.

Блокировка переживает попытку противника сменить ключ: доверие привязано к
*классу* идентичностей, а не к текущему ключу. Если скомпрометированный id
выпустит ротацию на новый ключ, узел, держащий это заявление, применит запрет ко
всему классу — «запрещён любой член ⇒ заблокированы все» (проверка
`TestOperatorDenyBeatsRebind`). Для этого заявление должно распространиться;
gossip несёт его сам, а блокировка по старому id работает независимо от того,
успело ли оно дойти.

Отзыв терминален: ключ, объявивший отзыв, больше не может ни во что
«перейти» — попытка выдать ротацию отозванным ключом отвергается (`Apply`
возвращает ошибку, событие `rebind_rejected` в журнале безопасности). Без этого
правила скомпрометированный узел мог бы увести своё доверие на свежий
идентификатор, и отзыв превратился бы в украшение.

Технические детали и проверки — в [docs/PROTOCOLS.md](PROTOCOLS.md) (§6, обмен
заявлениями о переходе ключей) и `internal/security/rebind_test.go` (в т.ч.
`TestRevocationIsTerminal`).

### 4.7. Обмен описаниями навыков

Узел объявляет два разных множества: имена навыков, которые он **готов
выполнять** (`capabilities.skills` — из них следует право исполнения), и
версионные описания тех же навыков (`capabilities.skill_docs` — что узел о них
рассказывает). Вторые не могут расширить полномочия: описание принимается только
для уже объявленного имени, а импорт от соседа описания в права не превращает.
Поэтому «у узла появился навык» и «у узла появилось описание навыка» — разные
события, и лечатся они по-разному.

Смена описания (`skill-set` или правка `capabilities.skill_docs` + рестарт)
поднимает локальный `skills_version` — монотонный счётчик, который растёт только
когда реально изменилось содержимое. Сосед видит расхождение в gossip
(`PeerState.skills_version`) и сам запрашивает дельту раз в
`capabilities.skill_exchange.interval`; руками — `zeptomesh-node skills-sync`.

```bash
zeptomesh-node skills -addr http://127.0.0.1:8081        # свои + выученные по соседям
zeptomesh-node skills-sync -addr http://127.0.0.1:8081   # немедленно подтянуть дельту
```

Диагностика:

| Симптом | Где смотреть | Причина |
|---|---|---|
| описаний соседа не видно | `zeptomesh_skills_sync_refused_total` **на отвечающем** узле; ответ при этом корректен и несёт `reason` (`disclosure_policy` / `exchange_disabled`) | `disclose_to` строже, чем статус пира: `trusted` требует доверия, `known` — знакомства. На просящем refusal неотличим от «новее нечего»: оба — пустой список дескрипторов |
| обмен идёт, версии не сходятся | `zeptomesh_skills_version` на обоих узлах + вывод `skills` | расхождение времени ни при чём: epoch локальный; проверьте, что описание действительно изменилось (совпадающий `contentKey` epoch не двигает) |
| `skills_sync_bad_signature` в журнале | `security_events_total{event="skills_sync_bad_signature"}` | ответ подписал не тот узел, что установлен в стриме, либо тело переписали в пути; импорт отклоняется целиком |
| выученные описания пропали после рестарта | `<data_dir>/skills.json` | `skill_exchange.persist: false` (бюджет `import_limit` на соседа при этом снимает только `DropPeer`) |
| описаний много, но не все | `skill_exchange.max_descriptors` (64) и `import_limit` (512) | это потолки одного ответа и одного соседа, а не отказ |

Хранить выученное в `skills.json` нужно не ради истории: без часов версий узел
после рестарта переанонсировал бы то, что сеть уже знает, и каждый цикл сверки
выглядел бы как обновление.

---

## 5. Резервное копирование и восстановление (ТЗ 22.9)

### 5.1. Что бэкапить (критичность)

| Данные | Путь | Критичность |
|---|---|---|
| Ключ идентичности | `$DATA_DIR/<i>/keys/peer.key` | **критично**: без него узел теряет Peer ID, подпись, доверие сети |
| БД узла (журнал задач/результатов/дедуп) | `$DATA_DIR/<i>/db/` | восстанавливает историю и дедуп-окно |
| Артефакты | `$DATA_DIR/<i>/artifacts/` | выходы задач (content-addressed) |
| Конфиг экземпляра/ENV | `$DATA_DIR/etc/node.yaml`, `$DATA_DIR/<i>/instance.env`, `peer_id.txt` | воспроизводимость развёртывания |
| Scratch задач | `$DATA_DIR/<i>/tasks/` | не нужен (пересоздаётся) |

### 5.2. Порядок бэкапа

BadgerDB открывает `db/` единолично; снимок делается на остановленном узле
(или в момент, когда процесс гарантированно не пишет):

```bash
systemctl stop zeptomesh@0
tar --numeric-owner -czf /backup/zepto-0-$(date -u +%F).tar.gz -C "$DATA_DIR" 0 etc
systemctl start zeptomesh@0
```

Практика: cron-задача на каждый хост; ключ `peer.key` дополнительно хранится
в защищённом хранилище секретов (Vault/agenix), отдельно от прочего бэкапа:
компрометация бэкапа данных не должна означать компрометацию идентичности.
Проверять восстанавливаемость копией на тестовом каталоге не реже раза в
квартал.

### 5.3. Восстановление экземпляра

```bash
systemctl stop zeptomesh@0            # или docker stop zeptomesh-0
rm -rf "$DATA_DIR/0"                  # только при целенаправленном откате!
tar -xzf /backup/zepto-0-YYYY-MM-DD.tar.gz -C "$DATA_DIR"
chown -R zeptomesh:zeptomesh "$DATA_DIR/0"   # для system-режима; в docker — volumes
systemctl start zeptomesh@0
zeptomesh-node status -addr http://127.0.0.1:8081 | grep -m1 peer_id  # Peer ID должен совпасть с peer_id.txt
```

Docker: дамп/копия named-volume — `docker run --rm -v zepto-0-data:/from -v
$PWD:/to alpine tar -czf /to/backup.tgz -C /from .` и симметрично
восстановление в чистый volume перед `up`.

### 5.4. Восстановление всего mesh

Порядок: сначала bootstrap-узел (или узел с `advertise_as_relay`), затем
рядовые экземпляры. mDNS/DHT/PEX восстановят связи сами; убедитесь, что PSK и
topic одинаковы на всех узлах (разные — это разные сети, соединения не
появится никогда).
