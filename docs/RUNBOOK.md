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
zeptomesh-node reload  -addr …            # hot-reload конфига (§1.5)
zeptomesh-node leave   -addr …            # штатный уход узла (§1.5)
# при заданном api.auth_token_env добавьте -token-env ZETOMESH_API_TOKEN
```

`-subtask '<skills>|<instruction>'` — пункт плана декомпозиции (повторяемый);
родительская задача при этом ничего не исполняет сама, а только собирает детей
в один подписанный итог (ТЗ 6.9.1/6.10.5).

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
Остальное (адреса прослушивания, PSK, навыки, лимиты задач, включение discovery)
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

### 2.2. Обязательные метрики (ТЗ 14.2.1) и алерты

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

### 2.3. Промышленные алерты (пример для Prometheus)

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
   (`PublishFull`, ограничение 64 состояния — см. STATUS).

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

### 4.5. Компрометация ключа узла

Ротации/отзыва идентификатора реализация не имеет (STATUS §4). Порядок
сегодня:

1. Остановить узел.
2. Внести его Peer ID в `blocked_peers_file` всех остальных узлов
   (блокирует и соединения, и задачи). Рестарт тем узлам не нужен: файл входит в
   hot-набор — достаточно `zeptomesh-node reload -addr …` (или
   `systemctl reload zeptomesh@<i>`) на каждом, где он поправлен (§1.5). Запись
   из файла добавляется к текущей политике, поэтому порядок применения
   «blocked-файл → reload» даёт мгновенный отзыв и для уже соединённого пира:
   `ConnectionGater` проверяет `AllowConnection` на новых попытках, а задачи
   отсекает `AllowTasksFrom`.
3. Сгенерировать новый ключ (`zeptomesh-node genkey --out <новый>`),
   обновить `identity.key_file`, удалить старый файл.
4. Перезапустить; добавить новый Peer ID в allow-списки (тоже применимо
   reload'ом).
5. Если старый ключ всё же валиден у части сети — сеть переживёт: задачи
   старого Peer ID будут отклоняться подписью (см. §3.5 про неверифицированные
   навыки).

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
