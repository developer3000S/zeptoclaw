# Руководство по развёртыванию

Документ для администратора: как установить и запустить сеть ZeptoClaw Agent
Mesh (ТЗ 19.5–19.6, 22). Эксплуатация после запуска — [RUNBOOK.md](RUNBOOK.md);
соответствие ТЗ и известные границы — [STATUS.md](STATUS.md).

Требования: Linux (amd64; arm64 не проверялась), для локального режима — Go
1.26+ на этапе сборки, Docker 24+ — для контейнерного. Сетевые порты узла:
mesh `4001` (tcp+udp QUIC), админ-API `8081` (tcp), метрики `9464` (tcp).

---

## 1. Быстрый старт: `install.sh`

Скрипт `install.sh` в корне репозитория — единая точка установки. Команды:

```bash
./install.sh install [флаги]     # собрать, установить, настроить автостарт, запустить
./install.sh status              # состояние экземпляров
./install.sh start | stop        # управление установленными
./install.sh uninstall           # снять сервисы (данные НЕ удаляются)
```

Ключевые флаги `install`:

| Флаг | Значение |
|---|---|
| `--mode local\|docker` | локальная systemd-установка или контейнеры (по умолчанию спрашивается; `-y` принимает default) |
| `--nodes N` | **сколько экземпляров агента** поднять (default 1) |
| `--data-dir PATH` | корень данных: экземпляры в `PATH/<i>/`, общий конфиг в `PATH/etc/` (default `/var/lib/zeptomesh`; без root — `~/.local/share/zeptomesh`) |
| `--api-host` | bind админ-API (default `127.0.0.1` — наружу не светим) |
| `--trust-mode open\|limited\|private` | политика доверия |
| `--pico-mode stub\|binary\|http` | режим адаптера PicoClaw (см. [PICOCLAW-INTEGRATION.md](PICOCLAW-INTEGRATION.md)) |
| `--bootstrap MULTIADDR[,…]` | точки входа существующей сети (формат `/ip4/…/tcp/4001/p2p/12D3K…`) |
| `--psk VALUE` | ключ закрытой сети (`zeptomesh-node psk` печатает валидный) |
| `--run-user NAME` | пользователь для systemd (default `zeptomesh` при root) |
| `--log-level LEVEL` | slog level |

Примеры:

```bash
# три узла на одном хосте, они образуют mesh между собой:
sudo ./install.sh install --mode local --nodes 3
# подключиться к действующей сети (bootstrap = публичный адрес первого узла):
sudo ./install.sh install --mode local --nodes 1 \
     --bootstrap /ip4/203.0.113.10/tcp/4001/p2p/12D3KooW… --psk "$ZETOMESH_PSK"
# docker-вариант той же тройки:
./install.sh install --mode docker --nodes 3
```

### Что делает локальный режим (`--mode local`)

1. Проверяет Go (`go env GOVERSION`, нужен ≥1.26), собирает
   `zeptomesh-node` (`-trimpath`, version-stamp через ldflags) →
   `/usr/local/bin` (system) или `~/.local/bin` (user).
2. Рендерит **один общий шаблон** `$DATA_DIR/etc/node.yaml` (на базе
   `configs/node.yaml`); различия экземпляров — только переменные окружения
   из `$DATA_DIR/<i>/instance.env` (`ZETOMESH_INDEX`, порты, пути).
3. Для каждого экземпляра: `genkey --out $DATA_DIR/<i>/keys/peer.key`
   (права 0600/0700), Peer ID в `peer_id.txt`.
4. Экземплярам `1..N-1` в `instance.env` дописывается bootstrap экземпляра 0
   (`/ip4/<основной IP>/tcp/4001/p2p/<peer_id_0>`) — узлы на одном хосте
   видят друг друга и без этого (реестр Unix-сокетов), но явный вход надёжнее.
5. Создаёт **шаблон юнита** `zeptomesh@%i`
   (`EnvironmentFile=$DATA_DIR/%i/instance.env`, hardening:
   `NoNewPrivileges`, `PrivateTmp`, `ProtectSystem=full`,
   `ReadWritePaths=$DATA_DIR`, `UMask=0077`), `systemctl enable --now
   zeptomesh@0 … @N-1`. Root-установка — system-юниты и сервисный пользователь
   `zeptomesh`; без root — user-юниты + `loginctl enable-linger`.
   **Автостарт при загрузке — часть установки** (ТЗ 22 п.4).

### Что делает docker-режим (`--mode docker`)

1. `docker build -f deploy/docker/Dockerfile` (образ `zeptomesh-node:<ver>`),
   если образа/бинарника ещё нет.
2. Пишет `$DATA_DIR/docker/docker-compose.yml` ровно на N сервисов
   `zepto-<i>`: порты `4001+i` (tcp+udp) публикуются, API `127.0.0.1:8081+i`
   и метрики `127.0.0.1:9464+i` — только loopback хоста; volumes
   `zepto-<i>-data`; healthcheck `/healthz`; `restart: unless-stopped`.
   Экземплярам `i>0` в environment кладётся bootstrap через
   `/dns4/zepto-0/tcp/4001/p2p/<peer_id_0>` (общая docker-сеть).
3. `docker compose up -d` — запуск сразу после установки.

Образ: многоэтапная сборка `golang:1.26-bookworm` → `debian:bookworm-slim`,
non-root (`zeptomesh`), шаблон конфига в `/etc/zeptomesh/node.yaml`,
`ENV ZETOMESH_API_HOST=0.0.0.0` (иначе published-порт недоступен извне
контейнера; наружу API всё равно публикуется на 127.0.0.1 хоста).

---

## 2. Ручная установка (без install.sh)

```bash
export PATH=$PATH:/usr/local/go/bin
GOFLAGS=-mod=mod go build -trimpath -o bin/ ./cmd/zeptomesh-node
DATA=$HOME/.local/share/zeptomesh; mkdir -p "$DATA/0" "$DATA/etc"
bin/zeptomesh-node genkey --out "$DATA/0/keys/peer.key" > "$DATA/0/peer_id.txt"
cp configs/node.yaml "$DATA/etc/node.yaml"   # или готовый профиль из configs/examples
ZETOMESH_DATA="$DATA" ZETOMESH_INDEX=0 bin/zeptomesh-node run -config "$DATA/etc/node.yaml"
```

Второй экземпляр на том же хосте — другой индекс, порты и каталог данных:

```bash
bin/zeptomesh-node genkey --out "$DATA/1/keys/peer.key" > "$DATA/1/peer_id.txt"
ZETOMESH_DATA="$DATA" ZETOMESH_INDEX=1 \
ZETOMESH_MESH_PORT=4002 ZETOMESH_API_PORT=8082 ZETOMESH_PROM_LISTEN=127.0.0.1:9465 \
bin/zeptomesh-node run -config "$DATA/etc/node.yaml"   # тот же шаблон конфига
```

Проверка: `bin/zeptomesh-node status -addr http://127.0.0.1:8081`,
`… peers -addr http://127.0.0.1:8082` (соседи должны появиться в течение
нескольких секунд), `curl -s http://127.0.0.1:9464/healthz`.

---

## 3. Порты и адреса

Экземпляр `i` получает порты от базы: `mesh 4001+i`, `API 8081+i`,
`метрики 9464+i`. В конфигу шаблона порт экземпляра задаётся переменными
`ZETOMESH_MESH_PORT` / `ZETOMESH_API_PORT` (слушатель метрик —
`ZETOMESH_PROM_LISTEN` адресом целиком); `install.sh` считает их от баз,
которые сдвигаются переменными `ZETOMESH_BASE_MESH_PORT`,
`ZETOMESH_BASE_API_PORT`, `ZETOMESH_BASE_PROM_PORT`.
Firewall между хостами: откройте **tcp+udp** `4001+i` (QUIC и TCP резервный).
API и метрики наружу не открывайте — доступ через SSH-туннель:

```bash
ssh -L 8081:127.0.0.1:8081 host '…'
```

| Переменная | Что задаёт |
|---|---|
| `ZETOMESH_DATA` | корень data_dir (в шаблоне `…/${ZETOMESH_INDEX:-0}`) |
| `ZETOMESH_INDEX` | номер экземпляра (имя `zepto-<i>`, пути) |
| `ZETOMESH_MESH_PORT` / `ZETOMESH_API_PORT` | порты mesh и API этого экземпляра |
| `ZETOMESH_PROM_LISTEN` | адрес слушателя метрик (`host:port`; пусто → выключен) |
| `ZETOMESH_BASE_MESH_PORT` / `ZETOMESH_BASE_API_PORT` / `ZETOMESH_BASE_PROM_PORT` | базы портов для `install.sh` |
| `ZETOMESH_API_HOST` | bind API |
| `ZETOMESH_PSK` | закрытая сеть (одинакова на всех узлах сети!) |
| `ZETOMESH_BOOTSTRAP` | через запятую — дописывается в `discovery.bootstrap` |
| `ZETOMESH_TRUST_MODE` / `ZETOMESH_PICO_MODE` / `ZETOMESH_LOG_LEVEL` | политики/агент/логи |
| `ZETOMESH_PUB_IP` | публичный адрес для `announce` (WAN-профиль) |
| `ZETOMESH_API_TOKEN` | bearer-токен API (если задан `api.auth_token_env`) |
| `PICOCLAW_MESH_PICO_TOKEN` | токен Pico Protocol для `picoclaw.mode: http` |

---

## 4. Выбор сетевого профиля

Готовые профили — `configs/examples/` (ТЗ 22.8). Это полные конфиги: запускать
`zeptomesh-node run -config configs/examples/<профиль>.yaml`, подставив env;
либо скопировать поверх шаблона до `install.sh`.

| Профиль | Когда | Ключевое |
|---|---|---|
| `configs/node.yaml` (шаблон install.sh) | базовый, любой хост | env-параметризован, `trust_mode` по умолчанию |
| `configs/examples/lan.yaml` | узлы в одном L2 | mDNS вкл, bootstrap `[]`, быстрые heartbeat, `accept_external_tasks: true`, `trust limited` |
| `configs/examples/wan.yaml` | узлы в Интернете | mDNS выкл, `announce: ZETOMESH_PUB_IP`, DHT `server`, `advertise_as_relay` по желанию, `accept_external_tasks: false`, rate-limit строже, allow/blocked-файлы |

### Closed network (PSK)

Одна и та же `private_network_psk` на всех узлах сети — чужие подключения
отбрасываются на уровне транспорта libp2p. Отличающийся PSK = другая сеть
(узлы не увидят друг друга никогда; типичная причина «распада mesh», см.
RUNBOOK §4.3). Генерация: `zeptomesh-node psk` (hex 32 байта). Хранить в
окружении/secret-хранилище, не в git.

### Allow/deny-списки и доверие

`security.allowed_peers_file` / `blocked_peers_file` — base58 Peer ID, по
строке на пира, `#` — комментарий. Blocked побеждает всё (соединения режет
ConnectionGater). `trust_mode`: `open` — подписанные задачи от любых
верифицированных; `limited` (default) — от известных (встречались/в allow/в
allow-list skills); `private` — только allow-список. `min_trust_for_tasks`
(подсказка уровня) + `require_task_signature`/`drop_invalid_signatures` —
всегда `true` в shipped-профилях.

### Bootstrap-конфигурация сети

1. Поднимите постоянный узел-«якорь» с публичным адресом (WAN-профиль,
   `announce`, DHT `server`; при желании — `relay.advertise_as_relay: true`
   для пиров за строгим NAT).
2. Его адрес: `/ip4/<PUB_IP>/tcp/4001/p2p/<Peer ID из peer_id.txt>`.
3. Остальным узлам: `ZETOMESH_BOOTSTRAP=<тот адрес>` (несколько — через
   запятую). Дальше PEX + gossip + DHT сами распространяют состав.

---

## 5. Многосетевой WAN-кластер (типовой сценарий)

```bash
# якорь (host-a, 203.0.113.10):
sudo ZETOMESH_PSK=… ./install.sh install --mode local --nodes 1
BOOT="/ip4/203.0.113.10/tcp/4001/p2p/$(cat /var/lib/zeptomesh/0/peer_id.txt)"

# узлы-исполнители (host-b/c, за NAT):
sudo ZETOMESH_PSK=<тот же> ./install.sh install --mode local --nodes 2 \
     --bootstrap "$BOOT" --trust-mode limited
```

`peer_id.txt` в `install.sh` пишется выводом `zeptomesh-node genkey`; его же
видно в `GET /healthz` (`{"status":"ok","peer_id":"12D3K…"}`) и
`zeptomesh-node status`.

На NAT-узлах включите `node.relay.enabled: true` (пользоваться чужим реле,
когда прямого пути нет) — транзит шифруется сквозным Noise, реле видит только
поток байтов. Профиль — `configs/examples/wan.yaml`.

---

## 6. Проверка после развёртывания

```bash
zeptomesh-node status -addr http://127.0.0.1:8081   # peer_id, адреса, adapter
zeptomesh-node peers  -addr http://127.0.0.1:8081   # соседи + skills (после верификации caps)
zeptomesh-node submit -addr http://127.0.0.1:8081 -i "ping" -w   # локальная задача
# задача на чужой навык (с другого узла, где этого навыка нет):
zeptomesh-node submit -addr http://127.0.0.1:8081 -i "ocr this" -skills ocr -w
curl -fsS http://127.0.0.1:9464/healthz && curl -fsS http://127.0.0.1:8081/healthz
```

Признаки здоровой сети: `peers_total>0` на всех, `connected>0`, задачи
`COMPLETED`, `security_events_total` не растёт без причин. Детали диагностики —
RUNBOOK §3–4; бэкап ключей — RUNBOOK §5 (ключ `peer.key` критичен: потеря =
потеря идентичности узла).

---

## 7. Безопасность развёртывания (чеклист)

- [ ] PSK сгенерирован (`zeptomesh-node psk`), одинаков у узлов одной сети, не в git.
- [ ] API (`8081+i`) и метрики (`9464+i`) не опубликованы наружу; при необходимости — `ZETOMESH_API_TOKEN`.
- [ ] `accept_external_tasks: false` на узлах, торчащих в Интернет, пока не решено, кому служить.
- [ ] `trust_mode: limited|private` + allow/blocked-файлы для WAN.
- [ ] `allow_shell: false`, `allow_network_tools: false` по умолчанию (ТЗ 11.4).
- [ ] Секреты провайдеров — только в окружении демона (`PICOCLAW_*`), не в `node.yaml`.
- [ ] Бэкап `keys/` настроен (RUNBOOK §5) и отделён от бэкапа данных.
- [ ] Учтите: лимиты ресурсов (ТЗ 6.8.4) исполняются (`max_parallel_tasks_per_peer`,
      `min_free_disk_bytes`, `max_workspace_bytes`, `max_task_memory_bytes`), но
      «отключение сети/оболочки» остаётся административным: технической границы
      (network namespace, drop привилегий) нет — см. STATUS §2.
