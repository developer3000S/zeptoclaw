# ТЗ: Фронтенд ZeptoClaw Mesh UI (веб-панель наблюдения и управления)

Веб-панель для **наблюдения** за mesh-сетью агентов ZeptoClaw и **управления**
узлами. Устанавливается в Docker **отдельно и независимо** от установки агентов:
свой образ, свой compose-стек, свой инсталлятор `install-ui.sh`. Изменений в
коде агентов (`zeptomesh-node`) не требуется — панель работает исключительно
через готовый админ-API узлов (`internal/api/admin.go`).

> Фундамент (BFF `cmd/zeptomesh-ui` + пакет `internal/ui`) уже реализован,
> собирается, проходит `go vet`/тесты и дымовitся вручную. Это ТЗ на
> **фронтенд и Docker-упаковку**, которые поверх него.

---

## 1. Цели и не-цели

**Цели:**

1. Видеть **всех доступных агентов** (managed-узлы из настроенного списка) с
   живым статусом: reachable, peer_id, версия, load, running/tracked задачи,
   соседи, адаптер, trust mode.
2. Видеть **всю созданную сеть агентов** — объединённый граф: managed-узлы +
   все discovered-пиры, о которых сообщают managed-узлы (union их `/peers`).
3. **Управлять** узлами: задачи (отправка/отмена/перезапуск/результат),
   документирование и синхронизация навыков, CRUD триггеров, hot-reload
   конфигурации, leave (рестарт узла), добавление/удаление узлов в список.
4. **Устанавливаться в Docker независимо**: свой образ, свой стек, свой
   инсталлятор; трогает только UI-контейнер, агенты не затрагивает.

**Не-цели (v1):**

- Ротация/отзыв ключей (`rotate-key`, `revoke`) — деструктивные операции,
  отдельным шагом после отдельного согласования.
- WebSocket/push-обновления — админ-API узлов не отдаёт событий, поллинга BFF
  достаточно.
- История метрик/графики (Prometheus) — отдельный экран при необходимости.
- Многоязычность — интерфейс русский, технические термины на английском
  (peer_id, trust, reload, skill).

---

## 2. Текущее состояние (сверено по коду)

**Реализовано и работает:**

- `cmd/zeptomesh-ui/main.go` — точка входа BFF, конфиг из env, graceful
  shutdown.
- `internal/ui/{config,nodes,aggregator,server}.go` — конфигурация, клиент
  админ-API узла, фоновый опрос → снимок сети, HTTP-сервер (статика +
  `/api/v1/mesh` + прокси действий + CRUD узлов).
- `internal/ui/{server,aggregator}_test.go` — httptest-тесты: сборка графа,
  проксирование, CRUD узлов, 401 без токена, unreachable-узел. Проходят.
- `internal/ui/static/index.html` — **stub** (380 байт), ссылается на
  `/static/css/style.css` и `/static/js/main.js`, которых **нет**.

**Отсутствует (реализуется этим ТЗ):**

- Весь фронтенд: `internal/ui/static/css/`, `internal/ui/static/js/`.
- Docker-упаковка: `deploy/ui/Dockerfile`, `deploy/ui/docker-compose.yml`.
- Инсталлятор `install-ui.sh`, цель `make build-ui`.
- (docs/UI.md местами расходится с кодом — исправляется на последнем шаге.)

### Реальный API BFF (контракт фронтенда)

Публичные (без авторизации): `GET /healthz`, `GET /` (index.html),
`GET /static/...`.

При заданном `ZETOMESH_UI_TOKEN` всё ниже требует `Authorization: Bearer <tok>`:

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/api/v1/mesh` | снимок сети (кэш фонового опроса) |
| GET | `/api/v1/nodes` | список узлов (без токенов) |
| POST | `/api/v1/nodes` | добавить `{name,url,token?}` |
| DELETE | `/api/v1/nodes/{name}` | удалить узел |
| GET\|POST | `/api/v1/nodes/{name}/tasks[?limit=&status=]` | список/отправка задач |
| GET | `/api/v1/nodes/{name}/tasks/{id}` | задача + результат |
| POST | `/api/v1/nodes/{name}/tasks/{id}/cancel` | отмена |
| POST | `/api/v1/nodes/{name}/tasks/{id}/resubmit` | перезапуск |
| GET\|POST | `/api/v1/nodes/{name}/skills` | навыки узла |
| DELETE | `/api/v1/nodes/{name}/skills/{skill}` | удалить документацию навыка |
| POST | `/api/v1/nodes/{name}/skills/sync` | синхронизация навыков |
| GET\|POST | `/api/v1/nodes/{name}/triggers` | триггеры |
| DELETE | `/api/v1/nodes/{name}/triggers/{id}` | удалить триггер |
| GET | `/api/v1/nodes/{name}/config` | конфиг узла |
| POST | `/api/v1/nodes/{name}/reload-config` | hot-reload |
| POST | `/api/v1/nodes/{name}/leave` | уход узла (рестарт супервизором) |

Форма `GET /api/v1/mesh` (реальная, по `MeshSnapshot`):

```jsonc
{
  "updated_unix": 1790101573,
  "poll_interval_ms": 5000,
  "nodes": [{
    "name": "zepto-0", "url": "http://zepto-0:18081",
    "peer_id": "12D3…", "reachable": true, "latency_ms": 12, "error": "",
    "status": { "…": "полный ответ /api/v1/status узла" },
    "peers":  [ "…": "PeerView из /api/v1/peers" ],
    "tasks":  [ "…": "последние задачи, limit 50" ]
  }],
  "graph": {
    "vertices": [
      {"id":"<peer_id>","label":"zepto-0","kind":"managed|discovered",
       "trust":"known","connected":true,"left":false,
       "skills":["general"],"load":0.0,"source":"zepto-0","self":true}
    ],
    "edges": [{"from":"<peer_id>","to":"<peer_id>","connected":true}]
  },
  "managed_peer_ids": ["12D3…"]
}
```

Примечания для рендера: managed-узел определяется по `graph.vertices[].kind ==
"managed"` (или вхождению в `managed_peer_ids`) — отдельного поля `managed` в
`nodes[]` **нет**; доверие/подключённость/уход берутся из вершин графа.

---

## 3. Технологии

- **Vanilla HTML/CSS/JS, ES-модули, без сборочного шага.** Встраиваются в
  бинарник BFF через `//go:embed all:static` (уже объявлено в `server.go`);
  отдаются через `http.FileServerFS`. Ноль новых зависимостей в go.mod, Docker
  остаётся двухэтапным «собрал Go-бинарь → положил статику».
- Альтернатива (d3/vis-network/cytoscape) **отвергнута**: CDN-зависимость
  невозможна для docker-установки без интернета, а vendoring библиотеки в
  статический embed эквивалентен самописному коду по объёму поддержки.
- Граф — **самописный force-directed layout на SVG** (repulsion + springs +
  centering через `requestAnimationFrame`, пересчёт только при изменении
  структуры графа).
- Хэш-роутинг (`#/network`, `#/agents`, …) — не требует серверной поддержки и
  работает за любым reverse-proxy.
- Токен доступа — в `localStorage`, подставляется в заголовки; при 401
  показывается форма ввода.

---

## 4. Структура файлов

```text
internal/ui/static/
  index.html              # shell: хэш-роутинг, каркасы вкладок, форма токена
  css/style.css           # тёмная тема, CSS grid, адаптив 800px+
  js/
    api.js                # обёртка над BFF API: fetch, bearer, обработка 401
    store.js              # состояние: снимок mesh + подписки контроллеров
    graph.js              # SVG force-directed layout и отрисовка
    network.js            # вкладка «Сеть»: граф + панель деталей + фильтры
    agents.js             # вкладка «Агенты»: карточки + CRUD узлов
    tasks.js              # вкладка «Задачи»
    skills.js             # вкладка «Навыки»
    triggers.js           # вкладка «Триггеры»
    config.js             # вкладка «Конфиг»
    main.js               # boot: роутер, auto-refresh, индикатор обновления
```

---

## 5. Функционал по вкладкам

### 5.1 Сеть (главная)

- SVG-граф всей созданной сети: managed-узлы (крупные, зелёные) и
  discovered-пиры (серые, меньше); `untrusted`/`blocked` — красная обводка;
  ушедшие (`left`) — пунктир; рёбра: подключённые — сплошные,
  известные-но-отключённые — пунктирные.
- Фильтры: по уровню доверия, «только подключённые», «только managed». **По
  умолчанию скрыты `untrusted`/`blocked` и отключённые** — узлы знают случайные
  интернет-пиры из DHT, без фильтра граф превращается в шум.
- Клик по вершине → панель деталей: peer_id, addrs, trust, load, skills,
  откуда увидели (`source`), категории.
- Пересчёт layout только при изменении структуры вершин/рёбер; между
  обновлениями — лёгкая интерполяция позиций.

### 5.2 Агенты

- Карточки managed-узлов из `nodes[]`: имя, URL, peer_id, версия, uptime, load,
  running/tracked задачи, соседи (connected/total), адаптер (name/healthy/model),
  trust_mode, индикатор reachable + latency, `error` если узел недоступен.
- Управление списком узлов: форма добавления (`POST /api/v1/nodes`), удаление
  (`DELETE /api/v1/nodes/{name}`) — с подтверждением.
- Кнопки действий на узле: «Задачи» (переход на вкладку с выбранным узлом),
  `reload-config`, `leave` (с подтверждением; leave = узел уходит и его
  перезапускает супервизор).

### 5.3 Задачи

- Выбор узла (по умолчанию — первый reachable) → список последних задач
  (`task_id`, status, worker, skills, время, priority), фильтр по статусу
  (pending/running/completed/failed/canceled/…).
- Форма отправки: `instruction` (обязательно), `required_skills`, `ttl`,
  `priority`, `timeout_seconds`, `allow_shell`. Отправка **без `wait`** —
  статус отслеживается поллингом; ответ `{task_id}` подсвечивается.
- `cancel` и `resubmit` на строке задачи (resubmit создаёт новый task_id —
  показать ссылку на него).
- Просмотр результата: `GET /tasks/{id}` → `{task, result}` — текст, артефакты
  (name/hash/size), `error_class`, `error`, подписи.

### 5.4 Навыки

- `GET /skills` узла: advertised + descriptors + peer skill views + эпоха.
- Кнопка `skills-sync` (`POST /skills/sync`) с показом `peers_queried`.
- Форма документирования навыка (`POST /skills`: name, description, models,
  attributes); удаление документации (`DELETE /skills/{skill}`).

### 5.5 Триггеры

- Список (`GET /triggers`): id, schedule, next_fire, last_run/run_count,
  enabled, последняя ошибка.
- Создание (`POST /triggers`: id, schedule cron, job: instruction/skills/ttl/
  priority); удаление (`DELETE /triggers/{id}`) с подтверждением.

### 5.6 Конфиг

- Просмотр конфига узла (`GET /config`) как pretty-JSON.
- Кнопка `reload-config` → показ `applied` и `requires_restart` с пояснением,
  что для применения рестарта нужен `leave`/`systemctl restart`.

### 5.7 Общее

- Live-обновления: поллинг `GET /api/v1/mesh` каждое `poll_interval_ms` из
  снимка; индикатор времени последнего обновления; кнопка паузы
  автообновления.
- Авторизация: при 401 — форма ввода `ZETOMESH_UI_TOKEN`, сохранение в
  `localStorage`, выход/смена токена; статика доступна без токена.
- Обработка ошибок: недоступный узел показывается карточкой с `error`, а не
  молчанием; сетевые ошибки BFF — всплывающее уведомление.

---

## 6. Docker-упаковка и независимая установка

### 6.1 `deploy/ui/Dockerfile` (по образцу `deploy/docker/Dockerfile`)

- Stage 1: `golang:1.26.0-bookworm`, `CGO_ENABLED=0`, `-trimpath`, ldflags как
  у агентов (`-s -w -X .../internal/version.{Version,GitCommit,BuildDate}`);
  собирает `cmd/zeptomesh-ui` (статика уходит в бинарь через embed — отдельного
  копирования статики в рантайм-образ не требуется).
- Stage 2: `debian:bookworm-slim` + `ca-certificates`, `curl` (HEALTHCHECK),
  `tzdata`; non-root пользователь `zeptomesh`;
  `VOLUME /var/lib/zeptomesh-ui` (там живёт `nodes.json`);
  `EXPOSE 28090`; `HEALTHCHECK` по `curl -fsS http://127.0.0.1:28090/healthz`.
- Рантайм-настройка только через env; конфигурационных файлов на образе нет.

### 6.2 `deploy/ui/docker-compose.yml` (отдельный проект)

- `name: zeptomesh-ui`, сервис `zeptomesh-ui`, образ `zeptomesh-ui:latest`.
- Порт `0.0.0.0:${ZETOMESH_UI_PORT:-28090}:28090` — контейнер слушает
  `ZETOMESH_UI_LISTEN=0.0.0.0:28090`, доступ с других машин; volume
  `zeptomesh-ui-data:/var/lib/zeptomesh-ui`.
- Подключение к сети агентов: `networks: default: external: true,
  name: zeptomesh_default` (стек агентов — проект `zeptomesh`, контейнеры
  `zeptomesh-N`, API-порты в env `ZETOMESH_API_PORT` каждого контейнера).
- Env: `ZETOMESH_UI_NODES`, `ZETOMESH_UI_TOKEN`, `ZETOMESH_UI_API_TOKEN`,
  `ZETOMESH_UI_POLL_INTERVAL`, `ZETOMESH_UI_LOG_LEVEL`, `TZ`.

### 6.3 `install-ui.sh` (независимый инсталлятор)

- Режим по умолчанию: проверка существования сети `zeptomesh_default`; если её
  нет — предупреждение и подсказка про `--host-gateway`.
- **Автоконфигурация адресов узлов**: для каждого контейнера `zeptomesh-*`
  читается env `ZETOMESH_API_PORT` через `docker inspect` и формируется
  `ZETOMESH_UI_NODES=http://zepto-0:<порт>,…`.
- `--host-gateway`: external-сеть не используется, вместо неё
  `extra_hosts: host-gateway`, узлы задаются как
  `http://host.docker.internal:<порт>` (для локальной/systemd установки
  агентов; порты оператор указывает сам или через `ZETOMESH_UI_NODES`).
- Собирает образ и поднимает стек; печатает URL `http://127.0.0.1:28090`.
- Команды управления: `install-ui.sh status|start|stop|uninstall` — трогает
  **только** UI-контейнер и его образ/стек; установку агентов не затрагивает.

### 6.4 Makefile

- Цель `build-ui`: `go build` → `bin/zeptomesh-ui` (ldflags как у агентов);
  цель `docker-build-ui` — собрать образ из `deploy/ui/Dockerfile`.

---

## 7. Тесты и приёмка

- `go test ./internal/ui/` — существующие тесты должны оставаться зелёными.
- **Новый тест-страж**: проверка, что `staticFS` (embed) содержит
  `index.html`, `css/style.css` и `js/main.js` — защита от «забыли положить
  файлы фронтенда в бинарник».
- Smoke после сборки (без Docker): запуск `bin/zeptomesh-ui`, проверки
  `GET /healthz`, `GET /api/v1/mesh`, `GET /`, `GET /static/js/main.js` → 200.
- Docker-приёмка: `install-ui.sh` на работающем стеке агентов → открытие
  `http://127.0.0.1:8090` в браузере: видны managed-узлы, граф сети, вкладки
  отвечают на действия (отправка тестовой задачи, reload-config).
- Ручная проверка адаптивности: 1280px — нормально, 800px — не ломается.

---

## 8. Пошаговый план реализации

Каждый шаг заканчивается верификацией (`go build`/`go test`/gofmt/smoke) и
отчётом перед переходом к следующему.

| # | Шаг | Результат |
|---|---|---|
| 0 | Скелет: `index.html` (shell + хэш-роутинг), `css/style.css`, `js/{api,store,main}.js` — boot, поллинг mesh, индикатор обновления, пауза, форма токена/401 | пустые вкладки рендерятся, `/api/v1/mesh` тянется |
| 1 | Вкладка «Сеть»: `graph.js` (force-directed SVG) + `network.js` — вершины/рёбра, фильтры, панель деталей | граф рисуется на данных BFF |
| 2 | Вкладка «Агенты»: карточки узлов, CRUD узлов, `reload-config`/`leave` | управление списком узлов из UI |
| 3 | Вкладка «Задачи»: список, фильтры, форма submit (без wait), cancel/resubmit, просмотр результата | полный цикл задачи из UI |
| 4 | Вкладки «Навыки» и «Триггеры» | документирование/синхронизация навыков, CRUD триггеров |
| 5 | Вкладка «Конфиг» | просмотр конфига + reload с показом `applied`/`requires_restart` |
| 6 | Docker-упаковка: `deploy/ui/Dockerfile`, `deploy/ui/docker-compose.yml`, `install-ui.sh`, `make build-ui` (+ тест-страж embed) | `install-ui.sh` поднимает UI на стеке агентов |
| 7 | Актуализация документации: `docs/UI.md` (путь статики, поле `managed`, разделы Docker/установки), упоминания `install-ui.sh` в `cmd/zeptomesh-ui/main.go` | доки совпадают с кодом |

---

## 9. Ограничения и безопасность

- Токены узлов хранятся только в BFF; `GET /api/v1/nodes` не отдаёт их в
  браузер.
- Прокси — только к узлам из вайт-листа (`nodes.json`); SSRF через UI
  невозможен. Новые узлы добавляет оператор (при включённом UI-токене —
  авторизованно).
- BFF слушает `127.0.0.1` по умолчанию; доступ — только с хоста, если не
  перенастроено.
- Таймауты: 5s на опрос узлов, до 60s на действия; ограничение размера тела.
- Деструктивные операции (`leave`) — с подтверждением в UI; `rotate-key`/
  `revoke` в v1 отсутствуют.
