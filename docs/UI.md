# ZeptoClaw Mesh UI — панель наблюдения и управления агентами

Веб-панель для **наблюдения** за mesh-сетью агентов ZeptoClaw и **управления**
узлами. Устанавливается в Docker **отдельно и независимо** от установки агентов:
свой образ, свой compose-стек, свой инсталлятор `install-ui.sh`. Изменений в
коде агентов (`zeptomesh-node`) не требуется — UI работает исключительно через
готовый админ-API узлов (`internal/api/admin.go`).

---

## 1. Возможности

- **Наблюдение**: список агентов с живым статусом, граф всей созданной сети
  агентов, журнал задач, навыки, триггеры, конфигурация узлов.
- **Управление**: отправка/отмена/перезапуск задач, документирование и
  синхронизация навыков, CRUD триггеров (cron), hot-reload конфигурации, leave
  (рестарт узла).

## 2. Архитектура

```text
браузер ──HTTP──► BFF (Go: cmd/zeptomesh-ui, статика web/ через go:embed)
                   │  ├ конфиг узлов: env ZETOMESH_UI_NODES + nodes.json (редактируется из UI)
                   │  └ фоновый опрос узлов каждые N секунд → кэш-снимок сети
                   └──HTTP/JSON──► админ-API каждого узла (healthz/status/peers/tasks/…)
```

BFF нужен по трём причинам, проверенным по коду агента:

1. **В админ-API нет CORS** (`internal/api/admin.go` его не отдаёт) — браузер не
   сможет ходить в API узлов напрямую.
2. **Токен `ZETOMESH_API_TOKEN` не должен попадать в браузер** — BFF хранит его
   у себя и прокладывает в заголовке.
3. Именно BFF объединяет данные нескольких узлов в **один граф сети**.

### Два уровня видимости

- **Managed-узлы** (управляемые): явный список endpoint'ов
  (`http://zepto-0:33498`) — на них выполняются действия управления.
- **Discovered-пиры** (обнаруженные): объединение `/api/v1/peers` всех
  managed-узлов — это и есть «вся созданная сеть». Если `peer_id` найденного
  пира совпадает с `peer_id` managed-узла (берётся из `/healthz`), пир
  помечается управляемым.

Админ-API не анонсируется в mesh-протоколе: `/peers` отдаёт mesh-multiaddrs
вида `/ip4/172.22.0.2/tcp/58583`, а не HTTP-адреса. Поэтому автообнаружение даёт
**топологию сети**, а **управляемые точки** — явный список, редактируемый из UI.

## 3. Структура репозитория

```text
cmd/zeptomesh-ui/main.go              # точка входа BFF (стиль cmd/zeptomesh-node/main.go)
internal/ui/
  config.go                           # конфиг из env + nodes.json (persistent-список узлов)
  nodes.go                            # клиент админ-API узла (bearer, таймауты)
  aggregator.go                       # фоновый опрос → снимок сети: узлы + union пиров + граф
  server.go                           # HTTP: static embed, /api/v1/mesh, прокси действий, CRUD узлов
  static/                             # vanilla HTML/CSS/JS, без сборочного шага (встраивается в бинарник)
    index.html, css/style.css
    js/{api,store,graph,nodes,tasks,skills,triggers,config,main}.js
  *_test.go                           # юнит-тесты на httptest-заглушках узлов
deploy/ui/Dockerfile                  # многоэтапная сборка (по образцу deploy/docker/Dockerfile)
deploy/ui/docker-compose.yml          # отдельный стек zeptomesh-ui
install-ui.sh                         # независимый инсталлятор (аналог install.sh)
```

Новых зависимостей в go.mod нет — BFF на stdlib (`net/http`, `encoding/json`,
`embed`, `log/slog`).

## 4. Конфигурация BFF

| Env | Назначение | По умолчанию |
|---|---|---|
| `ZETOMESH_UI_LISTEN` | адрес слушателя | `127.0.0.1:8090` |
| `ZETOMESH_UI_NODES` | seed-список узлов через запятую (`http://zepto-0:33498,...`) | пусто |
| `ZETOMESH_UI_NODES_FILE` | persistent-список узлов (JSON: `[{name,url,token?}]`), пишется из UI | `<data>/nodes.json` |
| `ZETOMESH_UI_TOKEN` | токен доступа к UI (пусто — без авторизации) | пусто |
| `ZETOMESH_UI_API_TOKEN` | bearer-токен для узлов по умолчанию (если у узла нет своего) | пусто |
| `ZETOMESH_UI_POLL_INTERVAL` | интервал фонового опроса узлов | `5s` |
| `ZETOMESH_UI_DATA` | каталог для nodes.json | `./zeptomesh-ui-data` |
| `ZETOMESH_UI_LOG_LEVEL` | уровень логов | `info` |

При старте: если `nodes.json` существует — он главный; иначе seed из env
разворачивается в файл.

## 5. API бэкенда

`GET /healthz` → `{status:"ok"}` — для HEALTHCHECK контейнера.

`GET /api/v1/mesh` — снимок сети из кэша фонового опроса:

```json
{
  "updated_unix": 1790101573,
  "poll_interval_ms": 5000,
  "nodes": [{
     "name": "zepto-0", "url": "http://zepto-0:33498",
     "peer_id": "12D3…", "managed": true, "reachable": true,
     "latency_ms": 12, "error": "",
     "status": { "…": "полный ответ /api/v1/status узла" },
     "tasks": [ "…": "последние задачи (limit 50)" ]
  }],
  "graph": {
    "vertices": [
      {"id": "<peer_id>", "label": "zepto-0", "kind": "managed|discovered",
       "trust": "known", "connected": true, "left": false,
       "skills": ["general"], "load": 0.0, "source": "zepto-0"}
    ],
    "edges": [{"from": "<peer_id узла>", "to": "<peer_id пира>", "connected": true}]
  }
}
```

Рёбра берутся из таблиц соседей каждого managed-узла (A знает B); дубли
`(from,to)` схлопываются. Рёбер между двумя discovered-пирами нет — их
API-адреса неизвестны; граф показывает «что видят наши узлы».

Прокси действий (метод/path/тело сохраняются, пробрасываются в админ-API
выбранного узла):

| BFF | Узел |
|---|---|
| `POST /api/v1/nodes/{name}/tasks` | `POST /api/v1/tasks` |
| `GET /api/v1/nodes/{name}/tasks?limit=&status=` | `GET /api/v1/tasks` |
| `GET /api/v1/nodes/{name}/tasks/{id}` | `GET /api/v1/tasks/{id}` |
| `POST /api/v1/nodes/{name}/tasks/{id}/cancel` | `POST /api/v1/tasks/{id}/cancel` |
| `POST /api/v1/nodes/{name}/tasks/{id}/resubmit` | `POST /api/v1/tasks/{id}/resubmit` |
| `GET\|POST /api/v1/nodes/{name}/skills` | `GET\|POST /api/v1/skills` |
| `DELETE /api/v1/nodes/{name}/skills/{skill}` | `DELETE /api/v1/skills/{skill}` |
| `POST /api/v1/nodes/{name}/skills/sync` | `POST /api/v1/skills/sync` |
| `GET\|POST /api/v1/nodes/{name}/triggers` | `GET\|POST /api/v1/triggers` |
| `DELETE /api/v1/nodes/{name}/triggers/{id}` | `DELETE /api/v1/triggers/{id}` |
| `GET /api/v1/nodes/{name}/config` | `GET /api/v1/config` |
| `POST /api/v1/nodes/{name}/reload-config` | `POST /api/v1/admin/reload-config` |
| `POST /api/v1/nodes/{name}/leave` | `POST /api/v1/admin/leave` |

Управление списком узлов: `GET /api/v1/nodes` (без токенов), `POST
/api/v1/nodes` `{name,url,token?}` → persist в nodes.json, `DELETE
/api/v1/nodes/{name}`.

Авторизация: при заданном `ZETOMESH_UI_TOKEN` все `/api/v1/*` (кроме
`/healthz`) отдают 401 без `Authorization: Bearer`; статика отдаётся открыто,
токен хранится в localStorage и подставляется в заголовки.

Submit выполняется **без `wait`** (неблокирующе) — UI следит за задачей
поллингом; так избегаем висящих запросов.

## 6. Фронтенд

SPA с hash-роутингом, вкладки:

1. **Сеть** (главная): SVG-граф с самописным force-directed layout (repulsion +
   springs + centering, requestAnimationFrame, пересчёт только при изменении
   структуры). Цвет/размер: managed — крупный зелёный, discovered — серый,
   `untrusted` — красная обводка, `left` — пунктир; рёбра: подключённые —
   сплошные, известные-но-отключённые — пунктир. Фильтры: по уровню доверия,
   «только подключённые», «только managed». По умолчанию скрыты
   `untrusted`/`blocked` и отключённые пиры — узел знает случайные
   интернет-пииры из DHT, без фильтра граф превратится в шум. Клик по вершине →
   панель деталей (peer_id, addrs, trust, load, skills, откуда увидели).
2. **Агенты**: карточки managed-узлов: имя, peer_id, версия, uptime, load,
   running/tracked, соседи, adapter (healthy/model), trust_mode, кнопки (задачи,
   reload-config, leave) и индикатор reachable.
3. **Задачи**: выбор узла → список (task_id, status, worker, skills, время,
   priority), фильтр по статусу; форма отправки (instruction, required_skills,
   ttl, priority, timeout_seconds, allow_shell); cancel/resubmit; просмотр
   результата (text, артефакты, error_class).
4. **Навыки**: advertised + descriptors + peer skill views; кнопка
   `skills-sync`; форма документирования навыка (name/description/models/
   attributes); удаление доки.
5. **Триггеры**: список (id, schedule, next_fire, last_run/run_count, enabled);
   форма создания (id, schedule cron, job.instruction/skills/ttl/priority);
   удаление.
6. **Конфиг**: просмотр конфига узла (pretty JSON), кнопка reload-config с
   показом `applied` / `requires_restart`.

Live-обновления: поллинг `GET /api/v1/mesh` раз в `poll_interval_ms`, индикатор
времени последнего обновления, кнопка паузы автообновления. Интерфейс — русский,
технические термины на английском (peer_id, trust, reload). Минимальная
адаптивность — CSS grid, нормально при 1280px, не ломается при 800px.

## 7. Docker-установка

`deploy/ui/Dockerfile` — двухэтапный по образцу `deploy/docker/Dockerfile`:
`golang:1.26-bookworm` собирает `cmd/zeptomesh-ui` (CGO_ENABLED=0, `-trimpath`,
ldflags как у агентов) → `debian:bookworm-slim` + ca-certificates/curl/tzdata,
non-root пользователь `zeptomesh`, `VOLUME /var/lib/zeptomesh-ui` (nodes.json),
`EXPOSE 8090`, `HEALTHCHECK` по `/healthz`.

`deploy/ui/docker-compose.yml` — отдельный проект `name: zeptomesh-ui`, сервис
`zeptomesh-ui`, порт `127.0.0.1:${ZETOMESH_UI_PORT:-8090}:8090`, volume и
подключение к сети агентов как `external: name: zeptomesh_default`.

`install-ui.sh` (инсталлятор, независимый от `install.sh`):

- проверяет наличие сети `zeptomesh_default`; если её нет — предупреждение и
  альтернатива: режим `--host-gateway` (без external-сети, `extra_hosts:
  host-gateway`, узлы через `http://host.docker.internal:<порт>` — для
  локальной/systemd установки агентов);
- **автоконфигурация адресов узлов** из работающего стека: для каждого
  контейнера `zeptomesh-*` читает env `ZETOMESH_API_PORT` через `docker inspect`
  и формирует `ZETOMESH_UI_NODES=http://zepto-0:<порт>,…`;
- собирает образ и поднимает стек; печатает URL `http://127.0.0.1:8090`;
- управление: `install-ui.sh status|start|stop|uninstall` — трогает только
  UI-контейнер, агенты не затрагивает.

## 8. Безопасность

- Токены узлов хранятся только в BFF; `GET /api/v1/nodes` не отдаёт их в
  браузер.
- Опциональный `ZETOMESH_UI_TOKEN` на доступ к API UI; статика доступна.
- BFF слушает `127.0.0.1` по умолчанию (как админ-API агентов) — доступ только с
  хоста, если не перенастроить.
- Прокси — только к узлам из вайт-листа (nodes.json); новые узлы добавляет
  оператор (при включённом UI-токене — авторизованно). SSRF через UI невозможен.
- Таймауты на запросы к узлам (5s на опрос, до 60s на действия), ограничение
  размера тела запроса.

## 9. Тесты и CI

- `internal/ui`: `httptest`-заглушки узлов → проверки: сборка графа (union
  пиров, связывание managed по `peer_id`, дедуп рёбер), проксирование действий
  (метод/path/тело/bearer уходят корректно), add/remove узлов с persist во
  temp-файл, 401 без токена, поведение при unreachable-узле.
- `make test` прогоняет новые тесты; в CI добавляется job сборки docker-образа
  UI.

## 10. Установка

### Docker (узлы уже установлены через `./install.sh --mode docker`)

```bash
./install-ui.sh                 # автоконфигурация адресов узлов из работающего стека
./install-ui.sh --host-gateway  # альтернатива: агенты стоят локально (systemd), API на хосте
```

### Вручную

```bash
make build-ui
ZETOMESH_UI_NODES=http://127.0.0.1:8081,http://127.0.0.1:8082 ./bin/zeptomesh-ui
# UI: http://127.0.0.1:8090
```

## 11. Что НЕ входит в v1

- Ротация/отзыв ключей (`rotate-key`, `revoke`) — деструктивные операции,
  отдельным шагом после согласования.
- Push-обновления через WebSocket — админ-API узлов не отдаёт событий, поллинга
  BFF достаточно.
- История/графики метрик (Prometheus) — отдельный экран при необходимости.
- Многоязычность — русский интерфейс.
