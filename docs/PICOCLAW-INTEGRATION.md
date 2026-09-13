# Интеграция с PicoClaw

Документ описывает **фактически реализованные** способы подключения узла mesh
к локальному агенту [PicoClaw](https://github.com/sipeed/picoclaw). Он
фиксирует, какие интерфейсы PicoClaw использованы, как именно, и чего адаптер
не делает. Всё проверено чтением кода `internal/picoclaw/`; ни один вызов
PicoClaw не выдуман.

---

## 1. Граница ответственности

Вся работа с агентом идёт через единственный интерфейс
(`internal/picoclaw/adapter.go`):

```go
type Adapter interface {
    Name() string
    Execute(ctx context.Context, req Request) (*Response, error)
    Healthy(ctx context.Context) error
    Capabilities(ctx context.Context) ([]string, error)
    Close() error
}
```

Менеджер задач (`internal/tasks/manager.go`) не знает, какой режим выбран: он
формирует `picoclaw.Request` и вызывает `Execute`. Выбор реализации —
`picoclaw.New` по значению `picoclaw.mode` (`internal/picoclaw/factory.go`):

| `picoclaw.mode` | Реализация | Файл |
|---|---|---|
| `stub` (по умолчанию, в т.ч. при пустом значении) | `StubAdapter` | `stub.go` |
| `binary` | `CLIAdapter` | `cli.go` |
| `http` | `WSAdapter` (Pico Protocol по WebSocket) | `ws.go` |

Иные значения `mode` возвращают ошибку конфигурации
(`picoclaw: unknown mode ...`); `Validate()` того же требует заранее.

### Поля `Request`

```go
type Request struct {
    TaskID       string
    Instruction  string
    Skills       []string
    Workspace    string        // каталог, в который задача пишет результат
    Home         string        // per-task PICOCLAW_HOME
    AllowShell   bool          // ограничения задачи (см. §5)
    AllowNetwork bool
    Timeout      time.Duration
    Model        string
    SessionKey   string
}
```

`Workspace` и `Home` создаёт менеджер (`prepareSandbox`):
`<data_dir>/tasks/<task_id>/{workspace,home}`, `0700`, рядом пишется изолированный
`config.json` вида `{"version":3,"agents":{"defaults":{"workspace":…}},
"gateway":{"host":"127.0.0.1","port":0}}`, чтобы экземпляр PicoClaw не трогал
`~/.picoclaw` оператора.

---

## 2. Проверенные интерфейсы PicoClaw

Два (и только два) способа программно driving'ить PicoClaw:

1. **CLI.** `picoclaw agent -m "<prompt>"` выполняет один ход, печатает ответ в
   stdout и завершается с кодом 0; при неудаче — сообщение в stderr, код 1.
   Перенос конфигурации и состояния — **только переменными окружения**:
   `PICOCLAW_CONFIG`, `PICOCLAW_HOME`,
   `PICOCLAW_AGENTS_DEFAULTS_WORKSPACE`. Флагов `--config` / `--workspace` нет.
2. **Pico Protocol.** WebSocket-канал шлюза `picoclaw gateway` на пути
   `/pico/ws` (порт по умолчанию 18790), аутентификация bearer-токеном.
   REST-эндпоинта, принимающего промпт, у шлюза нет — поэтому `http`-режим
   адаптера реализован именно поверх WebSocket, а не HTTP POST.

Команды `picoclaw run` не существует; ничего подобного адаптер не вызывает.

---

## 3. Режим `binary` (`CLIAdapter`)

### Что исполняется

```
picoclaw agent -m <prompt> [-s <session_key>] [--model <model>] <extra_args...>
```

- `<prompt>` — `renderPrompt`: при пустом `picoclaw.prompt_template` —
  инструкция как есть; шаблон без `%s` — конкатенация шаблона и инструкции;
  иначе `fmt.Sprintf(template, instruction)`.
- `-s` добавляется при непустом `Request.SessionKey`, `--model` — при
  непустом `Request.Model`.
- `picoclaw.extra_args` дописываются хвостом.
- Рабочий каталог процесса — `Request.Workspace`.

`picoclaw.binary` — имя (тогда ищется в PATH в момент запуска) либо путь
(тогда проверяется на существование при построении адаптера).

### Окружение (`baseEnv`)

К процессу прикладывается окружение демона плюс:

| Переменная | Значение |
|---|---|
| `PICOCLAW_CONFIG` | `picoclaw.config` (если задано) |
| `PICOCLAW_HOME` | `Request.Home` (изоляция сессий/памяти на задачу) |
| `PICOCLAW_AGENTS_DEFAULTS_WORKSPACE` | `Request.Workspace` |
| `NO_COLOR=1`, `TERM=dumb` | подавление баннера/раскраски |
| произвольные `picoclaw.env: {K: V}` | поверх, как у оператора |

### Разбор вывода

`ExtractAnswer` срезает stdout до маркера ответа (emoji-логотип, константа
`Logo`) либо, если маркера нет, отбрасывает строки баннера
(`isBannerLine`: блок из `█▀▄`, префиксы `PICOCLAW`, `TZ environment:`,
`ZONEINFO environment:`, `Debug mode enabled`). Ответом считается только
непустой текст: пустой stdout при коде 0 — ошибка
`picoclaw: agent produced no answer (exit N)`.

### Критерии успеха/неудачи

- `exit 0` и непустой ответ → успех.
- `context.DeadlineExceeded` → `picoclaw.ErrTimeout` («timeout after …»).
- ненулевой код → ошибка `picoclaw: agent exited N: <stderr>`, где stderr
  пропущен через `sanitize()` (маскировка `api_key`/`token`/`secret`/
  `password`/`authorization` и обрезка 512 байт) — **в mesh stderr не
  возвращается**, он попадает только в журнал узла.

### Параллелизм и здоровье

- `picoclaw.max_concurrent_agents` → буферизованный канал-семафор; лишний вызов
  ждёт освобождения или `ctx.Done()`.
- `Healthy`: `exec.LookPath` + `picoclaw version` с бюджетом 10 с; при неудаче
  — `ErrUnavailable`.
- `Capabilities` возвращает `(nil, nil)`: из CLI навыки не
  интроспектируются, их объявляет оператор через `capabilities.skills`.
- `Close()` — no-op: процесс привязан к контексту задачи.

---

## 4. Режим `http` (`WSAdapter`, Pico Protocol)

### Соединение

- URL: `picoclaw.http.base_url` (`http(s)://…`) конвертируется в `ws(s)://…`
  и дополняется `picoclaw.http.path` (по умолчанию `/pico/ws`) —
  `wsURLFromHTTP`.
- Аутентификация: заголовок `Authorization: Bearer <токен>`; токен читается из
  переменной, имя которой задано `picoclaw.http.token_env` (в поставляемом
  шаблоне — `PICOCLAW_MESH_PICO_TOKEN`). Пустой токен → ошибка конфигурации на
  старте (`PicoClaw requires channels.pico.token`).
- Подпротокол WebSocket: `token.<значение>` (эхо шлюза).
- На задачу открывается **отдельное** соединение (`dial`), к URL добавляется
  `session_id=<sanitized SessionKey>`; это изолирует диалоги задач.

### Обмен

Отправка:

```json
{"type":"message.send","id":"zeptomesh-<n>","session_id":"…",
 "payload":{"content":"<instruction>"}}
```

Приём (`readTurn`) — до конца хода:

| Тип кадра | Обработка |
|---|---|
| `message.create` / `message.update` | ответом считается `payload.content`; кадры с `payload.kind` = `thought` или `tool_calls` — прогресс, игнорируются; `model_name` запоминается |
| `typing.stop` | конец хода: возвращает последний содержательный ответ; без ответа — ошибка `turn ended without an answer` |
| `error` | ошибка `picoclaw: gateway error <code>: <message>` |
| `pong`, неизвестные | игнор / отладочный лог |
| чистое закрытие после наличия ответа | успех |

Пустой текст после успеха → ошибка `gateway returned an empty answer`.

### Здоровье и лимиты

- `Healthy`: `GET <base_url><health_path>` (по умолчанию `/health`), 5 с,
  ожидание `200`.
- Таймаут: `picoclaw.http.timeout_seconds` → иначе `picoclaw.timeout_seconds`
  → иначе 10 минут.
- Семафор `max_concurrent_agents`, как в CLI-режиме.
- Артефакты собираются обходом `Workspace` (`collectArtifacts`), как в CLI.

### Про «REST» в названии режима

Имя `http` исторически из ТЗ (внутренний API «gRPC или HTTP/JSON»). Фактически
это WebSocket-клиент: у PicoClaw нет эндпоинта для постановки задачи, и
адаптер его не выдумывает. Порты: gateway-канал Pico — 18790 (вниманию: это не
`ZETOMESH_API_PORT` админ-API mesh'а и не порт лаунчера).

---

## 5. Режим `stub` (`StubAdapter`)

Детерминированный исполнитель без сети и учётных данных; нужен для тестов,
демо и initial-развёртывания без модели.

- спит `picoclaw.stub.latency` (прерывается контекстом);
- ответ: `[zeptomesh-stub] task=<id> skills=[…] accepted`, плюс, при
  `picoclaw.stub.echo: true`, строка `instruction: <инструкция>`;
- пишет `result.txt` в `Workspace` (0600) и возвращает его как артефакт —
  так проверяется сквозная работа сборки артефактов;
- `Healthy` всегда `nil`; `Capabilities` — skonfiguriрованные навыки;
  `Calls()`/`Running()` — счётчики для тестов.

**Важно:** в логах узла режим помечен `note: "no real agent is invoked"`.
Принимать чужие задачи со `stub` в продакшене бессмысленно — узел честно
сообщит `accepted`, но результата агентной работы не будет.

---

## 6. Ограничения задачи vs возможности агента

`canExecute` (`internal/tasks/manager.go`) перед локальным исполнением
пересекает права: задача не может требовать больше, чем разрешает узел
(`constraints.allow_shell`, `constraints.allow_network_tools`,
`capabilities.*`). Частичная проверка «задача просит shell, а узел его не
даёт» ведёт к отказу/делегированию, а не к тайному понижению прав.

Ограничения уровня ОС, которые адаптер **всё же** применяет (ТЗ 6.8.4, unix):

- `limitedCommand` (`internal/picoclaw/limits_unix.go`) оборачивает запуск так,
  что дочерний процесс работает под `RLIMIT_AS` (`ulimit -v` из `/bin/sh`,
  который затем через `exec` заменяется агентом — лишнего процесса не остаётся).
  Ограничение best-effort: если жёсткий лимит оператора ниже запрошенного,
  `ulimit` не применяется и задача идёт без него (`|| true` в обёртке). На
  не-unix (`limits_other.go`) лимита памяти нет.
- `prepareChild` даёт процессу собственную группу (`Setpgid`) и
  `Pdeathsig=SIGKILL`, а `cmd.Cancel = killChildGroup` убивает всю группу при
  отмене/таймауте — «внучатые» процессы агента не осиротеют.
- `collectArtifacts(req.Workspace, req.MaxWorkspaceBytes)` держит кумулятивную
  квоту на рабочие файлы задачи: при превышении обход останавливается
  (`SkipAll`). Считается она при сборе результатов, а не файловым лимитом ядра.

Прочее:

- `CLIAdapter`/`WSAdapter`/`StubAdapter` **не применяют** `Request.AllowShell`
  и `Request.AllowNetwork` к дочернему процессу или шлюзу: аргументы
  формируются независимо от этих флагов. Дочерний процесс не помещается в
  отдельный network namespace и не теряет привилегий — «выключенная оболочка»
  остаётся политикой узла (не брать задачу), а не техническим ограничением уже
  запущенного агента; для жёсткой границы нужен контейнер/песочница. См.
  [STATUS.md](STATUS.md#2-реализовано-частично) (ТЗ 6.8.4, 11.4).
- Адаптеры не управляют моделью PicoClaw, если `Request.Model` пуст (тогда
  решает конфигурация PicoClaw); `capabilities.models` mesh'ом в `Request`
  не прокидывается.
- Таймаут (`context.WithTimeout` + завершение по нему) — единственное
  ограничение по времени, наследуемое от `tasks.max_timeout_seconds`.

---

## 7. Настройка: минимальные рабочие примеры

```yaml
# Оффлайн (по умолчанию): ничего не вызывать, только отвечать детерминированно
picoclaw:
  mode: stub
  stub: { latency: 50ms, echo: true }
```

```yaml
# Бинарный режим
picoclaw:
  mode: binary
  binary: /usr/local/bin/picoclaw
  config: /etc/picoclaw/mesh.json     # → PICOCLAW_CONFIG
  timeout_seconds: 600
  max_concurrent_agents: 4
  env:
    OPENROUTER_API_KEY: ""            # лучше задавать окружением демона, не файлом
```

```yaml
# Pico Protocol через запущенный `picoclaw gateway`
picoclaw:
  mode: http
  http:
    base_url: http://127.0.0.1:18790
    path: /pico/ws
    health_path: /health
    token_env: PICOCLAW_MESH_PICO_TOKEN   # экспорт токена — вне файла конфига
    timeout_seconds: 600
```

Ключевое правило: **секреты не хранятся в `node.yaml`.** Для PicoClaw они
приходят из окружения демона (`PICOCLAW_*`, ключи провайдеров), для mesh —
через `api.auth_token_env`, `security.*_peers_file`, `ZETOMESH_PSK`.

---

## 8. Диагностика

| Симптом | Где смотреть | Вероятная причина |
|---|---|---|
| `picoclaw: picoclaw not found in PATH` | `adapter` в `GET /api/v1/status`, лог `picoclaw_adapter` | `mode: binary`, бинарь не установлен/не в PATH |
| `gateway health returned 503` | лог узла, `curl -fsS http://127.0.0.1:18790/health` | шлюз не запущен или иной порт |
| `pico token is empty` | ошибка старта узла | не экспортирована переменная из `token_env` |
| `ws handshake … (http 401)` | лог узла | токен не совпадает с `channels.pico.token` в конфиге PicoClaw |
| `agent produced no answer (exit 0)` | stdout-разбор, `ExtractAnswer` | у другой версии CLI изменился формат вывода |
| задача зависла и упала по таймауту | `zeptomesh_tasks_timeout_total`, `picoclaw: execution timeout` | `timeout_seconds` меньше реального времени хода |
| `picoclaw_processes_running` растёт, задачи в очереди | метрики, `max_concurrent_agents` | параллелизм агента ниже, чем `tasks.max_parallel_tasks` |

Полезные команды: `zeptomesh-node status` (в ответе — `adapter: {name,
healthy, detail}` из `picoclaw.Describe`), `zeptomesh-node capabilities`.
