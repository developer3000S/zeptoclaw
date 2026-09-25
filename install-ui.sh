#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# ZeptoClaw Mesh UI — независимая установка веб-панели в Docker.
#
#   ./install-ui.sh                          автоопределение узлов zeptomesh-*
#   ./install-ui.sh --host-gateway           агенты стоят локально/systemd
#   ./install-ui.sh --nodes http://host:8081,...
#   ./install-ui.sh status|start|stop|uninstall
#
# Панель — отдельный образ и отдельный compose-стек (проект zeptomesh-ui).
# Установка и удаление трогают ТОЛЬКО UI-контейнер, его образ и его стек:
# настройку и данные агентов не затрагивает. Работает исключительно через
# готовый админ-API узлов (internal/api/admin.go).
#
# Режим по умолчанию подключает UI-контейнер к сети zeptomesh_default стека
# агентов (проект zeptomesh, контейнеры zeptomesh-N) и адреса узлов выводит
# из их env ZETOMESH_API_PORT: http://zepto-0:<порт>,…
# Режим --host-gateway сеть не использует: узлы видны через host-gateway как
# http://host.docker.internal:<порт> — для агентов, установленных через
# install.sh --mode local (systemd); порты задаёт оператор.
#
# Список узлов — источник истины UI (nodes.json в томе). ZETOMESH_UI_NODES
# сеет его при первом старте; дальнейшее изменение списка — через вкладку
# «Агенты» самой панели (POST/DELETE /api/v1/nodes).
# ---------------------------------------------------------------------------
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_DIR

IMAGE="zeptomesh-ui:latest"
COMPOSE_PROJECT="zeptomesh-ui"
AGENT_NETWORK="zeptomesh_default"   # сеть стека агентов (проект zeptomesh)
CONTAINER="zeptomesh-ui"

UI_PORT="${ZETOMESH_UI_PORT:-28090}"
NODES_ARG=""                        # --nodes / ZETOMESH_UI_NODES
GATEWAY_MODE=0
DATA_DIR=""
POLL_INTERVAL="${ZETOMESH_UI_POLL_INTERVAL:-5s}"
LOG_LEVEL="${ZETOMESH_UI_LOG_LEVEL:-info}"
ASSUME_YES=0

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m!!\033[0m %s\n' "$*" >&2; exit 1; }

is_root() { [[ "$(id -u)" -eq 0 ]]; }

ask() { # ask <вопрос> <по-умолчанию>
  local q="$1" def="${2:-}" ans
  if [[ $ASSUME_YES -eq 1 ]]; then printf '%s' "$def"; return; fi
  printf '%s [%s]: ' "$q" "$def" >&2
  IFS= read -r ans || true
  printf '%s' "${ans:-$def}"
}

default_data_dir() {
  if is_root; then printf '/var/lib/zeptomesh-ui'
  else printf '%s' "${XDG_DATA_HOME:-$HOME/.local/share}/zeptomesh-ui"; fi
}

guess_data_dir() {
  local c
  for c in /var/lib/zeptomesh-ui "${XDG_DATA_HOME:-$HOME/.local/share}/zeptomesh-ui"; do
    [[ -d "$c/docker" ]] && { printf '%s' "$c"; return; }
  done
  printf ''
}

require_docker() {
  command -v docker >/dev/null 2>&1 || die "не найден Docker Engine"
  docker compose version >/dev/null 2>&1 || die "нужен плагин docker compose v2"
  docker info >/dev/null 2>&1 || die "нет доступа к демону Docker (группа docker или root)"
}

usage() {
  cat <<'EOF'
Использование: ./install-ui.sh [команда] [опции]

Команды:
  install     установка и запуск (по умолчанию)
  start       запустить установленную панель
  stop        остановить (без удаления)
  status      состояние панели
  uninstall   удалить UI-контейнер, образ и стек (данные узлов сохраняются)

Опции install:
  --host-gateway      не подключаться к сети агентов; узлы видны через
                      host-gateway как http://host.docker.internal:<порт>
                      (для local/systemd установки агентов)
  --nodes URLS        список адресов админ-API узлов через запятую
                      (иначе env ZETOMESH_UI_NODES, иначе автоопределение)
  --port N            порт панели на хосте (default: 28090, env ZETOMESH_UI_PORT)
  --data-dir DIR      каталог для стека (default: /var/lib/zeptomesh-ui для root)
  --poll-interval D   интервал опроса узлов (default: 5s)
  --log-level L       trace|debug|info|warn|error (default: info)
  -y, --yes           не задавать вопросов

Доступ к API узлов:
  ZETOMESH_UI_TOKEN       токен доступа к самому API панели (включает авторизацию)
  ZETOMESH_UI_TOKEN       токен доступа к самому API панели (включает авторизацию)
  ZETOMESH_UI_API_TOKEN   bearer для узлов без собственного токена
  ZETOMESH_API_TOKEN      если узлы требуют токен, задайте его же здесь
EOF
}

# ---------- аргументы ----------

# Define UI_API_TOKEN if not already set.
# ${VAR:-} обязателен: скрипт под set -u, и проверка [ -z "$UI_API_TOKEN" ]
# на несуществующей переменной падает до тела блока.
if [ -z "${UI_API_TOKEN:-}" ]; then
    UI_API_TOKEN="$(openssl rand -hex 32)"
    export UI_API_TOKEN
fi

COMMAND="install"
if [[ $# -gt 0 && "$1" != --* ]]; then
  case "$1" in
    install|start|stop|status|uninstall) COMMAND="$1"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "неизвестная команда '$1' (./install-ui.sh --help)" ;;
  esac
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host-gateway)  GATEWAY_MODE=1; shift ;;
    --nodes)         NODES_ARG="${2:-}"; shift 2 ;;
    --port)          UI_PORT="${2:-}"; shift 2 ;;
    --data-dir)      DATA_DIR="${2:-}"; shift 2 ;;
    --poll-interval) POLL_INTERVAL="${2:-}"; shift 2 ;;
    --log-level)     LOG_LEVEL="${2:-}"; shift 2 ;;
    -y|--yes)        ASSUME_YES=1; shift ;;
    -h|--help)       usage; exit 0 ;;
    *) die "неизвестная опция '$1' (./install-ui.sh --help)" ;;
  esac
done

[[ "$UI_PORT" =~ ^[0-9]+$ ]] || die "--port: целое число, получено '$UI_PORT'"
(( UI_PORT > 0 && UI_PORT < 65536 )) || die "--port: допустимо 1..65535"

compose_file() { printf '%s' "${DATA_DIR}/docker/docker-compose.yml"; }

# ---------- проверка порта ----------

# port_in_use PORT — занят ли порт на хосте (внешние подключения, не loopback-
# Only биндинг тоже считается: панель слушает 0.0.0.0).
port_in_use() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -tuln 2>/dev/null | awk '{print $5}' | grep -qE "(^|[^0-9])${port}$" && return 0
  elif command -v netstat >/dev/null 2>&1; then
    netstat -tuln 2>/dev/null | awk '{print $4}' | grep -qE "(^|[^0-9])${port}$" && return 0
  fi
  if command -v docker >/dev/null 2>&1; then
    docker ps --format '{{.Ports}}' 2>/dev/null | grep -qE "(^|[^0-9])${port}->" && return 0
  fi
  return 1
}

# ensure_free_port — порт панели должен быть свободен к моменту старта.
# Сначала пытаемся освободить занятый порт (контейнер старой панели); если
# порт занят сторонним процессом, не имеющим отношения к Docker — уходим на
# следующий свободный, чтобы панель запустилась в любом случае.
ensure_free_port() {
  if ! port_in_use "$UI_PORT"; then
    log "порт панели: $UI_PORT (свободен)"
    return 0
  fi
  if clear_port "$UI_PORT"; then
    log "порт панели: $UI_PORT (освобождён)"
    return 0
  fi
  local p
  for (( p = UI_PORT + 1; p < 65536; p++ )); do
    if ! port_in_use "$p"; then
      warn "порт $UI_PORT занят сторонним процессом — панель переходит на порт $p"
      UI_PORT="$p"
      return 0
    fi
  done
  die "не найдено свободного порта начиная с $UI_PORT"
}

# ---------- авторизация панели ----------

# token_file — куда персистится токен доступа, чтобы при переустановке панели
# токен не менялся (иначе браузер оператора будет снова просить его ввести).
token_file() { printf '%s' "${DATA_DIR}/ui-token"; }

# gen_token — криптостойкий случайный токен (hex).
gen_token() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32 && return 0
  fi
  od -An -tx1 -N32 /dev/urandom 2>/dev/null | tr -d ' \n' && return 0
  die "нет ни openssl, ни /dev/urandom — нечем сгенерировать токен"
}

# resolve_token — выбирает токен доступа к API панели:
# 1) явный ZETOMESH_UI_TOKEN из окружения;
# 2) ранее сохранённый ${DATA_DIR}/ui-token (персистентность при переустановках);
# 3) новая случайная генерация.
# Результат — в глобальной UI_TOKEN, и он всегда записывается на диск (0600),
# чтобы оператор мог его прочитать, а контейнер — получить через compose.
UI_TOKEN=""
resolve_token() {
  local stored
  if [[ -n "${ZETOMESH_UI_TOKEN:-}" ]]; then
    UI_TOKEN="$ZETOMESH_UI_TOKEN"
    log "токен доступа: из ZETOMESH_UI_TOKEN окружения"
    return 0
  fi
  if [[ -f "$(token_file)" ]]; then
    stored="$(tr -d '[:space:]' < "$(token_file)")"
    if [[ -n "$stored" ]]; then
      UI_TOKEN="$stored"
      log "токен доступа: ранее сохранённый ($(token_file))"
      return 0
    fi
  fi
  UI_TOKEN="$(gen_token)"
  log "токен доступа: сгенерирован новый"
  printf '%s' "$UI_TOKEN" > "$(token_file)" 2>/dev/null \
    || warn "не удалось сохранить токен в $(token_file) — при следующей установке будет новый"
  chmod 600 "$(token_file)" 2>/dev/null
}

# ---------- обнаружение узлов ----------

# agent_containers — имена работающих контейнеров стека агентов по порядку.
agent_containers() {
  docker ps --format '{{.Names}}' 2>/dev/null \
    | grep -E '^zeptomesh-[0-9]+$' | sort -t- -k2 -n || true
}

# detect_agent_nodes — выводит "http://zepto-0:8081,…" по env ZETOMESH_API_PORT
# каждого контейнера zeptomesh-N. Внутри сети стека контейнеры видны по
# dns-имени службы (zepto-N), а порт админ-API контейнера совпадает с хостовым.
detect_agent_nodes() {
  local c idx port nodes=""
  while IFS= read -r c; do
    [[ -z "$c" ]] && continue
    port="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$c" 2>/dev/null \
      | sed -nE 's/^ZETOMESH_API_PORT=([0-9]+)$/\1/p' | head -n1 || true)"
    idx="${c#zeptomesh-}"
    if [[ -n "$port" ]]; then
      nodes="${nodes:+$nodes,}http://zepto-$idx:$port"
    else
      warn "контейнер $c: не найден env ZETOMESH_API_PORT — узел добавьте вручную (вкладка «Агенты»)"
    fi
  done < <(agent_containers)
  printf '%s' "$nodes"
}

# ---------- compose-стек ----------

# write_compose — генерирует стек панели. В отличие от стека агентов, у панели
# нет вариантов по количеству экземпляров, поэтому файл целиком детерминирован;
# переустановка перезапишет правки — для изменений используйте вкладку «Агенты».
write_compose() {
  local file="$1" nodes="$2"
  mkdir -p "$(dirname "$file")"
  : > "$file"
  {
    echo "# сгенерировано install-ui.sh: панель ZeptoClaw Mesh UI"
    echo "name: ${COMPOSE_PROJECT}"
    echo "services:"
    cat <<EOF
  zeptomesh-ui:
    image: ${IMAGE}
    container_name: ${CONTAINER}
    hostname: ${CONTAINER}
    restart: unless-stopped
    stop_grace_period: 15s
    init: true
    environment:
      # Слушатель обязан быть 0.0.0.0: иначе проброшенный на хост порт не ответит.
      ZETOMESH_UI_LISTEN: "0.0.0.0:${UI_PORT}"
      ZETOMESH_UI_NODES: "${nodes}"
      ZETOMESH_UI_TOKEN: "${UI_TOKEN}"
      ZETOMESH_UI_API_TOKEN: "${UI_API_TOKEN}"
      ZETOMESH_UI_POLL_INTERVAL: "${POLL_INTERVAL}"
      ZETOMESH_UI_LOG_LEVEL: "${LOG_LEVEL}"
      ZETOMESH_UI_DATA: "/var/lib/zeptomesh-ui"
      TZ: "${TZ:-UTC}"
    ports:
      # 0.0.0.0: доступ с других машин (панель слушает 0.0.0.0 внутри контейнера).
      # Для loopback-only замените на 127.0.0.1: и перезапустите стек.
      - "0.0.0.0:${UI_PORT}:${UI_PORT}"
    volumes:
      - zeptomesh-ui-data:/var/lib/zeptomesh-ui
EOF
    if [[ $GATEWAY_MODE -eq 1 ]]; then
      echo '    extra_hosts:'
      echo '      - "host.docker.internal:host-gateway"'
    else
      echo '    networks:'
      echo '      - agents'
    fi
    echo '    healthcheck:'
    echo "      test: [\"CMD\", \"curl\", \"-fsS\", \"http://127.0.0.1:${UI_PORT}/healthz\"]"
    echo '      interval: 30s'
    echo '      timeout: 5s'
    echo '      start_period: 20s'
    echo '      retries: 3'
    if [[ $GATEWAY_MODE -eq 0 ]]; then
      echo 'networks:'
      echo '  agents:'
      echo "    name: ${AGENT_NETWORK}"
      echo '    external: true'
    fi
    echo 'volumes:'
    echo '  zeptomesh-ui-data:'
  } > "$file"
  log "compose-стек панели: $file"
}

# ---------- установка ----------

# remove_old_image — удаляет старый образ панели до сборки нового и до
# проверки порта. Образ, занятый контейнером, удалить нельзя, поэтому контейнер
# предыдущей панели останавливается и удаляется — заодно освобождая порт.
remove_old_image() {
  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    log "старый образ $IMAGE не найден — удаление не требуется"
    return 0
  fi
  local c ids=()
  while IFS= read -r c; do
    [[ -n "$c" ]] && ids+=("$c")
  done < <(docker ps -aq --filter "ancestor=$IMAGE" 2>/dev/null)
  if ((${#ids[@]})); then
    log "удаление $IMAGE: образ занят контейнерами (${ids[*]}) — останавливаю и удаляю их"
    docker stop "${ids[@]}" >/dev/null 2>&1 || true
    docker rm -f "${ids[@]}" >/dev/null 2>&1 || true
  else
    log "удаление старого образа $IMAGE"
  fi
  docker image rm -f "$IMAGE" >/dev/null 2>&1 \
    || warn "не удалось удалить образ $IMAGE (удалите вручную: docker image rm -f $IMAGE)"
  # Пересборка без кэша оставляет висячие слои предыдущей версии — чистим.
  docker image prune -f >/dev/null 2>&1 || true
}

# clear_port PORT — освобождает порт, занятый docker-контейнером.
# Возвращает 0, если порт свободен или был освобождён; 1 — если порт занят
# процессом, не являющимся контейнером Docker (его не трогаем).
clear_port() {
  local port="$1" c
  if ! port_in_use "$port"; then return 0; fi
  local ids=()
  while IFS= read -r c; do
    [[ -n "$c" ]] && ids+=("$c")
  done < <(docker ps -q --filter "publish=$port" 2>/dev/null)
  if ((!${#ids[@]})); then return 1; fi
  log "порт $port занят контейнерами (${ids[*]}) — освобождаю"
  docker stop "${ids[@]}" >/dev/null 2>&1 || true
  docker rm -f "${ids[@]}" >/dev/null 2>&1 || true
  # docker-proxy отдаёт порт не мгновенно — даём ему время.
  local t=0
  while (( t < 20 )) && port_in_use "$port"; do sleep 0.5; t=$((t + 1)); done
  port_in_use "$port" && return 1
  return 0
}

build_image() {
  local dockerfile="$REPO_DIR/deploy/ui/Dockerfile"
  [[ -f "$dockerfile" ]] \
    || die "нет $dockerfile — выполните §6.1 docs/UI-ТЗ.md ('make docker-build-ui')"

  log "сборка образа ${IMAGE} (без кэша)"
  docker build --no-cache -f "$dockerfile" \
    --build-arg GIT_COMMIT="$(git -C "$REPO_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)" \
    --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -t "$IMAGE" "$REPO_DIR"
}

wait_healthy() {
  local tries=0
  while (( tries < 30 )); do
    if docker exec "$CONTAINER" sh -c "curl -fsS http://127.0.0.1:${UI_PORT}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
    tries=$((tries + 1))
  done
  warn "панель не ответила на /healthz за 60 с: docker compose -f \"$(compose_file)\" logs"
  return 1
}

install_ui() {
  require_docker
  DATA_DIR="${DATA_DIR:-$(default_data_dir)}"
  mkdir -p "$DATA_DIR/docker" || die "не могу создать $DATA_DIR/docker"

  # 1. Удаляем старый образ — до проверки порта: контейнер старой панели при
  #    этом останавливается, и порт освобождается до его проверки.
  remove_old_image

  # 2. Проверяем порт до сборки и генерации compose: занятый — освобождаем
  #    (старые контейнеры), иначе уходим на первый свободный.
  ensure_free_port

  # Токен доступа к API панели: из окружения, из предыдущей установки или
  # новый — он вписывается в compose при генерации ниже.
  resolve_token

  local nodes=""
  if [[ -n "$NODES_ARG" ]]; then
    nodes="$NODES_ARG"
  elif [[ -n "${ZETOMESH_UI_NODES:-}" ]]; then
    nodes="$ZETOMESH_UI_NODES"
  fi

  if [[ $GATEWAY_MODE -eq 1 ]]; then
    log "режим host-gateway: узлы как http://host.docker.internal:<порт>"
    if [[ -z "$nodes" ]]; then
      warn "список узлов пуст — задайте --nodes или env ZETOMESH_UI_NODES,"
      warn "иначе добавьте узлы позже через вкладку «Агенты» панели"
    fi
  else
    if [[ -z "$nodes" ]]; then
      log "автоопределение узлов стека агентов (контейнеры zeptomesh-*)"
      nodes="$(detect_agent_nodes)"
    fi
    if [[ -z "$nodes" ]]; then
      warn "контейнеры zeptomesh-* не найдены — mesh агентов не запущена?"
      warn "если агенты установлены локально/systemd — используйте --host-gateway"
    fi
    # external-сеть обязана существовать к моменту up, иначе compose упадёт.
    if ! docker network inspect "$AGENT_NETWORK" >/dev/null 2>&1; then
      die "сеть $AGENT_NETWORK не найдена. Поднимите стек агентов (./install.sh --mode docker) либо используйте --host-gateway"
    fi
  fi

  [[ -n "$nodes" ]] && log "узлы панели: ${nodes}"

  # 3. Собираем образ панели без кэша.
  build_image

  write_compose "$(compose_file)" "$nodes"

  log "запуск контейнера ${CONTAINER}"
  docker compose -f "$(compose_file)" up -d \
    || die "compose up завершился с ошибкой: docker compose -f \"$(compose_file)\" ps"

  wait_healthy || true

  echo
  log "готово: панель Mesh UI запущена (автостарт — restart: unless-stopped)"
  printf '  URL:   http://127.0.0.1:%s\n' "$UI_PORT"
  printf '  токен: %s\n' "$UI_TOKEN"
  printf '         доступ к API панели; сохранён в %s\n' "$(token_file)"
  printf '  стек:  %s\n' "$(compose_file)"
  if [[ $GATEWAY_MODE -eq 0 && -n "$nodes" ]]; then
    printf '  узлы:  %s\n' "$nodes"
  fi
  cat <<EOF

Управление:
  ./install-ui.sh status|start|stop|uninstall
  логи:       docker compose -f "$(compose_file)" logs -f
  данные (nodes.json): том zeptomesh-ui-data

Список узлов хранится в nodes.json и сеется значением ZETOMESH_UI_NODES только
при первом старте. Меняйте состав узлов через вкладку «Агенты» панели —
агенты при этом не перенастраиваются.
EOF
}

# ---------- управление ----------

cmd_status() {
  DATA_DIR="${DATA_DIR:-$(guess_data_dir)}"
  [[ -n "$DATA_DIR" && -f "$(compose_file)" ]] \
    || die "установка панели не найдена (укажите --data-dir)"
  docker compose -f "$(compose_file)" ps
  if [[ -f "$(token_file)" ]]; then
    echo
    log "токен доступа к API панели ($(token_file)):"
    printf '  %s\n' "$(tr -d '[:space:]' < "$(token_file)")"
  fi
}

cmd_start() {
  DATA_DIR="${DATA_DIR:-$(guess_data_dir)}"
  [[ -n "$DATA_DIR" && -f "$(compose_file)" ]] \
    || die "установка панели не найдена (укажите --data-dir)"
  docker compose -f "$(compose_file)" start
}

cmd_stop() {
  DATA_DIR="${DATA_DIR:-$(guess_data_dir)}"
  [[ -n "$DATA_DIR" && -f "$(compose_file)" ]] \
    || die "установка панели не найдена (укажите --data-dir)"
  docker compose -f "$(compose_file)" stop
}

# Удаляет только панель: контейнер, образ и стек. Имена томов/сети агентов
# (проект zeptomesh) не задействованы; nodes.json сохраняется в томе.
cmd_uninstall() {
  DATA_DIR="${DATA_DIR:-$(guess_data_dir)}"
  [[ -n "$DATA_DIR" && -f "$(compose_file)" ]] \
    || die "установка панели не найдена (укажите --data-dir)"
  docker compose -f "$(compose_file)" down --remove-orphans || true
  if [[ "$(ask "Удалить образ ${IMAGE}" "y")" == "y" ]]; then
    docker image rm "$IMAGE" 2>/dev/null || warn "образ $IMAGE не удалён (возможно, используется)"
  fi
  rm -f "$(compose_file)"
  log "панель удалена. Узлы и сеть агентов не затронуты; nodes.json остался в томе zeptomesh-ui-data"
}

# ---------- вход ----------

case "$COMMAND" in
  status)    cmd_status; exit 0 ;;
  start)     cmd_start; exit 0 ;;
  stop)      cmd_stop; exit 0 ;;
  uninstall) cmd_uninstall; exit 0 ;;
esac

install_ui
