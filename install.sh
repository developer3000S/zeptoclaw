#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# ZeptoClaw Agent Mesh — установка и автозапуск узлов.
#
#   ./install.sh                          интерактивный опрос
#   ./install.sh --mode docker --nodes 3
#   ./install.sh --mode local  --nodes 2 --yes
#   ./install.sh status|start|stop|uninstall
#
# Режим local собирает бинарник и создаёт systemd-юниты (system — от root,
# user — от обычного пользователя). Режим docker строит образ и поднимает
# compose-стек. В обоих случаях узлы стартуют сразу и стартуют при каждой
# загрузке системы.
#
# Экземпляры нумеруются с 0; по умолчанию каждому выбирается случайный
# свободный пятизначный порт (10000–65535). Если заданы переменные
# ZETOMESH_BASE_MESH_PORT, ZETOMESH_BASE_API_PORT или ZETOMESH_BASE_PROM_PORT,
# порты назначаются последовательно от указанной базы (base + index).
# ---------------------------------------------------------------------------
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_DIR

MESH_BASE_PORT="${ZETOMESH_BASE_MESH_PORT:-4001}"
API_BASE_PORT="${ZETOMESH_BASE_API_PORT:-8081}"
PROM_BASE_PORT="${ZETOMESH_BASE_PROM_PORT:-9464}"

# Если ни один базовый порт не задан явно — используем автоматический поиск
# свободных случайных пятизначных портов (10000–65535).
AUTO_PORTS=1
if [[ -n "${ZETOMESH_BASE_MESH_PORT:-}" || -n "${ZETOMESH_BASE_API_PORT:-}" || -n "${ZETOMESH_BASE_PROM_PORT:-}" ]]; then
  AUTO_PORTS=0
fi

MODE=""                 # local | docker
NODES=""                # количество экземпляров
DATA_DIR=""             # корень данных
API_HOST="127.0.0.1"    # адрес админ-API в local-режиме
TRUST_MODE="limited"    # open | limited | private
PICO_MODE="stub"        # stub | binary | http
BOOTSTRAP=""            # multiaddr через запятую
PSK=""                  # общий ключ закрытой сети
LOG_LEVEL="info"
ASSUME_YES=0
SERVICE="zeptomesh"     # имя шаблона юнита: ${SERVICE}@<i>
RUN_USER=""             # от имени кого запускать (local, system-юниты)

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m!!\033[0m %s\n' "$*" >&2; exit 1; }

# ---------- поиск свободных портов ----------

# Массив уже зарезервированных портов в текущем запуске.
_RESERVED_PORTS=()

# Массивы выделенных портов (индекс = номер экземпляра).
MESH_PORTS=()
API_PORTS=()
PROM_PORTS=()

# _port_in_use PORT — проверяет, занят ли порт (tcp или udp) на хосте или
# уже зарезервирован в текущем запуске.
_port_in_use() {
  local port="$1" p
  for p in "${_RESERVED_PORTS[@]+"${_RESERVED_PORTS[@]}"}"; do
    [[ "$p" == "$port" ]] && return 0
  done
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

# find_free_port — возвращает случайный свободный пятизначный порт (10000–65535).
find_free_port() {
  local attempts=0 port
  while (( attempts < 200 )); do
    if command -v shuf >/dev/null 2>&1; then
      port=$(shuf -i 10000-65535 -n 1)
    else
      port=$(( RANDOM + 10000 ))
    fi
    if ! _port_in_use "$port"; then
      _RESERVED_PORTS+=("$port")
      printf '%s' "$port"
      return 0
    fi
    attempts=$((attempts + 1))
  done
  die "не удалось найти свободный порт за 200 попыток"
}

# next_free_from PORT — первый свободный порт, начиная с PORT (сам PORT
# включительно). Используется в режиме последовательного распределения от
# базы: если базовый порт занят, сдвигаемся вверх, пока не найдём свободный.
# Результат — в глобальной RET_PORT (не stdout): функцию нельзя вызывать в
# командной подстановке — подоболочка потеряет резервирование.
RET_PORT=""
next_free_from() {
  local p="$1"
  while (( p < 65536 )); do
    if ! _port_in_use "$p"; then
      _RESERVED_PORTS+=("$p")
      RET_PORT="$p"
      return 0
    fi
    p=$((p + 1))
  done
  die "не найдено свободного порта начиная с $1"
}

# allocate_ports — выделяет mesh/api/prom порты для всех экземпляров.
# При AUTO_PORTS=1 ищет случайные свободные, иначе — последовательные от базы
# с проверкой занятости: занятый базовый порт сдвигается на первый свободный.
allocate_ports() {
  local i
  for ((i = 0; i < NODES; i++)); do
    if [[ $AUTO_PORTS -eq 1 ]]; then
      MESH_PORTS[$i]=$(find_free_port)
      API_PORTS[$i]=$(find_free_port)
      PROM_PORTS[$i]=$(find_free_port)
    else
      # next_free_from вызывается напрямую (не через $()): командная подстановка
      # запускает подоболочку, и резервирование в _RESERVED_PORTS потеряется.
      local mesh api prom
      next_free_from $((MESH_BASE_PORT + i)); mesh="$RET_PORT"
      next_free_from $((API_BASE_PORT + i));  api="$RET_PORT"
      next_free_from $((PROM_BASE_PORT + i)); prom="$RET_PORT"
      MESH_PORTS[$i]=$mesh
      API_PORTS[$i]=$api
      PROM_PORTS[$i]=$prom
    fi
  done
  log "порты выделены ($([ $AUTO_PORTS -eq 1 ] && echo 'случайные свободные' || echo 'последовательные от базы, с проверкой занятости')):"
  for ((i = 0; i < NODES; i++)); do
    log "  экземпляр $i: mesh=${MESH_PORTS[$i]} api=${API_PORTS[$i]} prom=${PROM_PORTS[$i]}"
  done
}

usage() {
  cat <<'EOF'
Использование: ./install.sh [команда] [опции]

Команды:
  install     установка и запуск (по умолчанию)
  start       запустить установленные экземпляры
  stop        остановить (без удаления)
  status      состояние экземпляров
  uninstall   удалить сервисы и автостарт (данные остаются на месте)

Опции install:
  --mode local|docker    способ установки (по умолчанию — опрос)
  --nodes N              количество экземпляров Агента (по умолчанию — опрос)
  --data-dir DIR         корень данных (default: /var/lib/zeptomesh для root,
                         ~/.local/share/zeptomesh для пользователя)
  --api-host HOST        адрес админ-API (default: 127.0.0.1)
  --trust-mode M         open | limited | private (default: limited)
  --pico-mode M          stub | binary | http (default: stub)
  --bootstrap ADDRS      точки входа mesh через запятую
  --psk KEY              общий ключ закрытой сети (одинаковый у всех узлов)
  --run-user NAME        пользователь сервисов (default: zeptomesh для root)
  --log-level L          debug | info | warn | error
  -y, --yes              не задавать вопросов

Позиционные параметры install (эквивалент ответов интерактивного опроса):
  ./install.sh                               # интерактивный опрос
  ./install.sh --mode docker --nodes 3       # запуск в Docker трёх экземпляров
  ./install.sh --mode local  --nodes 2 --yes # локальный запуск двух экземпляров
EOF
}

# ---------- аргументы ----------

COMMAND="install"
if [[ $# -gt 0 && "$1" != --* ]]; then
  case "$1" in
    install|start|stop|status|uninstall) COMMAND="$1"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "неизвестная команда '$1' (./install.sh --help)" ;;
  esac
fi

# Первые два позиционных аргумента повторяют вопросы интерактивного опроса:
# номер способа установки и количество экземпляров. Именованные опции имеют
# приоритет и могут использоваться вместе с позиционными параметрами.
if [[ "$COMMAND" == install && $# -gt 0 && "$1" != --* ]]; then
  case "$1" in
    1|local)  MODE=local ;;
    2|docker) MODE=docker ;;
    *) die "неизвестный способ установки '$1' (./install.sh --help)" ;;
  esac
  shift

  if [[ $# -gt 0 && "$1" != --* ]]; then
    [[ "$1" =~ ^[1-9][0-9]*$ ]] || die "количество экземпляров — целое > 0, получено '$1'"
    NODES="$1"
    shift
  fi

  [[ $# -eq 0 || "$1" == --* ]] || die "неожиданный аргумент '$1' (./install.sh --help)"
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)       MODE="${2:-}"; shift 2 ;;
    --nodes)      NODES="${2:-}"; shift 2 ;;
    --data-dir)   DATA_DIR="${2:-}"; shift 2 ;;
    --api-host)   API_HOST="${2:-}"; shift 2 ;;
    --trust-mode) TRUST_MODE="${2:-}"; shift 2 ;;
    --pico-mode)  PICO_MODE="${2:-}"; shift 2 ;;
    --bootstrap)  BOOTSTRAP="${2:-}"; shift 2 ;;
    --psk)        PSK="${2:-}"; shift 2 ;;
    --run-user)   RUN_USER="${2:-}"; shift 2 ;;
    --log-level)  LOG_LEVEL="${2:-}"; shift 2 ;;
    -y|--yes)     ASSUME_YES=1; shift ;;
    -h|--help)    usage; exit 0 ;;
    *) die "неизвестная опция '$1' (./install.sh --help)" ;;
  esac
done

is_root() { [[ "$(id -u)" -eq 0 ]]; }

ask() { # ask <вопрос> <по-умолчанию>
  local q="$1" def="${2:-}" ans
  if [[ $ASSUME_YES -eq 1 ]]; then printf '%s' "$def"; return; fi
  printf '%s [%s]: ' "$q" "$def" >&2
  IFS= read -r ans || true
  printf '%s' "${ans:-$def}"
}

prompt_settings() {
  if [[ -z "$MODE" ]]; then
    local c; c="$(ask "Способ установки — 1) локально (systemd) или 2) в Docker" "1")"
    case "$c" in
      1|local)  MODE=local ;;
      2|docker) MODE=docker ;;
      *)        die "непонятный способ установки '$c'" ;;
    esac
  fi
  case "$MODE" in local|docker) ;; *) die "--mode должен быть local или docker" ;; esac

  if [[ -z "$NODES" ]]; then
    NODES="$(ask "Сколько экземпляров Агента установить" "1")"
  fi
  [[ "$NODES" =~ ^[1-9][0-9]*$ ]] || die "количество экземпляров — целое > 0, получено '$NODES'"
  (( NODES <= 64 )) || die "слишком много экземпляров ($NODES): порты до $((MESH_BASE_PORT + NODES))"

  case "$TRUST_MODE" in open|limited|private) ;; *) die "--trust-mode: open|limited|private" ;; esac
  case "$PICO_MODE" in stub|binary|http) ;; *) die "--pico-mode: stub|binary|http" ;; esac
}

default_data_dir() {
  if is_root; then printf '/var/lib/zeptomesh'
  else printf '%s' "${XDG_DATA_HOME:-$HOME/.local/share}/zeptomesh"; fi
}

guess_data_dir() {
  local c
  for c in /var/lib/zeptomesh "${XDG_DATA_HOME:-$HOME/.local/share}/zeptomesh"; do
    [[ -d "$c" ]] && { printf '%s' "$c"; return; }
  done
  printf ''
}

require_cmd() { command -v "$1" >/dev/null 2>&1 || die "не найдена программа '$1'${2:+ — $2}"; }

check_go() {
  require_cmd go "нужен Go 1.26+ для локальной сборки"
  local want have
  want="$(sed -nE 's/^go ([0-9]+\.[0-9]+).*/\1/p' "$REPO_DIR/go.mod")"
  have="$(go env GOVERSION 2>/dev/null | sed -E 's/^go//' | cut -d. -f1,2)"
  if [[ "$(printf '%s\n%s\n' "$want" "$have" | sort -V | head -1)" != "$want" ]]; then
    die "нужен Go ${want}, установлен ${have:-неизвестно}"
  fi
}

systemd_kind() { # -> system | user | (пусто)
  command -v systemctl >/dev/null 2>&1 || { printf ''; return; }
  [[ -d /run/systemd/system ]] || { printf ''; return; }
  if is_root; then printf 'system'; else printf 'user'; fi
}

systemctl_kind() { local k="$1"; shift; if [[ "$k" == user ]]; then systemctl --user "$@"; else systemctl "$@"; fi; }

bin_path() { printf '%s' "${ZETOMESH_BIN:-$DATA_DIR/bin/zeptomesh-node}"; }

build_binary() {
  local out="$1" commit date
  commit="$(git -C "$REPO_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  mkdir -p "$(dirname "$out")"
  log "сборка $out (go $(go env GOVERSION))"
  ( cd "$REPO_DIR" && GOFLAGS=-mod=mod CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/developer3000S/zeptoclaw/internal/version.GitCommit=${commit} -X github.com/developer3000S/zeptoclaw/internal/version.BuildDate=${date}" \
      -o "$out" ./cmd/zeptomesh-node )
  chmod 0755 "$out"
}

primary_ip() {
  if command -v ip >/dev/null 2>&1; then
    ip -4 -o addr show scope global 2>/dev/null | awk '{split($4,a,"/"); print a[1]; exit}'
  elif command -v hostname >/dev/null 2>&1; then
    hostname -I 2>/dev/null | awk '{print $1}'
  fi
}

# ---------- конфигурация экземпляра ----------

# write_instance_env DIR INDEX — окружение одного экземпляра; шаблоном служит
# configs/node.yaml, поэтому файл конфигурации на индекс не плодится.
write_instance_env() {
  local dir="$1" idx="$2"
  local mesh="${MESH_PORTS[$idx]}" api="${API_PORTS[$idx]}" prom="${PROM_PORTS[$idx]}"
  mkdir -p "$dir"
  ( umask 077
    cat > "$dir/instance.env" <<EOF
# сгенерировано install.sh: экземпляр $idx
ZETOMESH_INDEX=$idx
ZETOMESH_NAME=$SERVICE-$idx
ZETOMESH_DATA=$DATA_DIR
ZETOMESH_MESH_PORT=$mesh
ZETOMESH_API_PORT=$api
ZETOMESH_PROM_LISTEN=$API_HOST:$prom
ZETOMESH_API_HOST=$API_HOST
ZETOMESH_TRUST_MODE=$TRUST_MODE
ZETOMESH_PICO_MODE=$PICO_MODE
ZETOMESH_MDNS=true
ZETOMESH_DHT_MODE=auto
ZETOMESH_LOG_LEVEL=$LOG_LEVEL
ZETOMESH_PSK=${PSK:-}
ZETOMESH_BOOTSTRAP=${BOOTSTRAP:-}
ZETOMESH_API_TOKEN=${ZETOMESH_API_TOKEN:-}
EOF
  )
  chmod 0600 "$dir/instance.env"
}

# install_config DEST — устанавливает общий шаблон конфигурации. Различия
# экземпляров задаются не копиями конфига, а переменными из instance.env
# (загрузчик раскрывает ${VAR:-default} сам, см. internal/config.expandEnv),
# поэтому один файл обслуживает все N узлов.
install_config() {
  local dest="$1" tmpl="$2"
  [[ -f "$dest" ]] && return 0
  cp "$tmpl" "$dest"
  chmod 0644 "$dest"
}

# ---------- local (systemd) ----------

install_local() {
  local kind; kind="$(systemd_kind)"
  if [[ -z "$kind" ]]; then
    die "systemd недоступен. Установите в Docker (--mode docker) либо запустите узел вручную:
    <бинарник> run -config $REPO_DIR/configs/node.yaml (передав ZETOMESH_* переменные)"
  fi
  check_go
  DATA_DIR="${DATA_DIR:-$(default_data_dir)}"
  local template="$REPO_DIR/configs/node.yaml"
  [[ -f "$template" ]] || die "нет шаблона конфигурации $template"

  mkdir -p "$DATA_DIR" || die "не могу создать $DATA_DIR"

  local bin; bin="$(bin_path)"
  build_binary "$bin"

  # Системные юниты не должны работать от root: создаём служебного пользователя.
  if [[ "$kind" == system ]]; then
    RUN_USER="${RUN_USER:-zeptomesh}"
    if ! id "$RUN_USER" >/dev/null 2>&1; then
      useradd --system --home-dir "$DATA_DIR" --shell /usr/sbin/nologin "$RUN_USER" \
        || die "не удалось создать пользователя $RUN_USER"
    fi
    chown -R "$RUN_USER:$RUN_USER" "$DATA_DIR"
  fi

  local cfg_file="$DATA_DIR/etc/node.yaml"
  mkdir -p "$(dirname "$cfg_file")"
  install_config "$cfg_file" "$template"
  allocate_ports
  log "создание $NODES экземпляр(ов) в $DATA_DIR"

  local i
  for ((i = 0; i < NODES; i++)); do
    local idir="$DATA_DIR/$i"
    mkdir -p "$idir/keys" "$idir/run" "$idir/db" "$idir/artifacts" "$idir/tasks" "$idir/audit"
    chmod 0700 "$idir/keys" "$idir/run"
    write_instance_env "$idir" "$i"

    # Ключ создаётся один раз: его потеря меняет peer ID узла.
    if [[ ! -f "$idir/keys/peer.key" ]]; then
      "$bin" genkey --out "$idir/keys/peer.key" > "$idir/peer_id.txt"
      chmod 0600 "$idir/keys/peer.key" "$idir/peer_id.txt"
      log "экземпляр $i: peer ID $(cat "$idir/peer_id.txt")"
    fi
  done
  [[ "$kind" == system ]] && chown -R "$RUN_USER:$RUN_USER" "$DATA_DIR"

  # При нескольких экземплярах остальные должны знать, к кому идти первому:
  # без этого mesh распадается на изолированные узлы.
  if (( NODES > 1 )) && [[ -z "$BOOTSTRAP" ]]; then
    local first_id first_ip first_addr
    first_id="$(cat "$DATA_DIR/0/peer_id.txt")"
    first_ip="$(primary_ip || true)"
    [[ -n "$first_ip" ]] || warn "не определён адрес хоста: узлы найдут друг друга по mDNS"
    first_addr="/ip4/${first_ip:-127.0.0.1}/tcp/${MESH_PORTS[0]}/p2p/$first_id"
    for ((i = 1; i < NODES; i++)); do
      sed -i -E "s|^ZETOMESH_BOOTSTRAP=.*|ZETOMESH_BOOTSTRAP=$first_addr|" "$DATA_DIR/$i/instance.env"
      chmod 0600 "$DATA_DIR/$i/instance.env"
    done
    log "точка входа для экземпляров 1..$((NODES - 1)): $first_addr"
  fi

  install_units "$kind" "$bin" "$cfg_file"
  start_all "$kind"
  report_local "$kind"
}

install_units() {
  local kind="$1" bin="$2" cfg_file="$3" unit_dir
  if [[ "$kind" == system ]]; then unit_dir=/etc/systemd/system
  else unit_dir="$HOME/.config/systemd/user"; fi
  mkdir -p "$unit_dir"

  local user_line=""
  [[ "$kind" == system ]] && user_line="User=$RUN_USER"

  cat > "$unit_dir/$SERVICE@" <<EOF
# ZeptoClaw Agent Mesh — экземпляр %i (сгенерировано install.sh)
[Unit]
Description=ZeptoClaw Agent Mesh node %i
Documentation=file:$REPO_DIR/docs/ARCHITECTURE.md
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
$user_line
Environment=ZETOMESH_DATA=$DATA_DIR
EnvironmentFile=$DATA_DIR/%i/instance.env
ExecStart=$bin run -config $cfg_file
WorkingDirectory=$DATA_DIR/%i
# SIGHUP перечитывает конфигурацию (совпадает с POST /admin/reload-config).
ExecReload=/bin/kill -HUP $MAINPID
# Узел при admin/leave завершается кодом 75 — systemd перезапускает его,
# и сеть получает корректное объявление об уходе вместо таймаута.
Restart=on-failure
RestartSec=5
TimeoutStopSec=30
KillSignal=SIGTERM
UMask=0077
# Опасные операции по умолчанию выключены (ТЗ 11.4); изоляция — жёсткая, но не
# мешающая писать в собственный каталог данных.
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=read-only
ReadWritePaths=$DATA_DIR

[Install]
WantedBy=$( [[ "$kind" == system ]] && echo multi-user.target || echo default.target )
EOF

  if [[ "$kind" == user ]] && command -v loginctl >/dev/null 2>&1; then
    loginctl enable-linger "$(id -un)" 2>/dev/null \
      || warn "linger не включён: user-юниты остановятся после выхода из сессии"
  fi
  log "юнит $unit_dir/$SERVICE@ (шаблон на N экземпляров)"
}

start_all() {
  local kind="$1" i
  systemctl_kind "$kind" daemon-reload
  for ((i = 0; i < NODES; i++)); do
    log "автостарт + запуск $SERVICE@$i"
    systemctl_kind "$kind" enable --now "$SERVICE@$i" >/dev/null 2>&1 \
      || die "не удалось запустить $SERVICE@$i: systemctl${kind:+ --user}$kind status $SERVICE@$i"
  done
}

report_local() {
  local kind="$1" i
  echo
  log "готово: $NODES экземпляр(ов) запущены, автостарт включён ($kind systemd)"
  for ((i = 0; i < NODES; i++)); do
    printf '  %-3s peer ID %-53s mesh tcp+udp/%s  API http://%s:%s  metrics %s:%s\n' \
      "$i" "$(cat "$DATA_DIR/$i/peer_id.txt" 2>/dev/null || echo '-')" \
      "${MESH_PORTS[$i]}" "$API_HOST" "${API_PORTS[$i]}" "$API_HOST" "${PROM_PORTS[$i]}"
  done
  cat <<EOF

Управление:
  ./install.sh status|start|stop|uninstall
  journalctl $( [[ "$(systemd_kind)" == user ]] && printf -- '--user ' )-u $SERVICE@0 -f
  данные и ключи: $DATA_DIR/<i>/
EOF
}

# ---------- docker ----------

# remove_old_image IMAGE — удаляет старый образ до сборки нового и до
# проверки портов. Образ, занятый контейнерами, удалить нельзя, поэтому
# контейнеры, использующие его (контейнеры предыдущего стека), останавливаются
# и удаляются — заодно освобождая порты, которые они держали.
remove_old_image() {
  local img="$1" c
  if ! docker image inspect "$img" >/dev/null 2>&1; then
    log "старый образ $img не найден — удаление не требуется"
    return 0
  fi
  local -a ids=()
  while IFS= read -r c; do
    [[ -n "$c" ]] && ids+=("$c")
  done < <(docker ps -aq --filter "ancestor=$img" 2>/dev/null)
  if ((${#ids[@]})); then
    log "удаление $img: образ занят контейнерами (${ids[*]}) — останавливаю и удаляю их"
    docker stop "${ids[@]}" >/dev/null 2>&1 || true
    docker rm -f "${ids[@]}" >/dev/null 2>&1 || true
  else
    log "удаление старого образа $img"
  fi
  docker image rm -f "$img" >/dev/null 2>&1 \
    || warn "не удалось удалить образ $img (удалите вручную: docker image rm -f $img)"
  # Пересборка без кэша оставляет висячие слои предыдущей версии — чистим,
  # иначе они копятся при каждом переустанавливающем запуске.
  docker image prune -f >/dev/null 2>&1 || true
}

# clear_port PORT — освобождает порт, занятый docker-контейнером на хосте.
# Сторонние процессы не трогаем: снимаем только контейнеры Docker.
# Возвращает 0, если порт свободен или был освобождён, 1 — если занят ещё
# кем-то (процесс вне Docker).
clear_port() {
  local port="$1" c
  if ! _port_in_use "$port"; then return 0; fi
  local -a ids=()
  while IFS= read -r c; do
    [[ -n "$c" ]] && ids+=("$c")
  done < <(docker ps -q --filter "publish=$port" 2>/dev/null)
  if ((!${#ids[@]})); then return 1; fi
  log "порт $port занят контейнерами (${ids[*]}) — освобождаю"
  docker stop "${ids[@]}" >/dev/null 2>&1 || true
  docker rm -f "${ids[@]}" >/dev/null 2>&1 || true
  # docker-proxy отдаёт порт не мгновенно — даём ему время.
  local t=0
  while (( t < 20 )) && _port_in_use "$port"; do sleep 0.5; t=$((t + 1)); done
  _port_in_use "$port" && return 1
  return 0
}

# clear_stack_ports — освобождает порты, которые стек будет занимать. В режиме
# автоматического подбира порты и так выбираются свободными; при распределении
# от базы проверяем базовые порты заранее и освобождаем занятые старыми
# контейнерами (остаток — забота allocate_ports, он сдвинется вверх).
clear_stack_ports() {
  if [[ $AUTO_PORTS -eq 1 ]]; then
    log "порты: автоматический выбор свободных (предочистка не требуется)"
    return 0
  fi
  local i
  for ((i = 0; i < NODES; i++)); do
    local mesh=$((MESH_BASE_PORT + i)) api=$((API_BASE_PORT + i)) prom=$((PROM_BASE_PORT + i))
    clear_port "$mesh" || warn "порт $mesh занят не контейнером — будет подобран свободный"
    clear_port "$api"  || warn "порт $api занят не контейнером — будет подобран свободный"
    clear_port "$prom" || warn "порт $prom занят не контейнером — будет подобран свободный"
  done
}

# build_docker_image — сборка образа узла; всегда без кэша, иначе новый код
# может притащить слои предыдущей сборки.
build_docker_image() {
  log "сборка образа zeptomesh-node:latest (без кэша)"
  docker build --no-cache -f "$REPO_DIR/deploy/docker/Dockerfile" \
    --build-arg GIT_COMMIT="$(git -C "$REPO_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)" \
    --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -t zeptomesh-node:latest "$REPO_DIR"
}

install_docker() {
  require_cmd docker "нужен Docker Engine"
  docker compose version >/dev/null 2>&1 || die "нужен плагин docker compose v2"
  docker info >/dev/null 2>&1 || die "нет доступа к демону Docker (группа docker или root)"

  DATA_DIR="${DATA_DIR:-$(default_data_dir)}"
  local stack_dir="$DATA_DIR/docker"
  mkdir -p "$stack_dir" || die "не могу создать $stack_dir"

  # 1. Удаляем старые образы — до проверки портов: контейнеры старого образа
  #    при этом останавливаются, и порты освобождаются до распределения.
  remove_old_image zeptomesh-node:latest

  # 2. Проверяем порты: занятые — освобождаем (старые контейнеры), затем
  #    распределяем mesh/api/prom по экземплярам.
  clear_stack_ports
  allocate_ports

  # 3. Собираем новый образ без кэша.
  build_docker_image

  write_compose "$stack_dir/docker-compose.yml"
  local compose_file="$stack_dir/docker-compose.yml"

  # Точка входа без /p2p/<peer id> узел отвергает на старте (адрес без
  # идентичности не к чему диалить), а peer ID узла становится известен только
  # после первого запуска — он рождается в ключе на томе. Поэтому поднимаем
  # якорь отдельно, спрашиваем его ID у его же /healthz и дописываем остальным.
  if (( NODES > 1 )) && [[ -z "$BOOTSTRAP" ]]; then
    log "запуск якоря zepto-0 (узел 0)"
    docker compose -f "$compose_file" up -d zepto-0 \
      || die "compose up zepto-0: docker compose -f $compose_file ps"
    local zp="" tries=0
    while (( tries < 30 )); do
      zp="$(docker exec zeptomesh-0 sh -c \
              "curl -fsS http://127.0.0.1:${API_PORTS[0]}/healthz" 2>/dev/null \
            | sed -nE 's/.*"peer_id":"([^"]+)".*/\1/p')"
      [[ -n "$zp" ]] && break
      sleep 2; tries=$((tries + 1))
    done
    if [[ -n "$zp" ]]; then
      local addr="/dns4/zepto-0/tcp/${MESH_PORTS[0]}/p2p/$zp"
      log "точка входа для узлов 1..$((NODES-1)): $addr"
      sed -i -E "s|%%ANCHOR%%|$addr|g" "$compose_file"
    else
      warn "peer ID якоря не получен за 60 с: остальные узлы стартуют без bootstrap — сойдутся через DHT/PEX или после ручного заполнения ZETOMESH_BOOTSTRAP (команда ниже)"
      sed -i -E "s|%%ANCHOR%%||g" "$compose_file"
    fi
  fi

  log "запуск $NODES контейнеров"
  docker compose -f "$compose_file" up -d \
    || die "compose up завершился с ошибкой: docker compose -f $compose_file ps"

  local i
  echo
  log "готово: $NODES экземпляр(ов), автостарт — restart: unless-stopped"
  for ((i = 0; i < NODES; i++)); do
    printf '  %-3s mesh tcp+udp/%s  API http://127.0.0.1:%s  metrics http://127.0.0.1:%s\n' \
      "$i" "${MESH_PORTS[$i]}" "${API_PORTS[$i]}" "${PROM_PORTS[$i]}"
  done
  cat <<EOF

Точки входа для других хостов (нужен p2p-адрес узла, без /p2p/… адрес не принимается):
  docker exec zeptomesh-0 sh -c 'curl -fsS http://127.0.0.1:${API_PORTS[0]}/healthz'
  # или с хоста:  curl -fsS http://127.0.0.1:${API_PORTS[0]}/healthz
Управление: ./install.sh status|start|stop|uninstall
EOF
}

write_compose() {
  local file="$1" i
  : > "$file"
  {
    echo "# сгенерировано install.sh: mesh на $NODES экземпляр(ов) — правки перезапишет переустановка"
    echo "name: zeptomesh"
    echo "services:"
    for ((i = 0; i < NODES; i++)); do
      local mesh="${MESH_PORTS[$i]}" api="${API_PORTS[$i]}" prom="${PROM_PORTS[$i]}"
      local boot="$BOOTSTRAP"
      # Внутри compose-сети узлы видны по dns-именам, но точка входа обязана
      # содержать /p2p/<peer id> якоря — а он известен только после его первого
      # запуска. Остальные помечаются маркером; install_docker поднимает якорь,
      # узнаёт его ID из /healthz и вписывает адрес в файл до общего запуска.
      [[ -z "$boot" && $i -gt 0 ]] && boot="%%ANCHOR%%"
      cat <<EOF
  zepto-$i:
    image: zeptomesh-node:latest
    container_name: zeptomesh-$i
    hostname: zepto-$i
    restart: unless-stopped
    stop_grace_period: 30s
    init: true
    environment:
      ZETOMESH_INDEX: "$i"
      ZETOMESH_NAME: "zepto-$i"
      ZETOMESH_MESH_PORT: "$mesh"
      ZETOMESH_API_PORT: "$api"
      ZETOMESH_PROM_LISTEN: "0.0.0.0:$prom"
      ZETOMESH_API_HOST: "0.0.0.0"
      ZETOMESH_TRUST_MODE: "$TRUST_MODE"
      ZETOMESH_PICO_MODE: "$PICO_MODE"
      ZETOMESH_MDNS: "false"
      ZETOMESH_DHT_MODE: "auto"
      ZETOMESH_LOG_LEVEL: "$LOG_LEVEL"
      ZETOMESH_BOOTSTRAP: "$boot"
      ZETOMESH_PSK: "\${ZETOMESH_PSK:-}"
      ZETOMESH_API_TOKEN: "\${ZETOMESH_API_TOKEN:-}"
      TZ: "${TZ:-UTC}"
    ports:
      - "$mesh:$mesh/tcp"
      - "$mesh:$mesh/udp"
      - "127.0.0.1:$api:$api"
      - "127.0.0.1:$prom:$prom"
    volumes:
      - zepto-$i-data:/var/lib/zeptomesh
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://127.0.0.1:$api/healthz"]
      interval: 30s
      timeout: 5s
      start_period: 25s
      retries: 3
EOF
    done
    echo "volumes:"
    for ((i = 0; i < NODES; i++)); do echo "  zepto-$i-data:"; done
  } > "$file"
  log "compose-стек: $file"
}

# ---------- управление ----------

installed_nodes() {
  local d
  for d in "$DATA_DIR"/[0-9]*/instance.env; do
    [[ -f "$d" ]] && basename "$(dirname "$d")"
  done | sort -n
}

docker_stack() { printf '%s' "$DATA_DIR/docker/docker-compose.yml"; }

cmd_status() {
  DATA_DIR="${DATA_DIR:-$(guess_data_dir)}"
  [[ -n "$DATA_DIR" && -d "$DATA_DIR" ]] || die "установка не найдена (укажите --data-dir)"
  if command -v docker >/dev/null 2>&1 && [[ -f "$(docker_stack)" ]]; then
    docker compose -f "$(docker_stack)" ps
    return
  fi
  local kind; kind="$(systemd_kind)"
  [[ -n "$kind" ]] || { warn "systemd не найден"; return; }
  local n=0 i
  for i in $(installed_nodes); do
    n=$((n + 1))
    local state
    state="$(systemctl_kind "$kind" is-active "$SERVICE@$i" 2>/dev/null || true)"
    printf '  %-3s %-10s peer ID %s\n' "$i" "${state:-unknown}" \
      "$(cat "$DATA_DIR/$i/peer_id.txt" 2>/dev/null || echo '-')"
  done
  [[ $n -gt 0 ]] || warn "экземпляры в $DATA_DIR не найдены"
}

cmd_start() {
  DATA_DIR="${DATA_DIR:-$(guess_data_dir)}"
  if [[ -f "$(docker_stack)" ]] && command -v docker >/dev/null 2>&1; then
    docker compose -f "$(docker_stack)" start; return
  fi
  local kind; kind="$(systemd_kind)"
  [[ -n "$kind" ]] || die "systemd недоступен"
  local i; for i in $(installed_nodes); do systemctl_kind "$kind" start "$SERVICE@$i"; done
}

cmd_stop() {
  DATA_DIR="${DATA_DIR:-$(guess_data_dir)}"
  if [[ -f "$(docker_stack)" ]] && command -v docker >/dev/null 2>&1; then
    docker compose -f "$(docker_stack)" stop; return
  fi
  local kind; kind="$(systemd_kind)"
  [[ -n "$kind" ]] || die "systemd недоступен"
  local i; for i in $(installed_nodes); do systemctl_kind "$kind" stop "$SERVICE@$i" || true; done
}

cmd_uninstall() {
  DATA_DIR="${DATA_DIR:-$(guess_data_dir)}"
  [[ -n "$DATA_DIR" ]] || die "укажите --data-dir"
  if [[ -f "$(docker_stack)" ]] && command -v docker >/dev/null 2>&1; then
    docker compose -f "$(docker_stack)" down --remove-orphans || true
    rm -f "$(docker_stack)"
  fi
  local kind; kind="$(systemd_kind)"
  if [[ -n "$kind" ]]; then
    local i
    for i in $(installed_nodes); do
      systemctl_kind "$kind" disable --now "$SERVICE@$i" >/dev/null 2>&1 || true
    done
    if [[ "$kind" == system ]]; then
      rm -f "/etc/systemd/system/$SERVICE@"
      systemctl daemon-reload
    else
      rm -f "$HOME/.config/systemd/user/$SERVICE@"
      systemctl --user daemon-reload
    fi
  fi
  log "сервисы и автостарт удалены. Данные сохранены в $DATA_DIR — удалите вручную, если они больше не нужны"
}

# ---------- вход ----------

main() {
  case "$COMMAND" in
    status)    cmd_status; exit 0 ;;
    start)     cmd_start; exit 0 ;;
    stop)      cmd_stop; exit 0 ;;
    uninstall) cmd_uninstall; exit 0 ;;
  esac

  prompt_settings
  log "режим=$MODE экземпляров=$NODES trust=$TRUST_MODE pico=$PICO_MODE"
  case "$MODE" in
    docker) install_docker ;;
    local)  install_local ;;
  esac
}

main "$@"
