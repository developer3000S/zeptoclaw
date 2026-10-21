#!/usr/bin/env bash
# =============================================================================
# install-ui.sh — установка ZeptoClaw Agent Mesh UI Dashboard
#
# Поддерживает два режима:
#   --mode local   — запуск напрямую (Rust-бинарь zeptomesh-ui)
#   --mode docker  — запуск в Docker-контейнере
#
# Опции:
#   --port PORT          порт UI (по умолчанию 28090)
#   --nodes ADDRS        список адресов API нод через запятую
#   --api-token TKN      bearer-токен для нод без собственного
#   --ui-token TKN       токен для авторизации самого дашборда (опционально)
#   --poll-interval SEC  интервал опроса нод в секундах (по умолчанию 5)
#   --data-dir DIR       каталог данных UI (по умолчанию ./zeptomesh-ui-data)
#   --name-prefix P      префикс имён контейнеров (по умолчанию zeptomesh-ui)
#   --net-mode MODE      host|bridge (по умолчанию host)
#   --log-level LEVEL    уровень логов BFF (по умолчанию info)
#   --yes                не спрашивать подтверждений
#   --uninstall          удалить установленный UI
#   -h, --help           показать эту справку
#
# UI — BFF на Rust (ZeptoClaw Mesh Dashboard, zeptoclaw_rust/src/ui/):
# хранит реестр управляемых нод в <data-dir>/nodes.json, агрегирует
# состояние mesh-сети в граф, проксирует операторские вызовы к admin-API
# узлов. SPA встроена в бинарь — отдельная статика не разворачивается.
# Токены к нодам в браузер не попадают.
#
# При установке в Docker:
#   - Удаляет старые образы zeptomesh-ui
#   - Проверяет порты на занятость, выбирает альтернативные
#   - Собирает Docker-образ из исходников репозитория
#   - Реестр нод переживает переустановку (том <data-dir>)
# =============================================================================

set -euo pipefail

# Цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'
BOLD='\033[1m'

# Значения по умолчанию
UI_PORT=28090
NODES=""
API_TOKEN=""
UI_TOKEN=""
POLL_INTERVAL=5
DATA_DIR="./zeptomesh-ui-data"
NAME_PREFIX="zeptomesh-ui"
MODE="docker"
NET_MODE="host"
LOG_LEVEL="info"
UNINSTALL_ONLY=false
SKIP_CONFIRM=false
COMPOSE_PROJECT_NAME="zeptomesh-ui"
IMAGE_NAME="zeptomesh-ui"
IMAGE_TAG="latest"

# Каталог репозитория (нужен для сборки образа и бинаря)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
while [[ ! -f "$REPO_ROOT/zeptoclaw_rust/Cargo.toml" ]]; do
    PARENT="$(cd "$REPO_ROOT/.." && pwd)"
    if [[ "$PARENT" == "$REPO_ROOT" ]]; then
        log_error "Не найден каталог zeptoclaw_rust (ожидался корень репозитория)"
        exit 1
    fi
    REPO_ROOT="$PARENT"
done

# =============================================================================
# Функции вывода
# =============================================================================

log_info() { echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_step() { echo -e "\n${BOLD}==>${NC} ${BOLD}$*${NC}"; }

# =============================================================================
# Помощь
# =============================================================================

show_help() {
    cat << EOF
${BOLD}USAGE:${NC}
    $0 [OPTIONS]

${BOLD}OPTIONS:${NC}
    --port PORT         Порт UI (по умолчанию: 28090)
    --nodes ADDRS       Список адресов API нод через запятую
                        Пример: http://127.0.0.1:14002,http://127.0.0.1:14003
    --api-token TKN     Bearer-токен для нод, у которых нет своего
    --ui-token TKN      Токен для авторизации самого дашборда (опционально)
    --poll-interval SEC Интервал опроса нод (по умолчанию: 5)
    --data-dir DIR      Каталог данных UI (по умолчанию: ./zeptomesh-ui-data)
    --name-prefix P     Префикс имён контейнеров (по умолчанию: zeptomesh-ui)
    --mode MODE         Режим: local|docker (по умолчанию: docker)
    --net-mode MODE     Сеть контейнера: host|bridge (по умолчанию: host)
    --log-level LEVEL   Уровень логов BFF: error|warn|info|debug (по умолчанию: info)
    --yes               Не спрашивать подтверждений
    --uninstall         Удалить установленный UI
    -h, --help          Показать эту справку

${BOLD}ПРИМЕРЫ:${NC}
    # Установить в Docker с автоопределением нод
    $0 --mode docker

    # Установить с конкретными нодами
    $0 --nodes "http://127.0.0.1:14002,http://127.0.0.1:14003"

    # Локальный запуск бинаря из исходников
    $0 --mode local --nodes "http://127.0.0.1:14002"

    # Удалить UI
    $0 --uninstall --mode docker

${BOLD}ДОСТУП:${NC}
    По умолчанию UI доступен: http://127.0.0.1:${UI_PORT}

EOF
}

# =============================================================================
# Парсинг аргументов
# =============================================================================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --port) UI_PORT="$2"; shift 2 ;;
            --nodes) NODES="$2"; shift 2 ;;
            --api-token) API_TOKEN="$2"; shift 2 ;;
            --ui-token) UI_TOKEN="$2"; shift 2 ;;
            --poll-interval) POLL_INTERVAL="$2"; shift 2 ;;
            --data-dir) DATA_DIR="$2"; shift 2 ;;
            --name-prefix) NAME_PREFIX="$2"; shift 2 ;;
            --mode) MODE="$2"; shift 2 ;;
            --net-mode) NET_MODE="$2"; shift 2 ;;
            --log-level) LOG_LEVEL="$2"; shift 2 ;;
            --yes) SKIP_CONFIRM=true; shift ;;
            --uninstall) UNINSTALL_ONLY=true; shift ;;
            -h|--help) show_help; exit 0 ;;
            *) log_error "Неизвестный аргумент: $1"; show_help; exit 1 ;;
        esac
    done

    if [[ "$MODE" != "local" && "$MODE" != "docker" ]]; then
        log_error "Неверный режим: $MODE. Допустимые: local, docker"
        exit 1
    fi
    if [[ "$NET_MODE" != "host" && "$NET_MODE" != "bridge" ]]; then
        log_error "Неверный режим сети: $NET_MODE. Допустимые: host, bridge"
        exit 1
    fi

    # Каталог данных должен быть абсолютным: compose разрешает относительные
    # пути относительно текущего каталога, а не каталога compose-файла.
    mkdir -p "$DATA_DIR"
    if [[ "$DATA_DIR" != /* ]]; then
        DATA_DIR="$PWD/$DATA_DIR"
    fi
}

# =============================================================================
# Проверка зависимостей
# =============================================================================

check_dependencies() {
    log_step "Проверка зависимостей"
    if [[ "$MODE" == "docker" ]]; then
        if ! command -v docker &> /dev/null; then
            log_error "Docker не установлен"
            exit 1
        fi
        if ! docker info &> /dev/null; then
            log_error "Docker демон не запущен"
            exit 1
        fi
        log_success "Docker: $(docker --version | head -1)"
    else
        if ! command -v rustc &> /dev/null; then
            log_error "Rust не установлен (нужен rustc + cargo)"
            exit 1
        fi
        if ! command -v cargo &> /dev/null; then
            log_error "Cargo не установлен"
            exit 1
        fi
        log_success "Rust: $(rustc --version | cut -d' ' -f2), Cargo: $(cargo --version | cut -d' ' -f2)"
    fi
}

# =============================================================================
# Поиск свободного порта
# =============================================================================

find_free_port() {
    local port=$1 attempt=0
    while [[ $attempt -lt 100 ]]; do
        if ! ss -tlnp 2>/dev/null | grep -qE ":${port}[[:space:]]" && \
           ! netstat -tlnp 2>/dev/null | grep -qE ":${port}[[:space:]]"; then
            echo "$port"; return 0
        fi
        attempt=$((attempt + 1))
        port=$((port + 1))
    done
    log_error "Не удалось найти свободный порт"
    return 1
}

# =============================================================================
# Автоопределение нод
# =============================================================================

discover_nodes() {
    log_step "Автоопределение нод"
    local discovered=""

    if [[ "$MODE" == "docker" ]]; then
        # У контейнеров узлов берём опубликованные на хосте порты admin-API.
        local container port api
        for container in $(docker ps --format '{{.Names}}' 2>/dev/null | grep -E '^zepto-[0-9]+$' || true); do
            port=$(docker inspect --format '{{range $p, $map := .NetworkSettings.Ports}}{{range $k, $v := $map}}{{printf "%s\n" $v.HostPort}}{{end}}{{end}}' "$container" 2>/dev/null | tail -1)
            if [[ -n "$port" ]]; then
                api="http://127.0.0.1:${port}"
                log_info "Найдена нода: $container -> $api"
                [[ -n "$discovered" ]] && discovered="${discovered}," || true
                discovered="${discovered}${api}"
            fi
        done
    else
        # Стандартные порты admin-API: 8081+ (конфиг узла) и 14001+
        # (install-nodes.sh: BASE_MESH_PORT + 10000).
        for port in 8081 8082 8083 8084 8085 14001 14002 14003 14004 14005 14006 14007 14008 14009 14010; do
            if curl -sf "http://127.0.0.1:${port}/healthz" &>/dev/null; then
                api="http://127.0.0.1:${port}"
                log_info "Найдена нода: $api"
                [[ -n "$discovered" ]] && discovered="${discovered}," || true
                discovered="${discovered}${api}"
            fi
        done
    fi

    if [[ -n "$discovered" ]]; then
        NODES="$discovered"
        log_success "Обнаружено нод: $(echo "$discovered" | tr ',' '\n' | wc -l)"
    else
        log_warn "Ноды не обнаружены автоматически — добавьте их вручную в дашборде"
    fi
}

# =============================================================================
# Нормализация списка нод
# =============================================================================

# Разбивает список URL по запятой, убирает пробелы и пустые элементы.
# Результат — список, по одному URL в строке.
normalize_nodes() {
    local raw="$1" url out=""
    local -a items=()
    IFS=',' read -ra items <<< "$raw" || true
    for url in "${items[@]}"; do
        url="$(echo "$url" | xargs)"
        [[ -z "$url" ]] && continue
        [[ -n "$out" ]] && out="${out}"$'\n'
        out="${out}${url}"
    done
    printf '%s' "$out"
}

# =============================================================================
# Генерация docker-compose.yml
# =============================================================================

generate_docker_compose() {
    log_step "Генерация docker-compose.yml"
    mkdir -p "$DATA_DIR"

    local nodes_env
    nodes_env="$(normalize_nodes "$NODES")"

    {
        echo 'services:'
        echo "  ${NAME_PREFIX}:"
        echo "    image: ${IMAGE_NAME}:${IMAGE_TAG}"
        echo "    container_name: ${NAME_PREFIX}"
        echo "    hostname: ${NAME_PREFIX}"
        echo "    restart: unless-stopped"
        echo "    environment:"
        echo "      ZEPTOMESH_UI_LISTEN: 0.0.0.0:${UI_PORT}"
        echo "      ZEPTOMESH_UI_DATA: /app/data"
        echo "      ZEPTOMESH_UI_NODES_FILE: /app/data/nodes.json"
        echo "      ZEPTOMESH_UI_LOG_LEVEL: ${LOG_LEVEL}"
        echo "      ZEPTOMESH_UI_POLL_INTERVAL: ${POLL_INTERVAL}"
        [[ -n "$nodes_env" ]] && echo "      ZEPTOMESH_UI_NODES: |-" && printf '%s\n' "$nodes_env" | sed 's/^/        /'
        [[ -n "$UI_TOKEN" ]] && echo "      ZEPTOMESH_UI_TOKEN: ${UI_TOKEN}"
        [[ -n "$API_TOKEN" ]] && echo "      ZEPTOMESH_UI_API_TOKEN: ${API_TOKEN}"
        echo "    volumes:"
        echo "      - ${DATA_DIR}:/app/data"
        if [[ "$NET_MODE" == "bridge" ]]; then
            echo "    extra_hosts:"
            echo '      - "host.docker.internal:host-gateway"'
        else
            echo "    network_mode: host"
        fi
    } > "$DATA_DIR/docker-compose.yml"

    log_success "docker-compose.yml создан: $DATA_DIR/docker-compose.yml"
}

# =============================================================================
# Установка в Docker
# =============================================================================

install_docker() {
    log_step "Установка UI в Docker"

    if [[ -z "$NODES" ]]; then
        discover_nodes
    fi

    log_info "Проверка порта $UI_PORT..."
    local free_port
    free_port=$(find_free_port "$UI_PORT") || exit 1
    if [[ "$free_port" != "$UI_PORT" ]]; then
        log_warn "Порт $UI_PORT занят, используем $free_port"
        UI_PORT="$free_port"
    fi
    log_success "Порт $UI_PORT свободен"

    generate_docker_compose

    log_info "Удаление старых образов ${IMAGE_NAME}..."
    docker rmi "$(docker images -q "${IMAGE_NAME}:${IMAGE_TAG}" 2>/dev/null)" 2>/dev/null || true

    log_info "Сборка образа из исходников (контекст $REPO_ROOT)..."
    log_info "Это занимает несколько минут при первом запуске."
    docker build -f "deploy/docker/Dockerfile.ui" -t "${IMAGE_NAME}:${IMAGE_TAG}" "$REPO_ROOT" || {
        log_error "Ошибка сборки Docker-образа"
        exit 1
    }
    log_success "Docker-образ собран"

    log_info "Запуск UI..."
    docker compose -p "$COMPOSE_PROJECT_NAME" -f "$DATA_DIR/docker-compose.yml" up -d

    log_info "Ожидание готовности UI..."
    local waited=0
    while [[ $waited -lt 60 ]]; do
        if curl -sf "http://127.0.0.1:${UI_PORT}/healthz" &>/dev/null; then
            log_success "UI готов!"
            break
        fi
        sleep 1
        waited=$((waited + 1))
    done
    if [[ $waited -ge 60 ]]; then
        log_error "UI не ответил на /healthz за 60 секунд"
        docker compose -p "$COMPOSE_PROJECT_NAME" -f "$DATA_DIR/docker-compose.yml" logs --tail=30 || true
        exit 1
    fi

    show_status
}

# =============================================================================
# Установка в локальном режиме
# =============================================================================

install_local() {
    log_step "Установка UI в локальном режиме"

    log_info "Проверка порта $UI_PORT..."
    local free_port
    free_port=$(find_free_port "$UI_PORT") || exit 1
    if [[ "$free_port" != "$UI_PORT" ]]; then
        log_warn "Порт $UI_PORT занят, используем $free_port"
        UI_PORT="$free_port"
    fi

    if [[ -z "$NODES" ]]; then
        discover_nodes
    fi

    log_step "Сборка zeptomesh-ui (cargo build --release)"
    ( cd "$REPO_ROOT/zeptoclaw_rust" && cargo build --release --bin zeptomesh-ui ) || {
        log_error "Ошибка сборки zeptomesh-ui"
        exit 1
    }
    log_success "Бинарь собран"

    local ui_dir="$REPO_ROOT/zeptomesh-ui-bin"
    rm -rf "$ui_dir"
    mkdir -p "$ui_dir"
    cp "$REPO_ROOT/zeptoclaw_rust/target/release/zeptomesh-ui" "$ui_dir/" || {
        log_error "Не удалось скопировать бинарь"
        exit 1
    }

    log_info "Запуск UI на порту $UI_PORT..."
    ZEPTOMESH_UI_LISTEN="0.0.0.0:${UI_PORT}" \
    ZEPTOMESH_UI_DATA="$DATA_DIR" \
    ZEPTOMESH_UI_NODES_FILE="$DATA_DIR/nodes.json" \
    ZEPTOMESH_UI_NODES="$(normalize_nodes "$NODES")" \
    ZEPTOMESH_UI_TOKEN="$UI_TOKEN" \
    ZEPTOMESH_UI_API_TOKEN="$API_TOKEN" \
    ZEPTOMESH_UI_POLL_INTERVAL="$POLL_INTERVAL" \
    ZEPTOMESH_UI_LOG_LEVEL="$LOG_LEVEL" \
        nohup "$ui_dir/zeptomesh-ui" > "$DATA_DIR/ui.log" 2>&1 &

    local waited=0
    while [[ $waited -lt 30 ]]; do
        if curl -sf "http://127.0.0.1:${UI_PORT}/healthz" &>/dev/null; then
            log_success "UI готов!"
            break
        fi
        sleep 1
        waited=$((waited + 1))
    done
    if [[ $waited -ge 30 ]]; then
        log_error "UI не ответил на /healthz за 30 секунд"
        tail -20 "$DATA_DIR/ui.log" 2>/dev/null || true
        exit 1
    fi

    show_status
}

# =============================================================================
# Деинсталляция
# =============================================================================

uninstall() {
    log_step "Деинсталляция UI"

    if [[ "$MODE" == "docker" ]]; then
        log_info "Остановка и удаление контейнеров..."
        docker compose -p "$COMPOSE_PROJECT_NAME" -f "$DATA_DIR/docker-compose.yml" down --remove-orphans 2>/dev/null || true
        docker rmi "$(docker images -q "${IMAGE_NAME}:${IMAGE_TAG}" 2>/dev/null)" 2>/dev/null || true
        log_success "Docker контейнеры удалены"
    else
        log_info "Остановка UI..."
        pkill -f "zeptomesh-ui-bin/zeptomesh-ui" 2>/dev/null || true
        rm -rf "$REPO_ROOT/zeptomesh-ui-bin"
        log_success "Локальный UI удалён"
    fi

    if [[ "$SKIP_CONFIRM" == "true" ]] || read -p "Удалить данные в $DATA_DIR? (y/N): " -n 1 -r && echo; then
        if [[ "$SKIP_CONFIRM" == "true" ]] || [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -rf "$DATA_DIR"
            log_success "Данные удалены"
        fi
    fi
}

# =============================================================================
# Статус
# =============================================================================

show_status() {
    log_step "Статус"
    echo ""
    echo -e "${BOLD}UI Dashboard:${NC}"
    echo "  URL: http://127.0.0.1:${UI_PORT}"
    echo "  Данные: $DATA_DIR"
    echo "  Реестр нод: $DATA_DIR/nodes.json"
    if [[ "$MODE" == "docker" ]]; then
        echo "  Режим: docker (сеть: $NET_MODE)"
        echo ""
        docker compose -p "$COMPOSE_PROJECT_NAME" -f "$DATA_DIR/docker-compose.yml" ps 2>/dev/null || echo "  Контейнер не запущен"
    else
        echo "  Режим: local"
    fi
    echo ""
    echo -e "${BOLD}Команды управления:${NC}"
    if [[ "$MODE" == "docker" ]]; then
        echo "  Логи:      docker compose -p $COMPOSE_PROJECT_NAME -f $DATA_DIR/docker-compose.yml logs -f"
        echo "  Остановка: docker compose -p $COMPOSE_PROJECT_NAME -f $DATA_DIR/docker-compose.yml down"
        echo "  Перезапуск: docker compose -p $COMPOSE_PROJECT_NAME -f $DATA_DIR/docker-compose.yml restart"
    else
        echo "  Логи:      tail -f $DATA_DIR/ui.log"
        echo "  Остановка: pkill -f zeptomesh-ui-bin/zeptomesh-ui"
    fi
    echo ""
}

# =============================================================================
# Основная функция
# =============================================================================

main() {
    parse_args "$@"

    echo ""
    echo -e "${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║${NC}        ${BOLD}ZeptoClaw Agent Mesh — Установка UI${NC}              ${BOLD}║${NC}"
    echo -e "${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
    echo ""

    log_info "Режим: $MODE"
    log_info "Сеть контейнера: $NET_MODE"
    log_info "Порт UI: $UI_PORT"
    log_info "Каталог данных: $DATA_DIR"

    if [[ "$UNINSTALL_ONLY" == "true" ]]; then
        uninstall
        log_success "Деинсталляция завершена"
        exit 0
    fi

    check_dependencies

    if [[ "$SKIP_CONFIRM" != "true" ]]; then
        echo ""
        read -p "Продолжить установку? (Y/n): " -n 1 -r || true
        echo
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            log_info "Установка отменена"
            exit 0
        fi
    fi

    if [[ "$MODE" == "docker" ]]; then
        install_docker
    else
        install_local
    fi
}

main "$@"
