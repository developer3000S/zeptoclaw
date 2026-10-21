#!/usr/bin/env bash
# =============================================================================
# install-nodes.sh — установка ZeptoClaw Agent Mesh нод
#
# Поддерживает два режима:
#   --mode local   — запуск напрямую (systemd user units)
#   --mode docker  — запуск в Docker-контейнерах
#
# Опции:
#   --nodes N        количество нод (по умолчанию 2)
#   --mode MODE      local|docker (по умолчанию docker)
#   --base-port M    начальный порт mesh (по умолчанию 4001)
#   --psk KEY        PSK для закрытой сети (опционально)
#   --pico-mode MODE pico mode: stub|binary|http|ollama (по умолчанию stub)
#   --ollama-url URL URL локального Ollama (по умолчанию пусто)
#   --name-prefix P  префикс имён нод (по умолчанию zepto)
#   --data-dir DIR   корневой каталог данных (по умолчанию ./zeptomesh-data)
#   --yes            не спрашивать подтверждений
#   --uninstall      удалить установленные ноды
#
# При установке в Docker:
#   - Удаляет старые образы zeptomesh-node
#   - Проверяет порты на занятость, выбирает альтернативные
#   - Собирает Docker-образ из текущего исходного кода
#
# =============================================================================

set -euo pipefail

# Цвета
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Значения по умолчанию
NUM_NODES=2
MODE="docker"
BASE_MESH_PORT=4001
PSK=""
PICO_MODE="stub"
OLLAMA_URL=""
NAME_PREFIX="zepto"
DATA_DIR="./zeptomesh-data"
UNINSTALL_ONLY=false
SKIP_CONFIRM=false

# Переменные для режима Docker
COMPOSE_PROJECT_NAME="zeptomesh"
IMAGE_NAME="zeptomesh-node"
IMAGE_TAG="latest"

# Массивы портов (глобальные — заполняются в prepare_docker / install_local
# и читаются в generate_docker_compose / install_local / install_docker)
declare -a MESH_PORTS=()
declare -a API_PORTS=()
declare -a PROM_PORTS=()

# Канонический репозиторий
REPO_URL="git@github.com:developer3000S/zeptoclaw.git"

# =============================================================================
# Функции вывода
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

log_step() {
    echo -e "\n${BOLD}==>${NC} ${BOLD}$*${NC}"
}

# =============================================================================
# Помощь
# =============================================================================

show_help() {
    cat << EOF
${BOLD}USAGE:${NC}
    $0 [OPTIONS]

${BOLD}OPTIONS:${NC}
    --nodes N         Количество нод для установки (по умолчанию: 2)
    --mode MODE       Режим: local|docker (по умолчанию: docker)
    --base-port M     Начальный mesh-порт (по умолчанию: 4001)
    --psk KEY         PSK для закрытой сети (опционально)
    --pico-mode MODE  PicoClaw mode: stub|binary|http|ollama (по умолчанию: stub)
    --ollama-url URL  URL локального Ollama (по умолчанию: пусто)
    --name-prefix P   Префикс имён нод (по умолчанию: zepto)
    --data-dir DIR    Корневой каталог данных (по умолчанию: ./zeptomesh-data)
    --yes             Не спрашивать подтверждений
    --uninstall       Только удалить установленные ноды
    -h, --help        Показать эту справку

${BOLD}EXAMPLES:${NC}
    # Установить 3 ноды в Docker
    $0 --nodes 3 --mode docker

    # Установить 2 ноды с PSK
    $0 --nodes 2 --psk "/1/..." --mode docker

    # Установить 1 ноду в локальном режиме
    $0 --nodes 1 --mode local

    # Установить с Ollama
    $0 --nodes 2 --pico-mode ollama --ollama-url http://127.0.0.1:11434

    # Удалить все ноды
    $0 --uninstall --mode docker

${BOLD}PORTS:${NC}
    Для N нод используются порты:
    - Mesh:     BASE_PORT, BASE_PORT+1, ..., BASE_PORT+N-1
    - API:      BASE_PORT+10000+0, BASE_PORT+10000+1, ..., BASE_PORT+10000+N-1
    - Prom:     BASE_PORT+20000+0, BASE_PORT+20000+1, ..., BASE_PORT+20000+N-1
    При конфликте автоматически выбираются свободные порты.

EOF
}

# =============================================================================
# Парсинг аргументов
# =============================================================================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --nodes)
                NUM_NODES="$2"
                shift 2
                ;;
            --mode)
                MODE="$2"
                shift 2
                ;;
            --base-port)
                BASE_MESH_PORT="$2"
                shift 2
                ;;
            --psk)
                PSK="$2"
                shift 2
                ;;
            --pico-mode)
                PICO_MODE="$2"
                shift 2
                ;;
            --ollama-url)
                OLLAMA_URL="$2"
                shift 2
                ;;
            --name-prefix)
                NAME_PREFIX="$2"
                shift 2
                ;;
            --data-dir)
                DATA_DIR="$2"
                shift 2
                ;;
            --yes)
                SKIP_CONFIRM=true
                shift
                ;;
            --uninstall)
                UNINSTALL_ONLY=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Неизвестный аргумент: $1"
                show_help
                exit 1
                ;;
        esac
    done

    # Валидация
    if [[ "$MODE" != "local" && "$MODE" != "docker" ]]; then
        log_error "Неверный режим: $MODE. Допустимые значения: local, docker"
        exit 1
    fi

    if [[ "$PICO_MODE" != "stub" && "$PICO_MODE" != "binary" && "$PICO_MODE" != "http" && "$PICO_MODE" != "ollama" ]]; then
        log_error "Неверный pico-mode: $PICO_MODE. Допустимые значения: stub, binary, http, ollama"
        exit 1
    fi
}

# =============================================================================
# Проверка зависимостей
# =============================================================================

check_dependencies() {
    log_step "Проверка зависимостей"

    if [[ "$MODE" == "docker" ]]; then
        if ! command -v docker &> /dev/null; then
            log_error "Docker не установлен. Установите Docker: https://docs.docker.com/get-docker/"
            exit 1
        fi

        if ! docker info &> /dev/null; then
            log_error "Docker демон не запущен. Запустите: sudo systemctl start docker"
            exit 1
        fi

        if ! command -v docker compose &> /dev/null && ! docker-compose --version &> /dev/null; then
            log_warn "docker compose plugin не найден. Проверка docker-compose..."
            if ! command -v docker-compose &> /dev/null; then
                log_error "docker compose не найден. Установите docker-compose или Docker Compose plugin."
                exit 1
            fi
        fi

        log_success "Docker: $(docker --version | head -1)"
    else
        # Проверка Rust
        if ! command -v cargo &> /dev/null; then
            log_error "Rust не установлен. Установите Rust: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
            exit 1
        fi

        # Проверка бинарника
        local binary="$DATA_DIR/../zeptoclaw_rust/target/release/zeptomesh-node"
        if [[ ! -f "$binary" ]]; then
            log_warn "Бинарник не найден: $binary"
            log_info "Сборка бинарника..."
            (
                cd "$DATA_DIR/../zeptoclaw_rust"
                cargo build --release
            )
        fi

        log_success "Rust: $(rustc --version | cut -d' ' -f2)"
    fi
}

# =============================================================================
# Поиск свободных портов
# =============================================================================

find_free_port() {
    local port=$1
    local max_attempts=${2:-100}
    local attempt=0

    while [[ $attempt -lt $max_attempts ]]; do
        if [[ "$MODE" == "docker" ]]; then
            # Проверяем, слушает ли какой-либо контейнер на этом порту
            if ! docker ps --format '{{.Ports}}' | grep -q ":${port}->"; then
                # Проверяем также локальные сокеты
                if ! ss -tlnp 2>/dev/null | grep -q ":${port} " && \
                   ! netstat -tlnp 2>/dev/null | grep -q ":${port} "; then
                    echo "$port"
                    return 0
                fi
            fi
        else
            if ! ss -tlnp 2>/dev/null | grep -q ":${port} " && \
               ! netstat -tlnp 2>/dev/null | grep -q ":${port} "; then
                echo "$port"
                return 0
            fi
        fi

        port=$((port + 1))
        attempt=$((attempt + 1))
    done

    log_error "Не удалось найти свободный порт после $max_attempts попыток"
    return 1
}

check_port() {
    local port=$1
    if [[ "$MODE" == "docker" ]]; then
        if docker ps --format '{{.Ports}}' 2>/dev/null | grep -q ":${port}->"; then
            return 1
        fi
    fi
    if ss -tlnp 2>/dev/null | grep -q ":${port} " || \
       netstat -tlnp 2>/dev/null | grep -q ":${port} "; then
        return 1
    fi
    return 0
}

# =============================================================================
# Генерация PSK
# =============================================================================

generate_psk() {
    if [[ -z "$PSK" ]]; then
        log_info "Генерация PSK для закрытой сети..."
        # Генерируем 32 байта случайных данных и кодируем в base32
        PSK="/1/$(head -c 32 /dev/urandom | base32 | tr -d '=' | head -c 52)"
        log_success "PSK сгенерирован: ${PSK:0:20}..."
    fi
}

# =============================================================================
# Подготовка Docker
# =============================================================================

prepare_docker() {
    log_step "Подготовка Docker"

    # Удаление старых образов
    log_info "Удаление старых образов zeptomesh..."
    docker rmi "$(docker images -q zeptomesh-node:latest 2>/dev/null)" 2>/dev/null || true
    docker rmi "$(docker images -q zeptomesh-node:* 2>/dev/null)" 2>/dev/null || true
    docker image prune -f 2>/dev/null || true
    log_success "Старые образы удалены"

    # Проверка и выбор свободных портов
    log_info "Проверка портов..."
    local mesh_port=$BASE_MESH_PORT
    local api_port=$((BASE_MESH_PORT + 10000))
    local prom_port=$((BASE_MESH_PORT + 20000))

    for i in $(seq 0 $((NUM_NODES - 1))); do
        local m_port
        m_port=$(find_free_port $mesh_port) || exit 1
        if [[ "$m_port" != "$mesh_port" ]]; then
            log_warn "Mesh-порт $mesh_port занят, используем $m_port"
        fi
        MESH_PORTS+=("$m_port")
        mesh_port=$((m_port + 1))

        local a_port
        a_port=$(find_free_port $api_port) || exit 1
        if [[ "$a_port" != "$api_port" ]]; then
            log_warn "API-порт $api_port занят, используем $a_port"
        fi
        API_PORTS+=("$a_port")
        api_port=$((a_port + 1))

        local p_port
        p_port=$(find_free_port $prom_port) || exit 1
        if [[ "$p_port" != "$prom_port" ]]; then
            log_warn "Prometheus-порт $prom_port занят, используем $p_port"
        fi
        PROM_PORTS+=("$p_port")
        prom_port=$((p_port + 1))
    done

    # Экспорт для docker-compose
    export MESH_PORTS_STR="${MESH_PORTS[*]}"
    export API_PORTS_STR="${API_PORTS[*]}"
    export PROM_PORTS_STR="${PROM_PORTS[*]}"
    export NUM_NODES
    export PSK
    export PICO_MODE
    export OLLAMA_URL
    export NAME_PREFIX
    export DATA_DIR
    export COMPOSE_PROJECT_NAME

    log_success "Порты проверены и свободны"
}

# =============================================================================
# Генерация docker-compose.yml
# =============================================================================

generate_docker_compose() {
    log_step "Генерация docker-compose.yml"

    local output_file="${DATA_DIR}/docker-compose.yml"

    # Создаём директорию
    mkdir -p "$DATA_DIR"

    # Абсолютный путь к корню репозитория (там лежат deploy/ и zeptoclaw_rust/)
    local repo_root
    repo_root="$(cd "$(dirname "$0")" && pwd)"

    cat > "$output_file" << 'DOCKER_COMPOSE_EOF'
services:
DOCKER_COMPOSE_EOF

    for i in $(seq 0 $((NUM_NODES - 1))); do
        local mesh_port=${MESH_PORTS[$i]}
        local api_port=${API_PORTS[$i]}
        local prom_port=${PROM_PORTS[$i]}
        local node_name="${NAME_PREFIX}-${i}"

        # Нода 0 — якорь: её адреса знают все остальные, сама она ни к кому
        # не подключается (соседей находит через gossip/DHT).
        # Rust-нода понимает только /ip4/../tcp/.., /ip6/../tcp/.. или голый
        # host:port — имя контейнера резолвится через DNS docker-сети.
        local bootstrap=""
        if [[ $i -gt 0 ]]; then
            bootstrap="${NAME_PREFIX}-0:${MESH_PORTS[0]}"
        fi

        cat >> "$output_file" << DOCKER_SERVICE_EOF
  ${node_name}:
    build:
      context: ${repo_root}
      dockerfile: deploy/docker/Dockerfile
    image: ${IMAGE_NAME}:${IMAGE_TAG}
    container_name: ${node_name}
    hostname: ${node_name}
    restart: unless-stopped
    ports:
      - "${mesh_port}:${mesh_port}"
      - "${api_port}:${api_port}"
      - "${prom_port}:${prom_port}"
    environment:
      - ZEPTOMESH_INDEX=${i}
      - ZEPTOMESH_NAME=${node_name}
      - ZEPTOMESH_DATA=/var/lib/zeptomesh
      - ZEPTOMESH_MESH_PORT=${mesh_port}
      - ZEPTOMESH_API_PORT=${api_port}
      - ZEPTOMESH_PROM_LISTEN=:${prom_port}
      - ZEPTOMESH_BOOTSTRAP=${bootstrap}
      - ZEPTOMESH_PSK=${PSK}
      - ZEPTOMESH_PICO_MODE=${PICO_MODE}
      - ZEPTOMESH_OLLAMA_URL=${OLLAMA_URL}
      - ZEPTOMESH_LOG_LEVEL=info
      - TZ=${TZ:-UTC}
    volumes:
      - ./data/${i}:/var/lib/zeptomesh
      - ./configs/node.yaml:/etc/zeptomesh/node.yaml:ro
    networks:
      - zeptomesh

DOCKER_SERVICE_EOF

    done

    # Добавляем network
    cat >> "$output_file" << 'DOCKER_NETWORK_EOF'

networks:
  zeptomesh:
    name: zeptomesh
    driver: bridge
DOCKER_NETWORK_EOF

    log_success "docker-compose.yml создан: $output_file"

    # Генерируем node.yaml для Docker
    generate_node_yaml_docker
}

# =============================================================================
# Генерация node.yaml для Docker
# =============================================================================

generate_node_yaml_docker() {
    local config_file="${DATA_DIR}/configs/node.yaml"
    mkdir -p "${DATA_DIR}/configs"

    cat > "$config_file" << 'NODE_YAML_EOF'
# ZeptoClaw Agent Mesh — шаблон узла для Docker
# envsubst подставляет переменные из docker-compose.yml

node:
  name: ${ZEPTOMESH_NAME:-zepto-${ZEPTOMESH_INDEX:-0}}
  data_dir: ${ZEPTOMESH_DATA:-/var/lib/zeptomesh}/${ZEPTOMESH_INDEX:-0}
  listen:
    - /ip4/0.0.0.0/tcp/${ZEPTOMESH_MESH_PORT:-4001}
  announce: []

identity:
  key_file: ${ZEPTOMESH_DATA:-/var/lib/zeptomesh}/${ZEPTOMESH_INDEX:-0}/keys/peer.key

discovery:
  local_registry: true
  mdns: true
  # Список, а не строка: конфиг парсится строго (Vec<String>), а адреса входа
  # в сеть доливаются из env ZEPTOMESH_BOOTSTRAP (через запятую).
  bootstrap: []
  dht: true
  dht_mode: auto

neighbors:
  min: 6
  target: 12
  max: 48

tasks:
  default_ttl: 5
  max_ttl: 10
  max_parallel_tasks: 4
  max_parallel_tasks_per_peer: 2
  default_timeout_seconds: 600
  max_timeout_seconds: 3600

capabilities:
  skills: [general]
  accept_external_tasks: true
  allow_shell: false
  allow_network_tools: true
  max_parallel_tasks: 4

picoclaw:
  mode: ${ZEPTOMESH_PICO_MODE:-stub}
  workspace_root: ${ZEPTOMESH_DATA:-/var/lib/zeptomesh}/workspace

security:
  require_task_signature: true
  require_origin_signature: true
  drop_invalid_signatures: true
  min_trust_for_tasks: limited

api:
  enabled: true
  listen: 0.0.0.0:${ZEPTOMESH_API_PORT:-8081}
  auth_token_env: ZEPTOMESH_API_TOKEN
  read_timeout: 15s
  write_timeout: 600s

telemetry:
  log_level: ${ZEPTOMESH_LOG_LEVEL:-info}
  structured_logs: true
  prometheus_listen: ${ZEPTOMESH_PROM_LISTEN:-}

storage:
  engine: badger
  dir: ${ZEPTOMESH_DATA:-/var/lib/zeptomesh}/${ZEPTOMESH_INDEX:-0}/db
NODE_YAML_EOF

    log_success "node.yaml создан: $config_file"
}

# =============================================================================
# Установка в Docker
# =============================================================================

install_docker() {
    log_step "Установка нод в Docker"

    # Подготовка
    prepare_docker

    # Создание директорий
    mkdir -p "${DATA_DIR}/data" "${DATA_DIR}/configs"

    # Проверяем наличие Dockerfile
    local dockerfile_path="deploy/docker/Dockerfile"
    if [[ ! -f "$dockerfile_path" ]]; then
        log_info "Создание Dockerfile..."
        mkdir -p deploy/docker
        create_dockerfile
    fi

    # Генерация конфигов
    generate_docker_compose

    # Сборка образа
    log_info "Сборка Docker-образа zeptomesh-node..."
    docker build -f deploy/docker/Dockerfile -t "${IMAGE_NAME}:${IMAGE_TAG}" . || {
        log_error "Ошибка сборки Docker-образа"
        exit 1
    }
    log_success "Docker-образ собран"

    # Запуск контейнеров
    log_info "Запуск ${NUM_NODES} нод..."

    # Запускаем первую ноду и ждём её готовности
    local first_node="${NAME_PREFIX}-0"
    docker compose -p "$COMPOSE_PROJECT_NAME" -f "${DATA_DIR}/docker-compose.yml" up -d "$first_node"

    # Ждём готовности первой ноды
    log_info "Ожидание готовности первой ноды..."
    local api_port=${API_PORTS[0]}
    local max_wait=60
    local waited=0
    while [[ $waited -lt $max_wait ]]; do
        if curl -sf "http://127.0.0.1:${api_port}/healthz" &>/dev/null; then
            log_success "Первая нода готова"
            break
        fi
        sleep 1
        waited=$((waited + 1))
    done

    if [[ $waited -ge $max_wait ]]; then
        log_warn "Первая нода не ответила за ${max_wait} секунд"
    fi

    # Запускаем остальные ноды
    if [[ $NUM_NODES -gt 1 ]]; then
        local remaining_nodes=""
        for i in $(seq 1 $((NUM_NODES - 1))); do
            remaining_nodes="${remaining_nodes} ${NAME_PREFIX}-${i}"
        done
        docker compose -p "$COMPOSE_PROJECT_NAME" -f "${DATA_DIR}/docker-compose.yml" up -d $remaining_nodes

        # Ждём запуска остальных
        sleep 5
    fi

    log_success "Установка завершена!"
    show_status
}

# =============================================================================
# Создание Dockerfile
# =============================================================================

create_dockerfile() {
    cat > deploy/docker/Dockerfile << 'DOCKERFILE_EOF'
# ZeptoClaw Agent Mesh - Docker
# Образ соответствует локальному тулчейну (rustc 1.99): Cargo.lock не
# коммитится, поэтому свежие транзитивные зависимости (icu_provider,
# uuid и др.) требуют актуальной версии rustc — не ниже локальной.
FROM rust:1.99-slim AS builder

WORKDIR /build

# Установка зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Копирование исходников
COPY zeptoclaw_rust/ ./zeptoclaw_rust/

# Сборка
WORKDIR /build/zeptoclaw_rust
RUN cargo build --release

# Минимальный runtime образ — та же версия Debian, что и builder (trixie):
# бинарник собран против glibc 2.41, bookworm (2.36) даёт ошибку
# «weak version `GLIBC_2.39' not found» на старте.
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3t64 \
    && rm -rf /var/lib/apt/lists/*

# Создание пользователя
RUN useradd -m -u 1000 zepto

WORKDIR /app

# Копирование бинарника
COPY --from=builder /build/zeptoclaw_rust/target/release/zeptomesh-node /app/zeptomesh-node

# Директории
RUN mkdir -p /var/lib/zeptomesh /var/log/zeptomesh && \
    chown -R zepto:zepto /var/lib/zeptomesh /var/log/zeptomesh

USER zepto

EXPOSE 4001 8081 9464

ENTRYPOINT ["/app/zeptomesh-node"]
CMD ["run", "-c", "/etc/zeptomesh/node.yaml"]
DOCKERFILE_EOF

    log_success "Dockerfile создан"
}

# =============================================================================
# Установка в локальном режиме
# =============================================================================

install_local() {
    log_step "Установка нод в локальном режиме"

    # Генерация PSK если нужно
    generate_psk

    # Проверка и выбор свободных портов
    log_info "Проверка портов..."
    local mesh_port=$BASE_MESH_PORT
    local api_port=$((BASE_MESH_PORT + 10000))

    for i in $(seq 0 $((NUM_NODES - 1))); do
        local m_port
        m_port=$(find_free_port $mesh_port) || exit 1
        if [[ "$m_port" != "$mesh_port" ]]; then
            log_warn "Mesh-порт $mesh_port занят, используем $m_port"
        fi
        MESH_PORTS+=("$m_port")
        mesh_port=$((m_port + 1))

        local a_port
        a_port=$(find_free_port $api_port) || exit 1
        if [[ "$a_port" != "$api_port" ]]; then
            log_warn "API-порт $api_port занят, используем $a_port"
        fi
        API_PORTS+=("$a_port")
        api_port=$((a_port + 1))
    done

    # Создание systemd user units
    log_info "Создание systemd user units..."
    mkdir -p "${DATA_DIR}/configs"

    # Генерация конфигов для каждой ноды
    for i in $(seq 0 $((NUM_NODES - 1))); do
        local mesh_port=${MESH_PORTS[$i]}
        local api_port=${API_PORTS[$i]}
        local node_name="${NAME_PREFIX}-${i}"
        local node_data="${DATA_DIR}/${i}"

        mkdir -p "$node_data"/{keys,db,run}

        # Генерация peer key если нет
        local key_file="$node_data/keys/peer.key"
        if [[ ! -f "$key_file" ]]; then
            log_info "Генерация ключа для $node_name..."
            "${DATA_DIR}/../zeptoclaw_rust/target/release/zeptomesh-node" genkey --out "$key_file" --print-id > /dev/null
        fi

        # Создание instance.env
        cat > "${DATA_DIR}/${node_name}.env" << ENV_EOF
ZEPTOMESH_INDEX=${i}
ZEPTOMESH_NAME=${node_name}
ZEPTOMESH_DATA=${node_data}
ZEPTOMESH_MESH_PORT=${mesh_port}
ZEPTOMESH_API_PORT=${api_port}
ZEPTOMESH_PROM_LISTEN=127.0.0.1:$((BASE_MESH_PORT + 20000 + i))
ZEPTOMESH_PSK=${PSK}
ZEPTOMESH_PICO_MODE=${PICO_MODE}
ZEPTOMESH_OLLAMA_URL=${OLLAMA_URL}
ZEPTOMESH_LOG_LEVEL=info
ZEPTOMESH_CONFIG=${DATA_DIR}/configs/${node_name}.yaml
ENV_EOF

        # Создание конфига для ноды
        cat > "${DATA_DIR}/configs/${node_name}.yaml" << YAML_EOF
node:
  name: ${node_name}
  data_dir: ${node_data}
  listen:
    - /ip4/0.0.0.0/tcp/${mesh_port}
    - /ip4/0.0.0.0/udp/${mesh_port}/quic-v1
YAML_EOF

        # Создание systemd unit
        mkdir -p ~/.config/systemd/user/
        cat > ~/.config/systemd/user/zeptomesh-${i}.service << SERVICE_EOF
[Unit]
Description=ZeptoClaw Agent Mesh Node ${i}
After=network.target

[Service]
Type=simple
EnvironmentFile=${DATA_DIR}/${node_name}.env
ExecStart=${DATA_DIR}/../zeptoclaw_rust/target/release/zeptomesh-node run -c ${DATA_DIR}/configs/${node_name}.yaml
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
SERVICE_EOF

        log_success "Конфигурация для $node_name создана (mesh: $mesh_port, API: $api_port)"
    done

    # Systemd reload
    log_info "Перезагрузка systemd..."
    systemctl --user daemon-reload 2>/dev/null || true

    # Запуск нод
    log_info "Запуск нод..."
    for i in $(seq 0 $((NUM_NODES - 1))); do
        systemctl --user start zeptomesh-${i}.service 2>/dev/null || {
            log_warn "Не удалось запустить через systemd, пробуем напрямую..."
            (
                source "${DATA_DIR}/${NAME_PREFIX}-${i}.env"
                cd "${DATA_DIR}/../zeptoclaw_rust"
                ZEPTOMESH_INDEX=${i} ZEPTOMESH_MESH_PORT=${MESH_PORTS[$i]} \
                ZEPTOMESH_API_PORT=${API_PORTS[$i]} \
                ./target/release/zeptomesh-node run -c "${DATA_DIR}/configs/${NAME_PREFIX}-${i}.yaml" &
            )
        }
    done

    log_success "Установка завершена!"
    show_status
}

# =============================================================================
# Деинсталляция
# =============================================================================

uninstall() {
    log_step "Деинсталляция нод"

    if [[ "$MODE" == "docker" ]]; then
        log_info "Остановка и удаление контейнеров..."
        docker compose -p "$COMPOSE_PROJECT_NAME" -f "${DATA_DIR}/docker-compose.yml" down --remove-orphans 2>/dev/null || true

        log_info "Удаление образов..."
        docker rmi "$(docker images -q zeptomesh-node:latest 2>/dev/null)" 2>/dev/null || true

        log_success "Docker контейнеры удалены"
    else
        log_info "Остановка systemd сервисов..."
        for i in $(seq 0 $((NUM_NODES - 1))); do
            systemctl --user stop zeptomesh-${i}.service 2>/dev/null || true
        done

        log_info "Удаление systemd units..."
        rm -f ~/.config/systemd/user/zeptomesh-*.service 2>/dev/null || true
        systemctl --user daemon-reload 2>/dev/null || true

        log_success "Локальные сервисы удалены"
    fi

    if [[ "$SKIP_CONFIRM" == "true" ]] || read -p "Удалить данные в ${DATA_DIR}? (y/N): " -n 1 -r && echo; then
        if [[ "$SKIP_CONFIRM" == "true" ]] || [[ $REPLY =~ ^[Yy]$ ]]; then
            log_info "Удаление данных..."
            rm -rf "${DATA_DIR}"
            log_success "Данные удалены"
        fi
    fi
}

# =============================================================================
# Статус
# =============================================================================

show_status() {
    log_step "Статус нод"

    echo ""
    echo -e "${BOLD}Установленные ноды:${NC}"
    echo ""

    if [[ "$MODE" == "docker" ]]; then
        docker compose -p "$COMPOSE_PROJECT_NAME" -f "${DATA_DIR}/docker-compose.yml" ps

        echo ""
        echo -e "${BOLD}Адреса API:${NC}"
        for i in $(seq 0 $((NUM_NODES - 1))); do
            local api_port=${API_PORTS[$i]}
            local peer_id
            peer_id=$(curl -s "http://127.0.0.1:${api_port}/healthz" 2>/dev/null | grep -o '"peer_id":"[^"]*"' | cut -d'"' -f4 || echo "N/A")
            echo -e "  ${NAME_PREFIX}-${i}: http://127.0.0.1:${api_port} (peer_id: ${peer_id})"
        done
    else
        for i in $(seq 0 $((NUM_NODES - 1))); do
            local api_port=${API_PORTS[$i]}
            local peer_id
            peer_id=$(curl -s "http://127.0.0.1:${api_port}/healthz" 2>/dev/null | grep -o '"peer_id":"[^"]*"' | cut -d'"' -f4 || echo "N/A")
            echo -e "  ${NAME_PREFIX}-${i}: http://127.0.0.1:${api_port} (peer_id: ${peer_id})"
        done
    fi

    echo ""
    echo -e "${BOLD}Команды управления:${NC}"
    echo "  Просмотр логов:  docker compose -p ${COMPOSE_PROJECT_NAME} logs -f"
    echo "  Остановка:      docker compose -p ${COMPOSE_PROJECT_NAME} down"
    echo "  Перезапуск:     docker compose -p ${COMPOSE_PROJECT_NAME} restart"
    echo ""
}

# =============================================================================
# Основная функция
# =============================================================================

main() {
    parse_args "$@"

    echo ""
    echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║${NC}         ${BOLD}ZeptoClaw Agent Mesh — Установка нод${NC}                 ${BOLD}║${NC}"
    echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    log_info "Режим: $MODE"
    log_info "Количество нод: $NUM_NODES"
    log_info "Базовый порт mesh: $BASE_MESH_PORT"
    log_info "PicoClaw mode: $PICO_MODE"
    log_info "Каталог данных: $DATA_DIR"

    if [[ -n "$PSK" ]]; then
        log_info "PSK: ${PSK:0:20}..."
    fi

    if [[ "$UNINSTALL_ONLY" == "true" ]]; then
        uninstall
        log_success "Деинсталляция завершена"
        exit 0
    fi

    # Проверка зависимостей
    check_dependencies

    # Подтверждение
    if [[ "$SKIP_CONFIRM" != "true" ]]; then
        echo ""
        read -p "Продолжить установку? (Y/n): " -n 1 -r || true
        echo
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            log_info "Установка отменена"
            exit 0
        fi
    fi

    # Установка
    if [[ "$MODE" == "docker" ]]; then
        install_docker
    else
        install_local
    fi
}

# Запуск
main "$@"
