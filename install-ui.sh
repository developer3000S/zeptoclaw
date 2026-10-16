#!/usr/bin/env bash
# =============================================================================
# install-ui.sh — установка ZeptoClaw Agent Mesh UI Dashboard
#
# Поддерживает два режима:
#   --mode local   — запуск напрямую (Go binary)
#   --mode docker  — запуск в Docker-контейнере
#
# Опции:
#   --port PORT          порт UI (по умолчанию 28090)
#   --nodes ADDRS        список адресов API нод через запятую
#   --api-token TKN      токен для доступа к API нод
#   --ui-token TKN       токен для авторизации UI (опционально)
#   --poll-interval SEC  интервал опроса в секундах (по умолчанию 5)
#   --data-dir DIR       каталог данных UI (по умолчанию ./zeptomesh-ui-data)
#   --name-prefix P      префикс имён контейнеров (по умолчанию zeptomesh-ui)
#   --yes                не спрашивать подтверждений
#   --uninstall          удалить установленный UI
#   -h, --help           показать эту справку
#
# При установке в Docker:
#   - Удаляет старые образы zeptomesh-ui
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
UNINSTALL_ONLY=false
SKIP_CONFIRM=false
COMPOSE_PROJECT_NAME="zeptomesh-ui"
IMAGE_NAME="zeptomesh-ui"
IMAGE_TAG="latest"

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
                        Пример: http://127.0.0.1:8081,http://127.0.0.1:8082
    --api-token TKN     Токен для доступа к API нод
    --ui-token TKN      Токен для авторизации UI (опционально)
    --poll-interval SEC Интервал опроса нод (по умолчанию: 5)
    --data-dir DIR      Каталог данных UI (по умолчанию: ./zeptomesh-ui-data)
    --name-prefix P     Префикс имён контейнеров (по умолчанию: zeptomesh-ui)
    --mode MODE         Режим: local|docker (по умолчанию: docker)
    --yes               Не спрашивать подтверждений
    --uninstall         Удалить установленный UI
    -h, --help          Показать эту справку

${BOLD}ПРИМЕРЫ:${NC}
    # Установить UI в Docker с автоопределением нод
    $0 --mode docker

    # Установить с конкретными нодами
    $0 --nodes "http://127.0.0.1:8081,http://127.0.0.1:8082" --api-token mysecret

    # Установить UI на альтернативном порту
    $0 --port 28091 --nodes "http://127.0.0.1:8081"

    # Установить в локальном режиме
    $0 --mode local --nodes "http://127.0.0.1:8081"

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
        if ! command -v go &> /dev/null; then
            log_error "Go не установлен"
            exit 1
        fi
        log_success "Go: $(go version | cut -d' ' -f3)"
    fi
}

# =============================================================================
# Поиск свободного порта
# =============================================================================

find_free_port() {
    local port=$1 attempt=0
    while [[ $attempt -lt 100 ]]; do
        if [[ "$MODE" == "docker" ]]; then
            if ! docker ps --format '{{.Ports}}' 2>/dev/null | grep -q ":${port}->" && \
               ! ss -tlnp 2>/dev/null | grep -q ":${port} " && \
               ! netstat -tlnp 2>/dev/null | grep -q ":${port} "; then
                echo "$port"; return 0
            fi
        else
            if ! ss -tlnp 2>/dev/null | grep -q ":${port} " && \
               ! netstat -tlnp 2>/dev/null | grep -q ":${port} "; then
                echo "$port"; return 0
            fi
        fi
        ((port++)); ((attempt++))
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
        for container in $(docker ps --format '{{.Names}}' 2>/dev/null | grep -E "^zepto-[0-9]+$" || true); do
            local port
            port=$(docker inspect --format '{{range $p, $map := .NetworkSettings.Ports}}{{range $k, $v := $map}}{{printf "%s\n" $v.HostPort}}{{end}}{{end}}' "$container" 2>/dev/null | tail -1)
            if [[ -n "$port" ]]; then
                local api="http://127.0.0.1:${port}"
                log_info "Найдена нода: $container -> $api"
                [[ -n "$discovered" ]] && discovered="${discovered}," || true
                discovered="${discovered}${api}"
            fi
        done
    else
        for port in 8081 8082 8083 8084 8085; do
            if curl -sf "http://127.0.0.1:${port}/healthz" &>/dev/null; then
                local api="http://127.0.0.1:${port}"
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
        log_warn "Ноды не обнаружены автоматически"
    fi
}

# =============================================================================
# Генерация HTML для UI
# =============================================================================

generate_html() {
    local output="$DATA_DIR/static/index.html"
    mkdir -p "$(dirname "$output")"

    # Формируем JSON массив нод
    local nodes_json="["
    local first=true
    for node in $(echo "$NODES" | tr ',' ' '); do
        node=$(echo "$node" | xargs)
        if [[ -n "$node" ]]; then
            [[ "$first" == "true" ]] && first=false || nodes_json+=","
            nodes_json+="\"${node}\""
        fi
    done
    nodes_json+="]"

    cat > "$output" << HTML_EOF
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ZeptoClaw Mesh Dashboard</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }
        .header { background: #1e293b; padding: 1rem 2rem; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 1rem; }
        .header h1 { font-size: 1.5rem; color: #38bdf8; }
        .status-badge { padding: 0.25rem 0.75rem; border-radius: 9999px; font-size: 0.875rem; }
        .status-ok { background: #22c55e20; color: #22c55e; }
        .status-error { background: #ef444420; color: #ef4444; }
        .container { padding: 2rem; max-width: 1400px; margin: 0 auto; }
        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(350px, 1fr)); gap: 1.5rem; }
        .card { background: #1e293b; border-radius: 0.75rem; padding: 1.5rem; border: 1px solid #334155; }
        .card h2 { font-size: 1.125rem; margin-bottom: 1rem; color: #38bdf8; }
        .node-item { padding: 1rem; background: #0f172a; border-radius: 0.5rem; margin-bottom: 0.75rem; }
        .node-item:last-child { margin-bottom: 0; }
        .node-id { font-family: monospace; color: #94a3b8; font-size: 0.75rem; word-break: break-all; margin-top: 0.25rem; }
        .node-name { font-weight: 600; color: #f1f5f9; }
        .node-stats { display: flex; gap: 1rem; margin-top: 0.5rem; font-size: 0.875rem; flex-wrap: wrap; }
        .stat { display: flex; flex-direction: column; }
        .stat-label { color: #64748b; font-size: 0.7rem; }
        .stat-value { color: #e2e8f0; font-weight: 500; }
        .refresh-btn { background: #38bdf8; color: #0f172a; border: none; padding: 0.5rem 1rem; border-radius: 0.5rem; cursor: pointer; font-weight: 500; }
        .refresh-btn:hover { background: #0ea5e9; }
        .loading { text-align: center; padding: 2rem; color: #64748b; }
        .error { background: #ef444420; border: 1px solid #ef444440; padding: 1rem; border-radius: 0.5rem; color: #ef4444; }
        .footer { text-align: center; padding: 1rem; color: #64748b; font-size: 0.75rem; border-top: 1px solid #334155; margin-top: 2rem; }
    </style>
</head>
<body>
    <div class="header">
        <h1>⚡ ZeptoClaw Mesh Dashboard</h1>
        <div>
            <span class="status-badge" id="overall-status">Загрузка...</span>
            <button class="refresh-btn" onclick="fetchData()">🔄 Обновить</button>
        </div>
    </div>
    <div class="container">
        <div class="grid">
            <div class="card">
                <h2>📡 Ноды сети</h2>
                <div id="nodes-container"><div class="loading">Загрузка данных...</div></div>
            </div>
            <div class="card">
                <h2>📊 Статистика сети</h2>
                <div id="stats-container"><div class="loading">Загрузка данных...</div></div>
            </div>
        </div>
    </div>
    <div class="footer">ZeptoClaw Agent Mesh | <span id="update-time"></span></div>

    <script>
        const NODES = ${nodes_json};
        const API_TOKEN = '${API_TOKEN}';
        const POLL_INTERVAL = ${POLL_INTERVAL};

        async function fetchJSON(url, fallback = null) {
            try {
                const headers = {};
                if (API_TOKEN) headers['Authorization'] = 'Bearer ' + API_TOKEN;
                const resp = await fetch(url, { headers, signal: AbortSignal.timeout(5000) });
                if (!resp.ok) throw new Error('HTTP ' + resp.status);
                return await resp.json();
            } catch (e) { return fallback; }
        }

        async function fetchData() {
            const nodesContainer = document.getElementById('nodes-container');
            const statsContainer = document.getElementById('stats-container');

            if (NODES.length === 0) {
                nodesContainer.innerHTML = '<div class="error">Ноды не настроены.<br>Запустите install-nodes.sh или укажите --nodes</div>';
                statsContainer.innerHTML = '';
                return;
            }

            nodesContainer.innerHTML = '<div class="loading">Загрузка данных...</div>';
            statsContainer.innerHTML = '<div class="loading">Загрузка данных...</div>';

            let totalPeers = 0, activeNodes = 0, totalCompleted = 0, totalFailed = 0;
            const results = await Promise.all(NODES.map(async url => {
                const [status, peers] = await Promise.all([
                    fetchJSON(url + '/api/v1/status', {}),
                    fetchJSON(url + '/api/v1/peers', [])
                ]);
                return { url, status, peers };
            }));

            let nodesHtml = '', statsHtml = '';
            results.forEach(({ url, status, peers }) => {
                if (status.error || !status.peer_id) {
                    nodesHtml += '<div class="error">' + url + ': ' + (status.error || 'No response') + '</div>';
                    return;
                }
                activeNodes++;
                totalPeers += Array.isArray(peers) ? peers.length : 0;
                totalCompleted += status.tasks_completed || 0;
                totalFailed += status.tasks_failed || 0;

                const peerId = status.peer_id || 'N/A';
                const nodeName = status.node_name || peerId.slice(0, 12);
                const neighbors = status.neighbors_total || 0;
                const connected = status.neighbors_connected || 0;
                const completed = status.tasks_completed || 0;
                const failed = status.tasks_failed || 0;
                const version = status.version || 'N/A';

                nodesHtml += `
                    <div class="node-item">
                        <div class="node-name">${nodeName}</div>
                        <div class="node-id">${peerId}</div>
                        <div class="node-stats">
                            <div class="stat"><span class="stat-label">Соседей</span><span class="stat-value">${connected}/${neighbors}</span></div>
                            <div class="stat"><span class="stat-label">Выполнено</span><span class="stat-value">${completed}</span></div>
                            <div class="stat"><span class="stat-label">Ошибок</span><span class="stat-value">${failed}</span></div>
                            <div class="stat"><span class="stat-label">Версия</span><span class="stat-value">${version}</span></div>
                        </div>
                    </div>`;
            });

            statsHtml = \`
                <div class="node-item">
                    <div class="stat"><span class="stat-label">Активных нод</span><span class="stat-value">\${activeNodes}/\${NODES.length}</span></div>
                </div>
                <div class="node-item">
                    <div class="stat"><span class="stat-label">Всего соседей в сети</span><span class="stat-value">\${totalPeers}</span></div>
                </div>
                <div class="node-item">
                    <div class="stat"><span class="stat-label">Всего выполнено задач</span><span class="stat-value">\${totalCompleted}</span></div>
                </div>
                <div class="node-item">
                    <div class="stat"><span class="stat-label">Всего ошибок</span><span class="stat-value">\${totalFailed}</span></div>
                </div>\`;

            nodesContainer.innerHTML = nodesHtml || '<div class="error">Ноды не найдены</div>';
            statsContainer.innerHTML = statsHtml;

            const statusEl = document.getElementById('overall-status');
            statusEl.textContent = activeNodes + ' нод онлайн';
            statusEl.className = 'status-badge ' + (activeNodes > 0 ? 'status-ok' : 'status-error');

            document.getElementById('update-time').textContent = 'Обновлено: ' + new Date().toLocaleTimeString();
        }

        fetchData();
        if (POLL_INTERVAL > 0) setInterval(fetchData, POLL_INTERVAL * 1000);
    </script>
</body>
</html>
HTML_EOF
    log_success "UI HTML создан: $output"
}

# =============================================================================
# Подготовка Docker
# =============================================================================

prepare_docker() {
    log_step "Подготовка Docker"
    log_info "Удаление старых образов..."
    docker rmi "$(docker images -q zeptomesh-ui:latest 2>/dev/null)" 2>/dev/null || true
    docker image prune -f 2>/dev/null || true

    log_info "Проверка порта $UI_PORT..."
    local free_port
    free_port=$(find_free_port $UI_PORT) || exit 1
    if [[ "$free_port" != "$UI_PORT" ]]; then
        log_warn "Порт $UI_PORT занят, используем $free_port"
        UI_PORT=$free_port
    fi
    log_success "Порт $UI_PORT свободен"
}

# =============================================================================
# Генерация docker-compose.yml
# =============================================================================

generate_docker_compose() {
    log_step "Генерация docker-compose.yml"

    mkdir -p "$DATA_DIR/data" "$DATA_DIR/static"

    # Создаём nginx конфиг
    cat > "$DATA_DIR/nginx.conf" << 'NGINX_EOF'
server {
    listen 80;
    server_name _;
    root /usr/share/nginx/html;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }

    location /healthz {
        return 200 '{"status":"ok"}';
        add_header Content-Type application/json;
    }
}
NGINX_EOF

    # docker-compose.yml
    cat > "$DATA_DIR/docker-compose.yml" << DOCKER_EOF
version: '3.8'

services:
  ${NAME_PREFIX}:
    image: ${IMAGE_NAME}:${IMAGE_TAG}
    container_name: ${NAME_PREFIX}
    hostname: ${NAME_PREFIX}
    restart: unless-stopped
    ports:
      - "${UI_PORT}:80"
    volumes:
      - ./static:/usr/share/nginx/html:ro
      - ./nginx.conf:/etc/nginx/conf.d/default.conf:ro
    networks:
      - zeptomesh

networks:
  zeptomesh:
    name: zeptomesh
    external: true
DOCKER_EOF
    log_success "docker-compose.yml создан"
}

# =============================================================================
# Создание Dockerfile
# =============================================================================

create_dockerfile() {
    mkdir -p deploy/docker
    cat > deploy/docker/Dockerfile.ui << 'DOCKERFILE_EOF'
# ZeptoClaw Agent Mesh UI - Docker
FROM nginx:alpine

# Копируем статику
COPY static/* /usr/share/nginx/html/

# Копируем конфиг nginx
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
DOCKERFILE_EOF
    log_success "Dockerfile.ui создан"
}

# =============================================================================
# Установка в Docker
# =============================================================================

install_docker() {
    log_step "Установка UI в Docker"

    prepare_docker

    if [[ -z "$NODES" ]]; then
        discover_nodes
    fi

    generate_html
    generate_docker_compose

    local dockerfile="deploy/docker/Dockerfile.ui"
    if [[ ! -f "$dockerfile" ]]; then
        create_dockerfile
    fi

    log_info "Сборка Docker-образа..."
    docker build -f "$dockerfile" -t "${IMAGE_NAME}:${IMAGE_TAG}" . || {
        log_error "Ошибка сборки Docker-образа"
        exit 1
    }
    log_success "Docker-образ собран"

    log_info "Запуск UI..."
    docker compose -p "$COMPOSE_PROJECT_NAME" -f "$DATA_DIR/docker-compose.yml" up -d

    # Ждём готовности
    log_info "Ожидание готовности UI..."
    local waited=0
    while [[ $waited -lt 30 ]]; do
        if curl -sf "http://127.0.0.1:${UI_PORT}/healthz" &>/dev/null; then
            log_success "UI готов!"
            break
        fi
        sleep 1
        ((waited++))
    done

    show_status
}

# =============================================================================
# Установка в локальном режиме
# =============================================================================

install_local() {
    log_step "Установка UI в локальном режиме"

    log_info "Проверка порта $UI_PORT..."
    local free_port
    free_port=$(find_free_port $UI_PORT) || exit 1
    if [[ "$free_port" != "$UI_PORT" ]]; then
        log_warn "Порт $UI_PORT занят, используем $free_port"
        UI_PORT=$free_port
    fi

    if [[ -z "$NODES" ]]; then
        discover_nodes
    fi

    # Создаём директорию для UI сервера
    local ui_dir="$DATA_DIR/../zeptomesh-ui-bin"
    mkdir -p "$ui_dir" "$DATA_DIR/static"

    # Генерируем HTML
    generate_html

    # Простой Go сервер
    cat > "$ui_dir/main.go" << 'GO_EOF'
package main

import (
	"embed"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

//go:embed static/*
var static embed.FS

func main() {
	port := os.Getenv("UI_PORT")
	if port == "" {
		port = "28090"
	}

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data, err := static.ReadFile("static/index.html")
		if err != nil {
			http.Error(w, "Not found", 404)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	})

	fmt.Printf("ZeptoClaw UI listening on :%s\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
GO_EOF

    # Копируем static
    mkdir -p "$ui_dir/static"
    cp -r "$DATA_DIR/static/"* "$ui_dir/static/"

    # Сборка
    cd "$ui_dir"
    go build -o ui-server . || {
        log_error "Ошибка сборки UI"
        exit 1
    }
    cd - > /dev/null
    log_success "UI собран"

    # Запуск
    log_info "Запуск UI на порту $UI_PORT..."
    UI_PORT=$UI_PORT nohup "$ui_dir/ui-server" > "$DATA_DIR/ui.log" 2>&1 &
    sleep 2

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
        docker rmi "$(docker images -q zeptomesh-ui:latest 2>/dev/null)" 2>/dev/null || true
        log_success "Docker контейнеры удалены"
    else
        log_info "Остановка UI..."
        pkill -f "zeptomesh-ui-bin" 2>/dev/null || true
        rm -rf "$DATA_DIR/../zeptomesh-ui-bin"
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
    if [[ "$MODE" == "docker" ]]; then
        echo ""
        docker compose -p "$COMPOSE_PROJECT_NAME" -f "$DATA_DIR/docker-compose.yml" ps 2>/dev/null || echo "  Контейнер не запущен"
    fi
    echo ""
    echo -e "${BOLD}Команды управления:${NC}"
    if [[ "$MODE" == "docker" ]]; then
        echo "  Логи:      docker compose -p $COMPOSE_PROJECT_NAME -f $DATA_DIR/docker-compose.yml logs -f"
        echo "  Остановка: docker compose -p $COMPOSE_PROJECT_NAME -f $DATA_DIR/docker-compose.yml down"
        echo "  Перезапуск: docker compose -p $COMPOSE_PROJECT_NAME -f $DATA_DIR/docker-compose.yml restart"
    else
        echo "  Логи:      tail -f $DATA_DIR/ui.log"
        echo "  Остановка: pkill -f zeptomesh-ui-bin"
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
