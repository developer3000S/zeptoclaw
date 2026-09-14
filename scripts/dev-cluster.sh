#!/usr/bin/env bash
#
# Поднимает несколько узлов ZeptoClaw на одной машине для отладки и рвёт их по
# команде. Сеть строится через discovery.bootstrap (mDNS/DHT выключены), поэтому
# кластер детерминирован и не зависит от сети.
#
#   ./scripts/dev-cluster.sh up [N]        — поднять N узлов (по умолчанию 3)
#   ./scripts/dev-cluster.sh down          — остановить все узлы кластера
#   ./scripts/dev-cluster.sh status        — живые pid'ы и сводка по каждому API
#   ./scripts/dev-cluster.sh logs [i]      — хвост лога (без i — всех узлов)
#   ./scripts/dev-cluster.sh peers [i]     — соседи узла i (по умолчанию 0)
#   ./scripts/dev-cluster.sh skills [i]    — навыки узла i и выученный обзор
#   ./scripts/dev-cluster.sh sync [i]      — немедленная сверка навыков
#   ./scripts/dev-cluster.sh rebinds [i]   — журнал смены идентичностей
#   ./scripts/dev-cluster.sh rotate [i]    — сменить ключ узла i и перезапустить
#   ./scripts/dev-cluster.sh submit [i] <текст> [навык] — поставить задачу
#   ./scripts/dev-cluster.sh down-and-clear — down + удалить каталог данных
#
# Экземпляры различаются навыками (см. SKILLS ниже), чтобы было видно, что
# задача уехала именно к носителю нужного навыка, а обмен скилами реально
# обновляет обзор соседей. Логи и данные — в .dev/cluster/<i>.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${ZETOMESH_BIN:-$ROOT/bin/zeptomesh-node}"
CFG="${ZETOMESH_CONFIG:-$ROOT/configs/examples/dev-node.yaml}"
DATA="${ZETOMESH_DATA:-$ROOT/.dev/cluster}"
MESH_BASE=${ZETOMESH_MESH_BASE:-4101}
API_BASE=${ZETOMESH_API_BASE:-8101}
PROM_BASE=${ZETOMESH_PROM_BASE:-9564}
MAX_SLOTS=16

# Навыки по экземплярам; дальше по кругу.
SKILLS=("research" "coding" "summarize" "general" "translate" "index")

log() { printf '\033[36m[dev-cluster]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[dev-cluster]\033[0m %s\n' "$*" >&2; exit 1; }

pidfile() { printf '%s/%s.pid' "$DATA" "$1"; }
read_pid() { cat "$(pidfile "$1")" 2>/dev/null || true; }

is_running() {
	local pf="$DATA/$1.pid" pid
	[[ -f "$pf" ]] || return 1
	pid="$(cat "$pf" 2>/dev/null || true)"
	[[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

api_of() { printf '127.0.0.1:%s' "$((API_BASE + $1))"; }

# running_nodes — индексы поднятых узлов, списком.
running_nodes() {
	local i
	for ((i = 0; i < MAX_SLOTS; i++)); do
		if is_running "$i"; then printf '%s\n' "$i"; fi
	done
	return 0
}

# bootstrap_list <self> <count> — адреса всех остальных узлов кластера.
#
# Пусто по умолчанию: знакомство процессов на одной машине делает реестр
# unix-сокетов (discovery.local_registry), записи которого содержат и peer id, и
# адреса. bootstrap-строка обязана содержать /p2p/<id> — узел отвергает адрес
# без идентификатора при старте, потому что libp2p не умеет звонить «в сокет без
# имени». Поэтому список собирается только когда ZETOMESH_BOOTSTRAP передан
# явно полностью (тогда скрипт передаёт его как есть).
bootstrap_list() {
	if [[ -n "${ZETOMESH_BOOTSTRAP:-}" ]]; then printf '%s' "$ZETOMESH_BOOTSTRAP"; return 0; fi
	printf ''
}

need_bin() {
	[[ -x "$BIN" ]] || die "$BIN не собран — выполните 'make build'"
	[[ -f "$CFG" ]] || die "нет файла конфигурации $CFG"
}

start_one() {
	local idx=$1 count=$2
	local mesh=$((MESH_BASE + idx)) api=$((API_BASE + idx)) prom=$((PROM_BASE + idx))
	local skill=${SKILLS[$((idx % ${#SKILLS[@]}))]}
	local boot; boot="$(bootstrap_list "$idx" "$count")"
	mkdir -p "$DATA/$idx"
	(
		cd "$ROOT"
		ZETOMESH_INDEX="$idx" ZETOMESH_DATA="$DATA" \
		ZETOMESH_MESH_PORT="$mesh" ZETOMESH_API_PORT="$api" ZETOMESH_PROM_PORT="$prom" \
		ZETOMESH_BOOTSTRAP="$boot" ZETOMESH_SKILL="$skill" \
		ZETOMESH_LOG_LEVEL="${ZETOMESH_LOG_LEVEL:-info}" \
			nohup "$BIN" run -config "$CFG" >"$DATA/$idx.log" 2>&1 &
		echo $! >"$(pidfile "$idx")"
	)
	log "узел $idx: pid $(read_pid "$idx") mesh=$mesh api=$api skill=$skill bootstrap=[${boot:-<нет>}]"
}

up() {
	need_bin
	local n=${1:-3}
	((n >= 1 && n <= MAX_SLOTS)) || die "N должно быть от 1 до $MAX_SLOTS"
	mkdir -p "$DATA"

	# Pid-файлы без процесса только сбивают с толку статус.
	local i pid
	for ((i = 0; i < MAX_SLOTS; i++)); do
		[[ -f "$DATA/$i.pid" ]] || continue
		pid="$(read_pid "$i")"
		if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then rm -f "$DATA/$i.pid"; fi
	done

	for ((i = 0; i < n; i++)); do
		if is_running "$i"; then log "узел $i уже запущен — пропущен"; continue; fi
		start_one "$i" "$n"
	done

	# Реестр unix-сокетов обходится на тике maintenance (5s), gossip- heartbeat 1s.
	# Раньше этого срока статус показывает пустую таблицу соседей, поэтому ждём
	# заведомо больше тика, а не «пару секунд для красоты».
	log "ожидание знакомства узлов (реестр + первый виток gossip)…"
	sleep 8
	status
	cat <<EOF

Дальше:
  ./scripts/dev-cluster.sh skills 0     # навыки + выученный обзор соседей
  ./scripts/dev-cluster.sh sync 0       # немедленная сверка
  ./scripts/dev-cluster.sh peers 0      # таблица соседей
  ./scripts/dev-cluster.sh submit 0 "найди причину ошибки в логе" research
  ./scripts/dev-cluster.sh rotate 0     # ротация ключа + перезапуск под новым
  ./scripts/dev-cluster.sh down         # остановить
EOF
}

down() {
	local i pid stopped=0
	for ((i = 0; i < MAX_SLOTS; i++)); do
		is_running "$i" || continue
		pid="$(read_pid "$i")"
		# SIGTERM: узел анонсирует уход и корректно закрывает хранилище.
		kill -TERM "$pid" 2>/dev/null || true
		local _
		for _ in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
		kill -KILL "$pid" 2>/dev/null || true
		rm -f "$(pidfile "$i")"
		stopped=$((stopped + 1))
	done
	log "остановлено узлов: $stopped"
}

status() {
	local i pid n=0
	for ((i = 0; i < MAX_SLOTS; i++)); do
		is_running "$i" || continue
		n=$((n + 1))
		pid="$(read_pid "$i")"
		local id="-" peers="-" member="-" skill="-"
		if [[ -n "${ZETOMESH_NO_CURL:-}" ]] || ! command -v curl >/dev/null 2>&1; then
			printf '  node %-2s pid %-8s api :%s (curl недоступен)\n' "$i" "$pid" "$((API_BASE + i))"
			continue
		fi
		local body; body="$(curl -fsS --max-time 3 "$(api_of "$i")/api/v1/status" 2>/dev/null || true)"
		if [[ -n "$body" ]]; then
			id="$(printf '%s' "$body" | sed -n 's/.*"peer_id":"\([^"]*\)".*/\1/p' | cut -c1-16)"
			peers="$(printf '%s' "$body" | sed -n 's/.*"neighbors_connected":\([0-9]*\).*/\1/p')"
			member="$(printf '%s' "$body" | sed -n 's/.*"membership_known":\([0-9]*\).*/\1/p')"
			skill="$(printf '%s' "$body" | sed -n 's/.*"skills":\[\([^]]*\)\].*/\1/p' | tr -d '"')"
		fi
		printf '  node %-2s pid %-8s peer %-18s соседей %s/%s навыки %-12s api :%s\n' \
			"$i" "$pid" "${id:--}" "${peers:--}" "${member:--}" "${skill:--}" "$((API_BASE + i))"
	done
	((n == 0)) && log "кластер не запущен"
	return 0
}

logs() {
	local i=${1:-}
	if [[ -z "$i" ]]; then tail -n 40 -f "$DATA"/*.log; else tail -n 100 -f "$DATA/$i.log"; fi
}

# cli <подкоманда> <узел> [аргументы…] — обращаемся к API через сам бинарник:
# он умеет и авторизацию по токену, и человекочитаемый вывод.
cli() {
	local sub=$1 idx=${2:-0}; shift 2 || true
	need_bin
	"$BIN" "$sub" -addr "$(api_of "$idx")" "$@"
}

peers()   { cli peers   "${1:-0}"; }
skills()  { cli skills  "${1:-0}"; }
sync()    { cli skills-sync "${1:-0}"; }
rebinds() { cli rebinds "${1:-0}"; }

# submit <узел> <текст задачи> [навык] — ставит задачу и ждёт результат.
submit() {
	local idx=${1:-0} text=${2:-} skill=${3:-}
	[[ -n "$text" ]] || die "укажите текст задачи: $0 submit 0 \"найдите причину ошибки\" research"
	if [[ -n "$skill" ]]; then
		cli submit "$idx" -i "$text" -skills "$skill" -w
	else
		cli submit "$idx" -i "$text" -w
	fi
}

# rotate меняет ключ узла, объявляет переход сети и перезапускает процесс под
# новым ключом — тот же порядок, что описан в RUNBOOK для systemd.
rotate() {
	local idx=${1:-0}
	need_bin
	is_running "$idx" || die "узел $idx не запущен (сначала ./scripts/dev-cluster.sh up)"
	cli rotate "$idx" -reason "dev-cluster manual rotation"
	local count; count="$(running_nodes | wc -l)"
	log "перезапуск узла $idx под новым ключом"
	local pid; pid="$(read_pid "$idx")"
	kill -TERM "$pid" 2>/dev/null || true
	sleep 1
	kill -KILL "$pid" 2>/dev/null || true
	rm -f "$(pidfile "$idx")"
	start_one "$idx" "$count"
	sleep 2
	log "журнал смены идентичностей на узле $idx:"
	rebinds "$idx"
}

case "${1:-}" in
	up) up "${2:-3}" ;;
	down) down ;;
	status) status ;;
	logs) logs "${2:-}" ;;
	peers) peers "${2:-0}" ;;
	skills) skills "${2:-0}" ;;
	sync) sync "${2:-0}" ;;
	rebinds) rebinds "${2:-0}" ;;
	submit) shift; submit "$@" ;;
	rotate) rotate "${2:-0}" ;;
	down-and-clear) down; rm -rf "$DATA"; log "данные удалены: $DATA" ;;
	*) die "использование: $0 {up [N]|down|status|logs [i]|peers [i]|skills [i]|sync [i]|rebinds [i]|submit [i] <текст> [навык]|rotate [i]|down-and-clear}" ;;
esac
