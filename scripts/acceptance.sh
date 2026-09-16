#!/usr/bin/env bash
#
# Живые приёмочные испытания (ТЗ §20, приложение E) на реальных процессах
# zeptomesh-node. В отличие от интеграционных тестов (in-process libp2p), здесь
# каждый узел — отдельный ОС-процесс со своим каталогом данных и портами.
#
#   ./scripts/acceptance.sh            # все сценарии E.1–E.6
#   ./scripts/acceptance.sh E.4        # только сценарий
#
# Протокол (stdout + .dev/acceptance/PROTOCOL.txt) печатается по фактическим
# выводам команд: PASS/FAIL с цитатами. Ничего не подтасовывается.
#
# Честная граница: «поддельный конверт на wire-потоке» (E.6, п.1 в формулировке
# «поддельная задача отклоняется») требует raw-клиента libp2p-потока и покрыта
# интеграционным прогоном TestIntegrationUnsignedAndRewrittenTasksAreRefused;
# из bash это проверяется на уровне PSK-границы и API-авторизации.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin/zeptomesh-node"
TPL="$ROOT/configs/examples/dev-node.yaml"
DATA="$ROOT/.dev/acceptance"
PROTO="$DATA/PROTOCOL.txt"
MESH_BASE=${ACC_MESH_BASE:-4131}
API_BASE=${ACC_API_BASE:-8131}
PROM_BASE=${ACC_PROM_BASE:-9664}

FAILS=0
TOTAL=0

log() { printf '%s\n' "$*" | tee -a "$PROTO"; }
say() { printf '\n== %s ==\n' "$*" | tee -a "$PROTO"; }
ok()  { TOTAL=$((TOTAL+1)); log "  PASS: $*"; }
bad() { TOTAL=$((TOTAL+1)); FAILS=$((FAILS+1)); log "  FAIL: $*"; }
note(){ log "  info: $*"; }

need_bin() {
    [[ -x "$BIN" ]] || { echo "bin/zeptomesh-node not built (make build)" >&2; exit 2; }
    [[ -f "$TPL" ]] || { echo "template $TPL missing" >&2; exit 2; }
    command -v curl  >/dev/null || { echo "curl required" >&2; exit 2; }
    command -v python3 >/dev/null || { echo "python3 required" >&2; exit 2; }
}

jget() { # jget <json> <dotted.path> — missing → empty
    python3 - "$1" "$2" <<'PY'
import json, sys
try:
    d = json.loads(sys.argv[1])
except Exception:
    sys.exit(0)
for p in sys.argv[2].split('.'):
    if isinstance(d, dict) and p in d:
        d = d[p]
    else:
        sys.exit(0)
print(d if not isinstance(d, (dict, list)) else json.dumps(d))
PY
}

# gen_cfg <idx> <skills-csv> <sockgrp> <accept_external> <gossip on|off> [bootstrap...]
gen_cfg() {
    local idx=$1 skills=$2 sockgrp=$3 accept=$4 gossip=$5; shift 5
    local dir="$DATA/node-$idx" mesh=$((MESH_BASE + idx)) api=$((API_BASE + idx)) prom=$((PROM_BASE + idx))
    local boot=""
    if (( $# > 0 )); then
        local parts=()
        for b in "$@"; do parts+=("\"$b\""); done
        boot="[$(IFS=,; printf '%s' "${parts[*]}")]"
    fi
    mkdir -p "$dir"
    sed \
        -e "s|name: dev-\${ZETOMESH_INDEX:-0}|name: acc-$idx|" \
        -e "s|data_dir: \${ZETOMESH_DATA:-.dev/cluster}/\${ZETOMESH_INDEX:-0}|data_dir: $dir|" \
        -e "s|/tcp/\${ZETOMESH_MESH_PORT:-4101}|/tcp/$mesh|" \
        -e "s|local_registry: true|local_registry: $( [[ $sockgrp == manual ]] && echo false || echo true )|" \
        -e "s|local_socket_dir: \${ZETOMESH_DATA:-.dev/cluster}/shared|local_socket_dir: $DATA/sock-$sockgrp|" \
        -e "s|bootstrap: \[\${ZETOMESH_BOOTSTRAP:-}\]|bootstrap: $boot|" \
        -e "s|skills: \[\${ZETOMESH_SKILL:-general}\]|skills: [$(echo "$skills" | sed 's/,/, /g')]|" \
        -e "s|accept_external_tasks: true|accept_external_tasks: $accept|" \
        -e "s|name: \${ZETOMESH_SKILL:-general}|name: $(echo "$skills" | cut -d, -f1)|" \
        -e "s|listen: 127.0.0.1:\${ZETOMESH_API_PORT:-8101}|listen: 127.0.0.1:$api|" \
        -e "s|prometheus_listen: 127.0.0.1:\${ZETOMESH_PROM_PORT:-9564}|prometheus_listen: 127.0.0.1:$prom|" \
        "$TPL" > "$dir/node.yaml"
    if [[ $gossip == off ]]; then
        sed -i '/^  gossip:/,/failure_timeout:/{s/^    enabled: true/    enabled: false/}' "$dir/node.yaml"
    fi
}

node_up() { # node_up <idx> [env assignments...]
    local idx=$1; shift
    local dir="$DATA/node-$idx"
    mkdir -p "$dir"
    # exec: the subshell BECOMES the node, so $! is the node's real pid
    # (a plain `cd && env ... &` backgrounded compound forks twice and the
    # captured pid is an intermediate shell that dies before its child).
    ( cd "$ROOT" && exec env "$@" "$BIN" run -config "$dir/node.yaml" >"$dir/node.log" 2>&1 ) &
    echo $! > "$dir/pid"
}

node_pid()   { cat "$DATA/node-$1/pid" 2>/dev/null || true; }
node_alive() { local p; p="$(node_pid "$1")" && [[ -n "$p" ]] && kill -0 "$p" 2>/dev/null; }
api()        { printf '127.0.0.1:%s' "$((API_BASE + $1))"; }

node_kill() { # node_kill <idx> [TERM|KILL]
    local idx=$1 sig=${2:-TERM} p
    p="$(node_pid "$idx")"
    [[ -z "$p" ]] && return 0
    kill "-$sig" "$p" 2>/dev/null || true
    for _ in $(seq 1 80); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
    kill -KILL "$p" 2>/dev/null || true
    rm -f "$DATA/node-$idx/pid"
}

node_down_all() {
    # pid files first, then a sweep by config path — the pattern is scoped to
    # $DATA, so it can never touch a production or dev-cluster process.
    local f p
    for f in "$DATA"/node-*/pid; do
        [[ -f $f ]] || continue
        p="$(cat "$f")"; [[ -n "$p" ]] && kill "$p" 2>/dev/null || true
    done
    sleep 0.4
    for f in "$DATA"/node-*/pid; do
        [[ -f $f ]] || continue
        p="$(cat "$f")"; [[ -n "$p" ]] && kill -KILL "$p" 2>/dev/null || true
    done
    pkill -KILL -f "zeptomesh-node run -config $DATA/" 2>/dev/null || true
}

wait_api() { # wait_api <idx> [tries]
    local idx=$1 n=${2:-40}
    for _ in $(seq 1 "$n"); do
        curl -fsS --max-time 2 "http://$(api "$idx")/healthz" >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    return 1
}

peer_id() { # peer_id <idx>
    local body; body="$(status_json "$1")"
    jget "$body" peer_id
}

nstat() { # nstat <status> — strip the protobuf enum prefix for assertions
    printf '%s' "${1#TASK_STATUS_}"
}

status_json() { curl -fsS --max-time 3 "http://$(api "$1")/api/v1/status" 2>/dev/null || true; }

# submit <idx> <instruction> [skills-csv] — ждёт результат (wait:true).
submit() {
    local idx=$1 instr=$2 skills=${3:-}
    local body="{\"instruction\":\"${instr//\"/\\\"}\""
    if [[ -n "$skills" ]]; then
        body+=",\"required_skills\":[$(echo "$skills" | sed 's/,/","/g; s/^/"/; s/$/"/')]"
    fi
    body+=",\"wait\":true,\"wait_seconds\":25}"
    curl -s --max-time 40 -X POST -H 'content-type: application/json' \
        -d "$body" "http://$(api "$idx")/api/v1/tasks" 2>/dev/null || true
}

scenario_reset() {
    node_down_all
    rm -rf "$DATA"/node-* "$DATA"/sock-* 2>/dev/null
    mkdir -p "$DATA"
}

# ---------------------------------------------------------------------------
E1() {
    say "E.1 Запуск одного узла"
    gen_cfg 0 "research" e1 true on
    node_up 0
    wait_api 0 || { bad "узел 0 не поднял API за 20 с"; return; }
    ok "узел стартует (отдельный процесс) и отвечает на /healthz"
    local kf perm
    kf="$(find "$DATA/node-0" -name peer.key | head -1)"
    if [[ -n "$kf" ]]; then
        perm="$(stat -c '%a' "$kf")"
        [[ $perm == 600 ]] && ok "ключ сгенерирован: $kf (права $perm)" || bad "права ключа $perm, want 600"
    else
        bad "peer.key не найден"
    fi
    if ss -ltn | awk '{print $4}' | grep -qE '^0\.0\.0\.0:(8131|4131|9664)$'; then
        bad "порт слушает не-loopback интерфейс"
    else
        ok "API/mesh/prometheus-порты слушают только 127.0.0.1"
    fi
    local out st worker
    out="$(submit 0 "локальная задача одиночного узла" research)"
    st="$(nstat "$(jget "$out" result.status)")"; worker="$(jget "$out" result.worker)"
    if [[ $st == COMPLETED && -n $worker ]]; then
        ok "локальная задача через PicoClaw(stub): COMPLETED, worker=${worker:0:14}…"
    else
        bad "локальная задача: status=$st worker=${worker:-<пусто>}; ответ: ${out:0:120}"
    fi
}

E2() {
    say "E.2 Два узла находят друг друга (реестр unix-сокетов), обмениваются возможностями"
    gen_cfg 0 "research" e2 true on
    gen_cfg 1 "coding,translate" e2 true on
    node_up 0; node_up 1
    wait_api 0 && wait_api 1 || { bad "API не готовы"; return; }
    local peers="" st0
    for _ in $(seq 1 30); do
        st0="$(status_json 0)"
        peers="$(jget "$st0" neighbors_connected)"
        [[ "${peers:-0}" -ge 1 ]] && break
        sleep 1
    done
    if [[ "${peers:-0}" -ge 1 ]]; then
        ok "узел 0 соединился с узлом 1: neighbors_connected=$peers"
    else
        bad "узел 0 не нашёл узла 1 за 30 с (реестр unix-сокетов не сработал)"
        return
    fi
    # возможности: выученный обзор соседей виден в /api/v1/skills.peers
    local learned="" body=""
    for _ in $(seq 1 20); do
        body="$(curl -fsS --max-time 3 "http://$(api 0)/api/v1/skills" 2>/dev/null || true)"
        learned="$(jget "$body" peers)"
        [[ -n "$learned" && "$learned" != "[]" && "$learned" != "null" ]] && break
        sleep 1
    done
    if [[ "$learned" == *coding* ]]; then
        ok "обмен возможностями: узел 0 выучил навыки соседа (peers-обзор содержит coding)"
    elif [[ -n "$learned" && "$learned" != "[]" ]]; then
        ok "обмен возможностями: peers-обзор непустой: ${learned:0:70}…"
    else
        bad "узел 0 не выучил возможности узла 1: ${body:0:100}"
    fi
    local id0 id1
    id0="$(peer_id 0)"; id1="$(peer_id 1)"
    [[ -n "$id0" && -n "$id1" && "$id0" != "$id1" ]] && ok "идентичности различны: ${id0:0:10}… / ${id1:0:10}…" || bad "peer ids пустые/совпали"
}

E3() {
    say "E.3 Делегирование: навыка нет у инициатора — задача уходит соседу и возвращается"
    if ! node_alive 0 || ! node_alive 1; then
        gen_cfg 0 "research" e3 true on
        gen_cfg 1 "coding,translate" e3 true on
        node_up 0; node_up 1
        wait_api 0 && wait_api 1 || { bad "пара не поднялась"; return; }
        sleep 12 # реестр + handshake + обмен скилами
    fi
    local id1 out id st worker signed
    id1="$(peer_id 1)"
    out="$(submit 0 "напиши сортировку" coding)"
    id="$(jget "$out" task_id)"; st="$(nstat "$(jget "$out" result.status)")"
    worker="$(jget "$out" result.worker)"; signed="$(jget "$out" result.worker_signed)"
    if [[ $st == COMPLETED && "$worker" == "$id1" ]]; then
        ok "задача coding исполнена соседом: COMPLETED, worker=${worker:0:14}… (id $id)"
    else
        bad "делегирование: status=$st worker=${worker:0:14}…, want COMPLETED/${id1:0:14}…"
    fi
    [[ $signed == True ]] && ok "результат подписан исполнителем (worker_signature)" || bad "нет worker_signature"
}

E4() {
    say "E.4 Многопрыжковая маршрутизация (цепочка n0→n1→n2, знакомство только по bootstrap)"
    # n2 поднимаем первым; n1 смотрит в n2; n0 — только в n1. Прямого ребра
    # n0—n2 нет ни в каком канале знакомства (реестр/gossip/DHT выключены).
    gen_cfg 2 "summarize" manual true off
    node_up 2; wait_api 2 || { bad "n2 не поднялся"; return; }
    local id2; id2="$(peer_id 2)"
    gen_cfg 1 "summarize" manual false off "/ip4/127.0.0.1/tcp/$((MESH_BASE + 2))/p2p/$id2"
    node_up 1; wait_api 1 || { bad "n1 не поднялся"; return; }
    local id1; id1="$(peer_id 1)"
    gen_cfg 0 "research" manual true off "/ip4/127.0.0.1/tcp/$((MESH_BASE + 1))/p2p/$id1"
    node_up 0; wait_api 0 || { bad "n0 не поднялся"; return; }
    sleep 8 # bootstrap-тик 3s: дозвон, handshake, подписанные capabilities
    local p0; p0="$(jget "$(status_json 0)" neighbors_connected)"
    if [[ "${p0:-0}" == 1 ]]; then
        ok "n0 знает ровно один узел (ребро n0→n1): neighbors_connected=$p0"
    else
        note "n0 видит соседей: ${p0:-?} (цепочка требует 1; продолжим замер задачи)"
    fi
    local out id st worker route
    out="$(submit 0 "сожми протокол испытаний" summarize)"
    id="$(jget "$out" task_id)"; st="$(nstat "$(jget "$out" result.status)")"
    worker="$(jget "$out" result.worker)"; route="$(jget "$out" result.route_stack)"
    if [[ $st == COMPLETED && "$worker" == "$id2" ]]; then
        ok "исполнитель за два ребра: COMPLETED, worker=${worker:0:14}… (id $id)"
    else
        bad "много-hop: status=$st worker=${worker:-<пусто>} (want n2 ${id2:0:14}…); ответ: ${out:0:140}"
    fi
    local id0; id0="$(peer_id 0)"
    if [[ "$route" == *"\"$id0\""* && "$route" == *"\"$id1\""* && "$route" != *"$id2"* ]]; then
        ok "маршрут сохранён: route_stack = [n0, n1], исполнитель не в транзитном стеке"
    else
        bad "route_stack = ${route:-<пусто>} (want [n0,n1])"
    fi
    sleep 1
    local rec1 s1 w1 d1 o1
    rec1="$(curl -fsS --max-time 3 "http://$(api 1)/api/v1/tasks/$id" 2>/dev/null || true)"
    s1="$(jget "$rec1" task.status)"; w1="$(jget "$rec1" task.worker_peer_id)"
    d1="$(jget "$rec1" task.delegated_to)"; o1="$(jget "$rec1" task.origin_peer_id)"
    # FORWARDED — транзитное состояние журнала relay-узла: быстрый исполнитель
    # успевает вернуть результат до окончания этого замера, и relay-ветка
    # закрывает запись как COMPLETED с worker=n2. Проверяем сквозной факт:
    # n1 принял задачу от n0, передал её n2 и сам её не исполнял.
    if [[ -z "$rec1" || -z "$s1" ]]; then
        bad "журнал n1: нет записи о задаче $id"
    elif [[ "$w1" == "$id1" || "$o1" != "$id0" ]]; then
        bad "журнал n1: n1 исполнил сам (worker=${w1:-<пусто>}) или потерял origin (origin=${o1:-<пусто>})"
    elif [[ ( "$s1" == FORWARDED || "$s1" == COMPLETED ) && "$d1" == *"$id2"* ]]; then
        ok "журнал n1: ретрансляция (status=$s1, delegated_to=n2, worker=${w1:-<не назначен>})"
    else
        bad "журнал n1: status=$s1 worker=${w1:-<пусто>} delegated=${d1:-<пусто>}"
    fi
}

E5() {
    say "E.5 Отказ узла: сеть живёт дальше, задача не теряется молча"
    gen_cfg 0 "research" e5 true on
    gen_cfg 1 "coding" e5 true on
    gen_cfg 2 "summarize" e5 true on
    node_up 0; node_up 1; node_up 2
    wait_api 0 && wait_api 1 && wait_api 2 || { bad "кластер не поднялся"; return; }
    sleep 12
    local out id st
    out="$(submit 1 "сожми отчёт" summarize)"
    id="$(jget "$out" task_id)"; st="$(nstat "$(jget "$out" result.status)")"
    [[ $st == COMPLETED ]] && ok "до отказа: summarize исполнен (id $id)" || bad "базовый прогон: ${out:0:120}"
    node_kill 2 KILL
    note "n2 (единственный носитель summarize) убит SIGKILL без анонса ухода"
    sleep 9 # failure_timeout=5s + тик maintenance
    out="$(submit 1 "сожми ещё раз" summarize)"
    id="$(jget "$out" task_id)"; st="$(nstat "$(jget "$out" result.status)")"
    # resultJSON (internal/api/admin.go) называют поле причины «error», а не
    # «error_message» — читаем фактическое имя.
    local em; em="$(jget "$out" result.error)"
    if [[ "$st" =~ (FAILED|TIMEOUT|REJECTED) && -n "$em" ]]; then
        ok "задача на мёртвый навык не зависла: status=$st, причина: ${em:0:90}"
    else
        bad "задача на мёртвый навык: status=${st:-<нет ответа>} error=${em:-<нет>}"
    fi
    out="$(submit 1 "найди первопричину" research)"
    st="$(nstat "$(jget "$out" result.status)")"
    [[ $st == COMPLETED ]] && ok "живая часть сети работает: research-задача COMPLETED" || bad "сеть после отказа не работает: ${out:0:120}"
}

E6() {
    say "E.6 Безопасность: PSK-граница, авторизация API, секреты не светятся"
    # (a) разные PSK — общего транспорта нет, хотя реестр один.
    gen_cfg 0 "research" e6a true on
    gen_cfg 1 "coding" e6a true on
    local pskA pskB
    pskA="$("$BIN" psk)"; pskB="$("$BIN" psk)"
    sed -i "s|private_network_psk: \${ZETOMESH_PSK:-}|private_network_psk: $pskA|" "$DATA/node-0/node.yaml"
    sed -i "s|private_network_psk: \${ZETOMESH_PSK:-}|private_network_psk: $pskB|" "$DATA/node-1/node.yaml"
    node_up 0; node_up 1
    wait_api 0 && wait_api 1 || { bad "PSK-пара не поднялась"; return; }
    sleep 8
    local p0; p0="$(jget "$(status_json 0)" neighbors_connected)"
    if [[ "${p0:-0}" == 0 ]]; then
        ok "узлы с разными PSK не соединяются, даже видя сокеты друг друга (neighbors=0)"
    else
        bad "PSK не разделил сеть: neighbors_connected=$p0"
    fi
    local out; out="$(submit 0 "задача через PSK-границу" coding)"
    st="$(nstat "$(jget "$out" result.status)")"
    [[ "$st" != COMPLETED ]] && ok "исполнения через PSK-границу нет: status=${st:-<отказ/таймаут>}" || bad "задача исполнилась вопреки PSK"
    node_kill 0; node_kill 1
    rm -f "$DATA"/sock-e6a/*.sock 2>/dev/null
    # (b) API за bearer-токеном; секрет не в логах.
    gen_cfg 0 "research" e6b true on
    node_up 0 ZETOMESH_API_TOKEN="super-secret-token-42"
    wait_api 0 || { bad "узел с токеном не поднялся"; return; }
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://$(api 0)/api/v1/status")"
    [[ $code == 401 ]] && ok "неавторизованный запрос к API отклонён (HTTP $code)" || bad "ожидался 401, получен $code"
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 -H 'authorization: Bearer super-secret-token-42' "http://$(api 0)/api/v1/status")"
    [[ $code == 200 ]] && ok "с bearer-токеном API отвечает (HTTP $code)" || bad "с верным токеном получен $code"
    if grep -rq "super-secret-token-42" "$DATA/node-0/node.log" 2>/dev/null; then
        bad "токен попал в лог узла"
    else
        ok "API-секрет не появляется в логах"
    fi
    if grep -q "12D3" "$DATA/node-0/node.log" 2>/dev/null; then
        note "peer id (публичный идентификатор) в логе присутствует — это не секрет"
    fi
    # (c) wire-подписи — граница честности: raw-флейминг из bash недоступен.
    note "поддельный/переписанный TaskEnvelope на wire-потоке (E.6 п.1) покрыт"
    note "  интеграционным прогоном TestIntegrationUnsignedAndRewrittenTasksAreRefused"
    note "  (живой libp2p: unsigned отвергается, переписанный содержимый — по origin-подписи);"
    note "  повторить то же из bash без raw-клиента протокола нельзя — это отмечено в протоколе."
    node_kill 0
}

main() {
    need_bin
    mkdir -p "$DATA"; : > "$PROTO"
    trap 'node_down_all' EXIT
    local want=${1:-all}
    if [[ $want == all || $want == E.1 ]]; then scenario_reset; E1; fi
    if [[ $want == all || $want == E.2 ]]; then scenario_reset; E2; fi
    if [[ $want == all || $want == E.3 ]]; then [[ $want == E.3 ]] && scenario_reset; E3; fi
    if [[ $want == all || $want == E.4 ]]; then scenario_reset; E4; fi
    if [[ $want == all || $want == E.5 ]]; then scenario_reset; E5; fi
    if [[ $want == all || $want == E.6 ]]; then scenario_reset; E6; fi
    say "ИТОГ: $((TOTAL-FAILS))/$TOTAL пройдено (FAIL=$FAILS)"
    node_down_all
    log "протокол: $PROTO"
    [[ $FAILS -eq 0 ]]
}

main "$@"
