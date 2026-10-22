#!/usr/bin/env bash
# Deploy verification: recreate 4 nodes with the new image, wait for mesh convergence, report state.
set -uo pipefail
cd /root/soft/zeptoclaw || exit 1
COMPOSE=/root/soft/zeptoclaw/zeptomesh-data/docker-compose.yml

echo "=== 1. Recreating 4 containers with the new image ==="
docker compose -p zeptomesh -f "$COMPOSE" up -d --force-recreate

echo
echo "=== 2. Waiting for mesh convergence (gossip heartbeat 10s + bootstrap retry + peer exchange) ==="
sleep 90

echo
echo "=== 3. Containers ==="
for i in 0 1 2 3; do
    st=$(docker inspect --format='{{.State.Status}}' "zepto-$i" 2>/dev/null || echo "missing")
    printf '  zepto-%s  %s\n' "$i" "$st"
done

echo
echo "=== 4. Image ==="
docker images --format '  {{.Repository}}:{{.Tag}}  {{.ID}}  {{.CreatedSince}}' | grep zeptomesh-node || true

echo
echo "=== 5. Per-node status ==="
total=0
for i in 0 1 2 3; do
    port=$((14001 + i))
    health=$(curl -s -m 5 "http://127.0.0.1:$port/healthz" | tr -d '\n\r')
    peers=$(curl -s -m 5 "http://127.0.0.1:$port/api/v1/peers")
    cnt=$(printf '%s' "$peers" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null || echo 0)
    ids=$(printf '%s' "$peers" | python3 -c '
import sys,json
d=json.load(sys.stdin)
print(", ".join((p["peer_id"] or "<empty>")[:12] for p in d["peers"]))' 2>/dev/null || echo "")
    addrs=$(printf '%s' "$peers" | python3 -c '
import sys,json
d=json.load(sys.stdin)
print(", ".join(a for p in d["peers"] for a in p.get("addrs",[])))' 2>/dev/null || echo "")
    sig=$(docker logs zepto-$i 2>&1 | grep -c "bad capabilities signature" || true)
    printf '  zepto-%s :%s  %s\n' "$i" "$port" "$health"
    printf '      peers=%s  [%s]\n' "$cnt" "$ids"
    printf '      addrs=[%s]\n' "$addrs"
    printf '      bad capabilities signature=%s\n' "$sig"
    total=$((total + cnt))
done

echo
echo "=== 6. Mesh edges (sum of per-node peer counts; full mesh of 4 nodes = 12) ==="
echo "  total: $total"
