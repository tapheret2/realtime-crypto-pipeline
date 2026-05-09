#!/usr/bin/env bash
# scripts/smoke.sh — minimal end-to-end check.
# 1. Bring up the stack.
# 2. Wait for raw ticks to land.
# 3. Wait for at least one minute aggregate row.
# Exits non-zero if either step times out.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PSQL="docker exec crypto-postgres psql -U crypto -d crypto -tAc"

echo "[smoke] starting stack"
docker compose up -d

count_ticks=0
for i in {1..60}; do
    count_ticks=$($PSQL "SELECT COUNT(*) FROM fact_price_tick" 2>/dev/null || echo 0)
    if [[ "$count_ticks" -gt 0 ]]; then
        echo "[smoke] raw ticks observed: $count_ticks (after ${i}s)"
        break
    fi
    sleep 5
done

if [[ "$count_ticks" -eq 0 ]]; then
    echo "[smoke] FAIL: no raw ticks after 5 minutes"
    docker compose logs --tail=200 producer
    exit 1
fi

count_aggs=0
for i in {1..36}; do
    count_aggs=$($PSQL "SELECT COUNT(*) FROM agg_price_minute" 2>/dev/null || echo 0)
    if [[ "$count_aggs" -gt 0 ]]; then
        echo "[smoke] minute aggregates observed: $count_aggs (after ${i}*5s)"
        echo "[smoke] PASS"
        exit 0
    fi
    sleep 5
done

echo "[smoke] FAIL: no minute aggregates after 3 minutes"
docker compose logs --tail=200 spark-stream
exit 1
