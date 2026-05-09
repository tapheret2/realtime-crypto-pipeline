#!/usr/bin/env bash
# scripts/start.sh — spin up the whole stack and wait for health.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[start] docker compose up -d"
docker compose up -d

echo "[start] waiting for postgres + kafka health"
for _ in {1..30}; do
    pg_state=$(docker inspect --format='{{.State.Health.Status}}' crypto-postgres 2>/dev/null || echo unknown)
    kafka_state=$(docker inspect --format='{{.State.Health.Status}}' crypto-kafka 2>/dev/null || echo unknown)
    if [[ "$pg_state" == "healthy" && "$kafka_state" == "healthy" ]]; then
        echo "[start] postgres + kafka healthy"
        break
    fi
    sleep 2
done

echo "[start] tail -f producer logs (Ctrl-C to detach):"
exec docker compose logs -f producer
