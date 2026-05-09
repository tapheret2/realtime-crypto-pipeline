#!/usr/bin/env bash
# scripts/stop.sh — tear down the stack and remove anonymous volumes.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[stop] docker compose down -v"
docker compose down -v
echo "[stop] done"
