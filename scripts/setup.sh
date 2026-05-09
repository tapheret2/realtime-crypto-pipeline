#!/usr/bin/env bash
# scripts/setup.sh — one-shot bootstrap for a fresh clone.
# - copies .env.example -> .env
# - creates a Python venv for local tests
# - installs producer + test deps
# Idempotent: safe to re-run.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
    echo "[setup] copying .env.example -> .env"
    cp .env.example .env
else
    echo "[setup] .env already exists, leaving alone"
fi

if [[ ! -d .venv ]]; then
    echo "[setup] creating .venv"
    python -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

echo "[setup] installing dev/test deps"
pip install --upgrade pip
pip install -r tests/requirements.txt
pip install -r producer/requirements.txt

echo "[setup] done. Next:"
echo "  - docker compose up -d"
echo "  - open http://localhost:8501 (dashboard)"
echo "  - open http://localhost:8080 (airflow, airflow/airflow)"
