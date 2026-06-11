#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
pkill -f "uvicorn app.backend_termux_app:fastapi_app" >/dev/null 2>&1 || true
pkill -f "reflex run --env prod" >/dev/null 2>&1 || true
[ -f .stepdaddy-backend.pid ] && rm -f .stepdaddy-backend.pid
[ -f .stepdaddy.pid ] && rm -f .stepdaddy.pid
echo "StepDaddyLiveHD stopped."
