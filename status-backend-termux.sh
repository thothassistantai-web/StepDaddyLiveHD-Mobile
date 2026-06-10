#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
if [ -f .env.termux ]; then
  set -a
  . ./.env.termux
  set +a
fi
: "${PORT:=3000}"
echo "PORT=$PORT"
ps -ef | grep -i "uvicorn StepDaddyLiveHD.backend_termux_app" | grep -v grep || true
curl -s -o /dev/null -w "health channels/status=%{http_code}\n" "http://127.0.0.1:${PORT}/channels/status" || true
