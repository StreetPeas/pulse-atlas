#!/usr/bin/env bash
set -euo pipefail
PORT="${1:-8504}"

PIDFILE=".streamlit_${PORT}.pid"
if [[ -f "$PIDFILE" ]]; then
  PID="$(cat "$PIDFILE" || true)"
  if [[ -n "${PID:-}" ]]; then
    echo "Stopping PID=$PID (from pidfile)"
    kill "$PID" || true
    sleep 1
  fi
  rm -f "$PIDFILE"
fi

# fallback: kill whoever listens on port
PID2="$(lsof -tiTCP:${PORT} -sTCP:LISTEN | head -n1 || true)"
if [[ -n "${PID2:-}" ]]; then
  echo "Force kill PID=$PID2 (port $PORT)"
  kill -9 "$PID2" || true
fi

echo "OK"
