#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8504}"
LOGDIR="logs"
mkdir -p "$LOGDIR"
LOG="${LOGDIR}/streamlit_${PORT}.log"
PIDFILE=".streamlit_${PORT}.pid"

python3 -m py_compile dashboard.py

# stop existing listener
PID="$(lsof -tiTCP:${PORT} -sTCP:LISTEN | head -n1 || true)"
if [[ -n "${PID:-}" ]]; then
  echo "Stopping PID=$PID (port $PORT)"
  kill "$PID" || true
  sleep 1
fi
PID2="$(lsof -tiTCP:${PORT} -sTCP:LISTEN | head -n1 || true)"
if [[ -n "${PID2:-}" ]]; then
  echo "Force kill PID=$PID2 (port $PORT)"
  kill -9 "$PID2" || true
fi

nohup streamlit run dashboard.py --server.port "$PORT" --server.fileWatcherType none \
  > "$LOG" 2>&1 &

NEWPID=$!
disown || true
echo "$NEWPID" > "$PIDFILE"

echo "OK: started streamlit PID=$NEWPID port=$PORT"
echo "LOG: $LOG"

# readiness (max 10s)
for _ in {1..20}; do
  if curl -sf "http://localhost:${PORT}" >/dev/null 2>&1; then
    echo "OK: streamlit is up http://localhost:${PORT} (PID=$NEWPID)"
    exit 0
  fi
  if ! ps -p "$NEWPID" >/dev/null 2>&1; then
    echo "ERR: streamlit process died (PID=$NEWPID)"
    tail -n 200 "$LOG" || true
    exit 2
  fi
  sleep 0.5
done

echo "ERR: streamlit did not open port $PORT within 10s (PID=$NEWPID)"
tail -n 200 "$LOG" || true
exit 3
