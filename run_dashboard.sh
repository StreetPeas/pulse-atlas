#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

PORT="${1:-8504}"

python3 -m py_compile dashboard.py

./stop_dashboard.sh "${PORT}"

# Чтобы не было сюрпризов от file-watcher на macOS, можно отключить:
exec streamlit run dashboard.py --server.port "${PORT}" --server.fileWatcherType none
