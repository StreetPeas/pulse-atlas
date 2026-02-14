#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

PY="/Library/Frameworks/Python.framework/Versions/3.14/bin/python3"

"$PY" rss_fetch.py
"$PY" score_signals.py

/usr/bin/sqlite3 data/atlas.db "SELECT datetime('now'), COUNT(*) total FROM signals;"
