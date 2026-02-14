#!/usr/bin/env bash
set -euo pipefail

WD="$HOME/Projects/atlas"
PLIST="$HOME/Library/LaunchAgents/com.atlas.run.plist"
ZIP="$HOME/Desktop/PulseAtlas_$(date +%Y%m%d_%H%M%S).zip"

echo "1) Stop LaunchAgent (safe for DB snapshot)..."
launchctl bootout gui/$(id -u) "$PLIST" 2>/dev/null || true

echo "2) SQLite safe backup..."
if [ -f "$WD/data/atlas.db" ]; then
  /usr/bin/sqlite3 "$WD/data/atlas.db" ".backup '$WD/data/atlas.backup.db'"
fi

echo "3) Build zip -> $ZIP"
cd "$HOME/Projects"
# упаковываем проект, исключаем мусор/логи; БД-бэкап оставляем
/usr/bin/zip -r "$ZIP" "atlas" \
  -x "atlas/logs/*" \
     "atlas/.venv/*" \
     "atlas/**/__pycache__/*" \
     "atlas/**/*.pyc" \
     "atlas/**/.DS_Store"

echo "OK: $ZIP"
echo "4) Start LaunchAgent back..."
launchctl bootstrap gui/$(id -u) "$PLIST" 2>/dev/null || true
launchctl kickstart -k gui/$(id -u)/com.atlas.run 2>/dev/null || true

echo "DONE"
