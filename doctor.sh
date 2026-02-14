#!/usr/bin/env bash
set -euo pipefail

WD="$HOME/Projects/atlas"
cd "$WD"

echo "== ATLAS DOCTOR =="
echo "WD: $WD"
echo "SHELL: ${SHELL:-unknown}"
echo "DATE: $(date)"
echo

echo "== run.sh (first 80 lines) =="
if [[ -f run.sh ]]; then
  /usr/bin/nl -ba run.sh | /usr/bin/sed -n '1,80p'
else
  echo "ERR: run.sh not found"
fi
echo

echo "== launchd status (rss/score) =="
/bin/launchctl print "gui/$(/usr/bin/id -u)/com.atlas.rss"  2>/dev/null | /usr/bin/egrep "state =|runs =|last exit code =|run interval =|program =|working directory =|stdout path =|stderr path =" || echo "WARN: com.atlas.rss not loaded"
echo
/bin/launchctl print "gui/$(/usr/bin/id -u)/com.atlas.score" 2>/dev/null | /usr/bin/egrep "state =|runs =|last exit code =|run interval =|program =|working directory =|stdout path =|stderr path =" || echo "WARN: com.atlas.score not loaded"
echo

echo "== pipeline run (rss_fetch -> score_signals) =="
/usr/bin/env python3 rss_fetch.py
/usr/bin/env python3 score_signals.py
echo

echo "== db counters =="
/usr/bin/sqlite3 data/atlas.db "SELECT datetime('now'), COUNT(*) total FROM signals;"
/usr/bin/sqlite3 data/atlas.db "SELECT source, COUNT(*) c FROM signals GROUP BY source ORDER BY c DESC LIMIT 12;"
echo

echo "== last logs (tail 60) =="
mkdir -p logs
touch logs/rss.out.log logs/rss.err.log logs/score.out.log logs/score.err.log
chmod 644 logs/rss.out.log logs/rss.err.log logs/score.out.log logs/score.err.log
echo "-- rss.out.log --"
/usr/bin/tail -n 60 logs/rss.out.log || true
echo "-- rss.err.log --"
/usr/bin/tail -n 60 logs/rss.err.log || true
echo "-- score.out.log --"
/usr/bin/tail -n 60 logs/score.out.log || true
echo "-- score.err.log --"
/usr/bin/tail -n 60 logs/score.err.log || true
echo

echo "== done =="
