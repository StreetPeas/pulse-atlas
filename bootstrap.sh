#!/usr/bin/env bash
set -euo pipefail

# ====== CONFIG ======
WD="${HOME}/Projects/atlas"
PY="$(command -v python3 || true)"
LA_DIR="${HOME}/Library/LaunchAgents"
RSS_PLIST="${LA_DIR}/com.atlas.rss.plist"
SCORE_PLIST="${LA_DIR}/com.atlas.score.plist"

if [[ -z "${PY}" ]]; then
  echo "ERR: python3 not found"
  exit 1
fi

if [[ ! -d "${WD}" ]]; then
  echo "ERR: atlas dir not found: ${WD}"
  exit 1
fi

cd "${WD}"

echo "== ATLAS BOOTSTRAP =="
echo "WD: ${WD}"
echo "PY: ${PY}"
echo "DATE: $(date)"
echo

# ====== DEPS ======
echo "== deps =="
python3 -m pip install -r requirements.txt >/dev/null 2>&1 || true
python3 -m pip install requests beautifulsoup4 feedparser >/dev/null 2>&1 || true
echo "OK: deps installed"
echo

# ====== LOGS ======
echo "== logs =="
mkdir -p logs
touch logs/rss.out.log logs/rss.err.log logs/score.out.log logs/score.err.log
chmod 644 logs/rss.out.log logs/rss.err.log logs/score.out.log logs/score.err.log
echo "OK: logs ready"
echo

# ====== DB INIT ======
echo "== db init =="
python3 - <<'PY'
import storage
storage.init_db()
print("OK: storage.init_db()")
PY
echo

# ====== DB SCHEMA + UNIQUE INDEX (source,url) ======
echo "== db schema / index =="
python3 - <<'PY'
import sqlite3
from pathlib import Path

db = Path("data/atlas.db")
db.parent.mkdir(parents=True, exist_ok=True)

need = {
  "ts":"TEXT",
  "source":"TEXT",
  "title":"TEXT",
  "text":"TEXT",
  "url":"TEXT",
  "summary":"TEXT",
  "raw":"TEXT",
  "score":"REAL",
  "color":"TEXT",
  "label":"TEXT",
  "rationale":"TEXT",
}

con = sqlite3.connect(str(db))
cur = con.cursor()

cur.execute("PRAGMA table_info(signals)")
cols = {r[1] for r in cur.fetchall()}

added=[]
for c,t in need.items():
    if c not in cols:
        cur.execute(f"ALTER TABLE signals ADD COLUMN {c} {t}")
        added.append(c)

# drop old url-only unique if exists
cur.execute("DROP INDEX IF EXISTS idx_signals_url_unique")

# ensure unique by (source,url)
cur.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_source_url_unique
ON signals(source, url)
WHERE source IS NOT NULL AND source != ''
  AND url IS NOT NULL AND url != '';
""")

con.commit()
cur.execute("PRAGMA index_list(signals)")
idx = cur.fetchall()
con.close()

print("OK: added_cols =", added)
print("OK: index_list =", idx)
PY
echo

# ====== RUN.SH ======
echo "== run.sh =="
cat > run.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 rss_fetch.py
python3 score_signals.py
sqlite3 data/atlas.db "SELECT datetime('now'), COUNT(*) total FROM signals;"
SH
chmod +x run.sh
echo "OK: run.sh written"
echo

# ====== LAUNCHD PLISTS ======
echo "== launchd plists =="
mkdir -p "${LA_DIR}"

cat > "${RSS_PLIST}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.atlas.rss</string>
  <key>WorkingDirectory</key><string>${WD}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PY}</string>
    <string>rss_fetch.py</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>StartInterval</key><integer>900</integer>
  <key>StandardOutPath</key><string>${WD}/logs/rss.out.log</string>
  <key>StandardErrorPath</key><string>${WD}/logs/rss.err.log</string>
</dict>
</plist>
PLIST

cat > "${SCORE_PLIST}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.atlas.score</string>
  <key>WorkingDirectory</key><string>${WD}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PY}</string>
    <string>score_signals.py</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>StartInterval</key><integer>900</integer>
  <key>StandardOutPath</key><string>${WD}/logs/score.out.log</string>
  <key>StandardErrorPath</key><string>${WD}/logs/score.err.log</string>
</dict>
</plist>
PLIST

plutil -lint "${RSS_PLIST}" >/dev/null
plutil -lint "${SCORE_PLIST}" >/dev/null
echo "OK: plists linted"
echo

# ====== (RE)LOAD LAUNCHD ======
echo "== launchd reload =="
UIDNUM="$(id -u)"
launchctl bootout "gui/${UIDNUM}" "${RSS_PLIST}"  2>/dev/null || true
launchctl bootout "gui/${UIDNUM}" "${SCORE_PLIST}" 2>/dev/null || true

launchctl bootstrap "gui/${UIDNUM}" "${RSS_PLIST}"
launchctl bootstrap "gui/${UIDNUM}" "${SCORE_PLIST}"

launchctl kickstart -k "gui/${UIDNUM}/com.atlas.rss"  || true
launchctl kickstart -k "gui/${UIDNUM}/com.atlas.score" || true
echo "OK: launchd loaded + kickstarted"
echo

# ====== TEST PIPELINE ======
echo "== test run =="
./run.sh
echo

# ====== STATUS ======
echo "== status (launchd) =="
launchctl print "gui/${UIDNUM}/com.atlas.rss"  2>/dev/null | egrep "state =|runs =|last exit code =|run interval =|program =|working directory =" || true
echo
launchctl print "gui/${UIDNUM}/com.atlas.score" 2>/dev/null | egrep "state =|runs =|last exit code =|run interval =|program =|working directory =" || true
echo

echo "== status (db counters) =="
sqlite3 data/atlas.db "SELECT source, COUNT(*) c FROM signals GROUP BY source ORDER BY c DESC LIMIT 12;"
echo
sqlite3 data/atlas.db "SELECT color, COUNT(*) c FROM signals WHERE rationale IS NOT NULL AND rationale!='' GROUP BY color ORDER BY c DESC;"
echo

echo "== tail logs (last 40) =="
echo "-- rss.out.log --";  tail -n 40 logs/rss.out.log  || true
echo "-- rss.err.log --";  tail -n 40 logs/rss.err.log  || true
echo "-- score.out.log --"; tail -n 40 logs/score.out.log || true
echo "-- score.err.log --"; tail -n 40 logs/score.err.log || true
echo

echo "== DONE =="
