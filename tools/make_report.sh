#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p transfer_report logs

echo "== doctor.sh ==" > transfer_report/doctor.txt
./doctor.sh >> transfer_report/doctor.txt 2>&1 || true

# DB counters (в отдельном .sql, чтобы вообще не было проблем с кавычками)
cat > transfer_report/db_counters.sql <<'SQL'
SELECT datetime('now') AS now;

SELECT 'total_signals', COUNT(*) FROM signals;

SELECT 'dupes_source_url', COUNT(*) FROM (
  SELECT source, url, COUNT(*) c
  FROM signals
  WHERE COALESCE(source,'')!='' AND COALESCE(url,'')!=''
  GROUP BY source, url
  HAVING c>1
);

SELECT 'top_sources' AS section, '' AS _;
SELECT source, COUNT(*) c
FROM signals
GROUP BY source
ORDER BY c DESC
LIMIT 20;

SELECT 'last_20' AS section, '' AS _;
SELECT id, substr(source,1,60) src, substr(url,1,90) url, COALESCE(color,'') color
FROM signals
ORDER BY id DESC
LIMIT 20;
SQL

sqlite3 data/atlas.db < transfer_report/db_counters.sql > transfer_report/db_counters.txt 2>&1 || true

# Logs tail
: > transfer_report/logs_tail.txt
for f in logs/rss.out.log logs/rss.err.log logs/score.out.log logs/score.err.log; do
  echo "===== $f =====" >> transfer_report/logs_tail.txt
  tail -n 200 "$f" >> transfer_report/logs_tail.txt 2>&1 || echo "(missing)" >> transfer_report/logs_tail.txt
  echo >> transfer_report/logs_tail.txt
done

cat transfer_report/doctor.txt transfer_report/db_counters.txt transfer_report/logs_tail.txt > transfer_report/ALL.txt 2>&1 || true

tar -czf transfer_report.tgz transfer_report
echo "OK: created $(pwd)/transfer_report.tgz"
ls -la transfer_report.tgz transfer_report | sed -n '1,120p'
