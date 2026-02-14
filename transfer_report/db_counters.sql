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
