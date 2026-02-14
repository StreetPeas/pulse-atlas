import hashlib
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import feedparser

DB_PATH = Path("data/atlas.db")
SOURCES_PATH = Path("rss_sources.txt")

def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def ensure_seen_table():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rss_seen (
            hash TEXT PRIMARY KEY,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def already_seen(hv: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM rss_seen WHERE hash=?", (hv,))
    ok = cur.fetchone() is not None
    conn.close()
    return ok

def mark_seen(hv: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO rss_seen(hash) VALUES(?)", (hv,))
    conn.commit()
    conn.close()

def parse_ts(entry):
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
    return datetime.now(timezone.utc).isoformat()

def insert_signal(ts, origin, title, text, url):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO signals(ts, source, origin, title, text, url, score, color, label)
        VALUES(?,?,?,?,?,?,?,?,?)
    """, (ts, "rss", origin, title, text, url, 0.35, "⚪", "neutral"))
    conn.commit()
    conn.close()

def run():
    if not SOURCES_PATH.exists():
        raise SystemExit("rss_sources.txt not found")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    ensure_seen_table()

    for line in SOURCES_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        feed = feedparser.parse(line)
        origin = getattr(feed, "feed", {}).get("title") or line

        for e in getattr(feed, "entries", []):
            url = e.get("link", "") or ""
            title = e.get("title", "") or ""
            text = (
                (e.get("content") and e.get("content")[0].get("value"))
                or e.get("summary")
                or e.get("description")
                or ""
            )

            if not text or len(text.strip()) < 120:
                continue

            hv = sha(url + "||" + title)
            if already_seen(hv):
                continue

            ts = parse_ts(e)
            insert_signal(ts, origin, title, text, url)
            mark_seen(hv)

if __name__ == "__main__":
    run()
