#!/usr/bin/env python3
"""
Akash Network collector for Pulse Atlas
Auto-fetches events from:
- GitHub releases (akash-network)
- RSS/Atom feeds (optional)
Stores normalized events into sqlite signals table via storage.save_signal()
"""

import os
import json
import time
import sqlite3
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from storage import init_db, save_signal

DB_PATH = os.getenv("ATLAS_DB_PATH", "data/atlas.db")

GITHUB_API = "https://api.github.com"
REPO = "akash-network/node"   # основной репозиторий (можно расширить позже)
USER_AGENT = "pulse-atlas/0.1 (+collector; akash)"


def http_get_json(url: str, token: str | None = None, timeout: int = 20):
    headers = {"User-Agent": USER_AGENT, "Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(url, headers=headers)
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def get_last_seen(conn: sqlite3.Connection, source: str) -> str | None:
    # хранение курсора в отдельной табличке (создадим на лету)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS cursors (source TEXT PRIMARY KEY, cursor TEXT, updated_at TEXT)"
    )
    row = conn.execute("SELECT cursor FROM cursors WHERE source = ?", (source,)).fetchone()
    return row[0] if row else None


def set_last_seen(conn: sqlite3.Connection, source: str, cursor: str):
    conn.execute(
        "INSERT INTO cursors(source, cursor, updated_at) VALUES(?,?,?) "
        "ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor, updated_at=excluded.updated_at",
        (source, cursor, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def normalize_release(rel: dict) -> dict:
    published_at = rel.get("published_at") or rel.get("created_at")
    return {
        "kind": "release",
        "title": rel.get("name") or rel.get("tag_name"),
        "url": rel.get("html_url"),
        "ts": published_at,
        "body": (rel.get("body") or "")[:4000],
        "tag": rel.get("tag_name"),
        "prerelease": bool(rel.get("prerelease")),
        "draft": bool(rel.get("draft")),
    }


def fetch_github_releases():
    token = os.getenv("GITHUB_TOKEN")
    url = f"{GITHUB_API}/repos/{REPO}/releases?per_page=10"
    return http_get_json(url, token=token)


def store_event(event: dict, raw: dict, source: str):
    """Store normalized event row into signals table via save_signal()."""
    payload = {
        "ts": event.get("ts"),
        "object": "Akash Network",
        "source": source,              # e.g. "akash/github"
        "kind": event.get("kind"),     # e.g. "release"
        "title": event.get("title"),
        "url": event.get("url"),
        "body": event.get("body", ""),
        "meta": json.dumps({k: event.get(k) for k in ["tag", "prerelease", "draft"] if k in event}, ensure_ascii=False),
        "raw": json.dumps(raw, ensure_ascii=False),
    }
    save_signal(payload)


def main():
    init_db()
    conn = sqlite3.connect(DB_PATH)

    # 1) GitHub releases cursor
    source = "akash/github/releases"
    last = get_last_seen(conn, source)

    try:
        releases = fetch_github_releases()
    except (HTTPError, URLError, TimeoutError) as e:
        print(f"[akash_fetch] error fetching releases: {e}")
        return 2

    # GitHub returns newest first
    new_items = []
    for rel in releases:
        ev = normalize_release(rel)
        # cursor = published timestamp + tag (чтобы устойчиво)
        cursor = f"{ev['ts']}|{ev.get('tag')}"
        if last is None:
            # первый прогон: не заливаем всё подряд — берём только самый новый как baseline
            new_items = [ (cursor, ev, rel) ]
            break
        if cursor == last:
            break
        new_items.append((cursor, ev, rel))

    if not new_items:
        print("[akash_fetch] no new releases")
        return 0

    # сохраняем в хронологическом порядке (старые→новые)
    new_items.reverse()
    newest_cursor = new_items[-1][0]

    for cursor, ev, rel in new_items:
        store_event(ev, rel, source="akash/github")
        print(f"[akash_fetch] stored: {ev['title']} ({ev.get('tag')})")

    set_last_seen(conn, source, newest_cursor)
    print(f"[akash_fetch] cursor updated: {newest_cursor}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
