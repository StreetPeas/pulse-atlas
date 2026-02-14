#!/usr/bin/env python3
from __future__ import annotations

import time
import sqlite3
from pathlib import Path

import requests
import feedparser

import storage

HN_RSS = "https://news.ycombinator.com/rss"


def read_sources(p: Path) -> list[str]:
    if not p.exists():
        return []
    out: list[str] = []
    for ln in p.read_text(encoding="utf-8").splitlines():
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    # unique preserve order
    seen = set()
    uniq = []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def fetch_url(url: str, timeout: int = 25) -> str:
    last = None
    for attempt in range(4):
        try:
            r = requests.get(
                url,
                timeout=timeout,
                headers={"User-Agent": "atlas/1.0 (+rss)"},
            )
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise last  # type: ignore[misc]


def normalize_entry(src: str, e) -> tuple[str, str, str, str]:
    src = (src or "").strip()
    ext = getattr(e, "link", "") or ""
    title = getattr(e, "title", "") or ""
    summary = getattr(e, "summary", "") or getattr(e, "description", "") or ""

    # HN: url = внешний линк (как ты уже решил), raw = HN item URL (comments)
    if src == HN_RSS:
        hn = getattr(e, "comments", "") or ""
        return ext, title, summary, hn

    raw = str(getattr(e, "published", "") or getattr(e, "updated", "") or "")
    return ext, title, summary, raw


def ensure_columns(con: sqlite3.Connection) -> None:
    # безопасно: если колонки уже есть — ничего не делаем
    need = {"summary": "TEXT", "raw": "TEXT"}
    cur = con.cursor()
    cur.execute("PRAGMA table_info(signals)")
    cols = {r[1] for r in cur.fetchall()}
    for c, t in need.items():
        if c not in cols:
            cur.execute(f"ALTER TABLE signals ADD COLUMN {c} {t}")
    con.commit()


def main() -> int:
    storage.init_db()

    sources = read_sources(Path("data/rss_sources.txt"))
    if not sources:
        print("ERR: no sources in data/rss_sources.txt")
        return 2

    # ensure optional cols exist
    con = sqlite3.connect("data/atlas.db")
    try:
        ensure_columns(con)
    finally:
        con.close()

    inserted = 0
    ignored = 0

    for src in sources:
        try:
            xml = fetch_url(src)
            feed = feedparser.parse(xml)

            for e in feed.entries[:50]:
                url, title, summary, raw = normalize_entry(src, e)
                if not url:
                    continue

                rc = storage.save_signal(
                    {
                        "source": src,
                        "title": title,
                        "url": url,
                        "summary": summary,
                        "raw": raw,
                        "text": (summary or title or ""),
                    }
                )
                if rc == 1:
                    inserted += 1
                else:
                    ignored += 1

        except Exception as ex:
            print(f"WARN: source failed: {src} :: {ex}")

    print(f"OK: rss_fetch inserted={inserted} ignored={ignored}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
