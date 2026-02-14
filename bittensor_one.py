#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

ATOM_FEEDS = {
    "Bittensor Releases": "https://github.com/opentensor/bittensor/releases.atom",
    "Subtensor Releases": "https://github.com/opentensor/subtensor/releases.atom",
}

def repo_root() -> str:
    return os.path.dirname(os.path.abspath(__file__))

def db_path() -> str:
    return os.path.join(repo_root(), "data", "atlas.db")

def utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def short(s: str, n: int = 1200) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1].rstrip() + "…"

def parse_atom_feed(url: str) -> List[Dict[str, Any]]:
    # без зависимостей
    import urllib.request
    import xml.etree.ElementTree as ET

    req = urllib.request.Request(url, headers={"User-Agent": "atlas-bittensor-one"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        xml = resp.read()

    root = ET.fromstring(xml)
    ns = {"a": "http://www.w3.org/2005/Atom"}

    out = []
    for entry in root.findall("a:entry", ns):
        title = (entry.findtext("a:title", default="", namespaces=ns) or "").strip()
        published = (entry.findtext("a:published", default="", namespaces=ns) or "").strip()
        updated = (entry.findtext("a:updated", default="", namespaces=ns) or "").strip()
        summary = (entry.findtext("a:content", default="", namespaces=ns) or
                   entry.findtext("a:summary", default="", namespaces=ns) or "").strip()

        link = ""
        for l in entry.findall("a:link", ns):
            if l.get("rel") in (None, "", "alternate"):
                link = l.get("href") or ""
                break

        ts = published or updated or utc_iso()
        out.append({"title": title, "link": link, "published": ts, "summary": summary})
    return out

def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    rows = cur.fetchall()
    return [r[1] for r in rows]

def pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    m = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in m:
            return m[cand.lower()]
    return None

def resolve_cols(cols: List[str]) -> Dict[str, Optional[str]]:
    return {
        "ts": pick_col(cols, ["ts", "timestamp", "created_at", "created", "time", "dt"]),
        "origin": pick_col(cols, ["origin", "source_name", "provider", "feed", "channel"]),
        "kind": pick_col(cols, ["kind", "type", "category"]),
        "level": pick_col(cols, ["level", "severity", "color"]),
        "horizon": pick_col(cols, ["horizon", "t_horizon", "thorizon"]),
        "title": pick_col(cols, ["title", "headline", "name"]),
        "text": pick_col(cols, ["text", "body", "content", "details", "summary", "message"]),
        "url": pick_col(cols, ["url", "link", "source_url"]),
        "hash": pick_col(cols, ["hash", "hv", "fingerprint"]),
        "project": pick_col(cols, ["project", "asset", "topic", "entity"]),
        "source": pick_col(cols, ["source", "origin_type"]),
        "meta": pick_col(cols, ["meta", "metadata", "json", "extra"]),
        # ВАЖНО: у тебя score NOT NULL
        "sentiment": pick_col(cols, ["sentiment", "label", "tone"]),
        "score": pick_col(cols, ["score"]),
    }

def already_in_db(conn: sqlite3.Connection, mapping: Dict[str, Optional[str]], url: str, hv: str) -> bool:
    if mapping.get("url"):
        col = mapping["url"]
        cur = conn.execute(f"SELECT 1 FROM signals WHERE {col} = ? LIMIT 1;", (url,))
        if cur.fetchone():
            return True
    if mapping.get("hash"):
        col = mapping["hash"]
        cur = conn.execute(f"SELECT 1 FROM signals WHERE {col} = ? LIMIT 1;", (hv,))
        if cur.fetchone():
            return True
    return False

def insert_signal(
    conn: sqlite3.Connection,
    mapping: Dict[str, Optional[str]],
    *,
    ts: str,
    origin: str,
    title: str,
    text: str,
    url: str,
    project: str = "bittensor",
    kind: str = "rss",
    level: str = "🟡",
    horizon: str = "T1",
    meta: Optional[Dict[str, Any]] = None,
    sentiment: str = "neutral",
    score: float = 0.35,
) -> int:
    meta = meta or {}
    values: Dict[str, Any] = {}

    if mapping.get("ts"): values[mapping["ts"]] = ts
    if mapping.get("origin"): values[mapping["origin"]] = origin
    if mapping.get("title"): values[mapping["title"]] = title
    if mapping.get("text"): values[mapping["text"]] = text
    if mapping.get("url"): values[mapping["url"]] = url
    if mapping.get("project"): values[mapping["project"]] = project
    if mapping.get("kind"): values[mapping["kind"]] = kind
    if mapping.get("source"): values[mapping["source"]] = "rss"
    if mapping.get("level"): values[mapping["level"]] = level
    if mapping.get("horizon"): values[mapping["horizon"]] = horizon
    if mapping.get("hash"): values[mapping["hash"]] = sha(url + "||" + title)
    if mapping.get("meta") and meta:
        values[mapping["meta"]] = json.dumps(meta, ensure_ascii=False)

    # КРИТИЧЕСКОЕ: обеспечить NOT NULL поля
    if mapping.get("sentiment"):
        values[mapping["sentiment"]] = sentiment
    if mapping.get("score"):
        values[mapping["score"]] = float(score)

    cols_sql = ", ".join(values.keys())
    ph = ", ".join(["?"] * len(values))
    sql = f"INSERT INTO signals ({cols_sql}) VALUES ({ph});"
    cur = conn.execute(sql, list(values.values()))
    conn.commit()
    return int(cur.lastrowid)

def build_text(origin: str, title: str, summary: str, url: str, published: str) -> str:
    base = f"{origin}\n{title}\n{published}\n{url}\n\n{summary}".strip()
    if len(base) < 140:
        base += "\n\nСобытие: релиз/изменение. Сигнал оценивается по структуре и последствиям во времени."
    return short(base, 3000)

def level_horizon_from_text(text: str) -> Tuple[str, str]:
    t = text.lower()
    red_keys = ["security", "vulnerability", "cve", "critical", "fork", "consensus", "breaking", "exploit"]
    if any(k in t for k in red_keys):
        return ("🔴", "T0")
    return ("🟡", "T1")

def show_tail(conn: sqlite3.Connection, mapping: Dict[str, Optional[str]], limit: int = 12) -> None:
    ts_col = mapping.get("ts") or "rowid"
    origin_col = mapping.get("origin")
    title_col = mapping.get("title")
    url_col = mapping.get("url")
    score_col = mapping.get("score")
    sent_col = mapping.get("sentiment")

    where = ""
    params: List[Any] = []
    if origin_col:
        where = f"WHERE {origin_col} IN (?, ?)"
        params = ["Bittensor Releases", "Subtensor Releases"]

    sql = f"SELECT * FROM signals {where} ORDER BY {ts_col} DESC LIMIT ?;"
    params.append(limit)
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    print("\nХвост сигналов (Bittensor/Subtensor):")
    for r in rows:
        rd = dict(zip([c[0] for c in cur.description], r))
        ts = rd.get(ts_col, "")
        origin = rd.get(origin_col, "") if origin_col else ""
        title = rd.get(title_col, "") if title_col else ""
        url = rd.get(url_col, "") if url_col else ""
        sent = rd.get(sent_col, "") if sent_col else ""
        sc = rd.get(score_col, "") if score_col else ""
        tail = f"{sent} {sc}".strip()
        print(f"- {ts} | {tail} | {origin} | {title}")
        if url:
            print(f"  {url}")

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--analyze", action="store_true")
    ap.add_argument("--pulse", action="store_true")
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--limit", type=int, default=12)
    args = ap.parse_args()

    path = db_path()
    if not os.path.exists(path):
        print(f"ОШИБКА: не найден atlas.db: {path}")
        sys.exit(1)

    conn = sqlite3.connect(path)
    try:
        cols = table_columns(conn, "signals")
        if not cols:
            print("ОШИБКА: таблица signals не найдена. Запусти init_db().")
            sys.exit(1)
        mapping = resolve_cols(cols)

        inserted = 0
        for origin, feed_url in ATOM_FEEDS.items():
            entries = parse_atom_feed(feed_url)
            for e in entries:
                title = (e.get("title") or "").strip()
                url = (e.get("link") or "").strip()
                published = (e.get("published") or utc_iso()).strip()
                summary = (e.get("summary") or "").strip()
                if not title or not url:
                    continue
                hv = sha(url + "||" + title)
                if already_in_db(conn, mapping, url, hv):
                    continue
                text = build_text(origin, title, summary, url, published)
                lvl, hz = level_horizon_from_text(text)
                insert_signal(
                    conn, mapping,
                    ts=published,
                    origin=origin,
                    title=title,
                    text=text,
                    url=url,
                    project="bittensor",
                    kind="rss",
                    level=lvl,
                    horizon=hz,
                    meta={"feed": feed_url},
                    sentiment="neutral",
                    score=0.35,
                )
                inserted += 1
            time.sleep(0.2)

        print(f"OK: вставлено новых сигналов: {inserted}")

        if args.analyze and os.path.exists(os.path.join(repo_root(), "analyze_signal.py")):
            print("-> analyze_signal.py")
            subprocess.run([sys.executable, "analyze_signal.py"], check=False)

        if args.pulse and os.path.exists(os.path.join(repo_root(), "atlas")):
            print("-> ./atlas пульс")
            subprocess.run(["./atlas", "пульс"], check=False)

        if args.show:
            show_tail(conn, mapping, limit=args.limit)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
