#!/usr/bin/env python3
import json
import re
import sqlite3
from datetime import datetime, timezone
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

DB_PATH = "data/atlas.db"
UA = "PulseAtlas/0.1 (+local)"

# Sources
GAEA_GITHUB_ORG = "aigaea"
GAEA_MEDIUM_FEED = "https://medium.com/feed/@aigaea3"
GAEA_SITE_PAGES = [
    "https://app.aigaea.net/missions/",
    "https://app.aigaea.net/engine/?view=whitepaper",
    "https://aigaea.net/engine/",
]

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def http_get(url: str, timeout=20) -> str:
    req = Request(url, headers={"User-Agent": UA})
    with urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")

def ensure_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cursors(
        source TEXT PRIMARY KEY,
        cursor TEXT
    )
    """)
    conn.commit()

def get_cursor(conn, source: str):
    row = conn.execute("SELECT cursor FROM cursors WHERE source=?", (source,)).fetchone()
    return row[0] if row else None

def set_cursor(conn, source: str, cursor: str):
    conn.execute(
        "INSERT INTO cursors(source,cursor) VALUES(?,?) "
        "ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor",
        (source, cursor)
    )
    conn.commit()

def _signals_schema(conn):
    cols = []
    notnull = set()
    for cid, name, ctype, nn, dflt, pk in conn.execute("PRAGMA table_info(signals)"):
        cols.append(name)
        if nn == 1:
            notnull.add(name)
    return set(cols), notnull

def _pick_col(cols, *names):
    for n in names:
        if n in cols:
            return n
    return None

def signal_exists(conn, url: str) -> bool:
    cols, _ = _signals_schema(conn)
    key = _pick_col(cols, "url", "link", "source_url", "href")
    if not key:
        return False
    row = conn.execute(f"SELECT 1 FROM signals WHERE {key}=? LIMIT 1", (url,)).fetchone()
    return bool(row)

def insert_signal(conn, payload: dict):
    cols, notnull = _signals_schema(conn)

    url_col  = _pick_col(cols, "url", "link", "source_url", "href")
    body_col = _pick_col(cols, "body", "content", "description", "summary")
    text_col = _pick_col(cols, "text")  # your DB has NOT NULL text

    # map url into actual url column if needed
    if url_col and "url" in payload and url_col != "url":
        payload[url_col] = payload["url"]

    # JSON stringify if those cols exist
    for k in ("meta", "raw"):
        if k in cols:
            v = payload.get(k)
            if isinstance(v, (dict, list)):
                payload[k] = json.dumps(v, ensure_ascii=False)
            elif v is None:
                payload[k] = json.dumps({}, ensure_ascii=False)

    # Ensure text not null
    if text_col and (payload.get(text_col) is None):
        payload[text_col] = payload.get("body") or payload.get("title") or ""

    # Ensure body-ish column
    if body_col and (payload.get(body_col) is None):
        payload[body_col] = payload.get("body") or ""

    # Fill all NOT NULL columns minimally
    for k in notnull:
        if k not in cols:
            continue
        if payload.get(k) is None:
            payload[k] = ""
    if text_col and payload.get(text_col) == "":
        payload[text_col] = payload.get("body") or payload.get("title") or ""

    # Filter to existing columns
    out_cols, out_vals = [], []
    for k, v in payload.items():
        if k in cols:
            out_cols.append(k)
            out_vals.append(v)

    sql = f"INSERT INTO signals ({','.join(out_cols)}) VALUES ({','.join(['?']*len(out_cols))})"
    conn.execute(sql, out_vals)
    conn.commit()

def store(conn, *, source: str, kind: str, title: str, url: str, body: str = "", meta=None, raw=None, ts=None):
    if not ts:
        ts = now_iso()
    if signal_exists(conn, url):
        return False

    payload = {
        "ts": ts,
        "object": "GAEA",
        "source": source,
        "kind": kind,
        "title": title,
        "url": url,
        "body": body or "",
        "meta": meta or {},
        "raw": raw or {},
    }
    insert_signal(conn, payload)
    return True

# ---------------- GitHub ----------------
def fetch_github(conn):
    source = "gaea/github"
    last = get_cursor(conn, source)  # ISO timestamp
    api_repos = f"https://api.github.com/orgs/{GAEA_GITHUB_ORG}/repos?per_page=100"
    try:
        repos = json.loads(http_get(api_repos))
    except Exception as e:
        print(f"[gaea/github] failed repos: {e}")
        return 0

    new_count = 0
    newest = last

    for repo in repos:
        name = repo.get("name")
        if not name:
            continue
        api_rel = f"https://api.github.com/repos/{GAEA_GITHUB_ORG}/{name}/releases?per_page=20"
        try:
            rels = json.loads(http_get(api_rel))
        except Exception:
            continue
        for r in rels:
            html_url = r.get("html_url")
            tag = r.get("tag_name") or ""
            pub = r.get("published_at") or ""
            if last and pub and pub <= last:
                continue
            if html_url:
                ok = store(
                    conn,
                    source=source,
                    kind="release",
                    title=f"{name}: {tag}".strip(": "),
                    url=html_url,
                    body=(r.get("body") or "")[:4000],
                    meta={"repo": name, "tag": tag, "prerelease": bool(r.get("prerelease")), "draft": bool(r.get("draft"))},
                    raw=r,
                    ts=pub or now_iso(),
                )
                if ok:
                    new_count += 1
            if pub and ((not newest) or (pub > newest)):
                newest = pub

    if newest and newest != last:
        set_cursor(conn, source, newest)
    print(f"[gaea/github] +{new_count} cursor={newest}")
    return new_count

# ---------------- Medium RSS ----------------
def parse_rss_items(xml_text: str):
    root = ET.fromstring(xml_text)
    channel = root.find("channel")
    if channel is None:
        return []
    items = []
    for it in channel.findall("item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        pub = (it.findtext("pubDate") or "").strip()
        desc = (it.findtext("description") or "").strip()
        items.append({"title": title, "link": link, "pubDate": pub, "description": desc})
    return items

def fetch_medium(conn):
    source = "gaea/medium"
    last = get_cursor(conn, source)  # last link
    try:
        xml = http_get(GAEA_MEDIUM_FEED)
        items = parse_rss_items(xml)
        print(f"[gaea/medium] items={len(items)}")
    except Exception as e:
        print(f"[gaea/medium] failed: {e}")
        return 0

    new_count = 0

    for it in items[:30]:
        link = it.get("link") or ""
        title = it.get("title") or "Medium post"
        body = re.sub(r"<[^>]+>", " ", it.get("description") or "")
        body = re.sub(r"\s+", " ", body).strip()
        if not link:
            continue
        if last and link == last:
            break
        ok = store(
            conn,
            source=source,
            kind="post",
            title=title[:300],
            url=link,
            body=body[:4000],
            meta={"feed": GAEA_MEDIUM_FEED},
            raw=it,
            ts=now_iso(),
        )
        if ok:
            new_count += 1

    if items:
        set_cursor(conn, source, items[0].get("link") or "")
    print(f"[gaea/medium] +{new_count}")
    return new_count

# ---------------- Site pages change detect ----------------
def simple_hash(text: str) -> str:
    import hashlib
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

def fetch_site_pages(conn):
    source = "gaea/site"
    total_new = 0
    for url in GAEA_SITE_PAGES:
        key = f"{source}:{url}"
        last = get_cursor(conn, key)
        try:
            html = http_get(url)
        except Exception as e:
            print(f"[gaea/site] failed {url}: {e}")
            continue
        h = simple_hash(html)
        if last and h == last:
            continue
        ok = store(
            conn,
            source=source,
            kind="page_update",
            title=f"GAEA page update: {url}",
            url=url,
            body="",
            meta={"hash": h},
            raw={"url": url, "hash": h},
            ts=now_iso(),
        )
        if ok:
            total_new += 1
        set_cursor(conn, key, h)

    print(f"[gaea/site] +{total_new}")
    return total_new

def fetch_x_hook(conn):
    print("[gaea/x] hook ready (not fetching yet)")
    return 0

def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)

    total = 0
    total += fetch_github(conn)
    total += fetch_medium(conn)
    total += fetch_site_pages(conn)
    total += fetch_x_hook(conn)

    conn.close()
    print(f"[gaea_fetch] done. new={total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
