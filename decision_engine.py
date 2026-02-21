# -*- coding: utf-8 -*-
"""
Pulse Atlas: Decision Engine
- Reads new signals from SQLite (by id cursor)
- Emits actions into actions table (dedup via unique index)
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional

DB = os.path.expanduser("~/Projects/atlas/data/atlas.db")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_state(cur: sqlite3.Cursor, k: str, default: str = "") -> str:
    row = cur.execute("SELECT v FROM engine_state WHERE k=?", (k,)).fetchone()
    return (row[0] if row and row[0] is not None else default) or default


def set_state(cur: sqlite3.Cursor, k: str, v: str) -> None:
    cur.execute(
        "INSERT INTO engine_state(k,v) VALUES(?,?) "
        "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (k, v),
    )


def dedup_key(action_type: str, signal_id: int, url: str) -> str:
    # No f-string tricks, always valid
    u = url or ""
    return action_type + ":" + str(signal_id) + ":" + u


def action_for_signal(row: tuple[Any, ...]) -> Optional[dict[str, Any]]:
    # row: (id, ts, source, url, title, score, color)
    signal_id, ts, source, url, title, score, color = row
    c = (color or "").strip()

    # RED -> investigate
    if c == "🔴":
        return {
            "signal_id": int(signal_id),
            "action_type": "investigate",
            "priority": 90,
            "title": title or "red signal",
            "url": url or "",
            "payload": {"source": source, "color": c, "score": score, "ts": ts},
        }

    # YELLOW -> monitor
    if c == "🟡":
        return {
            "signal_id": int(signal_id),
            "action_type": "monitor",
            "priority": 50,
            "title": title or "yellow signal",
            "url": url or "",
            "payload": {"source": source, "color": c, "score": score, "ts": ts},
        }

    # GREEN -> no action
    return None


def main() -> None:
    con = sqlite3.connect(DB)
    cur = con.cursor()

    last_id = int(get_state(cur, "engine:last_id", "0") or 0)

    rows = cur.execute(
        "SELECT id, ts, source, url, title, score, color "
        "FROM signals WHERE id > ? ORDER BY id ASC",
        (last_id,),
    ).fetchall()

    inserted = 0
    max_id = last_id

    for row in rows:
        sid = int(row[0])
        if sid > max_id:
            max_id = sid

        a = action_for_signal(row)
        if not a:
            continue

        dk = dedup_key(a["action_type"], int(a["signal_id"]), a.get("url", ""))
        try:
            cur.execute(
                "INSERT INTO actions(ts, signal_id, action_type, priority, title, url, payload, status, dedup_key) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    now_utc_iso(),
                    a.get("signal_id"),
                    a["action_type"],
                    int(a.get("priority", 0)),
                    a.get("title", ""),
                    a.get("url", ""),
                    json.dumps(a.get("payload") or {}, ensure_ascii=False),
                    "open",
                    dk,
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            # dedup hit
            pass

    set_state(cur, "engine:last_id", str(max_id))
    con.commit()
    con.close()

    print(f"[ok] db={DB} processed={len(rows)} inserted={inserted} last_id={max_id}")


if __name__ == "__main__":
    main()
