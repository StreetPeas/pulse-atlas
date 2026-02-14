import sqlite3
from pathlib import Path

DB_PATH = Path("data/atlas.db")

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS journal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project TEXT,
        note TEXT,
        signal TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        source TEXT NOT NULL,
        origin TEXT,
        title TEXT,
        text TEXT NOT NULL,
        url TEXT,
        tags TEXT,
        score REAL NOT NULL,
        color TEXT NOT NULL,
        label TEXT NOT NULL,
        rationale TEXT
    )
    """)
    conn.commit()
    conn.close()

def save_entry(project: str, note: str, signal: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO journal (project, note, signal) VALUES (?, ?, ?)",
        (project, note, signal)
    )
    conn.commit()
    conn.close()
def save_signal(
    ts: str,
    source: str,
    origin: str | None,
    title: str | None,
    text: str,
    url: str | None,
    tags: str | None,
    score: float,
    color: str,
    label: str,
    rationale: str | None,
):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """INSERT OR IGNORE INTO signals(ts, source, origin, title, text, url, tags, score, color, label, rationale)
           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (ts, source, origin, title, text, url, tags, score, color, label, rationale),
    )
    conn.commit()
    conn.close()


# --- ATLAS V2 STORAGE PATCH (dedup + ignore) ---
# Цель:
# 1) Глобальный дедуп по url (UNIQUE INDEX + INSERT OR IGNORE)
# 2) Не падать на дублях / NOT NULL
# 3) Совместимость: принимать любые kwargs (project/origin/source/label/color/...)

import sqlite3
import json
from datetime import datetime, timezone

# сохраним старые функции, если они уже были определены выше
_try_old_init_db = globals().get("init_db")
_try_old_atlas_db_path = globals().get("_atlas_db_path")

def _atlas_db_path_v2() -> str:
    if callable(_try_old_atlas_db_path):
        return _try_old_atlas_db_path()
    from pathlib import Path as _P
    return str(_P(__file__).resolve().parent / "data" / "atlas.db")

def init_db():
    # вызвать старый init_db (если был)
    if callable(_try_old_init_db):
        _try_old_init_db()

    # гарантировать индекс дедупа
    db = _atlas_db_path_v2()
    conn = sqlite3.connect(db)
    try:
        cur = conn.cursor()
        # частичный UNIQUE по url (только непустые)
        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_source_url_unique
        ON signals(source, url)
        WHERE source IS NOT NULL AND source != ''
          AND url IS NOT NULL AND url != '';
        """)
        conn.commit()
    finally:
        conn.close()

def save_signal(row: dict | None = None, **kwargs) -> int:
    """
    Универсальный сохранитель сигнала.
    Возвращает: 1 если вставлено, 0 если проигнорировано (дубль/конфликт).
    """
    if row is None:
        row = {}
    if kwargs:
        row.update(kwargs)

    db = _atlas_db_path_v2()
    conn = sqlite3.connect(db)
    try:
        cur = conn.cursor()

        info = cur.execute("PRAGMA table_info(signals)").fetchall()
        cols = [r[1] for r in info]
        notnull = {r[1]: r[3] for r in info}   # 1 if NOT NULL
        dflt = {r[1]: r[4] for r in info}      # default value (SQL literal) or None

        # базовые значения
        base_ts = row.get("ts") or row.get("created_at") or datetime.now(timezone.utc).isoformat()
        base_source = (row.get("source") or "").strip()  # keep exact source, no fallback
        if not base_source:
            base_source = "atlas"

        base_label = row.get("label") or row.get("project") or base_source
        base_title = row.get("title") or base_label
        base_text = row.get("text") or ""
        base_url = row.get("url") or ""

        # meta -> json
        if "meta" in cols and "meta" in row and not isinstance(row["meta"], str):
            row["meta"] = json.dumps(row["meta"], ensure_ascii=False)

        # минимальные дефолты по наиболее частым полям
        defaults = {
            "ts": base_ts,
            "created_at": base_ts,
            "source": base_source,
            "origin": row.get("origin") or base_source,
            "project": row.get("project") or base_label,
            "label": base_label,
            "title": base_title,
            "text": base_text,
            "summary": row.get("summary") or (base_text or base_title),
            "url": base_url,
            "kind": row.get("kind") or "event",
            "horizon": row.get("horizon") or "T2",
            "sentiment": row.get("sentiment") or "neutral",
            "score": row.get("score") if row.get("score") is not None else 0.35,
            # color: строкой (у тебя в БД так и хранится)
            "color": row.get("color") or row.get("level") or "neutral",
            "level": row.get("level") or "neutral",
        }
        for k, v in defaults.items():
            if k in cols and k not in row:
                row[k] = v

        # если какие-то NOT NULL поля всё ещё пустые — затычки
        for c in cols:
            if c == "id":
                continue
            if notnull.get(c, 0) == 1 and (c not in row or row[c] is None):
                if dflt.get(c) is not None:
                    row[c] = str(dflt[c]).strip("'")
                else:
                    if c in ("ts", "created_at"):
                        row[c] = base_ts
                    elif c == "label":
                        row[c] = base_label
                    elif c == "source":
                        row[c] = base_source
                    elif c == "title":
                        row[c] = base_title
                    elif c in ("text", "summary", "url"):
                        row[c] = ""
                    elif c == "score":
                        row[c] = 0.0
                    else:
                        row[c] = ""

        insert_cols = [c for c in cols if c != "id" and c in row]
        if not insert_cols:
            raise RuntimeError("signals table: no matching columns to insert")

        sql = "INSERT OR IGNORE INTO signals ({}) VALUES ({})".format(
            ",".join(insert_cols),
            ",".join(["?"] * len(insert_cols)),
        )

        cur.execute(sql, [row[c] for c in insert_cols])
        conn.commit()
        return 1 if cur.rowcount == 1 else 0

    finally:
        conn.close()
# --- /ATLAS V2 STORAGE PATCH ---
