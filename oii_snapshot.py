#!/usr/bin/env python3
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
import math
from collections import defaultdict

DB_PATH = Path("data/atlas.db")

# whitelist объектов (ядро + песочница)
WHITELIST = {"Akash", "Bittensor", "GAEA", "EigenLayer", "Render"}

# веса OII
W_RISK = 0.55
W_VOL  = 0.30
W_REC  = 0.15

DAYS_DEFAULT = 30

def ensure_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS object_index (
        ts TEXT NOT NULL,
        window_days INTEGER NOT NULL,
        object TEXT NOT NULL,
        n_total INTEGER NOT NULL,
        risk_share REAL NOT NULL,
        vol_norm REAL NOT NULL,
        recency REAL NOT NULL,
        oii REAL NOT NULL,
        PRIMARY KEY (ts, window_days, object)
    )
    """)
    conn.commit()

def to_float(x):
    try:
        return float(x)
    except Exception:
        return 0.0

def main():
    if not DB_PATH.exists():
        print("[oii] atlas.db not found")
        return 0

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_table(conn)

    DAYS = DAYS_DEFAULT
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=DAYS)).replace(microsecond=0).isoformat()

    # вытянуть сигналы по whitelist
    rows = conn.execute("""
        SELECT object, color, score, ts
        FROM signals
        WHERE ts >= ?
          AND COALESCE(object,'') != ''
    """, (since,)).fetchall()

    # агрегаты
    per_obj = defaultdict(list)      # (score, ts)
    risk_cnt = defaultdict(int)
    total_cnt = defaultdict(int)

    for r in rows:
        obj = (r["object"] or "").strip()
        if obj not in WHITELIST:
            continue
        sc = to_float(r["score"])
        ts = r["ts"] or ""
        per_obj[obj].append((sc, ts))
        total_cnt[obj] += 1
        if (r["color"] or "") == "🔴":
            risk_cnt[obj] += 1

    # vol: дисперсия score по объекту (стд)
    vol = {}
    for obj, items in per_obj.items():
        vals = [x[0] for x in items]
        if not vals:
            vol[obj] = 0.0
            continue
        m = sum(vals) / len(vals)
        var = sum((v - m) ** 2 for v in vals) / len(vals)
        vol[obj] = math.sqrt(var)

    max_vol = max(vol.values()) if vol else 1.0
    if max_vol == 0:
        max_vol = 1.0

    # recency: доля событий за последние 3 дня
    since3 = (now - timedelta(days=3)).replace(microsecond=0).isoformat()
    last3 = conn.execute("""
        SELECT object, COUNT(*) c
        FROM signals
        WHERE ts >= ?
          AND COALESCE(object,'') != ''
        GROUP BY object
    """, (since3,)).fetchall()
    last3_cnt = { (r["object"] or "").strip(): int(r["c"]) for r in last3 }

    ts_snapshot = now.replace(microsecond=0).isoformat()

    out = []
    for obj, n_total in total_cnt.items():
        n_risk = risk_cnt.get(obj, 0)
        risk_share = (n_risk / n_total) if n_total else 0.0
        vol_norm = (vol.get(obj, 0.0) / max_vol)
        recency = (last3_cnt.get(obj, 0) / n_total) if n_total else 0.0
        oii = W_RISK * risk_share + W_VOL * vol_norm + W_REC * recency
        out.append((ts_snapshot, DAYS, obj, n_total, risk_share, vol_norm, recency, oii))

    conn.executemany("""
        INSERT OR REPLACE INTO object_index
        (ts, window_days, object, n_total, risk_share, vol_norm, recency, oii)
        VALUES (?,?,?,?,?,?,?,?)
    """, out)
    conn.commit()

    out_sorted = sorted(out, key=lambda x: x[-1], reverse=True)
    print(f"[oii] snapshot={ts_snapshot} window={DAYS}d objects={len(out_sorted)}")
    for ts, wd, obj, n, rs, vn, rc, oii in out_sorted[:10]:
        print(f"  {obj:12s} oii={oii:.3f} n={n:4d} risk={rs:.2f} vol={vn:.2f} rec={rc:.2f}")

    conn.close()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
