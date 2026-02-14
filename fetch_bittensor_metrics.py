#!/usr/bin/env python3

"""
Сбор базовых метрик из сети Bittensor через SubtensorAPI.

Этот скрипт использует Bittensor SDK для получения номера текущего блока,
количества активных механизмов и распределения эмиссии для указанного
сабнета (netuid). Полученные данные сохраняются в таблицу `signals`
используя функцию `save_signal()` из вашего модуля signals. Уровень
сигнала по умолчанию — 🟡, горизонт — T2.
"""
from __future__ import annotations
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
ATLAS_METRICS_STEP = 10  # save metrics every N blocks

import os
import json
from typing import Any, Dict

try:
    import bittensor as bt
except ImportError as e:
    raise SystemExit(
        "Ошибка: пакет 'bittensor' не установлен. Установите его с помощью 'python3 -m pip install bittensor'."
    ) from e

try:
    # Предполагается, что signals.py находится в PYTHONPATH или в том же каталоге
    from storage import save_signal
except ImportError:
    raise SystemExit(
        "Ошибка: модуль 'signals' не найден. Убедитесь, что файл signals.py находится рядом со скриптом или добавлен в PYTHONPATH."
    )

def fetch_bittensor_metrics(netuid: int = 1) -> Dict[str, Any]:
    """Получает метрики из сети Bittensor."""
    sub = bt.SubtensorApi()
    metrics: Dict[str, Any] = {}
    try:
        current_block: int = sub.block  # текущий номер блока
        metrics["block"] = current_block
    except Exception as e:
        metrics["block_error"] = str(e)
    try:
        mech_count: int = sub.subnets.get_mechanism_count(netuid=netuid)
        metrics["mechanism_count"] = mech_count
    except Exception as e:
        metrics["mechanism_count_error"] = str(e)
    try:
        split = sub.subnets.get_mechanism_emission_split(netuid=netuid)
        total = sum(split) if split else 0
        emissions = [round(x / total, 4) for x in split] if total else []
        metrics["emission_split"] = emissions
    except Exception as e:
        metrics["emission_split_error"] = str(e)
    return metrics


# --- ATLAS PATCH: metrics -> SQLite ---
import sqlite3
from datetime import datetime, timezone

def _atlas_db_path() -> str:
    from pathlib import Path as _P
    return str(_P(__file__).resolve().parent / "data" / "atlas.db")

def _insert_signal_row(row: dict) -> None:
    db = _atlas_db_path()
    conn = sqlite3.connect(db)
    try:
        cur = conn.cursor()
        info = cur.execute("PRAGMA table_info(signals)").fetchall()
        cols = [r[1] for r in info]
        notnull = {r[1]: r[3] for r in info}  # 1 if NOT NULL
        dflt = {r[1]: r[4] for r in info}     # default value (SQL literal or None)

        # meta -> json string if column exists
        if "meta" in cols and "meta" in row and not isinstance(row["meta"], str):
            row["meta"] = json.dumps(row["meta"], ensure_ascii=False)

        base_ts = row.get("ts") or datetime.now(timezone.utc).isoformat()
        base_source = row.get("source") or row.get("origin") or row.get("project") or "bittensor"
        base_label  = row.get("label")  or row.get("project") or base_source
        base_title  = row.get("title")  or base_label
        base_text   = row.get("text")   or ""
        base_url    = row.get("url")    or ""

        # stable defaults if schema has these columns
        defaults = {
            "ts": base_ts,
            "source": base_source,
            "label": base_label,
            "title": base_title,
            "text": base_text,
            "summary": base_text or base_title,
            "origin": row.get("origin") or "bittensor",
            "project": row.get("project") or "bittensor",
            "kind": row.get("kind") or "metric",
            "horizon": row.get("horizon") or "T2",
            "sentiment": row.get("sentiment") or "neutral",
            "score": row.get("score") if row.get("score") is not None else 0.35,
            "url": base_url,
            "color": row.get("color") or "neutral",
            "level": row.get("level") or "neutral",
        }
        for k, v in defaults.items():
            if k in cols and k not in row:
                row[k] = v

        # If still missing NOT NULL columns: fill safe placeholder / default
        for c in cols:
            if c == "id":
                continue
            if notnull.get(c, 0) == 1 and (c not in row or row[c] is None):
                if dflt.get(c) is not None:
                    row[c] = str(dflt[c]).strip("'")
                else:
                    if c == "ts":
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

        # IMPORTANT: dedup-friendly
        sql = "INSERT OR IGNORE INTO signals ({}) VALUES ({})".format(
            ",".join(insert_cols),
            ",".join(["?"] * len(insert_cols)),
        )
        cur.execute(sql, [row[c] for c in insert_cols])
        conn.commit()
    finally:
        conn.close()

def save_bittensor_metrics_sqlite(metrics, netuid: int = 1) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    title = f"Bittensor metrics (netuid={netuid})"
    text = json.dumps(metrics, ensure_ascii=False, indent=2)

    row = {
        "ts": ts,
        "origin": "bittensor",
        "project": "bittensor",
        "kind": "metric",
        "title": title,
        # signature url to dedup
        "url": f"bt://metrics/netuid={netuid}/block={metrics.get('block','')}",
        "text": text,
        "meta": {"netuid": netuid, "metrics": metrics},
        "sentiment": "neutral",
        "score": 0.35,
        "color": "neutral",
        "label": "bittensor",
        "source": "bittensor",
        "horizon": "T2",
    }

    # dedup: do not write metrics more often than once per 10 minutes
    try:
        import sqlite3
        db = _atlas_db_path()
        con = sqlite3.connect(db)
        cur = con.cursor()
        cur.execute("""
            SELECT 1
            FROM signals
            WHERE url = ?
              AND ts >= datetime('now','-10 minutes')
            LIMIT 1
        """, (row.get("url",""),))
        if cur.fetchone():
            print("SKIP: metrics dedup (last 10 min)")
            return
    finally:
        try: con.close()
        except: pass

    # dedup: do not write metrics more often than once per 10 minutes
    try:
        import sqlite3
        db = _atlas_db_path()
        con = sqlite3.connect(db)
        cur = con.cursor()
        cur.execute("""
            SELECT 1
            FROM signals
            WHERE url = ?
              AND ts >= datetime('now','-10 minutes')
            LIMIT 1
        """, (row.get("url",""),))
        if cur.fetchone():
            print("SKIP: metrics dedup (last 10 min)")
            return
    finally:
        try: con.close()
        except: pass

    _insert_signal_row(row)
# --- /ATLAS PATCH ---

def main() -> None:
    netuid = int(os.environ.get("BT_NETUID", 1))
    metrics = fetch_bittensor_metrics(netuid=netuid)
    saved = save_bittensor_metrics_sqlite(metrics, netuid=netuid)
    if saved:
        print(f"Метрики для netuid={netuid} собраны и сохранены:\\n{json.dumps(metrics, ensure_ascii=False, indent=2)}")
    else:
        print(f"Метрики для netuid={netuid} собраны (SKIP save, step {ATLAS_METRICS_STEP}):\n{json.dumps(metrics, ensure_ascii=False, indent=2)}")

if __name__ == "__main__":
    main()
