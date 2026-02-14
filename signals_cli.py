\
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from signals import ingest, sample_items

DB_PATH = Path("data/atlas.db")

def cmd_ingest(args):
    if args.source == "sample":
        stats = ingest(sample_items(args.n))
        print(f"OK: ingest sample -> inserted={stats['inserted']} ignored={stats['ignored']}")
        return
    raise SystemExit(f"Unknown source: {args.source}")

def cmd_stats(_args):
    con = sqlite3.connect(str(DB_PATH))
    cur = con.cursor()
    cur.execute("""
        SELECT
          COUNT(*) AS total,
          COUNT(DISTINCT url) AS uniq_url
        FROM signals
        WHERE url IS NOT NULL AND url != '';
    """)
    total, uniq_url = cur.fetchone()
    print(f"signals: total={total} uniq_url={uniq_url}")
    con.close()

def main():
    p = argparse.ArgumentParser(prog="signals_cli")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_ing = sub.add_parser("ingest", help="Ingest signals from a source")
    p_ing.add_argument("source", choices=["sample"])
    p_ing.add_argument("--n", type=int, default=5)
    p_ing.set_defaults(func=cmd_ingest)

    p_stats = sub.add_parser("stats", help="Show signals stats")
    p_stats.set_defaults(func=cmd_stats)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
