#!/usr/bin/env python3
import subprocess, sys, os, glob
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def run(cmd):
    print(f"[run] {' '.join(cmd)}", flush=True)
    r = subprocess.call(cmd, cwd=str(ROOT))
    if r != 0:
        raise SystemExit(r)
    return r
def main():
    # Ensure DB schema exists
    try:
        import storage
        storage.init_db()
    except Exception as e:
        print(f"[warn] init_db failed: {e}")
    py = sys.executable or "python3"

    # 1) GAEA (если есть)
    if (ROOT / "gaea_fetch.py").exists():
        run([py, "-u", "gaea_fetch.py"])
    else:
        print("[skip] gaea_fetch.py not found")

    # 2) Автопоиск остальных fetch-скриптов (кроме dashboard и oii)
    candidates = []
    for pat in ("fetch_*.py", "*_fetch.py", "rss_*.py"):
        candidates += glob.glob(str(ROOT / pat))

    # убрать дубликаты / мусор
    bad = {"dashboard.py", "gaea_fetch.py", "oii_snapshot.py", "run_all.py"}
    uniq = []
    for p in sorted(set(map(lambda x: Path(x).name, candidates))):
        if p in bad:
            continue
        # не запускаем явно UI/сервисы
        if "dashboard" in p.lower():
            continue
        uniq.append(p)

    for p in uniq:
        # запускаем только если файл реально существует
        if (ROOT / p).exists():
            run([py, p])

    # 3) Пересчёт OII
    if (ROOT / "oii_snapshot.py").exists():
        run([py, "-u", "oii_snapshot.py"])
    else:
        print("[skip] oii_snapshot.py not found")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
