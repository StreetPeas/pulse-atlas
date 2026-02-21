#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

TS = time.strftime("%Y%m%d_%H%M%S")

@dataclass
class CheckResult:
    ok: bool
    title: str
    details: str = ""

def run(cmd: list[str], cwd: Path | None = None, timeout: int | None = None) -> tuple[int, str]:
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        text=True,
    )
    return p.returncode, p.stdout

def backup_file(p: Path) -> Path:
    bak = p.with_name(f"{p.name}.bak.{TS}")
    shutil.copy2(p, bak)
    return bak

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def report_line(s: str) -> str:
    return s.rstrip() + "\n"

def patch_run_all(pyfile: Path) -> tuple[bool, str]:
    """
    Safe patch:
    - make [run] printed with flush=True
    - fail-fast on non-zero
    - add -u to child python calls: run([py, "x.py"]) -> run([py, "-u", "x.py"])
    """
    s = pyfile.read_text(encoding="utf-8")

    changed = False
    out = []

    # 1) run() body patch
    if "flush=True" not in s or "raise SystemExit" not in s:
        # patch only the run() function block (best-effort)
        m = re.search(r"def run\(cmd\):\n(?:(?:    .*\n)|\n)+", s)
        if m:
            old = m.group(0)
            new = (
                "def run(cmd):\n"
                "    print(f\"[run] {' '.join(cmd)}\", flush=True)\n"
                "    r = subprocess.call(cmd, cwd=str(ROOT))\n"
                "    if r != 0:\n"
                "        raise SystemExit(r)\n"
                "    return r\n"
            )
            s = s.replace(old, new, 1)
            changed = True
            out.append("patched run(): flush + fail-fast")
        else:
            out.append("WARN: could not locate def run(cmd) block reliably")

    # 2) add -u to run([py, "file.py"])
    s2, n = re.subn(r"run\(\[py,\s*\"([^\"]+\.py)\"\]\)", r"run([py, \"-u\", \"\1\"])", s)
    if n:
        s = s2
        changed = True
        out.append(f"patched {n} run([py, ...]) calls to add -u")

    if changed:
        pyfile.write_text(s, encoding="utf-8")
    return changed, "; ".join(out) if out else "no changes"

def patch_dashboard(dash: Path) -> tuple[bool, str]:
    """
    Safe patch set:
    - replace use_container_width with width=... (Streamlit deprecation)
    - inject safe_df_for_display (dedupe columns + stringify objects) if missing
    - wrap st.dataframe(df, ...) -> st.dataframe(safe_df_for_display(df), ...) for simple cases
    """
    s = dash.read_text(encoding="utf-8")
    changed = False
    out = []

    # 1) Streamlit API: use_container_width -> width
    if "use_container_width" in s:
        s = s.replace("use_container_width=True", 'width="stretch"')
        s = s.replace("use_container_width=False", 'width="content"')
        changed = True
        out.append("replaced use_container_width -> width")

    # 2) Insert helpers if missing
    if "def safe_df_for_display" not in s:
        helper = """
def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"Make df.columns unique: a, a -> a, a__2 (stable).\"\"\"
    cols = list(df.columns)
    seen = {}
    new_cols = []
    for c in cols:
        c0 = str(c)
        n = seen.get(c0, 0) + 1
        seen[c0] = n
        new_cols.append(c0 if n == 1 else f\"{c0}__{n}\")
    df = df.copy()
    df.columns = new_cols
    return df

def safe_df_for_display(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"PyArrow/Streamlit safe: unique cols + stringified object columns.\"\"\"
    df = dedupe_columns(df)
    df = df.copy()
    for c in df.columns:
        try:
            if df[c].dtype == "object":
                df[c] = df[c].map(lambda v: "" if v is None else str(v))
        except Exception:
            pass
    return df
""".strip() + "\n\n"
        # put after imports block if possible
        m = re.search(r'^(?:import .+\n|from .+ import .+\n)+\n', s, flags=re.M)
        if m:
            s = s[:m.end()] + helper + s[m.end():]
        else:
            s = helper + s
        changed = True
        out.append("inserted safe_df_for_display() helpers")

    # 3) Wrap st.dataframe(<name>, ...) -> safe_df_for_display(<name>)
    def repl(m):
        var = m.group(1)
        return f"st.dataframe(safe_df_for_display({var}),"

    s2, n = re.subn(r"st\.dataframe\(\s*([A-Za-z_]\w*)\s*,", repl, s)
    if n:
        s = s2
        changed = True
        out.append(f"wrapped {n} st.dataframe(...) call(s)")

    if changed:
        dash.write_text(s, encoding="utf-8")
    return changed, "; ".join(out) if out else "no changes"

def write_bash_scripts(root: Path, port: int) -> tuple[bool, str]:
    """
    Ensure stop/start scripts exist and are bash-safe.
    No deletions, only overwrite with backup if exists.
    """
    changed = False
    out = []
    logs = root / "logs"
    ensure_dir(logs)

    stop_pid = root / "stop_dashboard_pid.sh"
    run_bg = root / "run_dashboard_bg.sh"

    stop_pid_content = f"""#!/usr/bin/env bash
set -euo pipefail
PORT="${{1:-{port}}}"

PIDFILE=".streamlit_${{PORT}}.pid"
if [[ -f "$PIDFILE" ]]; then
  PID="$(cat "$PIDFILE" || true)"
  if [[ -n "${{PID:-}}" ]]; then
    echo "Stopping PID=$PID (from pidfile)"
    kill "$PID" || true
    sleep 1
  fi
  rm -f "$PIDFILE"
fi

# fallback: kill whoever listens on port
PID2="$(lsof -tiTCP:${{PORT}} -sTCP:LISTEN | head -n1 || true)"
if [[ -n "${{PID2:-}}" ]]; then
  echo "Force kill PID=$PID2 (port $PORT)"
  kill -9 "$PID2" || true
fi

echo "OK"
"""

    run_bg_content = f"""#!/usr/bin/env bash
set -euo pipefail

PORT="${{1:-{port}}}"
LOGDIR="logs"
mkdir -p "$LOGDIR"
LOG="${{LOGDIR}}/streamlit_${{PORT}}.log"
PIDFILE=".streamlit_${{PORT}}.pid"

python3 -m py_compile dashboard.py

# stop existing listener
PID="$(lsof -tiTCP:${{PORT}} -sTCP:LISTEN | head -n1 || true)"
if [[ -n "${{PID:-}}" ]]; then
  echo "Stopping PID=$PID (port $PORT)"
  kill "$PID" || true
  sleep 1
fi
PID2="$(lsof -tiTCP:${{PORT}} -sTCP:LISTEN | head -n1 || true)"
if [[ -n "${{PID2:-}}" ]]; then
  echo "Force kill PID=$PID2 (port $PORT)"
  kill -9 "$PID2" || true
fi

nohup streamlit run dashboard.py --server.port "$PORT" --server.fileWatcherType none \\
  > "$LOG" 2>&1 &

NEWPID=$!
disown || true
echo "$NEWPID" > "$PIDFILE"

echo "OK: started streamlit PID=$NEWPID port=$PORT"
echo "LOG: $LOG"

# readiness (max 10s)
for _ in {{1..20}}; do
  if curl -sf "http://localhost:${{PORT}}" >/dev/null 2>&1; then
    echo "OK: streamlit is up http://localhost:${{PORT}} (PID=$NEWPID)"
    exit 0
  fi
  if ! ps -p "$NEWPID" >/dev/null 2>&1; then
    echo "ERR: streamlit process died (PID=$NEWPID)"
    tail -n 200 "$LOG" || true
    exit 2
  fi
  sleep 0.5
done

echo "ERR: streamlit did not open port $PORT within 10s (PID=$NEWPID)"
tail -n 200 "$LOG" || true
exit 3
"""

    for p, content, name in [
        (stop_pid, stop_pid_content, "stop_dashboard_pid.sh"),
        (run_bg, run_bg_content, "run_dashboard_bg.sh"),
    ]:
        if p.exists():
            # only rewrite if different
            cur = p.read_text(encoding="utf-8")
            if cur != content:
                backup_file(p)
                p.write_text(content, encoding="utf-8")
                changed = True
                out.append(f"updated {name} (backup created)")
        else:
            p.write_text(content, encoding="utf-8")
            changed = True
            out.append(f"created {name}")
        os.chmod(p, 0o755)

    return changed, "; ".join(out) if out else "no changes"

def check_port_listen(port: int) -> CheckResult:
    rc, out = run(["bash", "-lc", f"lsof -nP -iTCP:{port} -sTCP:LISTEN || true"])
    ok = (out.strip() == "")
    return CheckResult(ok=ok, title=f"Port {port} free", details=out.strip() or "NO LISTENER")

def check_streamlit_proc() -> CheckResult:
    rc, out = run(["bash", "-lc", 'pgrep -fl "streamlit run" || true'])
    ok = (out.strip() == "")
    return CheckResult(ok=ok, title="No streamlit processes", details=out.strip() or "NO streamlit processes")

def check_py_compile_all(root: Path) -> CheckResult:
    bad = []
    for p in root.rglob("*.py"):
        try:
            subprocess.run([sys.executable, "-m", "py_compile", str(p)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except subprocess.CalledProcessError as e:
            bad.append((str(p), (e.stderr or e.stdout or "").strip()))
    if bad:
        details = "\n".join([f"- {f}: {err}" for f, err in bad[:50]])
        return CheckResult(ok=False, title="py_compile all .py", details=f"FAIL ({len(bad)})\n{details}")
    return CheckResult(ok=True, title="py_compile all .py", details="OK")

def check_db(root: Path) -> CheckResult:
    db = root / "data" / "atlas.db"
    if not db.exists():
        return CheckResult(ok=False, title="SQLite DB exists", details=str(db))
    con = sqlite3.connect(str(db))
    cur = con.cursor()
    integrity = cur.execute("PRAGMA integrity_check;").fetchone()[0]
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()]
    counts = {}
    for t in tables:
        try:
            counts[t] = cur.execute(f"SELECT count(*) FROM {t};").fetchone()[0]
        except Exception:
            pass
    con.close()
    ok = (integrity == "ok")
    details = f"integrity_check: {integrity}\n" \
              f"tables: {tables}\n" \
              f"counts: {counts}"
    return CheckResult(ok=ok, title="SQLite DB integrity", details=details)

def env_info() -> str:
    parts = []
    parts.append(f"python: {sys.executable}")
    parts.append(f"python -V: {sys.version.replace(os.linesep,' ')}")
    # key packages
    pkgs = ["streamlit","pandas","pyarrow","numpy"]
    for m in pkgs:
        try:
            mod = __import__(m)
            parts.append(f"{m}: {getattr(mod,'__version__','?')}")
        except Exception as e:
            parts.append(f"{m}: MISSING ({e})")
    return "\n".join(parts)

def main() -> int:
    ap = argparse.ArgumentParser(description="Atlas doctor: diagnose + safe auto-fix (no deletions).")
    ap.add_argument("--fix", action="store_true", help="Apply safe fixes with .bak backups.")
    ap.add_argument("--port", type=int, default=8504, help="Dashboard port (default 8504).")
    ap.add_argument("--run-all", action="store_true", help="Run run_all.py and capture log.")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    ensure_dir(root / "logs")

    report_path = root / "logs" / f"doctor_report_{TS}.md"
    lines = []
    lines.append(f"# Atlas doctor report {TS}\n\n")
    lines.append("## Environment\n\n")
    lines.append("```\n" + env_info() + "\n```\n\n")

    results: list[CheckResult] = []

    # Pre-checks
    results.append(check_port_listen(args.port))
    results.append(check_streamlit_proc())
    results.append(check_py_compile_all(root))
    results.append(check_db(root))

    # Fixes (safe)
    if args.fix:
        lines.append("## Fixes applied\n\n")
        # run_all.py
        ra = root / "run_all.py"
        if ra.exists():
            bak = backup_file(ra)
            changed, msg = patch_run_all(ra)
            if not changed:
                # if no change, restore original backup to avoid noise
                ra.write_text(bak.read_text(encoding="utf-8"), encoding="utf-8")
                bak.unlink(missing_ok=True)
                lines.append(f"- run_all.py: no changes\n")
            else:
                lines.append(f"- run_all.py: {msg} (backup: {bak.name})\n")
        else:
            lines.append("- run_all.py: not found\n")

        # dashboard.py
        dash = root / "dashboard.py"
        if dash.exists():
            bak = backup_file(dash)
            changed, msg = patch_dashboard(dash)
            if not changed:
                dash.write_text(bak.read_text(encoding="utf-8"), encoding="utf-8")
                bak.unlink(missing_ok=True)
                lines.append("- dashboard.py: no changes\n")
            else:
                lines.append(f"- dashboard.py: {msg} (backup: {bak.name})\n")
        else:
            lines.append("- dashboard.py: not found\n")

        # bash scripts
        changed, msg = write_bash_scripts(root, args.port)
        lines.append(f"- scripts: {msg}\n")

        # re-run compile after fixes
        results.append(CheckResult(ok=True, title="--- after fixes ---", details=""))
        results.append(check_py_compile_all(root))

    # Optional: run_all
    if args.run_all:
        lines.append("\n## run_all.py execution\n\n")
        log = root / "logs" / f"run_all.doctor.{TS}.log"
        rc, out = run([sys.executable, "run_all.py"], cwd=root, timeout=600)
        log.write_text(out, encoding="utf-8")
        lines.append(f"- rc={rc}\n- log: {log}\n\n")
        # show tail
        tail = "\n".join(out.splitlines()[-60:])
        lines.append("### tail\n\n```\n" + tail + "\n```\n\n")

    # Results section
    lines.append("\n## Checks\n\n")
    for r in results:
        status = "OK" if r.ok else "FAIL"
        lines.append(f"### {status}: {r.title}\n\n")
        if r.details:
            lines.append("```\n" + r.details.strip() + "\n```\n\n")

    report_path.write_text("".join(lines), encoding="utf-8")
    print(f"OK: report -> {report_path}")

    # Exit code: fail if any FAIL in pre-checks
    any_fail = any((not r.ok) for r in results if r.title and not r.title.startswith("---"))
    return 2 if any_fail else 0

if __name__ == "__main__":
    raise SystemExit(main())
