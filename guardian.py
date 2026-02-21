#!/usr/bin/env python3
"""
Pulse Atlas Guardian (shadow mode)

Goals:
- Catch path hygiene issues (whitespace in tracked files)
- Ensure tracked .py files compile
- Ping DB init quickly
- Scan recent logs for obvious error patterns
- Optional: smoke-run-all

Exit policy:
- OK => 0
- WARN/FAIL => 2  (so schedulers/CI can treat it as attention required)
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple


@dataclass
class CheckResult:
    ok: bool
    severity: str  # OK | WARN | FAIL
    title: str
    details: str = ""


def _utc_ts() -> str:
    # unique even for multiple runs per second
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")


def ensure_dirs(root: Path) -> None:
    (root / "logs" / "guardian").mkdir(parents=True, exist_ok=True)


def run_cmd(cmd: List[str], cwd: Path | None = None, timeout: int = 120) -> Tuple[int, str]:
    try:
        p = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            text=True,
        )
        return p.returncode, p.stdout
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or "") + "\n[TIMEOUT]\n" + (e.stderr or "")
        return 124, out
    except Exception as e:
        return 1, f"[EXC] {e}"


def git_ls_files(root: Path) -> List[str]:
    rc, out = run_cmd(["git", "ls-files", "-z"], cwd=root, timeout=30)
    if rc != 0:
        return []
    items = [p for p in out.split("\0") if p]
    return items


def check_tracked_path_hygiene(root: Path) -> CheckResult:
    paths = git_ls_files(root)
    if not paths:
        return CheckResult(ok=False, severity="WARN", title="tracked path hygiene", details="git ls-files failed or repo has no tracked files")

    bad = [p for p in paths if any(ch.isspace() for ch in p)]
    if bad:
        return CheckResult(
            ok=False,
            severity="FAIL",
            title="tracked path hygiene",
            details="Whitespace in tracked paths:\n" + "\n".join(bad[:200]),
        )
    return CheckResult(ok=True, severity="OK", title="tracked path hygiene", details="no whitespace in tracked paths")


def check_py_compile_tracked(root: Path, python: str) -> CheckResult:
    paths = git_ls_files(root)
    pys = [p for p in paths if p.endswith(".py")]
    if not pys:
        return CheckResult(ok=True, severity="OK", title="py_compile tracked", details="no tracked .py files")

    # compile in chunks to avoid argv limits
    failed = []
    chunk = 200
    for i in range(0, len(pys), chunk):
        part = pys[i : i + chunk]
        rc, out = run_cmd([python, "-m", "py_compile", *part], cwd=root, timeout=120)
        if rc != 0:
            failed.append(out.strip())

    if failed:
        return CheckResult(ok=False, severity="FAIL", title="py_compile tracked", details="\n\n".join(failed)[:4000])

    return CheckResult(ok=True, severity="OK", title="py_compile tracked", details=f"compiled {len(pys)} files")


def check_db_ping(root: Path, python: str) -> CheckResult:
    # quick import + init_db()
    code = r"""
try:
    import storage
    storage.init_db()
    print("OK")
except Exception as e:
    print("ERR", repr(e))
    raise
"""
    rc, out = run_cmd([python, "-c", code], cwd=root, timeout=30)
    if rc == 0 and "OK" in out:
        return CheckResult(ok=True, severity="OK", title="db ping", details="storage.init_db() OK")
    return CheckResult(ok=False, severity="FAIL", title="db ping", details=out.strip()[:2000])


def check_recent_error_patterns(root: Path) -> CheckResult:
    logdir = root / "logs"
    if not logdir.exists():
        return CheckResult(ok=True, severity="OK", title="recent logs scan", details="logs/ not found (skip)")

    # scan last modified files under logs/ (excluding huge dirs)
    patterns = [
        r"traceback",
        r"\bexception\b",
        r"\berror\b",
        r"\bfail(ed)?\b",
        r"IndentationError",
        r"SyntaxError",
        r"No such file or directory",
    ]
    rx = re.compile("|".join(patterns), re.IGNORECASE)

    files = []
    for fp in logdir.rglob("*"):
        if fp.is_dir():
            continue
        # skip very large files
        try:
            if fp.stat().st_size > 2_000_000:
                continue
        except Exception:
            continue
        files.append(fp)

    # newest first, take top N
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    files = files[:25]

    hits = []
    for fp in files:
        try:
            txt = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for ln in txt.splitlines()[-400:]:
            if rx.search(ln):
                hits.append(f"{fp.relative_to(root)}: {ln.strip()}")
                if len(hits) >= 80:
                    break
        if len(hits) >= 80:
            break

    if hits:
        return CheckResult(ok=False, severity="WARN", title="recent logs scan", details="\n".join(hits))
    return CheckResult(ok=True, severity="OK", title="recent logs scan", details="no obvious error patterns in recent logs")


def smoke_run_all(root: Path, python: str, timeout: int = 600) -> CheckResult:
    ra = root / "run_all.py"
    if not ra.exists():
        return CheckResult(ok=False, severity="WARN", title="run_all smoke", details="run_all.py not found (skip)")

    rc, out = run_cmd([python, str(ra)], cwd=root, timeout=timeout)
    if rc == 0:
        tail = "\n".join(out.splitlines()[-80:])
        return CheckResult(ok=True, severity="OK", title="run_all smoke", details="tail:\n" + tail)
    tail = "\n".join(out.splitlines()[-120:])
    return CheckResult(ok=False, severity="FAIL", title="run_all smoke", details="rc=%s\n%s" % (rc, tail))


def write_report(root: Path, results: List[CheckResult], report_path: Path) -> None:
    lines = []
    lines.append("# Guardian report\n\n")
    lines.append(f"- ts_utc: `{datetime.now(timezone.utc).isoformat()}`\n")
    lines.append(f"- root: `{root}`\n")
    lines.append(f"- python: `{sys.executable}`\n\n")

    sev_rank = {"OK": 0, "WARN": 1, "FAIL": 2}
    worst = max((sev_rank.get(r.severity, 0) for r in results), default=0)
    inv = {0: "OK", 1: "WARN", 2: "FAIL"}

    lines.append("## Summary\n\n")
    lines.append(f"- status: **{inv[worst]}**\n")
    lines.append(f"- checks: {len(results)}\n\n")

    lines.append("## Checks\n\n")
    for r in results:
        lines.append(f"### {r.severity}: {r.title}\n\n")
        if r.details:
            lines.append("```text\n" + r.details.strip() + "\n```\n\n")

    report_path.write_text("".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Pulse Atlas Guardian (shadow mode)")
    ap.add_argument("--root", default=".", help="project root (default: .)")
    ap.add_argument("--python", default=sys.executable or "python3", help="python executable")
    ap.add_argument("--smoke-run-all", action="store_true", help="run run_all.py smoke check")
    ap.add_argument("--smoke-timeout", type=int, default=600, help="timeout for run_all smoke (sec)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    ensure_dirs(root)

    report = root / "logs" / "guardian" / f"guardian_report_{_utc_ts()}.md"

    results: List[CheckResult] = []
    results.append(check_tracked_path_hygiene(root))
    results.append(check_py_compile_tracked(root, args.python))
    results.append(check_db_ping(root, args.python))
    results.append(check_recent_error_patterns(root))

    if args.smoke_run_all:
        results.append(smoke_run_all(root, args.python, timeout=args.smoke_timeout))

    write_report(root, results, report)
    print(f"OK: report -> {report}")

    if any(r.severity == "FAIL" for r in results):
        return 2
    if any(r.severity == "WARN" for r in results):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
