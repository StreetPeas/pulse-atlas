import sqlite3
from datetime import datetime, timezone
from urllib.parse import urlparse

DB = "data/atlas.db"

GREEN = "green"
YELLOW = "yellow"
RED = "red"

KEYWORDS_RED = [
    "ban","banned","regulation","regulator","sec","fine","lawsuit","court","sanction",
    "breach","leak","hack","ransom","exploit","vulnerability","cve","critical",
    "surveillance","blocked","shutdown","arrest","fraud","scam","malware",
]
KEYWORDS_GREEN = [
    "release","launched","introducing","announcing","open source","benchmark","paper",
    "improves","performance","funding","partnership","integration","upgrade","stable",
    "tool","sdk","api","security fix","patch","mitigation",
]
KEYWORDS_YELLOW = [
    "rumor","report","preview","beta","maybe","analysis","opinion","thoughts","discussion",
]

HIGH_SIGNAL_DOMAINS = {
    "openai.com": 0.78,
    "blog.cloudflare.com": 0.72,
    "arstechnica.com": 0.65,
    "theverge.com": 0.58,
    "schneier.com": 0.70,
    "github.com": 0.62,
    "lwn.net": 0.68,
}

def norm_text(*parts: str) -> str:
    return " ".join([p.strip() for p in parts if p and p.strip()]).lower()

def domain_score(url: str) -> float:
    try:
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        for d, sc in HIGH_SIGNAL_DOMAINS.items():
            if host == d or host.endswith("." + d):
                return sc
        return 0.45
    except Exception:
        return 0.40

def classify(text: str) -> tuple[float, str, str]:
    t = text.lower()

    hit_red = any(k in t for k in KEYWORDS_RED)
    hit_green = any(k in t for k in KEYWORDS_GREEN)
    hit_yellow = any(k in t for k in KEYWORDS_YELLOW)

    if hit_red and not hit_green:
        return 0.72, RED, "risk/pressure"
    if hit_green and not hit_red:
        return 0.66, GREEN, "progress"
    if hit_red and hit_green:
        return 0.60, YELLOW, "mixed"
    if hit_yellow:
        return 0.52, YELLOW, "watch"
    return 0.48, YELLOW, "neutral"

def clamp(x: float) -> float:
    return max(0.0, min(1.0, x))


def norm_color(c: str) -> str:
    c = (c or "").strip()
    if c in ("🟢","green"): return "🟢"
    if c in ("🟡","yellow","⚪","white"): return "🟡"
    if c in ("🔴","red"): return "🔴"
    return "⚫"

def main(limit: int = 200) -> int:
    con = sqlite3.connect(DB)
    cur = con.cursor()

    rows = cur.execute(
        """
        SELECT id, source, title, text, summary, url, raw
        FROM signals
        WHERE (rationale IS NULL OR rationale = '')
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    if not rows:
        print("OK: score_signals none_to_score")
        con.close()
        return 0

    updated = 0
    now = datetime.now(timezone.utc).isoformat()

    for sid, source, title, text, summary, url, raw in rows:
        blob = norm_text(title or "", text or "", summary or "")
        base = domain_score(url or "")

        kscore, color, label = classify(blob)
        color = norm_color(color)
        score = clamp(0.5 * base + 0.5 * kscore)

        rationale = f"{label}; score={score:.2f}; source={source}; t={now}"
        cur.execute(
            """
            UPDATE signals
            SET score=?, color=?, label=?, rationale=?
            WHERE id=?
            """,
            (float(score), color, label, rationale, sid),
        )
        updated += 1

    con.commit()
    con.close()
    print(f"OK: score_signals updated={updated}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
