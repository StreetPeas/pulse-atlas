import sqlite3
from datetime import datetime, timezone

import storage


RISK_WORDS = {
    "hack", "hacked", "exploit", "breach", "leak", "malware", "ransom",
    "scam", "fraud", "lawsuit", "sec", "investigation", "ban",
    "panic", "crash", "attack", "vulnerability", "critical",
}

HYPE_WORDS = {
    "launch", "released", "partnership", "upgrade", "milestone",
    "record", "surge", "breakthrough", "wins", "adoption",
}

def classify(title: str, text: str):
    t = f"{title}\n{text}".lower()

    risk_hits = sum(1 for w in RISK_WORDS if w in t)
    hype_hits = sum(1 for w in HYPE_WORDS if w in t)

    # базовая, прозрачная логика (позже заменим на LLM)
    if risk_hits >= 1 and risk_hits >= hype_hits:
        return 0.85, "risk", "🔴", f"rule:risk hits={risk_hits}"
    if hype_hits >= 2 and hype_hits > risk_hits:
        return 0.70, "hype", "🟢", f"rule:hype hits={hype_hits}"
    return 0.35, "neutral", "⚪", "rule:neutral"

def run(limit: int = 200):
    storage.init_db()
    conn = sqlite3.connect(storage.DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, COALESCE(title,''), COALESCE(text,''), COALESCE(url,'')
        FROM signals
        WHERE source='rss' AND (rationale IS NULL OR rationale='')
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()

    updated = 0
    for _id, title, text, url in rows:
        score, label, color, rationale = classify(title, text)
        if url:
            rationale = f"{rationale}; url={url}"

        cur.execute(
            """
            UPDATE signals
            SET score=?, label=?, color=?, rationale=?
            WHERE id=?
            """,
            (float(score), label, color, rationale, _id),
        )
        updated += 1

    conn.commit()
    conn.close()

    ts = datetime.now(timezone.utc).isoformat()
    print(f"[analyze] {ts} updated={updated}")

if __name__ == "__main__":
    run()
