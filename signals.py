\
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Iterable

from storage import init_db, save_signal

@dataclass
class SignalItem:
    source: str
    url: str
    title: str = ""
    summary: str = ""
    raw: str = ""

def ingest(items: Iterable[SignalItem]) -> dict:
    init_db()
    inserted = 0
    ignored = 0

    for it in items:
        url = (it.url or "").strip()
        if not url:
            continue

        raw = it.raw or ""
        if raw and not isinstance(raw, str):
            raw = json.dumps(raw, ensure_ascii=False)

        res = save_signal(
            source=it.source,
            url=url,
            title=(it.title or "").strip(),
            summary=(it.summary or "").strip(),
            raw=raw,
        )

        # res зависит от твоей реализации save_signal()
        if res:
            inserted += 1
        else:
            ignored += 1

    return {"inserted": inserted, "ignored": ignored}

def sample_items(n: int = 5) -> list[SignalItem]:
    out = []
    for i in range(n):
        out.append(SignalItem(
            source="sample",
            url=f"https://example.com/sample/{i}",
            title=f"sample {i}",
            summary="plumbing test",
            raw=json.dumps({"i": i}, ensure_ascii=False),
        ))
    return out
