from dataclasses import dataclass
import re
from typing import Dict, List, Literal

Decision = Literal["DROP", "KEEP", "KEEP_PRIORITY"]
Project = Literal["bittensor", "akash", "gaea", "unknown"]

@dataclass
class FilterResult:
    decision: Decision
    project: Project
    reasons: List[str]
    features: Dict[str, object]

ANCHORS = {
    "bittensor": [r"\bbittensor\b", r"\btao\b", r"\bsubnet\b"],
    "akash": [r"\bakash\b", r"\bakt\b", r"\bprovider\b"],
    "gaea": [r"\bgaea\b"],
}

DROP_PATTERNS = [
    r"\bairdrop\b",
    r"\bwhitelist\b",
    r"\bgiveaway\b",
    r"\bjoin now\b",
    r"\bdiscount\b",
]

MIN_LEN = 40

def detect_project(text: str) -> Project:
    t = text.lower()
    for proj, pats in ANCHORS.items():
        for p in pats:
            if re.search(p, t):
                return proj  # type: ignore
    return "unknown"

def filter_event(text: str, source: str = "", author: str = "") -> FilterResult:
    t = (text or "").strip()
    tlow = t.lower()

    reasons: List[str] = []
    features: Dict[str, object] = {"len": len(t), "source": source, "author": author}

    for p in DROP_PATTERNS:
        if re.search(p, tlow):
            reasons.append(f"drop_pattern:{p}")
            return FilterResult("DROP", "unknown", reasons, features)

    if len(t) < MIN_LEN:
        reasons.append("too_short")
        return FilterResult("DROP", "unknown", reasons, features)

    proj = detect_project(t)
    features["project"] = proj

    if proj != "unknown":
        reasons.append("anchor_project_match")
        return FilterResult("KEEP_PRIORITY", proj, reasons, features)

    reasons.append("generic_keep")
    return FilterResult("KEEP", "unknown", reasons, features)
