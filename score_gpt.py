def score_text(text: str) -> str:
    """
    Very simple heuristic scoring.
    Returns: GREEN / YELLOW / RED
    """

    t = text.lower()

    negative = [
        "exploit",
        "rug",
        "collapse",
        "shutdown",
        "attack",
        "breach",
        "exit scam",
        "critical bug",
        "halted",
        "drift"
    ]

    warning = [
        "delay",
        "change",
        "adjust",
        "update",
        "migration",
        "incentive",
        "emission",
        "vote",
        "governance",
        "proposal"
    ]

    for word in negative:
        if word in t:
            return "RED"

    for word in warning:
        if word in t:
            return "YELLOW"

    return "GREEN"


if __name__ == "__main__":
    # quick self-test
    tests = [
        "protocol announces incentive adjustment",
        "critical exploit detected in validator set",
        "weekly update released"
    ]

    for t in tests:
        print(t, "→", score_text(t))
