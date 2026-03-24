from __future__ import annotations

from typing import Dict, List


HIGH_SIGNAL_HINTS = (
    "/team",
    "/about",
    "/leadership",
    "/company",
    "/contact",
    "/people",
    "/author",
    "/press",
    "/speakers",
    "/careers",
)


def discover_priority_pages(domain: str) -> List[Dict[str, object]]:
    """
    Lightweight page discovery fallback.
    Returns deterministic high-signal candidates used by downstream crawlers.
    """
    d = (domain or "").strip().lower()
    if not d:
        return []
    if not d.startswith("http"):
        d = f"https://{d}"
    d = d.rstrip("/")

    pages: List[Dict[str, object]] = []
    for hint in HIGH_SIGNAL_HINTS:
        score = 8 if hint in ("/team", "/leadership") else 5
        pages.append(
            {
                "url": f"{d}{hint}",
                "page_type": hint.lstrip("/"),
                "page_score": score,
            }
        )
    return pages
