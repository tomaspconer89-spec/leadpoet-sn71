from __future__ import annotations

from typing import Dict, List, Tuple

# Paths align with Leadpoet high-yield template (team/leadership first; contact low;
# legal/support noise lowest). Scores are 0–10; crawl uses page_score_threshold (e.g. 8).
_HIGH_SIGNAL_PATHS: Tuple[Tuple[str, int], ...] = (
    ("/team", 10),
    ("/people", 10),
    ("/leadership", 10),
    ("/our-team", 10),
    ("/leadership-team", 10),
    ("/about", 8),
    ("/about-us", 8),
    ("/company", 8),
    ("/careers", 8),
    ("/blog", 8),
    ("/authors", 8),
    ("/press", 6),
    ("/speakers", 6),
    ("/author", 6),
    ("/contact", 2),
    ("/privacy", 1),
    ("/terms", 1),
    ("/legal", 1),
    ("/support", 1),
)


def discover_priority_pages(
    domain: str, min_page_score: int = 0
) -> List[Dict[str, object]]:
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
    for hint, score in _HIGH_SIGNAL_PATHS:
        if score < min_page_score:
            continue
        pages.append(
            {
                "url": f"{d}{hint}",
                "page_type": hint.lstrip("/"),
                "page_score": score,
            }
        )
    return pages
