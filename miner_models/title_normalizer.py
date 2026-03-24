from __future__ import annotations

from typing import Dict


def normalize_title(raw_title: str) -> Dict[str, str]:
    """
    Normalize noisy title text into routing-friendly buckets.
    """
    raw = (raw_title or "").strip()
    low = raw.lower()

    if not raw:
        return {
            "normalized_title": "",
            "seniority": "unknown",
            "persona_bucket": "non_target",
            "target_fit": "low",
        }

    if any(k in low for k in ("ceo", "chief", "founder", "owner", "managing partner", "partner")):
        return {
            "normalized_title": raw,
            "seniority": "executive",
            "persona_bucket": "economic_buyer",
            "target_fit": "high",
        }

    if any(k in low for k in ("vp", "vice president", "head of", "director")):
        bucket = "functional_owner"
        if any(k in low for k in ("sales", "growth", "revenue", "marketing", "operations")):
            bucket = "likely_champion"
        return {
            "normalized_title": raw,
            "seniority": "director_plus",
            "persona_bucket": bucket,
            "target_fit": "high",
        }

    if any(k in low for k in ("manager", "lead", "principal", "consultant", "advisor")):
        return {
            "normalized_title": raw,
            "seniority": "manager_plus",
            "persona_bucket": "influencer",
            "target_fit": "medium",
        }

    if any(k in low for k in ("intern", "assistant", "coordinator", "associate", "student")):
        return {
            "normalized_title": raw,
            "seniority": "junior",
            "persona_bucket": "too_junior",
            "target_fit": "low",
        }

    return {
        "normalized_title": raw,
        "seniority": "unknown",
        "persona_bucket": "non_target",
        "target_fit": "low",
    }
