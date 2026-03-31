"""
Normalize lead payloads to the SN71 queue shape.

This keeps gateway-relevant keys and ensures recommended optional fields are present
in queue artifacts (`phone_numbers`, `socials`) so downstream tooling sees a
consistent payload schema.

Attestation fields (wallet_ss58, terms_version_hash, etc.) are added at submit time by
attach_gateway_attestation_fields — do not store them in queue JSON.
"""

from __future__ import annotations

from typing import Any, Dict

# Order matches gateway REQUIRED_FIELDS + region/HQ (see gateway/api/submit.py, docs/AVOID-REJECTIONS.md).
# phone_numbers / socials are optional for SN71 but kept when present (including empty [] / {}).
_MINIMAL_KEYS_ORDER: tuple[str, ...] = (
    "business",
    "full_name",
    "first",
    "last",
    "email",
    "role",
    "website",
    "industry",
    "sub_industry",
    "country",
    "state",
    "city",
    "linkedin",
    "company_linkedin",
    "source_url",
    "description",
    "employee_count",
    "hq_country",
    "hq_state",
    "hq_city",
    "source_type",
    "phone_numbers",
    "socials",
)

_KEEP_EMPTY_COLLECTION = frozenset({"phone_numbers", "socials"})
_SOCIAL_KEYS: tuple[str, ...] = (
    "linkedin",
    "github",
    "twitter",
    "telegram",
    "instagram",
    "facebook",
    "youtube",
)
_STRING_KEYS = frozenset(
    k for k in _MINIMAL_KEYS_ORDER if k not in {"phone_numbers", "socials"}
)


def minimal_gateway_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a SN71-shaped lead dict with only allowed keys.

    - Keeps known keys in `_MINIMAL_KEYS_ORDER`.
    - Preserves existing non-empty values.
    - Ensures recommended optional fields are present (`phone_numbers`, `socials`).
    - Adds missing string keys as empty strings to keep a stable schema.
    """
    out: Dict[str, Any] = {}
    for k in _MINIMAL_KEYS_ORDER:
        if k in lead:
            v = lead[k]
            if v is None:
                v = [] if k == "phone_numbers" else ({} if k == "socials" else "")
            if isinstance(v, str):
                out[k] = v.strip()
            elif isinstance(v, list):
                out[k] = v
            elif isinstance(v, dict):
                out[k] = v
            else:
                out[k] = str(v)
        else:
            if k == "phone_numbers":
                out[k] = []
            elif k == "socials":
                out[k] = {}
            elif k in _STRING_KEYS:
                out[k] = ""

    # Normalize recommended optional collections.
    if not isinstance(out.get("phone_numbers"), list):
        out["phone_numbers"] = []
    socials_raw = out.get("socials")
    socials: Dict[str, Any] = socials_raw if isinstance(socials_raw, dict) else {}
    normalized_socials: Dict[str, Any] = {}
    for sk in _SOCIAL_KEYS:
        val = socials.get(sk)
        if val is None:
            normalized_socials[sk] = None
        elif isinstance(val, str):
            normalized_socials[sk] = val.strip() or None
        else:
            normalized_socials[sk] = val
    out["socials"] = normalized_socials
    return out
