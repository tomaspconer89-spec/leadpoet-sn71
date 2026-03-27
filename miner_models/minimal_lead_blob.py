"""
Strip miner-internal fields; keep gateway-required keys plus optional phone_numbers / socials.

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


def minimal_gateway_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    """Return a new dict with only allowed keys; skip empty strings / None / empty collections."""
    out: Dict[str, Any] = {}
    for k in _MINIMAL_KEYS_ORDER:
        if k not in lead:
            continue
        v = lead[k]
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if isinstance(v, (list, dict)) and len(v) == 0:
            if k not in _KEEP_EMPTY_COLLECTION:
                continue
        out[k] = v
    return out
