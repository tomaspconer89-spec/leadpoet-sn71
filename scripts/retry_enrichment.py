from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from miner_models.lead_normalization import apply_email_classification, is_generic_email

# High-profit: only spend enrichment on near-pass identity gaps (max 2 attempts inside handlers).
MAX_RETRY_ATTEMPTS = 2

# Substrings matched against precheck `reason` (lowercased) before running LinkedIn / name fixups.
RETRY_ALLOWED_SUBSTRINGS = (
    "missing_required_fields: linkedin",
    "invalid_linkedin",
    "invalid_linkedin_url",
    "missing_required_fields: last",
    "missing_first_or_last_name",
    "email_domain_mismatch",
)


def should_run_targeted_retry(reason: Optional[str]) -> bool:
    if not (reason or "").strip():
        return False
    low = reason.strip().lower()
    return any(s in low for s in RETRY_ALLOWED_SUBSTRINGS)


def _fix_missing_last(lead: Dict[str, Any]) -> bool:
    full_name = (lead.get("full_name") or "").strip()
    if not full_name:
        return False
    parts = [p for p in full_name.split() if p]
    if len(parts) < 2:
        return False
    if not lead.get("first"):
        lead["first"] = parts[0]
    if not lead.get("last"):
        lead["last"] = " ".join(parts[1:])
    return bool(lead.get("last"))


def _name_confidence_for_linkedin_search(lead: Dict[str, Any]) -> bool:
    """Avoid burning enrichment on hopeless rows."""
    nq = (lead.get("name_quality") or "").strip().lower()
    full = (lead.get("full_name") or "").strip()
    first = (lead.get("first") or "").strip()
    last = (lead.get("last") or "").strip()
    bus = (lead.get("business") or "").strip()
    if not bus:
        return False
    if nq == "full" or (full and " " in full):
        return True
    if first and last and len(first) >= 2 and len(last) >= 2:
        return True
    if first and len(first) >= 2 and (lead.get("role") or "").strip():
        return True
    return False


def _try_email_domain_alias_match(lead: Dict[str, Any]) -> bool:
    """
    If website vs email roots differ, check comma-separated alias pairs:
    LEAD_EMAIL_DOMAIN_ALIASES=corp.com:brand.com,foo.com:bar.com
    """
    raw = (os.environ.get("LEAD_EMAIL_DOMAIN_ALIASES") or "").strip()
    if not raw:
        return False
    email = (lead.get("email") or "").strip().lower()
    website = (lead.get("website") or "").strip()
    if "@" not in email or not website:
        return False
    e_dom = email.split("@")[-1]
    try:
        whost = urlparse(website if website.startswith("http") else f"https://{website}").netloc.lower()
    except Exception:
        return False
    if whost.startswith("www."):
        whost = whost[4:]
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    for pair in parts:
        if ":" not in pair:
            continue
        a, b = pair.split(":", 1)
        a, b = a.strip(), b.strip()
        if not a or not b:
            continue
        if (e_dom == a and whost.endswith(b)) or (e_dom == b and whost.endswith(a)):
            lead["email_domain_match"] = True
            lead["email_domain_mismatch_note"] = f"alias_allowlist:{a}<->{b}"
            return True
    return False


def targeted_retry_enrichment(
    lead: Dict[str, Any],
    reason: str,
    *,
    enrich_linkedin: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
) -> Tuple[Dict[str, Any], List[str], int]:
    """
    Failure-specific recovery before second precheck.
    Only runs for reasons in RETRY_ALLOWED_SUBSTRINGS (LinkedIn / last name / email-domain).
    """
    out = dict(lead)
    recovered: List[str] = []
    attempts = 0
    r = (reason or "").strip().lower()

    if not should_run_targeted_retry(reason):
        return out, recovered, attempts

    # Generic inbox: account-level signal; try named-person enrichment only when plausible
    if r.startswith("general_purpose_email:") or "general_purpose_email" in r:
        attempts += 1
        out["account_only_signal"] = True
        out["email_type"] = "generic"
        if enrich_linkedin is not None and _name_confidence_for_linkedin_search(out):
            before_p = (out.get("linkedin") or "").strip()
            before_c = (out.get("company_linkedin") or "").strip()
            try:
                out = enrich_linkedin(out)
            except Exception:
                pass
            if not before_p and (out.get("linkedin") or "").strip():
                recovered.append("linkedin")
            if not before_c and (out.get("company_linkedin") or "").strip():
                recovered.append("company_linkedin")

    li_val = (out.get("linkedin") or "").lower()
    missing_person_li = "/in/" not in li_val
    linkedin_gap = (
        "missing_required_fields: linkedin" in r
        or "invalid_linkedin" in r
        or ("linkedin" in r and missing_person_li and not r.startswith("general_purpose_email"))
    )
    if linkedin_gap and enrich_linkedin is not None and _name_confidence_for_linkedin_search(out):
        attempts += 1
        before = (out.get("linkedin") or "").strip()
        try:
            out = enrich_linkedin(out)
        except Exception:
            pass
        if not before and (out.get("linkedin") or "").strip():
            recovered.append("linkedin")
        if not before:
            attempts += 1
            try:
                out = enrich_linkedin(out)
            except Exception:
                pass
            if (out.get("linkedin") or "").strip():
                recovered.append("linkedin_retry2")

    if "missing_required_fields: last" in r or "missing_first_or_last_name" in r:
        attempts += 1
        if _fix_missing_last(out):
            recovered.append("last")
        elif enrich_linkedin is not None and _name_confidence_for_linkedin_search(out):
            attempts += 1
            try:
                out = enrich_linkedin(out)
            except Exception:
                pass
            if _fix_missing_last(out):
                recovered.append("last_via_enrich")

    if "email_domain_mismatch" in r:
        attempts += 1
        out["email_domain_match"] = False
        if _try_email_domain_alias_match(out):
            recovered.append("email_domain_alias")

    apply_email_classification(out)
    return out, recovered, attempts
