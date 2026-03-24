from __future__ import annotations

from typing import Any, Dict, Optional


def _strong_identity(lead: Dict[str, Any]) -> bool:
    li = (lead.get("linkedin") or "").strip().lower()
    if "/in/" in li:
        return True
    nm = (lead.get("name_quality") or "").strip().lower()
    et = (lead.get("email_type") or "").strip().lower()
    on_site = bool(lead.get("person_found_on_company_site"))
    if nm == "full" and et == "direct" and on_site:
        return True
    if (lead.get("signals") or {}).get("multi_source_match"):
        return True
    return bool(
        et == "direct"
        and on_site
        and (lead.get("person_confidence_score") or 0) >= 14
    )


def _recoverable_reason(reason: Optional[str]) -> bool:
    if not reason:
        return False
    low = reason.strip().lower()
    prefixes = (
        "missing_required_fields: linkedin",
        "missing_required_fields: last",
        "missing_first_or_last_name",
        "email_domain_mismatch",
        "name_not_in_email",
        "invalid_linkedin",
        "invalid_employee_count",
        "invalid_sub_industry",
        "invalid_industry_pairing",
        "desc_too_short",
        "role_too_short",
    )
    return any(low.startswith(p) or p in low for p in prefixes)


def _junk_reason(reason: Optional[str]) -> bool:
    if not reason:
        return False
    low = reason.strip().lower()
    if "invalid_email" in low or "no_email" in low:
        return True
    if "linkedin_not_allowed" in low and "website" in low:
        return True
    return False


def route_lead(
    lead: Dict[str, Any],
    *,
    precheck_ok: bool,
    precheck_reason: Optional[str],
) -> str:
    """
    Graded queues A–E with stricter submit-ready (A) vs enrichment (B).
    """
    bucket = (lead.get("person_confidence_bucket") or "").strip().lower()
    target_fit = (lead.get("target_fit") or "").strip().lower()
    et = (lead.get("email_type") or "").strip().lower()
    has_generic = et == "generic" or bool(lead.get("account_only_signal"))

    has_company = bool((lead.get("business") or "").strip())
    has_named_person = bool(
        (lead.get("last") or "").strip()
        or ((lead.get("name_quality") or "").strip().lower() == "full")
    )

    if precheck_ok:
        strong_a = (
            bucket == "strong"
            and not has_generic
            and target_fit in ("high", "medium")
            and _strong_identity(lead)
        )
        if strong_a:
            return "A_ready_submit"
        return "B_retry_enrichment"

    reason = precheck_reason or ""

    if _junk_reason(reason) and bucket == "reject":
        return "E_reject"

    if has_company and (not has_named_person or has_generic):
        if _recoverable_reason(reason) and bucket in ("near_pass", "weak_retryable", "strong"):
            return "B_retry_enrichment"
        return "C_good_account_needs_person"

    if (
        bucket in ("near_pass", "weak_retryable")
        or _recoverable_reason(reason)
    ):
        return "B_retry_enrichment"

    if bucket == "reject":
        return "E_reject"

    return "D_low_confidence_hold"
