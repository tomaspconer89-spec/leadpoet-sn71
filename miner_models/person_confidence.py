from __future__ import annotations

from typing import Any, Dict

from miner_models.lead_normalization import is_generic_email


def score_person_confidence(lead: Dict[str, Any], *, title_matches_persona: bool = False) -> Dict[str, Any]:
    """
    Evidence-driven person confidence before precheck (high-profit tiers).

    Targets: strong (>=16), near_pass (>=10), weak_retryable (>=5), else reject.
    Aligns with direct /in/ LinkedIn, non-generic email, email-domain match
    (+4 company_site_match when address matches site), and title fit.
    """
    full_name = (lead.get("full_name") or "").strip()
    first = (lead.get("first") or "").strip()
    last = (lead.get("last") or "").strip()
    role = (lead.get("role") or "").strip()
    email = (lead.get("email") or "").strip()
    linkedin = (lead.get("linkedin") or "").strip()
    company_linkedin = (lead.get("company_linkedin") or "").strip()
    source_url = (lead.get("source_url") or "").strip()
    website = (lead.get("website") or "").strip()

    name_quality = (lead.get("name_quality") or "").strip().lower()
    if not name_quality:
        parts = [p for p in full_name.split() if p] if full_name else []
        if len(parts) >= 2:
            name_quality = "full"
        elif len(parts) == 1 or (first and not last):
            name_quality = "first_only"
        else:
            name_quality = "missing"

    person_on_site = bool(lead.get("person_found_on_company_site"))

    source_type = (lead.get("source_type") or "").strip().lower()
    email_type = (lead.get("email_type") or "").strip().lower()
    if not email_type:
        email_type = (
            "generic"
            if is_generic_email(email)
            else ("direct" if "@" in email else "unknown")
        )

    email_domain_match = lead.get("email_domain_match")
    if email_domain_match is None:
        from miner_models.lead_normalization import email_domain_matches_website

        email_domain_match = email_domain_matches_website(lead)

    source_count = int(lead.get("source_count") or 0) or len(
        lead.get("source_urls") or ([] if not source_url else [source_url])
    )
    identity_conflict = bool(lead.get("identity_conflict"))
    try:
        stale_days = int(lead.get("source_freshness_days") or 0)
    except (TypeError, ValueError):
        stale_days = 0

    has_direct_email = bool(email and "@" in email and email_type == "direct")
    has_generic_email = bool(email and (email_type == "generic" or is_generic_email(email)))
    has_linkedin_raw = "/in/" in linkedin.lower()
    has_full_name = name_quality == "full" or bool(full_name and " " in full_name.strip())
    has_last_name = bool(last) or name_quality == "full"
    has_title = bool(role)
    has_company_li = bool(company_linkedin and "/company/" in company_linkedin.lower())
    multi_source = bool(
        (has_linkedin_raw and has_company_li)
        or (source_count >= 2 and has_linkedin_raw and person_on_site)
    )

    high_signal_source = source_type in ("team_page", "about_page")

    score = 0
    if has_full_name:
        score += 4
    if has_last_name:
        score += 3
    if has_title:
        score += 3
    if title_matches_persona:
        score += 4
    if person_on_site:
        score += 3
    li_bonus = 4 if has_linkedin_raw else 0
    if identity_conflict and li_bonus:
        li_bonus = min(li_bonus, 2)
    score += li_bonus
    if has_direct_email:
        score += 5
    if email and "@" in email:
        if email_domain_match:
            score += 4
        else:
            score -= 5
    if has_company_li:
        score += 2
    if multi_source:
        score += 2
    if high_signal_source:
        score += 2

    if has_generic_email or (lead.get("account_only_signal")):
        score -= 6
    if name_quality == "first_only" or (first and not last):
        score -= 4
    if identity_conflict:
        score -= 4
    if not person_on_site and not high_signal_source:
        score -= 3
    if stale_days > 365:
        score -= 2
    if not role:
        score -= 3
    if not source_url:
        score -= 4

    if score >= 16:
        bucket = "strong"
    elif score >= 10:
        bucket = "near_pass"
    elif score >= 5:
        bucket = "weak_retryable"
    else:
        bucket = "reject"

    recommended = "submit_ready"
    if bucket in ("near_pass", "weak_retryable"):
        recommended = "retry_enrichment"
    if bucket == "reject":
        recommended = "reject"

    return {
        "person_confidence_score": score,
        "person_confidence_bucket": bucket,
        "signals": {
            "has_full_name": has_full_name,
            "has_last_name": has_last_name,
            "has_title": has_title,
            "title_matches_persona": title_matches_persona,
            "has_direct_email": has_direct_email,
            "has_generic_email": has_generic_email,
            "has_linkedin": has_linkedin_raw,
            "found_on_company_site": person_on_site,
            "multi_source_match": multi_source,
            "name_quality": name_quality,
            "source_type": source_type or "unknown",
            "email_domain_match": bool(email_domain_match),
            "company_site_match": bool(
                email and "@" in email and bool(email_domain_match)
            ),
            "source_count": source_count,
            "identity_conflict": identity_conflict,
        },
        "recommended_action": recommended,
    }
