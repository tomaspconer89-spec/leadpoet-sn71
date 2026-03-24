"""
Normalize legacy Lead Sorcerer dicts before title/person confidence/precheck.

Adds evidence-oriented fields so downstream scoring and routing do less guessing.
"""

from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlparse

GENERIC_EMAIL_PREFIXES = (
    "info@",
    "hello@",
    "contact@",
    "support@",
    "team@",
    "admin@",
    "office@",
    "mail@",
    "help@",
    "hi@",
    "sales@",
    "enquiries@",
    "inquiries@",
)


def is_generic_email(email: str) -> bool:
    val = (email or "").strip().lower()
    return bool(val and any(val.startswith(p) for p in GENERIC_EMAIL_PREFIXES))


def _host_from_url(raw: str) -> str:
    if not raw or not str(raw).strip():
        return ""
    val = str(raw).strip()
    parsed = urlparse(val if "://" in val else f"https://{val}")
    host = (parsed.hostname or "").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _extract_root_domain(domain: str) -> str:
    domain = (domain or "").lower().strip()
    parts = domain.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return domain


def email_domain_matches_website(lead: Dict[str, Any]) -> bool:
    """Mirror precheck email/website root comparison (no side effects)."""
    email = (lead.get("email") or "").strip()
    website = (lead.get("website") or "").strip()
    if not email or "@" not in email or not website:
        return True
    try:
        parsed = urlparse(website if website.startswith("http") else f"https://{website}")
        host = (parsed.netloc or website).lower().strip()
        if not host:
            return True
        email_domain = email.split("@")[-1].lower()
        email_root = _extract_root_domain(email_domain)
        site_root = _extract_root_domain(host)
        return email_root == site_root
    except Exception:
        return True


def infer_source_type(source_url: str) -> str:
    if not (source_url or "").strip():
        return "unknown"
    low = source_url.lower()
    if any(seg in low for seg in ("/team", "/people", "/staff", "/leadership")):
        return "team_page"
    if "/about" in low or "/company" in low:
        return "about_page"
    if "/contact" in low:
        return "contact_page"
    if "linkedin.com" in low:
        return "linkedin"
    return "search_result"


def normalize_legacy_lead_shape(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    In-place enrichment of evidence fields. Safe to call multiple times.
    """
    website = (lead.get("website") or "").strip()
    source_url = (lead.get("source_url") or "").strip()
    full_name = (lead.get("full_name") or "").strip()
    first = (lead.get("first") or "").strip()
    last = (lead.get("last") or "").strip()
    role = (lead.get("role") or "").strip()
    email = (lead.get("email") or "").strip()

    site_host = _host_from_url(website) or _host_from_url(source_url)
    lead["company_domain_canonical"] = site_host

    urls: List[str] = []
    if source_url:
        urls.append(source_url)
    for k in ("email_source_url", "linkedin_source_url"):
        v = (lead.get(k) or "").strip()
        if v and v not in urls:
            urls.append(v)
    existing = lead.get("source_urls")
    if isinstance(existing, list):
        for u in existing:
            if isinstance(u, str) and u.strip() and u.strip() not in urls:
                urls.append(u.strip())
    lead["source_urls"] = urls
    lead["source_type"] = infer_source_type(source_url)
    lead["title_raw"] = lead.get("title_raw") or role

    if not full_name and first:
        full_name = f"{first} {last}".strip()
        if full_name:
            lead["full_name"] = full_name
    parts = [p for p in (lead.get("full_name") or "").split() if p.strip()]
    if len(parts) >= 2:
        lead["name_quality"] = "full"
    elif len(parts) == 1 or (first and not last):
        lead["name_quality"] = "first_only"
    elif not parts and not first:
        lead["name_quality"] = "missing"
    else:
        lead["name_quality"] = "full" if last else "first_only"

    lead["email_observed"] = lead.get("email_observed") or email
    if email and source_url and not (lead.get("email_source_url") or "").strip():
        lead["email_source_url"] = source_url

    if (lead.get("linkedin") or "").strip() and not (lead.get("linkedin_source_url") or "").strip():
        lead["linkedin_source_url"] = source_url or ""

    wh = _host_from_url(website)
    sh = _host_from_url(source_url)
    lead["person_found_on_company_site"] = bool(
        wh and sh and (wh == sh or sh.endswith("." + wh) or wh.endswith("." + sh))
    )

    lead["email_type"] = (
        "generic"
        if is_generic_email(email)
        else ("direct" if "@" in email else "unknown")
    )
    lead["email_domain_match"] = email_domain_matches_website(lead)
    lead["source_count"] = len(lead["source_urls"])
    lead.setdefault("identity_conflict", False)
    lead.setdefault("source_freshness_days", 0)
    lead.setdefault("account_only_signal", False)

    return lead


def apply_email_classification(lead: Dict[str, Any]) -> None:
    """Refresh email_type and domain match after enrichment mutates email/website."""
    email = (lead.get("email") or "").strip()
    lead["email_type"] = (
        "generic"
        if is_generic_email(email)
        else ("direct" if "@" in email else "unknown")
    )
    lead["email_domain_match"] = email_domain_matches_website(lead)
