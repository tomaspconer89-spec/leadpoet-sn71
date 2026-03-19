"""
Miner-side pre-validation to avoid gateway and validator rejections.

Run these checks before submitting a lead so we don't consume submission
attempts or rejection quota on leads that would fail at the gateway or
validator. Enable with USE_LEAD_PRECHECK=1.

All checks are local (no external API). See docs/AVOID-REJECTIONS.md.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

# -----------------------------------------------------------------------------
# Constants (must stay in sync with gateway / validator)
# -----------------------------------------------------------------------------

REQUIRED_FIELDS = [
    "business", "full_name", "first", "last", "email", "role", "website",
    "industry", "sub_industry", "country", "city",
    "linkedin", "company_linkedin", "source_url", "description", "employee_count",
]

VALID_EMPLOYEE_COUNTS = [
    "0-1", "2-10", "11-50", "51-200", "201-500",
    "501-1,000", "1,001-5,000", "5,001-10,000", "10,001+",
]

GENERAL_PURPOSE_PREFIXES = [
    "info@", "hello@", "owner@", "ceo@", "founder@", "contact@", "support@",
    "team@", "admin@", "office@", "mail@", "connect@", "help@", "hi@",
]

FREE_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "yahoo.co.uk", "yahoo.fr",
    "yahoo.co.in", "yahoo.co.jp", "outlook.com", "hotmail.com", "live.com",
    "msn.com", "aol.com", "mail.com", "protonmail.com", "proton.me",
    "icloud.com", "me.com", "mac.com", "zoho.com", "yandex.com",
    "gmx.com", "gmx.net", "mail.ru", "qq.com", "163.com", "126.com",
    "foxmail.com", "sina.com", "rediffmail.com", "tutanota.com",
    "web.de", "t-online.de", "wanadoo.fr", "naver.com", "daum.net",
    "hanmail.net", "139.com", "sohu.com", "aliyun.com",
}

# Name: no credentials/suffixes in first/last/full_name (gateway blocklist)
NAME_BLOCKLIST = {
    "ii", "iv", "jr", "sr", "dr", "mr", "mrs", "ms", "prof",
    "phd", "mba", "rn", "cpa", "esq", "dds", "np",
    "lcsw", "pmp", "cfa", "cfp", "cissp", "sphr", "scp",
}

MIN_ROLE_LEN = 2
MAX_ROLE_LEN = 80
MIN_DESC_LEN = 20
MIN_NAME_MATCH_LEN = 3

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def _get(lead: Dict[str, Any], key: str, default: str = "") -> str:
    v = lead.get(key, default)
    return (v or "").strip() if isinstance(v, str) else str(v or "").strip()


def _industry_taxonomy() -> Dict[str, Any]:
    try:
        from validator_models.industry_taxonomy import INDUSTRY_TAXONOMY
        return INDUSTRY_TAXONOMY
    except Exception:
        return {}


def _check_required_fields(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    missing = [f for f in REQUIRED_FIELDS if not _get(lead, f)]
    if missing:
        return False, f"missing_required_fields: {', '.join(missing)}"
    return True, None


def _check_name_sanity(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    bad_chars = re.compile(r"[,.()\[\]{}0-9]")
    allcaps = re.compile(r"\b[A-Z]{3,}\b")
    for field in ("first", "last", "full_name"):
        val = _get(lead, field)
        if bad_chars.search(val):
            return False, f"name_invalid_chars: {field} contains invalid characters"
        if allcaps.search(val):
            words = val.lower().split()
            for w in words:
                if w in NAME_BLOCKLIST:
                    return False, f"name_credential: {field} contains '{w}'"
    return True, None


def _check_email_format(email: str) -> bool:
    return bool(email and EMAIL_RE.match(email))


def _check_name_email_match(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    email = _get(lead, "email")
    first = _get(lead, "first")
    last = _get(lead, "last")
    if not email or "@" not in email:
        return False, "no_email"
    if not first or not last:
        return False, "missing_first_or_last_name"
    local = email.split("@")[0].lower()
    first_n = re.sub(r"[^a-z0-9]", "", first.lower())
    last_n = re.sub(r"[^a-z0-9]", "", last.lower())
    local_n = re.sub(r"[^a-z0-9]", "", local)
    patterns = []
    if len(first_n) >= MIN_NAME_MATCH_LEN:
        patterns.append(first_n)
    if len(last_n) >= MIN_NAME_MATCH_LEN:
        patterns.append(last_n)
    patterns.append(f"{first_n}{last_n}")
    if first_n:
        patterns.append(f"{first_n[0]}{last_n}")
        patterns.append(f"{last_n}{first_n[0]}")
    patterns = [p for p in patterns if p and len(p) >= MIN_NAME_MATCH_LEN]
    if any(p in local_n for p in patterns):
        return True, None
    if len(local_n) >= MIN_NAME_MATCH_LEN:
        if len(first_n) >= len(local_n) and first_n.startswith(local_n):
            return True, None
        if len(last_n) >= len(local_n) and last_n.startswith(local_n):
            return True, None
        for length in range(MIN_NAME_MATCH_LEN, min(len(first_n) + 1, 7)):
            if first_n[:length] == local_n or first_n[:length] in local_n:
                return True, None
        for length in range(MIN_NAME_MATCH_LEN, min(len(last_n) + 1, 7)):
            if last_n[:length] == local_n or last_n[:length] in local_n:
                return True, None
    return False, "name_not_in_email"


def _check_general_purpose_email(email: str) -> Tuple[bool, Optional[str]]:
    if not email:
        return False, "no_email"
    el = email.lower()
    for prefix in GENERAL_PURPOSE_PREFIXES:
        if el.startswith(prefix):
            return False, f"general_purpose_email:{prefix}"
    return True, None


def _check_free_email_domain(email: str) -> Tuple[bool, Optional[str]]:
    if not email or "@" not in email:
        return False, "no_email"
    domain = email.split("@")[-1].lower()
    if domain in FREE_EMAIL_DOMAINS:
        return False, f"free_email_domain:{domain}"
    return True, None


def _check_employee_count(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    ec = _get(lead, "employee_count")
    if ec not in VALID_EMPLOYEE_COUNTS:
        return False, f"invalid_employee_count:'{ec}'"
    return True, None


def _check_industry_sub_industry(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    taxonomy = _industry_taxonomy()
    if not taxonomy:
        return True, None  # skip if we can't load
    sub = _get(lead, "sub_industry")
    ind = _get(lead, "industry")
    if not sub:
        return False, "missing_sub_industry"
    sub_key = None
    for k in taxonomy.keys():
        if k.lower() == sub.lower():
            sub_key = k
            break
    if not sub_key:
        return False, f"invalid_sub_industry:'{sub}'"
    valid_industries = taxonomy[sub_key].get("industries", [])
    if not ind:
        return False, "missing_industry"
    if ind not in valid_industries:
        ind_lower = ind.lower()
        if not any(i.lower() == ind_lower for i in valid_industries):
            return False, f"invalid_industry_pairing:'{ind}' for sub_industry '{sub_key}'"
    return True, None


def _check_role_sanity(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    role = _get(lead, "role")
    if len(role) < MIN_ROLE_LEN:
        return False, "role_too_short"
    if len(role) > MAX_ROLE_LEN:
        return False, "role_too_long"
    if not any(c.isalpha() for c in role):
        return False, "role_no_letters"
    if "http" in role.lower() or "www." in role.lower():
        return False, "role_contains_url"
    return True, None


def _check_description(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    desc = _get(lead, "description")
    if len(desc) < MIN_DESC_LEN:
        return False, f"desc_too_short:{len(desc)}"
    return True, None


def _check_linkedin_urls(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    person = _get(lead, "linkedin").lower()
    company = _get(lead, "company_linkedin").lower()
    if "/in/" not in person:
        return False, "invalid_linkedin_url: person URL must contain /in/"
    if "/company/" not in company:
        return False, "invalid_company_linkedin: company URL must contain /company/"
    if "/in/" in company:
        return False, "invalid_company_linkedin: company URL must not contain /in/"
    return True, None


def _check_location(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    country = _get(lead, "country")
    state = _get(lead, "state")
    city = _get(lead, "city")
    if not country:
        return False, "country_empty"
    if not city:
        return False, "city_empty"
    us_like = country.lower() in ("united states", "usa", "us", "u.s.", "u.s.a.")
    if us_like and not state:
        return False, "state_empty_for_usa"
    return True, None


def _check_source_url(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    source_url = _get(lead, "source_url")
    source_type = _get(lead, "source_type")
    if source_type == "proprietary_database" and source_url != "proprietary_database":
        return False, "source_provenance_mismatch"
    if "linkedin" in source_url.lower():
        return False, "linkedin_not_allowed_in_source_url"
    return True, None


def _extract_root_domain(domain: str) -> str:
    domain = (domain or "").lower().strip()
    parts = domain.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return domain


def _check_email_domain_matches_website(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    email = _get(lead, "email")
    website = _get(lead, "website")
    if not email or "@" not in email or not website:
        return True, None
    from urllib.parse import urlparse
    try:
        parsed = urlparse(website if website.startswith("http") else f"https://{website}")
        host = (parsed.netloc or website).lower().strip()
        if not host:
            return True, None
        email_domain = email.split("@")[-1].lower()
        email_root = _extract_root_domain(email_domain)
        site_root = _extract_root_domain(host)
        if email_root != site_root:
            return False, "email_domain_mismatch"
    except Exception:
        pass
    return True, None


def precheck_lead(lead: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Run all local pre-validation checks. Returns (True, None) if pass,
    (False, reason_string) if the lead would likely be rejected.
    """
    checks = [
        _check_required_fields,
        _check_name_sanity,
        lambda L: (True, None) if _check_email_format(_get(L, "email")) else (False, "invalid_email_format"),
        _check_name_email_match,
        lambda L: _check_general_purpose_email(_get(L, "email")),
        lambda L: _check_free_email_domain(_get(L, "email")),
        _check_employee_count,
        _check_industry_sub_industry,
        _check_role_sanity,
        _check_description,
        _check_linkedin_urls,
        _check_location,
        _check_source_url,
        _check_email_domain_matches_website,
    ]
    for check in checks:
        ok, reason = check(lead)
        if not ok:
            return False, reason
    return True, None


def filter_leads_by_precheck(leads: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Tuple[Dict[str, Any], str]]]:
    """
    Split leads into those that pass precheck (to submit) and those that fail (with reason).
    Returns (passed_list, [(failed_lead, reason), ...]).
    """
    passed = []
    failed = []
    for lead in leads:
        ok, reason = precheck_lead(lead)
        if ok:
            passed.append(lead)
        else:
            failed.append((lead, reason or "precheck_failed"))
    return passed, failed
