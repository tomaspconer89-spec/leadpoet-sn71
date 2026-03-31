#!/usr/bin/env python3
"""
Convert raw crawl artifacts (e.g. lead_queue/raw_generated or
lead_queue/raw_generated_fresh_only) into SN71 payloads, run precheck,
and write valid leads into lead_queue/pending for submit_queued_leads.py.

To submit from raw_generated_fresh_only without touching those files, use:
  scripts/submit_raw_generated_readonly.py (or scripts/submit-from-raw-generated-fresh-only.sh).

This script only builds lead_queue/pending when you want on-disk queue copies.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from miner_models.lead_precheck import precheck_lead
from validator_models.industry_taxonomy import INDUSTRY_TAXONOMY

_REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_REPO_ROOT / ".env")


VALID_EMPLOYEE_COUNTS = {
    "0-1", "2-10", "11-50", "51-200", "201-500",
    "501-1,000", "1,001-5,000", "5,001-10,000", "10,001+",
}

GENERIC_EMAIL_PREFIXES = {
    "info", "hello", "contact", "support", "admin", "office", "team",
    "sales", "enquiries", "inquiries", "mail", "help", "hi",
}


def normalize_employee_count(raw: str) -> str:
    v = (raw or "").strip()
    if v in VALID_EMPLOYEE_COUNTS:
        return v
    low = v.lower()
    if low in {">1", "1-10"}:
        return "2-10"
    if low in {">10", "10-50"}:
        return "11-50"
    if low in {">50", "50-200"}:
        return "51-200"
    if low in {">200", "200-500"}:
        return "201-500"
    if low in {">500", "500-1000"}:
        return "501-1,000"
    if low in {">1000", "1000-5000"}:
        return "1,001-5,000"
    if low in {">5000", "5000-10000"}:
        return "5,001-10,000"
    if low in {">10000", "10000+"}:
        return "10,001+"
    return "11-50"


def split_name(full_name: str) -> Tuple[str, str]:
    name = " ".join((full_name or "").strip().split())
    if not name:
        return "", ""
    parts = name.split(" ")
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def is_generic_email(email: str) -> bool:
    e = (email or "").strip().lower()
    if "@" not in e:
        return True
    local = e.split("@", 1)[0]
    return local in GENERIC_EMAIL_PREFIXES


def score_person(candidate: Dict[str, Any]) -> int:
    """
    Higher score = better chance of passing precheck.
    Prefer person-like (non-generic) email + full name.
    """
    if not isinstance(candidate, dict):
        return -1
    email = (candidate.get("email") or "").strip()
    name = (candidate.get("name") or "").strip()
    first, last = split_name(name)
    score = 0
    if email and "@" in email:
        score += 1
        if not is_generic_email(email):
            score += 4
    if name:
        score += 1
    if first and last:
        score += 2
    if candidate.get("decision_maker") is True:
        score += 1
    return score


def name_matches_email(name: str, email: str) -> bool:
    n = " ".join((name or "").strip().lower().split())
    e = (email or "").strip().lower()
    if not n or "@" not in e:
        return False
    local = re.sub(r"[^a-z0-9]", "", e.split("@", 1)[0])
    parts = [re.sub(r"[^a-z0-9]", "", p) for p in n.split(" ") if p.strip()]
    if not parts:
        return False
    first = parts[0]
    last = parts[-1] if len(parts) > 1 else ""
    patterns = [first, last, f"{first}{last}"]
    if first and last:
        patterns.extend([f"{first[0]}{last}", f"{last}{first[0]}"])
    patterns = [p for p in patterns if len(p) >= 3]
    return any(p in local for p in patterns)


def pick_best_person(team: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not team:
        return {}
    valid = [t for t in team if isinstance(t, dict)]
    if not valid:
        return {}
    matched = [
        t for t in valid
        if name_matches_email((t.get("name") or "").strip(), (t.get("email") or "").strip())
    ]
    if matched:
        return max(matched, key=score_person)
    return max(valid, key=score_person)


def normalize_sub_industry_and_industry(sub_industry: str, industry: str) -> Tuple[str, str]:
    sub = (sub_industry or "").strip()
    ind = (industry or "").strip()
    if not sub:
        return sub, ind

    # Exact key match first
    exact_key = next((k for k in INDUSTRY_TAXONOMY if k.lower() == sub.lower()), None)
    if exact_key:
        valid_inds = INDUSTRY_TAXONOMY[exact_key].get("industries", [])
        if valid_inds and not any(i.lower() == ind.lower() for i in valid_inds):
            ind = valid_inds[0]
        return exact_key, ind

    # Fuzzy contains match for common variants (e.g. "Startup Advisory Services")
    sub_low = sub.lower()
    contains_key = next(
        (k for k in INDUSTRY_TAXONOMY if sub_low in k.lower() or k.lower() in sub_low),
        None,
    )
    if contains_key:
        valid_inds = INDUSTRY_TAXONOMY[contains_key].get("industries", [])
        if valid_inds and not any(i.lower() == ind.lower() for i in valid_inds):
            ind = valid_inds[0]
        return contains_key, ind

    # Heuristic fallback for advisory/consulting phrases to taxonomy-safe value.
    if any(tok in sub_low for tok in ("advisory", "consult", "consulting", "advisor")):
        fallback = "Management Consulting"
        valid_inds = INDUSTRY_TAXONOMY[fallback].get("industries", [])
        if valid_inds:
            ind = valid_inds[0]
        return fallback, ind

    return sub, ind


def parse_hq_location(hq: str) -> Tuple[str, str, str]:
    """
    Parse "City, State, Country" into (city, state, country).
    """
    txt = (hq or "").strip()
    if not txt:
        return "", "", ""
    parts = [p.strip() for p in txt.split(",") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], parts[-1]
    if len(parts) == 2:
        city, second = parts[0], parts[1]
        second_low = second.strip().lower()
        us_state_tokens = {
            # abbreviations
            "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id",
            "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms",
            "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
            "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv",
            "wi", "wy", "dc",
            # common state names
            "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
            "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
            "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
            "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
            "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
            "new mexico", "new york", "north carolina", "north dakota", "ohio",
            "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
            "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
            "washington", "west virginia", "wisconsin", "wyoming",
        }
        if second_low in us_state_tokens:
            return city, second, "United States"
        return city, "", second
    return "", "", parts[0]


def normalize_country_name(country: str) -> str:
    c = (country or "").strip()
    low = c.lower()
    if low in {"us", "usa", "u.s.", "u.s.a.", "united states of america"}:
        return "United States"
    if low in {"uae", "u.a.e."}:
        return "United Arab Emirates"
    if low in {"uk", "u.k."}:
        return "United Kingdom"
    return c


def _clean_and_cap_description(desc: str, max_len: int = 600) -> str:
    # Keep descriptions readable and compact for queue/submit workflows.
    txt = re.sub(r"\s+", " ", (desc or "").strip())
    if len(txt) <= max_len:
        return txt
    clipped = txt[:max_len].rstrip(" ,;:-")
    # Prefer trimming at sentence boundary when available.
    cut = max(clipped.rfind(". "), clipped.rfind("; "), clipped.rfind(": "))
    if cut >= 120:
        clipped = clipped[: cut + 1].rstrip()
    return clipped


def ensure_min_description(description: str, company_name: str, industry: str, sub_industry: str, website: str) -> str:
    max_desc_len = int(os.getenv("LEAD_MAX_DESCRIPTION_LEN", "600") or "600")
    desc = _clean_and_cap_description(description, max_len=max_desc_len)
    if len(desc) >= 70:
        return desc
    name = (company_name or "This company").strip()
    ind = (industry or "business services").strip()
    sub = (sub_industry or "advisory services").strip()
    site = (website or "").strip()
    fallback = (
        f"{name} operates in {ind} with a focus on {sub}, serving clients through its primary website {site}."
    )
    if len(fallback) < 70:
        fallback += " Contact and company profile details were sourced from public business pages."
    return _clean_and_cap_description(fallback, max_len=max_desc_len)


def clean_linkedin(url: str, kind: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not u.startswith("http"):
        u = f"https://{u.lstrip('/')}"
    if "linkedin.com" not in u.lower():
        return ""
    if kind == "person":
        return u if "/in/" in u else ""
    return u if "/company/" in u and "/in/" not in u else ""


def extract_domain(url: str) -> str:
    u = (url or "").strip().lower()
    if not u:
        return ""
    if not u.startswith("http"):
        u = f"https://{u}"
    try:
        host = urllib.parse.urlparse(u).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


def http_json(url: str, method: str = "GET", headers: Optional[Dict[str, str]] = None, body: Optional[bytes] = None) -> Dict[str, Any]:
    req = urllib.request.Request(url=url, method=method, headers=headers or {}, data=body)
    with urllib.request.urlopen(req, timeout=12) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)


def _collect_linkedin_urls(obj: Any, out: List[str]) -> None:
    if isinstance(obj, dict):
        for v in obj.values():
            _collect_linkedin_urls(v, out)
        return
    if isinstance(obj, list):
        for v in obj:
            _collect_linkedin_urls(v, out)
        return
    if isinstance(obj, str):
        s = obj.strip()
        if "linkedin.com/" in s.lower():
            out.append(s)


def apify_search_urls(query: str) -> List[str]:
    token = os.getenv("APIFY_API_TOKEN", "").strip()
    if not token:
        return []
    actor_id = os.getenv("APIFY_SEARCH_ACTOR_ID", "apify/google-search-scraper").strip()
    if "/" in actor_id and "~" not in actor_id:
        actor_id = actor_id.replace("/", "~", 1)
    run_url = (
        "https://api.apify.com/v2/acts/"
        f"{urllib.parse.quote(actor_id, safe='~')}/run-sync-get-dataset-items"
        f"?token={urllib.parse.quote(token)}"
    )
    payload = {
        # google-search-scraper expects queries as a string
        "queries": query,
        "resultsPerPage": 10,
        "maxPagesPerQuery": 1,
        "mobileResults": False,
    }
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    try:
        data = http_json(run_url, method="POST", headers=headers, body=json.dumps(payload).encode("utf-8"))
        urls: List[str] = []
        _collect_linkedin_urls(data, urls)
        deduped: List[str] = []
        seen = set()
        for u in urls:
            if u not in seen:
                seen.add(u)
                deduped.append(u)
        return deduped
    except Exception:
        return []


def apify_search_items(query: str) -> List[Dict[str, Any]]:
    """Return raw Apify search result items for richer enrichment signals."""
    token = os.getenv("APIFY_API_TOKEN", "").strip()
    if not token:
        return []
    actor_id = os.getenv("APIFY_SEARCH_ACTOR_ID", "apify/google-search-scraper").strip()
    if "/" in actor_id and "~" not in actor_id:
        actor_id = actor_id.replace("/", "~", 1)
    run_url = (
        "https://api.apify.com/v2/acts/"
        f"{urllib.parse.quote(actor_id, safe='~')}/run-sync-get-dataset-items"
        f"?token={urllib.parse.quote(token)}"
    )
    payload = {
        "queries": query,
        "resultsPerPage": 10,
        "maxPagesPerQuery": 1,
        "mobileResults": False,
    }
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    try:
        data = http_json(
            run_url,
            method="POST",
            headers=headers,
            body=json.dumps(payload).encode("utf-8"),
        )
    except Exception:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _apify_run_sync_items(actor_id: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    token = os.getenv("APIFY_API_TOKEN", "").strip()
    aid = (actor_id or "").strip()
    if not token or not aid:
        return []
    if "/" in aid and "~" not in aid:
        aid = aid.replace("/", "~", 1)
    run_url = (
        "https://api.apify.com/v2/acts/"
        f"{urllib.parse.quote(aid, safe='~')}/run-sync-get-dataset-items"
        f"?token={urllib.parse.quote(token)}"
    )
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    try:
        data = http_json(
            run_url,
            method="POST",
            headers=headers,
            body=json.dumps(payload).encode("utf-8"),
        )
    except Exception:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def apify_linkedin_person_profile_items(linkedin_person_url: str) -> List[Dict[str, Any]]:
    """Run configured Apify LinkedIn person profile actor and return dataset items."""
    url = (linkedin_person_url or "").strip()
    if "/in/" not in url.lower():
        return []
    actor_id = os.getenv("APIFY_LINKEDIN_PERSON_ACTOR_ID", "").strip()
    if not actor_id:
        return []
    payload_candidates = [
        {"profileUrls": [url]},
        {"linkedinUrls": [url]},
        {"profile_urls": [url]},
        {"startUrls": [{"url": url}]},
        {"url": url},
    ]
    for payload in payload_candidates:
        items = _apify_run_sync_items(actor_id, payload)
        if items:
            return items
    return []


def apify_linkedin_company_profile_items(
    company_linkedin_urls: List[str],
) -> List[Dict[str, Any]]:
    """
    Run configured Apify LinkedIn company profile actor.
    If the actor supports bulk input, this call fetches all company URLs at once.
    """
    urls = [u.strip() for u in company_linkedin_urls if "/company/" in (u or "").lower()]
    if not urls:
        return []
    actor_id = os.getenv("APIFY_LINKEDIN_COMPANY_ACTOR_ID", "").strip()
    if not actor_id:
        return []
    payload_candidates = [
        {"companyUrls": urls},
        {"linkedinCompanyUrls": urls},
        {"profileUrls": urls},
        {"startUrls": [{"url": u} for u in urls]},
    ]
    for payload in payload_candidates:
        items = _apify_run_sync_items(actor_id, payload)
        if items:
            return items
    return []


def _pick_first_nonempty_str(obj: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _collect_values_by_keys(obj: Any, keys: set[str], out: List[str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys and isinstance(v, str) and v.strip():
                out.append(v.strip())
            _collect_values_by_keys(v, keys, out)
        return
    if isinstance(obj, list):
        for v in obj:
            _collect_values_by_keys(v, keys, out)


def _pick_first_nested_str(obj: Any, keys: List[str]) -> str:
    vals: List[str] = []
    _collect_values_by_keys(obj, set(keys), vals)
    return vals[0] if vals else ""


def _extract_location_from_value(loc: str) -> Tuple[str, str, str]:
    txt = (loc or "").strip()
    if not txt:
        return "", "", ""
    parts = [p.strip() for p in txt.split(",") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], normalize_country_name(parts[2])
    if len(parts) == 2:
        # "City, State" is most common US shorthand in profile snippets.
        state = parts[1]
        country = "United States" if len(state) == 2 else ""
        return parts[0], state, country
    return parts[0], "", ""


def _collect_text_blobs(obj: Any, out: List[str]) -> None:
    if isinstance(obj, dict):
        for v in obj.values():
            _collect_text_blobs(v, out)
        return
    if isinstance(obj, list):
        for v in obj:
            _collect_text_blobs(v, out)
        return
    if isinstance(obj, str):
        s = " ".join(obj.strip().split())
        if s:
            out.append(s)


def _name_from_linkedin_slug(url: str) -> Tuple[str, str, str]:
    m = re.search(r"linkedin\.com/in/([^/?#]+)", (url or "").strip(), flags=re.I)
    if not m:
        return "", "", ""
    slug = re.sub(r"[-_]+", " ", m.group(1)).strip()
    parts = [p for p in slug.split() if p and p.isalpha()]
    if len(parts) >= 2:
        first = parts[0].title()
        last = " ".join(parts[1:]).title()
        return f"{first} {last}", first, last
    if len(parts) == 1:
        first = parts[0].title()
        return first, first, ""
    return "", "", ""


def _extract_email_from_blobs(blobs: List[str]) -> str:
    for b in blobs:
        m = re.search(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}\b", b)
        if m:
            e = m.group(0).strip().lower()
            if not is_generic_email(e):
                return e
    return ""


def _extract_location_from_blobs(blobs: List[str]) -> Tuple[str, str, str]:
    # Patterns like "Location: Austin, TX, United States"
    for b in blobs:
        m = re.search(
            r"\bLocation[:\s]+([A-Z][A-Za-z .'-]{1,60}),\s*([A-Z]{2}|[A-Z][A-Za-z .'-]{2,30})(?:,\s*([A-Z][A-Za-z .'-]{2,40}))?",
            b,
        )
        if m:
            city = m.group(1).strip(" ,")
            state = m.group(2).strip(" ,")
            country = normalize_country_name((m.group(3) or "").strip() or ("United States" if len(state) == 2 else ""))
            return city, state, country
    # Patterns like "... Based in New Jersey ..."
    for b in blobs:
        m = re.search(r"\b[Bb]ased in ([A-Z][A-Za-z .'-]{2,40})\b", b)
        if m:
            city = m.group(1).strip(" ,")
            return city, "", "United States"
    return "", "", ""


def _extract_role_from_blobs(blobs: List[str]) -> str:
    role_rx = re.compile(
        r"\b(Founder|Co[- ]Founder|CEO|Chief [A-Za-z ]{2,30}|Managing Director|Partner|Principal|Owner|President|Vice President|VP [A-Za-z ]{2,30})\b",
        re.I,
    )
    for b in blobs:
        m = role_rx.search(b)
        if m:
            return m.group(1).strip()
    return ""


def _extract_non_linkedin_website_from_blobs(blobs: List[str]) -> str:
    url_rx = re.compile(r"https?://[a-z0-9.-]+\.[a-z]{2,24}(?:/[^\s\"'<>]*)?", re.I)
    for b in blobs:
        for u in url_rx.findall(b):
            lu = u.lower()
            if "linkedin.com/" in lu:
                continue
            return u
    return ""


def _scrapingdog_google_json(query: str) -> Dict[str, Any]:
    key = os.getenv("SCRAPINGDOG_API_KEY", "").strip()
    if not key:
        return {}
    try:
        data = http_json(
            "https://api.scrapingdog.com/google"
            f"?api_key={urllib.parse.quote(key)}"
            f"&query={urllib.parse.quote(query)}"
            "&results=20&country=us&page=0"
        )
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _scrapingdog_linkedin_profile_json(linkedin_person_url: str) -> Dict[str, Any]:
    key = os.getenv("SCRAPINGDOG_API_KEY", "").strip()
    if not key or "/in/" not in (linkedin_person_url or "").lower():
        return {}
    m = re.search(r"linkedin\.com/in/([^/?#]+)", linkedin_person_url, re.I)
    if not m:
        return {}
    slug = m.group(1).strip().rstrip("/")
    if not slug:
        return {}
    q = urllib.parse.urlencode(
        {
            "api_key": key,
            "type": "profile",
            "id": slug,
        }
    )
    try:
        data = http_json(f"https://api.scrapingdog.com/profile?{q}")
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    msg = str(data.get("message") or data.get("error") or "").lower()
    if msg and ("invalid" in msg or "api key" in msg or "credit" in msg):
        return {}
    return data


def _looks_low_quality(value: str) -> bool:
    s = (value or "").strip()
    if not s:
        return True
    low = s.lower()
    if len(s) > 120:
        return True
    if "..." in s:
        return True
    if low in {"unknown", "n/a", "not listed", "-", "na"}:
        return True
    if "tap " in low and "location" in low:
        return True
    if "founder of" in low and "based in" in low:
        return True
    return False


def _extract_person_name_from_blobs(blobs: List[str]) -> Tuple[str, str, str]:
    # e.g. "Joan Nowak - Hybrid Business Advisors | LinkedIn"
    rx = re.compile(
        r"\b([A-Z][a-z]{1,24})\s+([A-Z][a-z]{1,24})\s*-\s*.{1,140}(?:\|\s*LinkedIn|LinkedIn)\b"
    )
    for b in blobs:
        m = rx.search(b)
        if not m:
            continue
        first = m.group(1).strip()
        last = m.group(2).strip()
        if first.lower() == last.lower():
            continue
        return f"{first} {last}", first, last
    return "", "", ""


def _extract_employee_count_from_blobs(blobs: List[str]) -> str:
    for b in blobs:
        m = re.search(r"\b(\d[\d,]{0,5})\s*\+\s*employees?\b", b, re.I)
        if m:
            n = int(m.group(1).replace(",", ""))
            if n >= 10001:
                return "10,001+"
            if n >= 5001:
                return "5,001-10,000"
            if n >= 1001:
                return "1,001-5,000"
            if n >= 501:
                return "501-1,000"
            if n >= 201:
                return "201-500"
            if n >= 51:
                return "51-200"
            if n >= 11:
                return "11-50"
            if n >= 2:
                return "2-10"
            return "0-1"
        m2 = re.search(
            r"\b(0-1|1-10|2-10|11-50|51-200|201-500|501-1,?000|1,?001-5,?000|5,?001-10,?000|10,?001\+)\b",
            b,
            re.I,
        )
        if m2:
            return normalize_employee_count(m2.group(1).replace(" ", ""))
    return ""


def _company_name_from_company_linkedin(url: str) -> str:
    m = re.search(r"linkedin\.com/company/([^/?#]+)", (url or "").strip(), re.I)
    if not m:
        return ""
    slug = re.sub(r"[-_]+", " ", m.group(1)).strip()
    return " ".join(p.capitalize() for p in slug.split())


def scrapingdog_search_urls(query: str) -> List[str]:
    key = os.getenv("SCRAPINGDOG_API_KEY", "").strip()
    if not key:
        return []
    try:
        data = http_json(
            "https://api.scrapingdog.com/google"
            f"?api_key={urllib.parse.quote(key)}"
            f"&query={urllib.parse.quote(query)}"
            "&results=20&country=us&page=0"
        )
        urls: List[str] = []
        _collect_linkedin_urls(data, urls)
        deduped: List[str] = []
        seen = set()
        for u in urls:
            if u not in seen:
                seen.add(u)
                deduped.append(u)
        return deduped
    except Exception:
        return []


def search_urls(query: str) -> List[str]:
    """
    Query search provider(s) and return top URLs.
    Priority: Apify -> (optional) ScrapingDog -> Serper -> Brave -> GSE.
    ScrapingDog is used only when USE_SCRAPINGDOG_ENRICHMENT=1.
    """
    urls: List[str] = []
    serper_key = os.getenv("SERPER_API_KEY", "").strip()
    brave_key = os.getenv("BRAVE_API_KEY", "").strip()
    gse_key = os.getenv("GSE_API_KEY", "").strip()
    gse_cx = os.getenv("GSE_CX", "").strip()
    force_scrapingdog = (
        os.getenv("FORCE_SCRAPINGDOG_ENRICHMENT", "0").strip() == "1"
    )
    if force_scrapingdog:
        scrapingdog_urls = scrapingdog_search_urls(query)
        if scrapingdog_urls:
            return scrapingdog_urls
    apify_urls = apify_search_urls(query)
    if apify_urls:
        return apify_urls
    use_scrapingdog = os.getenv("USE_SCRAPINGDOG_ENRICHMENT", "0").strip() == "1"
    if use_scrapingdog:
        scrapingdog_urls = scrapingdog_search_urls(query)
        if scrapingdog_urls:
            return scrapingdog_urls

    try:
        if serper_key:
            payload = json.dumps({"q": query, "num": 10, "page": 1}).encode("utf-8")
            data = http_json(
                "https://google.serper.dev/search",
                method="POST",
                headers={"X-API-KEY": serper_key, "Content-Type": "application/json"},
                body=payload,
            )
            for item in data.get("organic", []):
                u = (item.get("link") or "").strip()
                if u:
                    urls.append(u)
            return urls
    except Exception:
        pass

    try:
        if brave_key:
            q = urllib.parse.quote(query)
            data = http_json(
                f"https://api.search.brave.com/res/v1/web/search?q={q}&count=10&offset=0",
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip",
                    "X-Subscription-Token": brave_key,
                },
            )
            for item in data.get("web", {}).get("results", []):
                u = (item.get("url") or "").strip()
                if u:
                    urls.append(u)
            return urls
    except Exception:
        pass

    try:
        if gse_key and gse_cx:
            q = urllib.parse.quote(query)
            data = http_json(
                f"https://www.googleapis.com/customsearch/v1?key={gse_key}&cx={gse_cx}&q={q}&start=1&num=10"
            )
            for item in data.get("items", []):
                u = (item.get("link") or "").strip()
                if u:
                    urls.append(u)
            return urls
    except Exception:
        pass

    return urls


def enrich_linkedin_fields(
    lead: Dict[str, Any],
    *,
    enrich_debug: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Fill missing LinkedIn fields and opportunistically enrich other lead fields
    (name/role/location/email/website) from Apify search evidence.

    If ``enrich_debug`` is a dict, it is filled with Apify raw search items and
    query metadata for offline inspection (used by collect_leads_precheck_only).
    """
    business = (lead.get("business") or "").strip()
    website = (lead.get("website") or "").strip()
    domain = extract_domain(website)
    person_name = (lead.get("full_name") or "").strip()
    company_queries = [
        f"site:linkedin.com/company {business}",
        f"{business} linkedin company",
        f"site:linkedin.com/company {domain}",
    ]
    person_queries: List[str] = []
    if person_name and person_name.lower() not in {"not listed", "unknown", "n/a"}:
        person_queries.append(f'site:linkedin.com/in "{person_name}" "{business}"')
        person_queries.append(f'site:linkedin.com/in "{person_name}" "{domain}"')
    person_queries.append(f'site:linkedin.com/in "{business}" founder')
    person_queries.append(f'site:linkedin.com/in "{business}" ceo')
    all_queries = [q for q in (company_queries + person_queries) if q.strip()]

    # Prefer Apify raw items for richer evidence extraction.
    apify_items: List[Dict[str, Any]] = []
    for q in all_queries:
        apify_items.extend(apify_search_items(q))
    apify_blobs: List[str] = []
    apify_linkedin_urls: List[str] = []
    for item in apify_items:
        _collect_text_blobs(item, apify_blobs)
        _collect_linkedin_urls(item, apify_linkedin_urls)

    if not lead.get("company_linkedin"):
        for u in apify_linkedin_urls:
            cu = clean_linkedin(u, "company")
            if cu:
                lead["company_linkedin"] = cu
                break
        for q in company_queries:
            for u in search_urls(q):
                cu = clean_linkedin(u, "company")
                if cu:
                    lead["company_linkedin"] = cu
                    break
            if lead.get("company_linkedin"):
                break

    if not lead.get("linkedin"):
        for u in apify_linkedin_urls:
            pu = clean_linkedin(u, "person")
            if pu:
                lead["linkedin"] = pu
                break
        for q in person_queries:
            for u in search_urls(q):
                pu = clean_linkedin(u, "person")
                if pu:
                    lead["linkedin"] = pu
                    break
            if lead.get("linkedin"):
                break

    # Build broader evidence corpus: Apify search + optional Apify profile scrapers.
    evidence_blobs: List[str] = list(apify_blobs)
    apify_person_profile_items: List[Dict[str, Any]] = []
    apify_company_profile_items: List[Dict[str, Any]] = []
    if lead.get("linkedin"):
        apify_person_profile_items = apify_linkedin_person_profile_items(
            str(lead.get("linkedin") or "")
        )
        for item in apify_person_profile_items:
            _collect_text_blobs(item, evidence_blobs)
    if lead.get("company_linkedin"):
        apify_company_profile_items = apify_linkedin_company_profile_items(
            [str(lead.get("company_linkedin") or "")]
        )
        for item in apify_company_profile_items:
            _collect_text_blobs(item, evidence_blobs)

    # Additional non-linkedin enrichment from LinkedIn URL + evidence corpus.
    if lead.get("linkedin"):
        full, first, last = _name_from_linkedin_slug(str(lead.get("linkedin") or ""))
        if _looks_low_quality(str(lead.get("full_name") or "")) and full:
            lead["full_name"] = full
        if _looks_low_quality(str(lead.get("first") or "")) and first:
            lead["first"] = first
        if _looks_low_quality(str(lead.get("last") or "")) and last:
            lead["last"] = last

    if _looks_low_quality(str(lead.get("full_name") or "")) or _looks_low_quality(str(lead.get("last") or "")):
        f2, p2, l2 = _extract_person_name_from_blobs(evidence_blobs)
        if f2:
            lead["full_name"] = f2
        if p2:
            lead["first"] = p2
        if l2:
            lead["last"] = l2

    em = _extract_email_from_blobs(evidence_blobs)
    if not em and apify_person_profile_items:
        em = _pick_first_nonempty_str(
            apify_person_profile_items[0],
            ["email", "publicEmail", "workEmail", "businessEmail"],
        ).lower()
        if "@" not in em:
            em = ""
    cur_email = str(lead.get("email") or "").strip().lower()
    if em and (_looks_low_quality(cur_email) or is_generic_email(cur_email)):
        lead["email"] = em

    role = _extract_role_from_blobs(evidence_blobs)
    if not role and apify_person_profile_items:
        role = _pick_first_nested_str(
            apify_person_profile_items[0],
            ["headline", "title", "position", "jobTitle", "occupation"],
        )
    if role and _looks_low_quality(str(lead.get("role") or "")):
        lead["role"] = role

    city, state, country = _extract_location_from_blobs(evidence_blobs)
    if (not city or not country) and apify_person_profile_items:
        loc_hint = _pick_first_nested_str(
            apify_person_profile_items[0],
            ["location", "geoLocation", "cityStateCountry"],
        )
        c2, s2, co2 = _extract_location_from_value(loc_hint)
        city = city or c2
        state = state or s2
        country = country or co2
    if (not city or not country) and apify_company_profile_items:
        loc_hint = _pick_first_nested_str(
            apify_company_profile_items[0],
            ["location", "headquarters", "hqLocation", "address"],
        )
        c2, s2, co2 = _extract_location_from_value(loc_hint)
        city = city or c2
        state = state or s2
        country = country or co2
    if city and _looks_low_quality(str(lead.get("city") or "")):
        lead["city"] = city
    if state and _looks_low_quality(str(lead.get("state") or "")):
        lead["state"] = state
    if country and _looks_low_quality(str(lead.get("country") or "")):
        lead["country"] = normalize_country_name(country)
    if lead.get("country") and not lead.get("hq_country"):
        lead["hq_country"] = lead.get("country")
    if lead.get("state") and not lead.get("hq_state"):
        lead["hq_state"] = lead.get("state")
    if lead.get("city") and not lead.get("hq_city"):
        lead["hq_city"] = lead.get("city")

    if _looks_low_quality(str(lead.get("business") or "")):
        cn = _company_name_from_company_linkedin(str(lead.get("company_linkedin") or ""))
        if cn:
            lead["business"] = cn
    if apify_company_profile_items and _looks_low_quality(str(lead.get("business") or "")):
        nm = _pick_first_nonempty_str(
            apify_company_profile_items[0],
            ["name", "companyName", "company", "title"],
        )
        if not nm:
            nm = _pick_first_nested_str(
                apify_company_profile_items[0],
                ["companyName", "name", "company", "organization"],
            )
        if nm:
            lead["business"] = nm

    emp = _extract_employee_count_from_blobs(evidence_blobs)
    if emp and (_looks_low_quality(str(lead.get("employee_count") or "")) or str(lead.get("employee_count") or "") not in VALID_EMPLOYEE_COUNTS):
        lead["employee_count"] = normalize_employee_count(emp)
    if apify_company_profile_items and (
        _looks_low_quality(str(lead.get("employee_count") or ""))
        or str(lead.get("employee_count") or "") not in VALID_EMPLOYEE_COUNTS
    ):
        emp_hint = _pick_first_nonempty_str(
            apify_company_profile_items[0],
            ["employees", "employeeCount", "companySize", "staffCount"],
        )
        if not emp_hint:
            emp_hint = _pick_first_nested_str(
                apify_company_profile_items[0],
                ["employees", "employeeCount", "companySize", "staffCount"],
            )
        if emp_hint:
            lead["employee_count"] = normalize_employee_count(emp_hint)

    if _looks_low_quality(str(lead.get("website") or "")) or "linkedin.com/" in str(lead.get("website") or "").lower():
        w = _extract_non_linkedin_website_from_blobs(evidence_blobs)
        if w:
            lead["website"] = w
    if apify_company_profile_items and _looks_low_quality(str(lead.get("website") or "")):
        w2 = _pick_first_nonempty_str(
            apify_company_profile_items[0],
            ["website", "companyWebsite", "url"],
        )
        if not w2:
            w2 = _pick_first_nested_str(
                apify_company_profile_items[0],
                ["website", "companyWebsite", "url"],
            )
        if w2 and "linkedin.com/" not in w2.lower():
            lead["website"] = w2
    if not str(lead.get("source_url") or "").strip():
        lead["source_url"] = str(lead.get("website") or "")
    if _looks_low_quality(str(lead.get("description") or "")):
        lead["description"] = ensure_min_description(
            str(lead.get("description") or ""),
            str(lead.get("business") or ""),
            str(lead.get("industry") or ""),
            str(lead.get("sub_industry") or ""),
            str(lead.get("website") or ""),
        )
    if apify_company_profile_items and _looks_low_quality(str(lead.get("description") or "")):
        d2 = _pick_first_nonempty_str(
            apify_company_profile_items[0],
            ["description", "about", "summary", "headline"],
        )
        if not d2:
            d2 = _pick_first_nested_str(
                apify_company_profile_items[0],
                ["description", "about", "summary", "headline", "tagline"],
            )
        if d2:
            lead["description"] = ensure_min_description(
                d2,
                str(lead.get("business") or ""),
                str(lead.get("industry") or ""),
                str(lead.get("sub_industry") or ""),
                str(lead.get("website") or ""),
            )

    if enrich_debug is not None:
        enrich_debug.clear()
        enrich_debug.update(
            {
                "provider_primary": "apify_google_search",
                "queries": list(all_queries),
                "apify_items": apify_items,
                "apify_item_count": len(apify_items),
                "apify_linkedin_urls": list(apify_linkedin_urls),
                "apify_person_profile_items": apify_person_profile_items,
                "apify_company_profile_items": apify_company_profile_items,
                "resolved_company_linkedin": lead.get("company_linkedin"),
                "resolved_person_linkedin": lead.get("linkedin"),
            }
        )

    return lead


def _sync_location_fields(lead: Dict[str, Any]) -> None:
    # Keep base and HQ location fields coherent.
    if lead.get("hq_country") and not lead.get("country"):
        lead["country"] = lead["hq_country"]
    if lead.get("hq_state") and not lead.get("state"):
        lead["state"] = lead["hq_state"]
    if lead.get("hq_city") and not lead.get("city"):
        lead["city"] = lead["hq_city"]
    if lead.get("country") and not lead.get("hq_country"):
        lead["hq_country"] = lead["country"]
    if lead.get("state") and not lead.get("hq_state"):
        lead["hq_state"] = lead["state"]
    if lead.get("city") and not lead.get("hq_city"):
        lead["hq_city"] = lead["city"]


def validate_and_fix_with_scrapingdog(lead: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Dict[str, str]]]:
    """
    Validate/enrich a lead using ScrapingDog-first discovery and return:
      (updated_lead, changed_fields)
    changed_fields format:
      {"field_name": {"old": "...", "new": "..."}}
    """
    candidate = dict(lead)
    before = dict(candidate)

    prev_use = os.environ.get("USE_SCRAPINGDOG_ENRICHMENT")
    prev_force = os.environ.get("FORCE_SCRAPINGDOG_ENRICHMENT")
    try:
        os.environ["USE_SCRAPINGDOG_ENRICHMENT"] = "1"
        os.environ["FORCE_SCRAPINGDOG_ENRICHMENT"] = "1"
        candidate = enrich_linkedin_fields(candidate)
    finally:
        if prev_use is None:
            os.environ.pop("USE_SCRAPINGDOG_ENRICHMENT", None)
        else:
            os.environ["USE_SCRAPINGDOG_ENRICHMENT"] = prev_use
        if prev_force is None:
            os.environ.pop("FORCE_SCRAPINGDOG_ENRICHMENT", None)
        else:
            os.environ["FORCE_SCRAPINGDOG_ENRICHMENT"] = prev_force

    candidate["linkedin"] = clean_linkedin(str(candidate.get("linkedin") or ""), "person")
    candidate["company_linkedin"] = clean_linkedin(
        str(candidate.get("company_linkedin") or ""), "company"
    )
    candidate["country"] = normalize_country_name(str(candidate.get("country") or ""))
    candidate["hq_country"] = normalize_country_name(str(candidate.get("hq_country") or ""))
    candidate["description"] = ensure_min_description(
        str(candidate.get("description") or ""),
        str(candidate.get("business") or ""),
        str(candidate.get("industry") or ""),
        str(candidate.get("sub_industry") or ""),
        str(candidate.get("website") or ""),
    )
    _sync_location_fields(candidate)

    watched_fields = (
        "full_name",
        "first",
        "last",
        "email",
        "linkedin",
        "company_linkedin",
        "country",
        "state",
        "city",
        "hq_country",
        "hq_state",
        "hq_city",
        "description",
    )
    changed: Dict[str, Dict[str, str]] = {}
    for k in watched_fields:
        old = str(before.get(k) or "")
        new = str(candidate.get(k) or "")
        if old != new:
            changed[k] = {"old": old, "new": new}
    return candidate, changed


def derive_source_url(doc: Dict[str, Any], domain: str) -> str:
    serp = doc.get("serp_results") or []
    if serp and isinstance(serp, list):
        first = serp[0]
        if isinstance(first, dict):
            url = (first.get("url") or "").strip()
            if url:
                return url
    return f"https://{domain}"


def map_raw_to_lead(doc: Dict[str, Any], filename: str) -> Optional[Dict[str, Any]]:
    domain = (doc.get("domain") or filename.replace(".json", "").split("__")[0]).strip()
    extracted = doc.get("extracted_data") or {}
    company = extracted.get("company") if isinstance(extracted, dict) else {}
    team = extracted.get("team_members") if isinstance(extracted, dict) else []
    if not isinstance(company, dict):
        company = {}
    if not isinstance(team, list):
        team = []

    best_person = pick_best_person(team) if team else {}
    if not isinstance(best_person, dict):
        best_person = {}

    full_name = (best_person.get("name") or "").strip()
    first, last = split_name(full_name)
    email = (best_person.get("email") or "").strip()
    role = (best_person.get("role") or "").strip() or "Advisor"
    phone = best_person.get("phone") or ""

    company_name = (company.get("name") or "").strip()
    description = (company.get("description") or "").strip()
    industry = (company.get("industry") or "").strip()
    sub_industry = (company.get("sub_industry") or "").strip()
    sub_industry, industry = normalize_sub_industry_and_industry(sub_industry, industry)
    emp = normalize_employee_count(str(company.get("employee_count") or ""))
    hq_location = (company.get("hq_location") or "").strip()
    city, state, country = parse_hq_location(hq_location)
    country = normalize_country_name(country)
    if country in {"United States"} and state:
        state = state.strip()

    socials = company.get("socials") if isinstance(company.get("socials"), dict) else {}
    company_linkedin = clean_linkedin((socials.get("linkedin") or ""), "company")
    person_linkedin = clean_linkedin((best_person.get("linkedin") or ""), "person")

    source_url = derive_source_url(doc, domain)
    website = f"https://{domain}" if domain else source_url
    description = ensure_min_description(description, company_name, industry, sub_industry, website)

    lead = {
        "business": company_name,
        "full_name": full_name,
        "first": first,
        "last": last,
        "email": email,
        "role": role,
        "linkedin": person_linkedin,
        "website": website,
        "industry": industry,
        "sub_industry": sub_industry,
        "country": country,
        "state": state,
        "city": city,
        "company_linkedin": company_linkedin,
        "description": description,
        "employee_count": emp,
        "source_url": source_url,
        "source_type": "company_site",
        "phone_numbers": [phone] if phone else [],
        "hq_country": country,
        "hq_state": state,
        "hq_city": city,
    }

    # Keep location fields coherent between direct and HQ fields.
    if lead.get("hq_country") and not lead.get("country"):
        lead["country"] = lead["hq_country"]
    if lead.get("hq_state") and not lead.get("state"):
        lead["state"] = lead["hq_state"]
    if lead.get("hq_city") and not lead.get("city"):
        lead["city"] = lead["hq_city"]
    if lead.get("country") and not lead.get("hq_country"):
        lead["hq_country"] = lead["country"]
    if lead.get("state") and not lead.get("hq_state"):
        lead["hq_state"] = lead["state"]
    if lead.get("city") and not lead.get("hq_city"):
        lead["hq_city"] = lead["city"]

    # Reject obviously incomplete mappings early.
    # Also avoid generic inbox contacts that fail precheck quality gates.
    required_min = [
        "business", "full_name", "first", "last", "email",
        "industry", "sub_industry", "country", "city"
    ]
    if any(not str(lead.get(k, "")).strip() for k in required_min):
        return None
    if is_generic_email(lead.get("email", "")):
        return None
    return lead


def queue_key(lead: Dict[str, Any]) -> str:
    parts = [
        str(lead.get("email", "")).strip().lower(),
        str(lead.get("business", "")).strip().lower(),
        str(lead.get("linkedin", "")).strip().lower(),
        str(lead.get("website", "")).strip().lower(),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert raw_generated to pending queue")
    parser.add_argument("--in-dir", default="lead_queue/raw_generated")
    parser.add_argument("--pending-dir", default="lead_queue/pending")
    parser.add_argument("--failed-dir", default="lead_queue/failed")
    parser.add_argument("--limit", type=int, default=0, help="0 = all files")
    parser.add_argument(
        "--enrich-linkedin",
        type=int,
        default=1,
        help="1=search and fill missing linkedin fields, 0=disable",
    )
    args = parser.parse_args()

    def _queue_path(p: str) -> Path:
        path = Path(p)
        if path.is_absolute():
            return path
        return _REPO_ROOT / path

    in_dir = _queue_path(args.in_dir)
    pending_dir = _queue_path(args.pending_dir)
    failed_dir = _queue_path(args.failed_dir)
    pending_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(in_dir.glob("*.json"))
    if args.limit > 0:
        files = files[: args.limit]

    total = len(files)
    converted = 0
    valid = 0
    skipped_existing = 0
    skipped_invalid_map = 0
    precheck_failed = 0

    for f in files:
        try:
            doc = json.loads(f.read_text())
        except Exception:
            continue

        lead = map_raw_to_lead(doc, f.name)
        if not lead:
            skipped_invalid_map += 1
            continue

        if args.enrich_linkedin == 1:
            lead = enrich_linkedin_fields(lead)
        converted += 1

        ok, reason = precheck_lead(lead)
        if not ok:
            precheck_failed += 1
            fail_obj = {
                "source_file": f.name,
                "reason": reason,
                "lead_preview": {
                    "business": lead.get("business"),
                    "email": lead.get("email"),
                    "website": lead.get("website"),
                    "industry": lead.get("industry"),
                    "sub_industry": lead.get("sub_industry"),
                },
            }
            fail_path = failed_dir / f"{f.stem}.precheck_failed.json"
            fail_path.write_text(json.dumps(fail_obj, ensure_ascii=True, indent=2))
            continue

        key = queue_key(lead)
        out_file = pending_dir / f"{key}.json"
        if out_file.exists():
            skipped_existing += 1
            continue
        out_file.write_text(json.dumps(lead, ensure_ascii=True, indent=2))
        valid += 1

    print(f"Input files:             {total}")
    print(f"Mapped leads:            {converted}")
    print(f"Valid queued to pending: {valid}")
    print(f"Precheck failed:         {precheck_failed}")
    print(f"Skipped (existing):      {skipped_existing}")
    print(f"Skipped (invalid map):   {skipped_invalid_map}")
    print(f"Pending dir:             {pending_dir.resolve()}")
    print(f"Failed dir:              {failed_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
