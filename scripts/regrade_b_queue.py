#!/usr/bin/env python3
"""
Re-run normalization + title + person confidence + precheck + route_lead on leads in
lead_queue/B_retry_enrichment/. Updates JSON in place when still B; moves to A_ready_submit
(or other buckets) when routing changes. Updates lead_queue/collected_pass when precheck passes.

Do not raw-mv B -> A: folder placement must match computed route_lead(...).

Usage (repo root):
  python3 scripts/regrade_b_queue.py
  python3 scripts/regrade_b_queue.py --enrich-linkedin 1
  python3 scripts/regrade_b_queue.py --scrapingdog-fix 0   # skip ScrapingDog (Apify-first search in enrich step)
  python3 scripts/regrade_b_queue.py --dry-run
  python3 scripts/regrade_b_queue.py --report-linkedin-url "https://www.linkedin.com/in/example"
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
from datetime import datetime, timezone
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv

from miner_models.lead_normalization import apply_email_classification, normalize_legacy_lead_shape
from miner_models.minimal_lead_blob import minimal_gateway_lead
from miner_models.person_confidence import score_person_confidence
from miner_models.title_normalizer import normalize_title
from scripts.queue_router import route_lead
from scripts.retry_enrichment import should_run_targeted_retry, targeted_retry_enrichment

load_dotenv(_REPO / ".env")


def _load_enrich():
    conv = _REPO / "scripts" / "convert_raw_to_pending.py"
    if not conv.is_file():
        return None
    spec = importlib.util.spec_from_file_location("_crtp", conv)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return getattr(mod, "enrich_linkedin_fields", None)


def _load_scrapingdog_fixer():
    conv = _REPO / "scripts" / "convert_raw_to_pending.py"
    if not conv.is_file():
        return None
    spec = importlib.util.spec_from_file_location("_crtp_fix", conv)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return getattr(mod, "validate_and_fix_with_scrapingdog", None)


def _load_location_helpers():
    conv = _REPO / "scripts" / "convert_raw_to_pending.py"
    if not conv.is_file():
        return None, None
    spec = importlib.util.spec_from_file_location("_crtp_loc", conv)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return (
        getattr(mod, "parse_hq_location", None),
        getattr(mod, "normalize_country_name", None),
    )


_US_STATE_ABBR = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC", "New Jersey",
}

_US_STATE_FULL_TO_ABBR: dict[str, str] = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
}
_BASED_IN_STATE_RE = re.compile(
    r"\bbased in\s+("
    + "|".join(re.escape(n) for n in sorted(_US_STATE_FULL_TO_ABBR.keys(), key=len, reverse=True))
    + r")\b",
    re.I,
)

_BOGUS_LOCATION_SUBSTRINGS = (
    "i'm ",
    "i work with",
    "founder of",
    "passionate business",
    "who are ready",
    "ready to",
    "serving clients nationwide",
    "tap add a location",
    "tap the tab",
    "tap show results",
    "show results",
    "such as all",
    "category you wish to search",
)


def _is_bogus_location_str(value: str) -> bool:
    s = (value or "").strip()
    if not s:
        return True
    if len(s) > 52:
        return True
    if "\n" in s or "\r" in s:
        return True
    low = s.lower()
    if "..." in s or "\u00a0" in s:
        return True
    if any(b in low for b in _BOGUS_LOCATION_SUBSTRINGS):
        return True
    return False


def _location_fields_need_enrichment(lead: dict) -> bool:
    keys = ("city", "state", "country", "hq_city", "hq_state", "hq_country")
    for k in keys:
        v = str(lead.get(k) or "").strip()
        if not v or _is_bogus_location_str(v):
            return True
    return False


def _is_plausible_us_state_token(state: str) -> bool:
    st = (state or "").strip()
    if not st:
        return False
    if len(st) == 2 and st.upper() in _US_STATE_ABBR:
        return True
    if st in _US_STATE_FULL_TO_ABBR:
        return True
    return False


def _is_plausible_location_pair(city: str, state: str) -> bool:
    if not city or not state:
        return False
    if _is_bogus_location_str(city) or _is_bogus_location_str(state):
        return False
    return _is_plausible_us_state_token(state)


def _scrapingdog_google_json(query: str) -> dict:
    key = os.getenv("SCRAPINGDOG_API_KEY", "").strip()
    if not key:
        return {}
    url = (
        "https://api.scrapingdog.com/google"
        f"?api_key={urllib.parse.quote(key)}"
        f"&query={urllib.parse.quote(query)}"
        "&results=10&country=us&page=0"
    )
    req = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return {}
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _scrapingdog_linkedin_profile_json(linkedin_person_url: str) -> dict:
    """
    ScrapingDog LinkedIn **person** profile API (not /scrape — LinkedIn blocks generic scrape).

    Docs: https://docs.scrapingdog.com/linkedin-scraper-api — uses ``/profile`` with ``type=profile``
    and the ``/in/{slug}`` id. Requires ``SCRAPINGDOG_API_KEY``. Optional ``SCRAPINGDOG_PROFILE_PREMIUM=1``.
    """
    key = os.getenv("SCRAPINGDOG_API_KEY", "").strip()
    if not key or "/in/" not in (linkedin_person_url or "").lower():
        return {}
    m = re.search(r"linkedin\.com/in/([^/?#]+)", linkedin_person_url, re.I)
    if not m:
        return {}
    slug = m.group(1).strip().rstrip("/")
    if not slug:
        return {}
    qs: list[tuple[str, str]] = [
        ("api_key", key),
        ("type", "profile"),
        ("id", slug),
    ]
    if os.getenv("SCRAPINGDOG_PROFILE_PREMIUM", "0").strip() == "1":
        qs.append(("premium", "true"))
    url = "https://api.scrapingdog.com/profile?" + urllib.parse.urlencode(qs)
    req = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=65) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return {}
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    msg = str(data.get("message") or data.get("error") or "").lower()
    if msg and ("api key" in msg or "invalid" in msg or "credit" in msg):
        return {}
    return data


def _gather_profile_location_hints(obj: Any, out: list[str], depth: int = 0) -> None:
    if depth > 7:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if any(
                s in kl
                for s in (
                    "location",
                    "address",
                    "locality",
                    "geo",
                    "region",
                    "country",
                    "area",
                )
            ):
                if isinstance(v, str) and v.strip():
                    out.append(v.strip())
                elif isinstance(v, dict):
                    for subk in ("name", "defaultlocalizedname", "default", "title"):
                        x = v.get(subk)
                        if isinstance(x, str) and x.strip():
                            out.append(x.strip())
                            break
            elif isinstance(v, (dict, list)):
                _gather_profile_location_hints(v, out, depth + 1)
    elif isinstance(obj, list):
        for x in obj[:40]:
            _gather_profile_location_hints(x, out, depth + 1)


def _location_from_scrapingdog_profile(payload: dict) -> tuple[str, str, str]:
    """Derive (city, state_abbr, country) from ScrapingDog /profile JSON."""
    if not payload:
        return "", "", ""
    hints: list[str] = []
    _gather_profile_location_hints(payload, hints)
    seen: set[str] = set()
    for h in hints:
        if not h or h in seen:
            continue
        seen.add(h)
        c, st, co = _extract_us_location(h)
        if c and st and _is_plausible_location_pair(c, st):
            norm_co = co or "United States"
            if not norm_co.strip():
                norm_co = "United States"
            return c, st, norm_co
        c3, st3, co3 = _extract_based_in_us_state(h)
        if c3 and st3 and _is_plausible_location_pair(c3, st3):
            return c3, st3, co3
        parts = [p.strip() for p in h.split(",") if p.strip()]
        if len(parts) >= 3:
            city, st_name, ctry = parts[0], parts[1], parts[2]
            clow = ctry.lower()
            if clow in ("united states", "usa", "us", "u.s.", "u.s.a."):
                abbr = _US_STATE_FULL_TO_ABBR.get(st_name) or (
                    st_name.upper() if len(st_name) == 2 and st_name.upper() in _US_STATE_ABBR else ""
                )
                if abbr and _is_plausible_location_pair(city, abbr):
                    return city, abbr, "United States"
        if (
            len(parts) == 2
            and parts[1].lower() in ("united states", "usa", "us")
            and parts[0] in _US_STATE_FULL_TO_ABBR
        ):
            full = parts[0]
            return full, _US_STATE_FULL_TO_ABBR[full], "United States"
    return "", "", ""


def _collect_text_blobs(obj, out: list[str]) -> None:
    if isinstance(obj, dict):
        for v in obj.values():
            _collect_text_blobs(v, out)
        return
    if isinstance(obj, list):
        for v in obj:
            _collect_text_blobs(v, out)
        return
    if isinstance(obj, str):
        s = obj.strip()
        if s:
            out.append(s)


def _slug_to_words(value: str) -> str:
    s = (value or "").strip().strip("/").lower()
    if not s:
        return ""
    s = s.replace("-", " ").replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _company_from_linkedin_url(url: str) -> str:
    u = (url or "").strip().lower()
    if "linkedin.com/company/" not in u:
        return ""
    m = re.search(r"linkedin\.com/company/([^/?#]+)", u)
    if not m:
        return ""
    return _slug_to_words(m.group(1))


def _person_from_linkedin_url(url: str) -> tuple[str, str, str]:
    u = (url or "").strip().lower()
    if "linkedin.com/in/" not in u:
        return "", "", ""
    m = re.search(r"linkedin\.com/in/([^/?#]+)", u)
    if not m:
        return "", "", ""
    slug = _slug_to_words(m.group(1))
    parts = [p for p in slug.split() if p and not p.isdigit()]
    if not parts:
        return "", "", ""
    if len(parts) == 1:
        first = parts[0].title()
        return first, "", first
    first = parts[0].title()
    last = parts[-1].title()
    full = f"{first} {last}".strip()
    return first, last, full


def _looks_placeholder_name(v: str) -> bool:
    s = (v or "").strip().lower()
    return s in {"", "hello lead", "lead", "unknown", "n/a", "na", "-"}


def _extract_website_from_text(blobs: list[str], prefer_domain: str = "") -> str:
    domains: list[str] = []
    url_re = re.compile(r"https?://[a-z0-9.-]+\.[a-z]{2,24}(?:/[^\s\"'<>]*)?", re.I)
    dom_re = re.compile(r"\b([a-z0-9][a-z0-9.-]{1,100}\.[a-z]{2,24})\b", re.I)
    for b in blobs:
        for u in url_re.findall(b):
            if "linkedin.com/" in u.lower():
                continue
            domains.append(u.strip())
        for d in dom_re.findall(b):
            d_l = d.lower()
            if "linkedin.com" in d_l:
                continue
            if d_l.startswith("www."):
                d_l = d_l[4:]
            domains.append(f"https://{d_l}")
    if not domains:
        return ""
    if prefer_domain:
        pd = prefer_domain.lower().strip()
        for cand in domains:
            if pd in cand.lower():
                return cand
    return domains[0]


def _extract_person_name_from_linkedin_serp(blobs: list[str]) -> tuple[str, str, str]:
    """Parse 'First Last - ... | LinkedIn' style titles/snippets from Google SERP blobs."""
    pat = re.compile(
        r"\b([A-Z][a-z]{1,24})\s+([A-Z][a-z]{1,24})\s*-\s*.{1,120}(?:\|\s*LinkedIn|LinkedIn)\b"
    )
    for b in blobs:
        m = pat.search(b or "")
        if m:
            f, ln = m.group(1), m.group(2)
            if f.lower() == ln.lower():
                continue
            return f, ln, f"{f} {ln}"
    return "", "", ""


def _repair_from_linkedin_evidence(lead: dict) -> tuple[dict, list[str]]:
    """
    Compare lead fields against LinkedIn URL evidence and repair obvious mismatches.
    This is intentionally conservative: only empty/placeholder fields are overwritten,
    except for linkedin/company_linkedin/url keys when a stronger canonical value exists.
    """
    changed: list[str] = []
    candidate = dict(lead)

    person_url = str(candidate.get("linkedin") or "").strip()
    company_url = str(candidate.get("company_linkedin") or "").strip()
    urls = [u for u in (person_url, company_url) if "linkedin.com/" in u.lower()]
    if not urls:
        return candidate, changed

    blobs: list[str] = []
    for u in urls:
        try:
            data = _scrapingdog_google_json(f"\"{u}\"")
        except Exception:
            data = {}
        _collect_text_blobs(data, blobs)

    company_from_url = _company_from_linkedin_url(company_url)
    first_from_url, last_from_url, full_from_url = _person_from_linkedin_url(person_url)
    website_from_serp = _extract_website_from_text(
        blobs, prefer_domain=(company_from_url or "").replace(" ", "")
    )

    # Canonical LinkedIn URLs (safe to update if malformed/non-linkedin).
    if company_url and "linkedin.com/company/" in company_url.lower():
        norm_company = re.sub(r"[?#].*$", "", company_url).rstrip("/")
        if candidate.get("company_linkedin") != norm_company:
            candidate["company_linkedin"] = norm_company
            changed.append("company_linkedin")
    if person_url and "linkedin.com/in/" in person_url.lower():
        norm_person = re.sub(r"[?#].*$", "", person_url).rstrip("/")
        if candidate.get("linkedin") != norm_person:
            candidate["linkedin"] = norm_person
            changed.append("linkedin")

    # Business name: fill missing/placeholder from company LinkedIn slug.
    business = str(candidate.get("business") or "").strip()
    if company_from_url and (not business or _looks_placeholder_name(business)):
        candidate["business"] = company_from_url.title()
        changed.append("business")

    # Person fields: fill missing/placeholder from /in/ slug.
    if full_from_url and _looks_placeholder_name(str(candidate.get("full_name") or "")):
        candidate["full_name"] = full_from_url
        changed.append("full_name")
    if first_from_url and _looks_placeholder_name(str(candidate.get("first") or "")):
        candidate["first"] = first_from_url
        changed.append("first")
    if last_from_url and _looks_placeholder_name(str(candidate.get("last") or "")):
        candidate["last"] = last_from_url
        changed.append("last")

    # Single-token /in/slug (e.g. joannowak): SERP title often has "Joan Nowak - ... | LinkedIn".
    if _looks_placeholder_name(str(candidate.get("last") or "")):
        sf, sl, sfull = _extract_person_name_from_linkedin_serp(blobs)
        if sl:
            candidate["first"] = sf
            candidate["last"] = sl
            candidate["full_name"] = sfull
            for k in ("first", "last", "full_name"):
                if k not in changed:
                    changed.append(k)

    # Website/source_url: if empty or linkedin URL, replace with company website hint.
    for k in ("website", "source_url"):
        cur = str(candidate.get(k) or "").strip()
        if website_from_serp and (not cur or "linkedin.com/" in cur.lower()):
            candidate[k] = website_from_serp
            changed.append(k)

    return candidate, sorted(set(changed))


def _extract_us_location(text: str) -> tuple[str, str, str]:
    s = (text or "").strip()
    if not s:
        return "", "", ""
    m = re.search(r"\b([A-Z][A-Za-z .'-]{1,60}),\s*([A-Z]{2})\b", s)
    if m:
        city = m.group(1).strip(" ,")
        st = m.group(2).upper()
        if st in _US_STATE_ABBR:
            return city, st, "United States"
    m = re.search(
        r"\b([A-Z][A-Za-z .'-]{1,60}),\s*"
        r"(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming)\b",
        s,
    )
    if m:
        return m.group(1).strip(" ,"), m.group(2).strip(), "United States"
    return "", "", ""


def _extract_based_in_us_state(text: str) -> tuple[str, str, str]:
    """
    Parse phrases like 'Based in New Jersey' from LinkedIn SERP snippets.
    Returns (city, state, country) with city = full state name (no finer city in text)
    and state = USPS abbreviation (precheck-friendly for US leads).
    """
    m = _BASED_IN_STATE_RE.search(text or "")
    if not m:
        return "", "", ""
    full = m.group(1).strip()
    abbr = _US_STATE_FULL_TO_ABBR.get(full)
    if not abbr:
        return "", "", ""
    return full, abbr, "United States"


def _enrich_location_from_linkedin(
    lead: dict,
    parse_hq_location,
    normalize_country_name,
) -> tuple[dict, list[str]]:
    """
    Location enrichment via ScrapingDog (in order):

    1. **LinkedIn profile API** — ``GET https://api.scrapingdog.com/profile`` (``type=profile``,
       ``id=`` /in/ slug). This is what ScrapingDog expects for LinkedIn; generic ``/scrape`` on
       linkedin.com returns a JSON notice, not HTML.
    2. **Google SERP API** — ``/google`` with quoted profile/company URLs; parse snippets
       (``City, ST``, ``Based in New Jersey``, etc.).
    3. Optional **parse_hq_location** on SERP text blobs (guarded so Maps UI strings are rejected).
    """
    changed: list[str] = []
    candidate = dict(lead)
    urls = [
        str(candidate.get("linkedin") or "").strip(),
        str(candidate.get("company_linkedin") or "").strip(),
    ]
    urls = [u for u in urls if "linkedin.com/" in u.lower()]
    if not urls:
        return candidate, changed

    if not _location_fields_need_enrichment(candidate):
        return candidate, changed

    found_city = found_state = found_country = ""
    person_li = str(candidate.get("linkedin") or "").strip()
    if "/in/" in person_li.lower():
        prof = _scrapingdog_linkedin_profile_json(person_li)
        fc, fs, fco = _location_from_scrapingdog_profile(prof)
        if fc and fs and _is_plausible_location_pair(fc, fs):
            found_city, found_state, found_country = fc, fs, fco or "United States"
    if not (found_city and found_state):
        for u in urls:
            try:
                data = _scrapingdog_google_json(f"\"{u}\" location")
            except Exception:
                continue
            blobs: list[str] = []
            _collect_text_blobs(data, blobs)
            # Prefer structured SERP parse; avoid parse_hq_location on UI chrome earlier in the tree.
            for b in blobs:
                c, st, co = _extract_us_location(b)
                if c and st and _is_plausible_location_pair(c, st):
                    found_city, found_state, found_country = c, st, co or "United States"
                    break
            if not (found_city and found_state):
                for b in blobs:
                    c3, st3, co3 = _extract_based_in_us_state(b)
                    if c3 and st3 and _is_plausible_location_pair(c3, st3):
                        found_city, found_state, found_country = c3, st3, co3
                        break
            if not (found_city and found_state) and parse_hq_location is not None:
                for b in blobs:
                    c2, s2, co2 = parse_hq_location(b)
                    if not (c2 and (s2 or co2)):
                        continue
                    fc = c2.strip()
                    fs = (s2 or "").strip()
                    fco = (co2 or "").strip()
                    if normalize_country_name is not None:
                        fco = normalize_country_name(fco)
                    if _is_plausible_location_pair(fc, fs):
                        found_city, found_state, found_country = fc, fs, fco or "United States"
                        break
            if found_city and found_state:
                break

    if not (found_city and found_state):
        return candidate, changed

    def _set_if_empty_or_bogus(key: str, value: str):
        if not value:
            return
        cur = str(candidate.get(key) or "").strip()
        if not cur or _is_bogus_location_str(cur):
            candidate[key] = value
            changed.append(key)

    country_val = found_country or "United States"
    _set_if_empty_or_bogus("city", found_city)
    _set_if_empty_or_bogus("state", found_state)
    _set_if_empty_or_bogus("country", country_val)
    _set_if_empty_or_bogus("hq_city", found_city)
    _set_if_empty_or_bogus("hq_state", found_state)
    _set_if_empty_or_bogus("hq_country", country_val)
    return candidate, changed


_NORMAL_GATEWAY_FIELDS = (
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
)


def _ensure_normal_fields(lead: dict) -> dict:
    """
    Keep SN71-normal fields present in queue files even when value is empty.
    This avoids disappearing keys like state/city/hq_state/hq_city in B queue.
    """
    # Strip extras, then rebuild in exact README order.
    base = minimal_gateway_lead(dict(lead))
    out: dict = {}
    for k in _NORMAL_GATEWAY_FIELDS:
        v = base.get(k, "")
        out[k] = "" if v is None else v
    out["phone_numbers"] = (
        base.get("phone_numbers")
        if isinstance(base.get("phone_numbers"), list)
        else []
    )
    out["socials"] = (
        base.get("socials")
        if isinstance(base.get("socials"), dict)
        else {}
    )
    return out


def _queue_key(lead: dict) -> str:
    import hashlib

    parts = [
        str(lead.get("email", "")).strip().lower(),
        str(lead.get("business", "")).strip().lower(),
        str(lead.get("linkedin", "")).strip().lower(),
        str(lead.get("website", "")).strip().lower(),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _canonical_linkedin_profile_url(u: str) -> str:
    """Normalize a person LinkedIn URL for equality checks."""
    s = (u or "").strip().rstrip("/").lower()
    needle = "linkedin.com/in/"
    if needle not in s:
        return s
    i = s.index(needle) + len(needle)
    slug = s[i:].split("/")[0].split("?")[0]
    if not slug:
        return s
    return f"https://www.linkedin.com/in/{slug}"


def _find_b_retry_for_linkedin(target_url: str) -> tuple[Path | None, dict]:
    want = _canonical_linkedin_profile_url(target_url)
    b_dir = _REPO / "lead_queue" / "B_retry_enrichment"
    for path in sorted(b_dir.glob("*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        inner = raw.get("lead") if isinstance(raw.get("lead"), dict) else None
        lead = inner if inner is not None else raw
        if not isinstance(lead, dict):
            continue
        li = _canonical_linkedin_profile_url(str(lead.get("linkedin", "")))
        if li == want:
            return path, dict(lead)
    return None, {}


def _run_linkedin_report(
    target_url: str,
    *,
    report_out: str,
    enrich_linkedin: int,
    scrapingdog_fix: int,
) -> int:
    path, _ = _find_b_retry_for_linkedin(target_url)
    if path is None:
        print(
            f"No lead_queue/B_retry_enrichment/*.json with linkedin matching {target_url!r}",
            file=sys.stderr,
        )
        return 2
    raw_in = json.loads(path.read_text(encoding="utf-8"))
    inner = raw_in.get("lead") if isinstance(raw_in.get("lead"), dict) else None
    lead_in = dict(inner if inner is not None else raw_in)

    enrich_fn = _load_enrich() if enrich_linkedin == 1 else None
    scrapingdog_fixer = _load_scrapingdog_fixer() if scrapingdog_fix == 1 else None
    parse_hq_location, normalize_country_name = _load_location_helpers()

    lead, pre_ok, reason, bucket, fixed_fields = _process_one(
        lead_in,
        enrich_fn=enrich_fn,
        scrapingdog_fixer=scrapingdog_fixer,
        prefer_scrapingdog_linkedin_search=scrapingdog_fix == 1,
        parse_hq_location=parse_hq_location,
        normalize_country_name=normalize_country_name,
    )
    store = _ensure_normal_fields(lead)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = _canonical_linkedin_profile_url(target_url).rsplit("/", 1)[-1]
    out = Path(report_out) if report_out else _REPO / "reports" / f"regrade_b_queue_{slug}_{ts}.json"
    if not out.is_absolute():
        out = _REPO / out
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": ts,
        "input_queue_file": str(path.relative_to(_REPO)),
        "report_linkedin_url": target_url,
        "precheck_ok": pre_ok,
        "precheck_reason": reason,
        "route_lead_bucket": bucket,
        "field_fixes_applied": fixed_fields,
        "lead_normalized": store,
        "lead_input_snapshot": lead_in,
    }
    out.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    print(out)
    return 0


def _process_one(
    lead: dict,
    *,
    enrich_fn,
    scrapingdog_fixer=None,
    prefer_scrapingdog_linkedin_search: bool = True,
    parse_hq_location=None,
    normalize_country_name=None,
) -> tuple[dict, bool, str | None, str, list[str]]:
    from miner_models.lead_precheck import precheck_lead

    lead = dict(lead)
    normalize_legacy_lead_shape(lead)
    fixed_fields: list[str] = []
    if scrapingdog_fixer is not None:
        try:
            fixed_lead, changed = scrapingdog_fixer(dict(lead))
            if changed:
                lead = fixed_lead
                fixed_fields = sorted(changed.keys())
        except Exception:
            pass
    if enrich_fn is not None:
        prev_sd = os.environ.get("USE_SCRAPINGDOG_ENRICHMENT")
        try:
            if prefer_scrapingdog_linkedin_search:
                os.environ["USE_SCRAPINGDOG_ENRICHMENT"] = "1"
            lead = enrich_fn(dict(lead))
        except Exception:
            pass
        finally:
            if prev_sd is None:
                os.environ.pop("USE_SCRAPINGDOG_ENRICHMENT", None)
            else:
                os.environ["USE_SCRAPINGDOG_ENRICHMENT"] = prev_sd
    try:
        lead, repaired = _repair_from_linkedin_evidence(lead)
        for k in repaired:
            if k not in fixed_fields:
                fixed_fields.append(k)
    except Exception:
        pass
    try:
        lead, loc_changed = _enrich_location_from_linkedin(
            lead,
            parse_hq_location=parse_hq_location,
            normalize_country_name=normalize_country_name,
        )
        for k in loc_changed:
            if k not in fixed_fields:
                fixed_fields.append(k)
    except Exception:
        pass
    normalize_legacy_lead_shape(lead)
    apply_email_classification(lead)

    title_meta = normalize_title(str(lead.get("role", "")))
    lead["title_normalized"] = title_meta["normalized_title"]
    lead["seniority"] = title_meta["seniority"]
    lead["persona_bucket"] = title_meta["persona_bucket"]
    lead["target_fit"] = title_meta["target_fit"]

    conf = score_person_confidence(
        lead, title_matches_persona=title_meta["target_fit"] in ("high", "medium")
    )
    lead.update(conf)
    apply_email_classification(lead)

    pre_ok, reason = precheck_lead(lead)
    retry_attempts = 0
    if (not pre_ok) and reason:
        if "name_not_in_email" in (reason or "") or "email_domain_mismatch" in (reason or ""):
            lead["identity_conflict"] = True
        if should_run_targeted_retry(reason):
            lead, _, retry_attempts = targeted_retry_enrichment(
                lead, reason, enrich_linkedin=enrich_fn
            )
        if retry_attempts > 0:
            normalize_legacy_lead_shape(lead)
            apply_email_classification(lead)
            conf2 = score_person_confidence(
                lead,
                title_matches_persona=title_meta["target_fit"] in ("high", "medium"),
            )
            lead.update(conf2)
            pre_ok, reason = precheck_lead(lead)

    bucket = route_lead(lead, precheck_ok=pre_ok, precheck_reason=reason)
    return lead, pre_ok, reason, bucket, fixed_fields


def main() -> int:
    p = argparse.ArgumentParser(description="Re-grade B_retry_enrichment leads; promote to A when eligible")
    p.add_argument("--enrich-linkedin", type=int, default=1, choices=(0, 1))
    p.add_argument(
        "--scrapingdog-fix",
        type=int,
        default=1,
        choices=(0, 1),
        help="1=validate/fix fields + LinkedIn discovery via ScrapingDog (default); 0=skip",
    )
    p.add_argument(
        "--route",
        type=int,
        default=0,
        choices=(0, 1),
        help="When 1, move failed leads to routed bucket; when 0, keep failed leads in B_retry_enrichment",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--report-linkedin-url",
        default="",
        help=(
            "Match one lead in B_retry_enrichment by person LinkedIn URL; run the same "
            "pipeline as batch regrade; write JSON to reports/ (or --report-out); "
            "does not move or delete queue files."
        ),
    )
    p.add_argument(
        "--report-out",
        default="",
        help="Output path for --report-linkedin-url (default: reports/regrade_b_queue_<slug>_UTC.json)",
    )
    args = p.parse_args()

    if args.report_linkedin_url.strip():
        return _run_linkedin_report(
            args.report_linkedin_url.strip(),
            report_out=args.report_out.strip(),
            enrich_linkedin=args.enrich_linkedin,
            scrapingdog_fix=args.scrapingdog_fix,
        )

    queue = _REPO / "lead_queue"
    b_dir = queue / "B_retry_enrichment"
    graded_dirs = {
        "A_ready_submit": queue / "A_ready_submit",
        "B_retry_enrichment": queue / "B_retry_enrichment",
        "C_good_account_needs_person": queue / "C_good_account_needs_person",
        "D_low_confidence_hold": queue / "D_low_confidence_hold",
        "E_reject": queue / "E_reject",
    }
    pass_dir = queue / "collected_pass"
    fail_dir = queue / "collected_precheck_fail"
    for d in (*graded_dirs.values(), pass_dir, fail_dir):
        d.mkdir(parents=True, exist_ok=True)

    enrich_fn = _load_enrich() if args.enrich_linkedin == 1 else None
    scrapingdog_fixer = _load_scrapingdog_fixer() if args.scrapingdog_fix == 1 else None
    parse_hq_location, normalize_country_name = _load_location_helpers()
    files = sorted(b_dir.glob("*.json"))
    if not files:
        print(f"No JSON in {b_dir}")
        return 0

    promoted = updated = failed = 0
    for old_path in files:
        business = "?"
        try:
            lead_in = json.loads(old_path.read_text(encoding="utf-8"))
            business = str(lead_in.get("business", "?"))
            lead, pre_ok, reason, bucket, fixed_fields = _process_one(
                lead_in,
                enrich_fn=enrich_fn,
                scrapingdog_fixer=scrapingdog_fixer,
                prefer_scrapingdog_linkedin_search=args.scrapingdog_fix == 1,
                parse_hq_location=parse_hq_location,
                normalize_country_name=normalize_country_name,
            )
            key = _queue_key(lead)
            # Always promote precheck-passing leads to A_ready_submit.
            if pre_ok:
                dest_bucket = "A_ready_submit"
            else:
                dest_bucket = bucket if args.route == 1 else "B_retry_enrichment"
            new_graded = graded_dirs[dest_bucket] / f"{key}.json"

            if args.dry_run:
                print(
                    f"[dry-run] {old_path.name} -> {dest_bucket} (suggested={bucket}) "
                    f"precheck={'OK' if pre_ok else 'FAIL'} {reason or ''} "
                    f"fixed_fields={fixed_fields} business={business[:50]!r}"
                )
                continue

            if old_path.resolve() != new_graded.resolve():
                old_path.unlink(missing_ok=True)

            # Keep all SN71-normal fields present (including location keys),
            # and update with any ScrapingDog/LinkedIn corrections.
            store_graded = _ensure_normal_fields(lead)
            new_graded.write_text(json.dumps(store_graded, ensure_ascii=True, indent=2), encoding="utf-8")

            if pre_ok:
                (pass_dir / f"{key}.json").write_text(
                    json.dumps(store_graded, ensure_ascii=True, indent=2), encoding="utf-8"
                )
            else:
                payload = {
                    "reason": reason,
                    "business": business,
                    "lead": store_graded,
                    "queue_bucket": bucket,
                }
                (fail_dir / f"{key}.precheck_failed.json").write_text(
                    json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8"
                )

            if pre_ok:
                promoted += 1
                print(f"  PROMOTE -> A: {business[:60]!r} ({key[:16]}…)")
            else:
                failed += 1
                print(
                    f"  precheck FAIL ({'routed' if args.route == 1 else 'kept in B'}): "
                    f"{business[:60]!r} ({reason}) fixed_fields={fixed_fields}"
                )
                if fixed_fields:
                    print("    changed:", ", ".join(fixed_fields))
        except Exception as e:
            print(f"  ERROR {old_path.name}: {type(e).__name__}: {e}")
            return 1

    if not args.dry_run:
        print(f"Done. promoted_to_A={promoted} updated_still_B={updated} precheck_fail={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
