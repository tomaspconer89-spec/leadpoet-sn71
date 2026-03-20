#!/usr/bin/env python3
"""
Convert lead_queue/raw_generated artifacts into SN71 payloads, run precheck,
and write valid leads into lead_queue/pending for submit_queued_leads.py.
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
        return parts[0], "", parts[1]
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


def ensure_min_description(description: str, company_name: str, industry: str, sub_industry: str, website: str) -> str:
    desc = (description or "").strip()
    if len(desc) >= 70:
        return desc
    name = (company_name or "This company").strip()
    ind = (industry or "business services").strip()
    sub = (sub_industry or "advisory services").strip()
    site = (website or "").strip()
    fallback = (
        f"{name} operates in {ind} with a focus on {sub}, serving clients through its primary website {site}."
    )
    return fallback if len(fallback) >= 70 else (fallback + " Contact and company profile details were sourced from public business pages.")


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


def harvest_search_urls(query: str) -> List[str]:
    harvest_key = os.getenv("HARVEST_API_KEY", "").strip()
    if not harvest_key:
        return []
    encoded_q = urllib.parse.quote(query)
    try:
        data = http_json(
            f"https://api.harvest-api.com/linkedin/profile-search?search={encoded_q}",
            headers={"X-API-Key": harvest_key, "Accept": "application/json"},
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


def search_urls(query: str) -> List[str]:
    """
    Query search provider(s) and return top URLs.
    Priority: HarvestAPI -> Apify -> Serper -> Brave -> GSE.
    """
    urls: List[str] = []
    serper_key = os.getenv("SERPER_API_KEY", "").strip()
    brave_key = os.getenv("BRAVE_API_KEY", "").strip()
    gse_key = os.getenv("GSE_API_KEY", "").strip()
    gse_cx = os.getenv("GSE_CX", "").strip()
    harvest_urls = harvest_search_urls(query)
    if harvest_urls:
        return harvest_urls
    apify_urls = apify_search_urls(query)
    if apify_urls:
        return apify_urls

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


def enrich_linkedin_fields(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fill missing linkedin/company_linkedin with search-derived URLs.
    """
    business = (lead.get("business") or "").strip()
    website = (lead.get("website") or "").strip()
    domain = extract_domain(website)
    person_name = (lead.get("full_name") or "").strip()

    if not lead.get("company_linkedin"):
        company_queries = [
            f"site:linkedin.com/company {business}",
            f"{business} linkedin company",
            f"site:linkedin.com/company {domain}",
        ]
        for q in company_queries:
            for u in search_urls(q):
                cu = clean_linkedin(u, "company")
                if cu:
                    lead["company_linkedin"] = cu
                    break
            if lead.get("company_linkedin"):
                break

    if not lead.get("linkedin"):
        person_queries: List[str] = []
        if person_name and person_name.lower() not in {"not listed", "unknown", "n/a"}:
            person_queries.append(f'site:linkedin.com/in "{person_name}" "{business}"')
            person_queries.append(f'site:linkedin.com/in "{person_name}" "{domain}"')
        person_queries.append(f'site:linkedin.com/in "{business}" founder')
        person_queries.append(f'site:linkedin.com/in "{business}" ceo')
        for q in person_queries:
            for u in search_urls(q):
                pu = clean_linkedin(u, "person")
                if pu:
                    lead["linkedin"] = pu
                    break
            if lead.get("linkedin"):
                break

    return lead


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

    in_dir = Path(args.in_dir)
    pending_dir = Path(args.pending_dir)
    failed_dir = Path(args.failed_dir)
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
