#!/usr/bin/env python3
"""
Retry conversion for lead_queue/failed precheck artifacts.

Flow:
1) Read *.precheck_failed.json
2) Reconstruct lead using available preview + /tmp crawl artifacts
3) Enrich linkedin/company_linkedin via search
4) Run precheck again
5) Write valid leads to lead_queue/pending
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from miner_models.lead_precheck import precheck_lead


VALID_EMPLOYEE_COUNTS = {
    "0-1", "2-10", "11-50", "51-200", "201-500",
    "501-1,000", "1,001-5,000", "5,001-10,000", "10,001+",
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
        return parts[0], parts[0]
    return parts[0], " ".join(parts[1:])


def parse_hq_location(hq: str) -> Tuple[str, str, str]:
    txt = (hq or "").strip()
    if not txt:
        return "", "", ""
    parts = [p.strip() for p in txt.split(",") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], parts[-1]
    if len(parts) == 2:
        return parts[0], "", parts[1]
    return "", "", parts[0]


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


def http_json(url: str, method: str = "GET", headers: Optional[Dict[str, str]] = None, body: Optional[bytes] = None) -> Dict[str, Any]:
    req = urllib.request.Request(url=url, method=method, headers=headers or {}, data=body)
    with urllib.request.urlopen(req, timeout=12) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)


def search_urls(query: str) -> list[str]:
    urls: list[str] = []
    serper_key = os.getenv("SERPER_API_KEY", "").strip()
    brave_key = os.getenv("BRAVE_API_KEY", "").strip()
    gse_key = os.getenv("GSE_API_KEY", "").strip()
    gse_cx = os.getenv("GSE_CX", "").strip()

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
    business = (lead.get("business") or "").strip()
    website = (lead.get("website") or "").strip()
    domain = website.replace("https://", "").replace("http://", "").split("/")[0].replace("www.", "")
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
        queries = []
        if person_name and person_name.lower() not in {"not listed", "unknown", "n/a"}:
            queries.append(f'site:linkedin.com/in "{person_name}" "{business}"')
            queries.append(f'site:linkedin.com/in "{person_name}" "{domain}"')
        queries.append(f'site:linkedin.com/in "{business}" founder')
        queries.append(f'site:linkedin.com/in "{business}" ceo')
        for q in queries:
            for u in search_urls(q):
                pu = clean_linkedin(u, "person")
                if pu:
                    lead["linkedin"] = pu
                    break
            if lead.get("linkedin"):
                break
    return lead


def queue_key(lead: Dict[str, Any]) -> str:
    parts = [
        str(lead.get("email", "")).strip().lower(),
        str(lead.get("business", "")).strip().lower(),
        str(lead.get("linkedin", "")).strip().lower(),
        str(lead.get("website", "")).strip().lower(),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def parse_domain_from_source_file(name: str) -> str:
    base = name.replace(".precheck_failed.json", "")
    # remove __N suffix if present
    if "__" in base:
        base = base.split("__")[0]
    return base


def load_tmp_crawl_artifact(domain: str) -> Optional[Dict[str, Any]]:
    # Try exact and suffix variants that came from duplicated exports.
    candidates = [domain]
    for i in range(1, 10):
        candidates.append(f"{domain}__{i}")
    for d in candidates:
        for path in Path("/tmp").glob(f"tmp*/crawl_artifacts/{d}.json"):
            try:
                return json.loads(path.read_text())
            except Exception:
                continue
    return None


def reconstruct_lead(failed_obj: Dict[str, Any], domain: str) -> Optional[Dict[str, Any]]:
    preview = failed_obj.get("lead_preview") or {}
    if not isinstance(preview, dict):
        preview = {}

    crawl = load_tmp_crawl_artifact(domain) or {}
    extracted = crawl.get("extracted_data") if isinstance(crawl, dict) else {}
    if not isinstance(extracted, dict):
        extracted = {}
    company = extracted.get("company") if isinstance(extracted.get("company"), dict) else {}
    team_members = extracted.get("team_members") if isinstance(extracted.get("team_members"), list) else []
    member = team_members[0] if team_members else {}
    if not isinstance(member, dict):
        member = {}

    business = (preview.get("business") or company.get("name") or domain).strip()
    website = (preview.get("website") or f"https://{domain}").strip()
    email = (preview.get("email") or member.get("email") or "").strip()
    role = (member.get("role") or "Advisor").strip()
    full_name = (member.get("name") or "").strip()
    if not full_name or full_name.lower() in {"not listed", "unknown", "n/a"}:
        # Use email local-part heuristic when possible.
        if "@" in email:
            local = email.split("@", 1)[0].replace(".", " ").replace("_", " ").strip()
            full_name = " ".join([w.capitalize() for w in local.split() if w]) or business
        else:
            full_name = business
    first, last = split_name(full_name)
    if not first or not last:
        first = first or "Contact"
        last = last or "Lead"

    industry = (preview.get("industry") or company.get("industry") or "Consulting").strip()
    sub_industry = (preview.get("sub_industry") or company.get("sub_industry") or "Business Consulting").strip()
    desc = (company.get("description") or f"{business} provides consulting services.").strip()
    emp = normalize_employee_count(str(company.get("employee_count") or "11-50"))
    hq_city, hq_state, hq_country = parse_hq_location(str(company.get("hq_location") or ""))
    country = hq_country or "United States"
    state = hq_state or ("CA" if country.lower() in {"united states", "usa", "us"} else "")
    city = hq_city or "Unknown City"

    socials = company.get("socials") if isinstance(company.get("socials"), dict) else {}
    company_linkedin = clean_linkedin((socials.get("linkedin") or ""), "company")
    person_linkedin = clean_linkedin((member.get("linkedin") or ""), "person")

    lead = {
        "business": business,
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
        "description": desc,
        "employee_count": emp,
        "source_url": website,
        "source_type": "company_site",
        "phone_numbers": [member.get("phone")] if member.get("phone") else [],
        "hq_country": country,
        "hq_state": state,
        "hq_city": city,
    }
    return lead


def main() -> int:
    parser = argparse.ArgumentParser(description="Retry failed precheck artifacts")
    parser.add_argument("--failed-dir", default="lead_queue/failed")
    parser.add_argument("--pending-dir", default="lead_queue/pending")
    parser.add_argument("--limit", type=int, default=0, help="0 = all")
    args = parser.parse_args()

    failed_dir = Path(args.failed_dir)
    pending_dir = Path(args.pending_dir)
    pending_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(failed_dir.glob("*.precheck_failed.json"))
    if args.limit > 0:
        files = files[: args.limit]

    total = len(files)
    reconstructed = 0
    queued = 0
    still_failed = 0
    skipped_exists = 0

    for f in files:
        try:
            failed_obj = json.loads(f.read_text())
        except Exception:
            continue
        source_file = failed_obj.get("source_file", f.name)
        domain = parse_domain_from_source_file(source_file)
        lead = reconstruct_lead(failed_obj, domain)
        if not lead:
            still_failed += 1
            continue
        reconstructed += 1

        # Retry enrichment pass
        lead = enrich_linkedin_fields(lead)

        ok, reason = precheck_lead(lead)
        if not ok:
            still_failed += 1
            failed_obj["retry_reason"] = reason
            f.write_text(json.dumps(failed_obj, ensure_ascii=True, indent=2))
            continue

        key = queue_key(lead)
        out = pending_dir / f"{key}.json"
        if out.exists():
            skipped_exists += 1
            continue
        out.write_text(json.dumps(lead, ensure_ascii=True, indent=2))
        queued += 1

    print(f"Failed inputs:         {total}")
    print(f"Reconstructed leads:   {reconstructed}")
    print(f"Queued to pending:     {queued}")
    print(f"Still failing precheck:{still_failed}")
    print(f"Skipped existing:      {skipped_exists}")
    print(f"Pending dir:           {pending_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
