#!/usr/bin/env python3
"""
Convert lead_queue/raw_generated artifacts into SN71 payloads, run precheck,
and write valid leads into lead_queue/pending for submit_queued_leads.py.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

    best_person = team[0] if team else {}
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
    emp = normalize_employee_count(str(company.get("employee_count") or ""))
    hq_location = (company.get("hq_location") or "").strip()
    city, state, country = parse_hq_location(hq_location)

    socials = company.get("socials") if isinstance(company.get("socials"), dict) else {}
    company_linkedin = clean_linkedin((socials.get("linkedin") or ""), "company")
    person_linkedin = clean_linkedin((best_person.get("linkedin") or ""), "person")

    source_url = derive_source_url(doc, domain)
    website = f"https://{domain}" if domain else source_url

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
    required_min = ["business", "full_name", "email", "industry", "sub_industry", "country", "city"]
    if any(not str(lead.get(k, "")).strip() for k in required_min):
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
