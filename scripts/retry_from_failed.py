#!/usr/bin/env python3
"""
Retry enrichment for precheck-failed artifacts.

Flow:
1) Read *.precheck_failed.json from ``--failed-dir``
2) Reconstruct lead from embedded lead payload (or preview + /tmp crawl artifacts)
3) Run ``enrich_linkedin_fields`` from ``convert_raw_to_pending.py`` (Apify search
   to discover URLs if missing, then Apify LinkedIn person/company actors from
   ``APIFY_LINKEDIN_*_ACTOR_ID`` to fill fields — same as the main pipeline)
4) Run precheck again
5) **Pass:** write to ``--pending-dir`` (e.g. collected_pass)
6) **Still fail:** write to ``--still-fail-dir`` with ``reason`` (and prior reason),
   then **remove** the artifact from ``--failed-dir`` (unless ``--update-source-failed``)
7) **Pass:** write to ``--pending-dir`` and **remove** the artifact from ``--failed-dir``

Requires repo-root ``.env`` with ``APIFY_API_TOKEN`` and actor IDs (see ``env.example``).
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

# Ensure repository root is importable when running as:
#   python3 scripts/retry_from_failed.py
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from miner_models.lead_precheck import precheck_lead


def _load_enrich_linkedin_fields() -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    conv = REPO_ROOT / "scripts" / "convert_raw_to_pending.py"
    if not conv.is_file():
        raise FileNotFoundError(f"Missing {conv}")
    spec = importlib.util.spec_from_file_location("_crtp_retry_failed", conv)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module spec for {conv}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    fn = getattr(mod, "enrich_linkedin_fields", None)
    if fn is None:
        raise AttributeError(f"{conv} has no enrich_linkedin_fields")
    return fn


enrich_linkedin_fields = _load_enrich_linkedin_fields()


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


def _unlink_if_under_failed_dir(path: Path, failed_dir: Path) -> None:
    """Remove ``path`` only if it is a file inside ``failed_dir`` (safety guard)."""
    try:
        path.resolve().relative_to(failed_dir.resolve())
    except ValueError:
        return
    try:
        if path.is_file():
            path.unlink()
    except OSError:
        pass


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
    # Newer queue keys are sha256 digests; they are not domains.
    if "." not in base:
        return ""
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
    embedded = failed_obj.get("lead")
    if isinstance(embedded, dict) and embedded:
        lead = dict(embedded)
        if not (lead.get("website") or "").strip() and domain:
            lead["website"] = f"https://{domain}"
        if not (lead.get("source_url") or "").strip():
            lead["source_url"] = str(lead.get("website") or "")
        if not (lead.get("source_type") or "").strip():
            lead["source_type"] = "company_site"
        return lead

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
    parser.add_argument("--failed-dir", default="lead_queue/collected_precheck_fail")
    parser.add_argument("--pending-dir", default="lead_queue/collected_pass")
    parser.add_argument(
        "--still-fail-dir",
        default="lead_queue/collected_precheck_fail_after_retry",
        help="Where to save leads that still fail precheck after retry (with reason fields).",
    )
    parser.add_argument(
        "--update-source-failed",
        action="store_true",
        help="Keep and rewrite the original in --failed-dir (still-fail case); default is delete after copy to --still-fail-dir.",
    )
    parser.add_argument("--limit", type=int, default=0, help="0 = all")
    args = parser.parse_args()

    failed_dir = Path(args.failed_dir)
    pending_dir = Path(args.pending_dir)
    still_fail_dir = Path(args.still_fail_dir)
    pending_dir.mkdir(parents=True, exist_ok=True)
    still_fail_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(failed_dir.glob("*.precheck_failed.json"))
    if args.limit > 0:
        files = files[: args.limit]

    total = len(files)
    reconstructed = 0
    queued = 0
    still_failed = 0
    skipped_exists = 0
    fail_reasons: Counter[str] = Counter()

    for f in files:
        try:
            failed_obj = json.loads(f.read_text())
        except Exception:
            continue
        source_file = failed_obj.get("source_file", f.name)
        domain = parse_domain_from_source_file(source_file)
        if not domain:
            lead_obj = failed_obj.get("lead") if isinstance(failed_obj, dict) else {}
            if isinstance(lead_obj, dict):
                website = str(lead_obj.get("website") or lead_obj.get("source_url") or "").strip()
                if website:
                    host = website.replace("https://", "").replace("http://", "").split("/")[0]
                    domain = host.replace("www.", "")
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
            r = (reason or "unknown").strip() or "unknown"
            fail_reasons[r] += 1
            prev_reason = failed_obj.get("reason")
            key = queue_key(lead)
            after_payload: Dict[str, Any] = {
                "reason": r,
                "retry_reason": r,
                "previous_reason": prev_reason,
                "business": lead.get("business"),
                "lead": lead,
                "source_failed_basename": f.name,
                "source_failed_path": str(f.resolve()),
                "queue_key": key,
            }
            out_still = still_fail_dir / f"{key}.precheck_failed_after_retry.json"
            out_still.write_text(
                json.dumps(after_payload, ensure_ascii=True, indent=2, default=str)
            )
            if args.update_source_failed:
                failed_obj["lead"] = lead
                failed_obj["retry_reason"] = r
                failed_obj["previous_reason"] = prev_reason
                failed_obj["after_retry_artifact"] = str(out_still.resolve())
                f.write_text(json.dumps(failed_obj, ensure_ascii=True, indent=2, default=str))
            else:
                _unlink_if_under_failed_dir(f, failed_dir)
            continue

        key = queue_key(lead)
        out = pending_dir / f"{key}.json"
        if out.exists():
            skipped_exists += 1
        else:
            out.write_text(json.dumps(lead, ensure_ascii=True, indent=2))
            queued += 1
        _unlink_if_under_failed_dir(f, failed_dir)

    print(f"Failed inputs:         {total}")
    print(f"Reconstructed leads:   {reconstructed}")
    print(f"Queued to pending:     {queued}")
    print(f"Still failing precheck:{still_failed}")
    print(f"Skipped existing:      {skipped_exists}")
    print(f"Pending dir:           {pending_dir.resolve()}")
    print(f"Still fail after retry:{still_fail_dir.resolve()}")
    if fail_reasons:
        print("Top fail reasons:")
        for reason, count in fail_reasons.most_common(10):
            print(f"  {count} :: {reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
