#!/usr/bin/env python3
"""
Capture ScrapingDog API responses (Google + LinkedIn profile) for a person URL, then run the
same regrade-style pipeline (_process_one) and save both artifacts under reports/.

Does not run Lead Sorcerer (domain/crawl). Stops after writing reports — no queue moves.

Usage (repo root):
  python3 scripts/scrapingdog_lead_report.py --linkedin-url "https://www.linkedin.com/in/example"
  python3 scripts/scrapingdog_lead_report.py --linkedin-url "..." --no-enrich
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

load_dotenv(_REPO / ".env")


def _load_regrade():
    path = _REPO / "scripts" / "regrade_b_queue.py"
    spec = importlib.util.spec_from_file_location("_regrade_lead_report", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _norm_linkedin(u: str) -> str:
    s = (u or "").strip().split("?")[0].rstrip("/")
    if not s.lower().startswith("http"):
        s = "https://" + s.lstrip("/")
    return s


def _slug(u: str) -> str:
    low = u.rstrip("/").lower()
    if "/in/" in low:
        i = low.index("/in/") + len("/in/")
        return low[i:].split("/")[0][:80] or "profile"
    return "profile"


def main() -> int:
    p = argparse.ArgumentParser(
        description="Save ScrapingDog raw JSON + derived lead (regrade pipeline) to reports/"
    )
    p.add_argument("--linkedin-url", required=True, help="Person profile linkedin.com/in/...")
    p.add_argument(
        "--output-dir",
        default="reports",
        help="Directory under repo root (default: reports)",
    )
    p.add_argument(
        "--no-enrich",
        action="store_true",
        help="Skip enrich_linkedin_fields (Apify etc.); keep ScrapingDog repair + location in _process_one",
    )
    p.add_argument("--scrapingdog-fix", type=int, default=1, choices=(0, 1))
    args = p.parse_args()

    li = _norm_linkedin(args.linkedin_url)
    if "/in/" not in li.lower():
        print("Need a linkedin.com/in/... URL", file=sys.stderr)
        return 2

    rb = _load_regrade()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = _slug(li)
    out_dir = _REPO / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_path = out_dir / f"scrapingdog_raw_{slug}_{ts}.json"
    lead_path = out_dir / f"lead_pipeline_{slug}_{ts}.json"

    raw_bundle = {
        "saved_at_utc": ts,
        "linkedin_url": li,
        "scrapingdog": {
            "google_quoted_url": {
                "query": f'"{li}"',
                "response": rb._scrapingdog_google_json(f'"{li}"'),
            },
            "google_quoted_url_location": {
                "query": f'"{li}" location',
                "response": rb._scrapingdog_google_json(f'"{li}" location'),
            },
            "linkedin_profile": {
                "endpoint": "https://api.scrapingdog.com/profile (type=profile, id=slug)",
                "response": rb._scrapingdog_linkedin_profile_json(li),
            },
        },
    }
    raw_path.write_text(json.dumps(raw_bundle, ensure_ascii=True, indent=2), encoding="utf-8")
    print(raw_path)

    enrich_fn = rb._load_enrich() if not args.no_enrich else None
    scrapingdog_fixer = rb._load_scrapingdog_fixer() if args.scrapingdog_fix == 1 else None
    parse_hq_location, normalize_country_name = rb._load_location_helpers()

    lead_in = {"linkedin": li}
    lead, pre_ok, reason, bucket, fixed_fields = rb._process_one(
        lead_in,
        enrich_fn=enrich_fn,
        scrapingdog_fixer=scrapingdog_fixer,
        prefer_scrapingdog_linkedin_search=args.scrapingdog_fix == 1,
        parse_hq_location=parse_hq_location,
        normalize_country_name=normalize_country_name,
    )
    store = rb._ensure_normal_fields(lead)
    payload = {
        "generated_at_utc": ts,
        "linkedin_url": li,
        "scrapingdog_raw_report": str(raw_path.relative_to(_REPO)),
        "no_enrich": args.no_enrich,
        "scrapingdog_fix": args.scrapingdog_fix,
        "precheck_ok": pre_ok,
        "precheck_reason": reason,
        "route_lead_bucket": bucket,
        "field_fixes_applied": fixed_fields,
        "lead_normalized": store,
        "lead_input_snapshot": lead_in,
    }
    lead_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    print(lead_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
