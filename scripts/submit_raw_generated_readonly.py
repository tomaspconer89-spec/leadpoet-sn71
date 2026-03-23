#!/usr/bin/env python3
"""
Submit leads by reading raw crawl JSON only (read-only on disk).

- Reads *.json from a directory (default: lead_queue/raw_generated_fresh_only).
- Does NOT rename, move, delete, or write any file in that directory.
- Does NOT create lead_queue/pending or move files to submitted/failed.

Maps each document with the same logic as convert_raw_to_pending.map_raw_to_lead,
optionally enriches LinkedIn fields, then runs precheck + gateway submit (in memory).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_scripts = _REPO_ROOT / "scripts"
if str(_scripts) not in sys.path:
    sys.path.insert(0, str(_scripts))

from dotenv import load_dotenv

load_dotenv(_REPO_ROOT / ".env")


def _resolve_dir(p: str) -> Path:
    path = Path(p)
    if path.is_absolute():
        return path
    return _REPO_ROOT / path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Submit from raw_generated JSON (read-only; no queue file changes)"
    )
    parser.add_argument(
        "--in-dir",
        default="lead_queue/raw_generated_fresh_only",
        help="Directory of raw JSON files (only read; never modified)",
    )
    parser.add_argument("--wallet-name", default="YOUR_COLDKEY_NAME")
    parser.add_argument("--wallet-hotkey", default="culture")
    parser.add_argument("--max", type=int, default=100, help="Max raw files to attempt")
    parser.add_argument(
        "--enrich-linkedin",
        type=int,
        default=1,
        help="1=search and fill missing linkedin (uses APIs), 0=skip",
    )
    args = parser.parse_args()

    # Defer imports: `Leadpoet.utils.cloud_db` pulls in bittensor, which hijacks argv if loaded first.
    import convert_raw_to_pending as raw_conv
    import bittensor as bt
    from Leadpoet.utils.cloud_db import (
        check_email_duplicate,
        check_linkedin_combo_duplicate,
        gateway_get_presigned_url,
        gateway_upload_lead,
        gateway_verify_submission,
    )
    from miner_models.lead_precheck import precheck_lead

    in_dir = _resolve_dir(args.in_dir)
    if not in_dir.is_dir():
        print(f"Not a directory: {in_dir}", file=sys.stderr)
        return 1

    wallet = bt.wallet(name=args.wallet_name, hotkey=args.wallet_hotkey)
    files = sorted(in_dir.glob("*.json"))[: args.max]

    if not files:
        print(f"No *.json in {in_dir}")
        return 0

    print(f"Read-only submit from {in_dir} ({len(files)} file(s)); queue files untouched.")

    verified_count = 0
    skipped_map = 0
    skipped_precheck = 0
    duplicate_count = 0
    failed_gateway = 0

    for path in files:
        try:
            doc = json.loads(path.read_text())
        except Exception as e:
            print(f"Skip {path.name}: invalid JSON ({e})")
            skipped_map += 1
            continue

        lead = raw_conv.map_raw_to_lead(doc, path.name)
        if not lead:
            skipped_map += 1
            continue

        if args.enrich_linkedin == 1:
            lead = raw_conv.enrich_linkedin_fields(lead)

        business_name = lead.get("business", "Unknown")
        email = lead.get("email", "")
        linkedin_url = lead.get("linkedin", "")
        company_linkedin_url = lead.get("company_linkedin", "")

        ok_precheck, precheck_reason = precheck_lead(lead)
        if not ok_precheck:
            print(f"Precheck skip: {business_name} ({precheck_reason})")
            skipped_precheck += 1
            continue

        if check_email_duplicate(email):
            print(f"Duplicate email skip: {business_name} ({email})")
            duplicate_count += 1
            continue

        if (
            linkedin_url
            and company_linkedin_url
            and check_linkedin_combo_duplicate(linkedin_url, company_linkedin_url)
        ):
            print(f"Duplicate person+company skip: {business_name}")
            duplicate_count += 1
            continue

        presign_result = gateway_get_presigned_url(wallet, lead)
        if not presign_result:
            print(f"Presign failed: {business_name} (gateway/network); trying next.")
            failed_gateway += 1
            continue

        if not gateway_upload_lead(presign_result["s3_url"], lead):
            print(f"S3 upload failed: {business_name}")
            failed_gateway += 1
            continue

        if gateway_verify_submission(wallet, presign_result["lead_id"]):
            verified_count += 1
            print(f"Verified: {business_name}")
        else:
            print(f"Verification failed: {business_name}")
            failed_gateway += 1

    print(
        f"Done. submitted={verified_count} skipped_unmappable={skipped_map} "
        f"precheck_fail={skipped_precheck} duplicate={duplicate_count} gateway_fail={failed_gateway}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
