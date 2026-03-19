#!/usr/bin/env python3
"""
Submit locally queued leads (lead_queue/pending) when gateway is reachable.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import bittensor as bt

from Leadpoet.utils.cloud_db import (
    check_email_duplicate,
    check_linkedin_combo_duplicate,
    gateway_get_presigned_url,
    gateway_upload_lead,
    gateway_verify_submission,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit queued leads")
    parser.add_argument("--wallet-name", default="YOUR_COLDKEY_NAME")
    parser.add_argument("--wallet-hotkey", default="YOUR_HOTKEY_NAME")
    parser.add_argument("--max", type=int, default=100, help="Max pending leads to try")
    args = parser.parse_args()

    queue_root = Path("lead_queue")
    pending_dir = queue_root / "pending"
    submitted_dir = queue_root / "submitted"
    failed_dir = queue_root / "failed"
    pending_dir.mkdir(parents=True, exist_ok=True)
    submitted_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)

    wallet = bt.wallet(name=args.wallet_name, hotkey=args.wallet_hotkey)
    pending_files = sorted(pending_dir.glob("*.json"))[: args.max]
    if not pending_files:
        print("No queued leads found in lead_queue/pending")
        return 0

    print(f"Found {len(pending_files)} queued lead(s)")
    verified_count = 0
    duplicate_count = 0

    for path in pending_files:
        try:
            lead = json.loads(path.read_text())
        except Exception as e:
            print(f"Invalid JSON in {path.name}: {e}")
            path.rename(failed_dir / path.name)
            continue

        business_name = lead.get("business", "Unknown")
        email = lead.get("email", "")
        linkedin_url = lead.get("linkedin", "")
        company_linkedin_url = lead.get("company_linkedin", "")

        if check_email_duplicate(email):
            print(f"Skipping duplicate email: {business_name} ({email})")
            duplicate_count += 1
            path.rename(failed_dir / path.name)
            continue

        if linkedin_url and company_linkedin_url and check_linkedin_combo_duplicate(linkedin_url, company_linkedin_url):
            print(f"Skipping duplicate person+company: {business_name}")
            duplicate_count += 1
            path.rename(failed_dir / path.name)
            continue

        presign_result = gateway_get_presigned_url(wallet, lead)
        if not presign_result:
            print(f"Failed presign for {business_name}; gateway may be down. Stopping.")
            break

        if not gateway_upload_lead(presign_result["s3_url"], lead):
            print(f"Failed S3 upload for {business_name}; keeping in queue.")
            continue

        verification_result = gateway_verify_submission(wallet, presign_result["lead_id"])
        if verification_result:
            verified_count += 1
            print(f"Verified: {business_name}")
            path.rename(submitted_dir / path.name)
        else:
            print(f"Verification failed: {business_name}; keeping in queue.")

    print(f"Verified/submitted: {verified_count}")
    print(f"Duplicates moved to failed: {duplicate_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
