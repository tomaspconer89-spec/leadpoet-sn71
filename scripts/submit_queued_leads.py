#!/usr/bin/env python3
"""
Submit locally queued leads (lead_queue/pending) when gateway is reachable.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Allow execution from any cwd.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dotenv import load_dotenv
load_dotenv(_REPO_ROOT / ".env")

import bittensor as bt


def _wallet(name: str, hotkey: str):
    w = getattr(bt, "wallet", None)
    if callable(w):
        return w(name=name, hotkey=hotkey)
    W = getattr(bt, "Wallet", None)
    if W is not None:
        return W(name=name, hotkey=hotkey)
    raise RuntimeError("bittensor: cannot construct wallet (no bt.wallet / bt.Wallet)")


from Leadpoet.utils.cloud_db import (
    attach_gateway_attestation_fields,
    check_email_duplicate,
    check_linkedin_combo_duplicate,
    gateway_get_presigned_url,
    gateway_upload_lead,
    gateway_verify_submission_outcome,
)
from miner_models.lead_precheck import precheck_lead
from miner_models.minimal_lead_blob import minimal_gateway_lead


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit queued leads")
    parser.add_argument("--wallet-name", default="YOUR_COLDKEY_NAME")
    parser.add_argument("--wallet-hotkey", default="culture")
    parser.add_argument("--max", type=int, default=100, help="Max pending leads to try")
    parser.add_argument(
        "--submit-delay-seconds",
        type=float,
        default=18.0,
        help="Pause before each gateway submit after the first (anti-spam cooldown).",
    )
    args = parser.parse_args()

    queue_root = _REPO_ROOT / "lead_queue"
    pending_dir = queue_root / "pending"
    submitted_dir = queue_root / "submitted"
    failed_dir = queue_root / "failed"
    pending_dir.mkdir(parents=True, exist_ok=True)
    submitted_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)

    wallet = _wallet(args.wallet_name, args.wallet_hotkey)
    pending_files = sorted(pending_dir.glob("*.json"))[: args.max]
    if not pending_files:
        print("No queued leads found in lead_queue/pending")
        return 0

    print(f"Found {len(pending_files)} queued lead(s)")
    verified_count = 0
    duplicate_count = 0
    gateway_reject_count = 0
    gateway_submit_serial = 0

    for path in pending_files:
        try:
            lead = json.loads(path.read_text())
        except Exception as e:
            print(f"Invalid JSON in {path.name}: {e}")
            path.rename(failed_dir / path.name)
            continue

        lead = minimal_gateway_lead(lead)

        business_name = lead.get("business", "Unknown")
        email = lead.get("email", "")
        linkedin_url = lead.get("linkedin", "")
        company_linkedin_url = lead.get("company_linkedin", "")

        ok_precheck, precheck_reason = precheck_lead(lead)
        if not ok_precheck:
            print(f"Skipping precheck-failed lead: {business_name} ({precheck_reason})")
            path.rename(failed_dir / path.name)
            continue

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

        try:
            lead = attach_gateway_attestation_fields(lead, wallet)
        except RuntimeError as e:
            print(f"Attestation setup failed for {business_name}: {e}")
            continue

        if gateway_submit_serial > 0 and args.submit_delay_seconds > 0:
            time.sleep(args.submit_delay_seconds)
        gateway_submit_serial += 1

        presign_result = gateway_get_presigned_url(wallet, lead)
        if not presign_result:
            print(
                f"Failed presign for {business_name}; gateway/network issue. "
                "Keeping in pending and trying next lead."
            )
            continue

        if not gateway_upload_lead(presign_result["s3_url"], lead):
            print(f"Failed S3 upload for {business_name}; keeping in queue.")
            continue

        verify_out = gateway_verify_submission_outcome(wallet, presign_result["lead_id"])
        if verify_out.ok:
            verified_count += 1
            print(f"Verified: {business_name}")
            path.rename(submitted_dir / path.name)
        elif verify_out.terminal_conflict:
            gateway_reject_count += 1
            reject_path = failed_dir / f"{path.stem}.gateway_rejected.json"
            reject_path.write_text(
                json.dumps(
                    {
                        "source_file": path.name,
                        "lead_id": presign_result.get("lead_id"),
                        "http_status": verify_out.http_status,
                        "error": verify_out.error_code,
                        "message": verify_out.message,
                        "lead": lead,
                    },
                    ensure_ascii=True,
                    indent=2,
                    default=str,
                )
            )
            path.unlink(missing_ok=True)
            print(
                f"Gateway conflict (terminal): {business_name} -> failed/{reject_path.name}"
            )
        else:
            print(f"Verification failed: {business_name}; keeping in queue.")

    print(f"Verified/submitted: {verified_count}")
    print(f"Duplicates moved to failed: {duplicate_count}")
    print(f"Gateway 409 rejects (failed/): {gateway_reject_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
