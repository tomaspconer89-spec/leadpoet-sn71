#!/usr/bin/env python3
"""
Re-precheck JSON leads in lead_queue/collected_pass/, submit those that pass via the
gateway, and move verified files to lead_queue/submitted/.

- Precheck failure: written to lead_queue/collected_precheck_fail/<stem>.precheck_failed.json,
  original removed from collected_pass.
- Duplicate email / person+company LinkedIn: moved to lead_queue/failed/ (same as submit_queued_leads).
- Presign / S3 / verify failure: file stays in collected_pass for retry.
- If lead_queue/submitted/<same filename> already exists: skipped.

Usage (from repo root):
  python3 scripts/submit_collected_pass.py
  python3 scripts/submit_collected_pass.py --max 10 --enrich-linkedin 1
  # If gateway /presign says hotkey not registered (metagraph lag), stage for the miner:
  python3 scripts/submit_collected_pass.py --enqueue-pending
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

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
    check_email_duplicate,
    check_linkedin_combo_duplicate,
    gateway_get_presigned_url,
    gateway_upload_lead,
    gateway_verify_submission,
)
from miner_models.lead_precheck import precheck_lead


def _load_enrich():
    conv = _REPO_ROOT / "scripts" / "convert_raw_to_pending.py"
    if not conv.is_file():
        return None
    spec = importlib.util.spec_from_file_location("_crtp", conv)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return getattr(mod, "enrich_linkedin_fields", None)


def main() -> int:
    parser = argparse.ArgumentParser(description="Precheck + submit leads from collected_pass")
    parser.add_argument("--wallet-name", default="YOUR_COLDKEY_NAME")
    parser.add_argument("--wallet-hotkey", default="culture")
    parser.add_argument("--max", type=int, default=100, help="Max files to process from collected_pass")
    parser.add_argument(
        "--enrich-linkedin",
        type=int,
        default=0,
        choices=(0, 1),
        help="1 = run enrich_linkedin_fields from convert_raw_to_pending before precheck",
    )
    parser.add_argument(
        "--enqueue-pending",
        action="store_true",
        help="After precheck + duplicate checks, copy to lead_queue/pending/ and remove from "
        "collected_pass (no gateway). Use when /presign rejects registration temporarily.",
    )
    args = parser.parse_args()

    queue_root = _REPO_ROOT / "lead_queue"
    collected = queue_root / "collected_pass"
    fail_dir = queue_root / "collected_precheck_fail"
    submitted_dir = queue_root / "submitted"
    failed_dir = queue_root / "failed"
    pending_dir = queue_root / "pending"
    for d in (collected, fail_dir, submitted_dir, failed_dir, pending_dir):
        d.mkdir(parents=True, exist_ok=True)

    enrich = _load_enrich() if args.enrich_linkedin == 1 else None

    files = sorted(collected.glob("*.json"))[: args.max]
    if not files:
        print("No *.json in lead_queue/collected_pass")
        return 0

    wallet = None if args.enqueue_pending else _wallet(args.wallet_name, args.wallet_hotkey)
    verified_count = 0
    duplicate_count = 0
    precheck_fail_count = 0
    skipped_count = 0
    enqueued_count = 0

    print(f"Processing up to {len(files)} file(s) from {collected}")
    if args.enqueue_pending:
        print("Mode: --enqueue-pending (precheck → lead_queue/pending/, no gateway submit)")

    for path in files:
        if (submitted_dir / path.name).exists():
            print(f"Skip (already in submitted): {path.name}")
            skipped_count += 1
            continue

        try:
            lead = json.loads(path.read_text())
        except Exception as e:
            print(f"Invalid JSON {path.name}: {e}")
            path.rename(failed_dir / path.name)
            continue

        if enrich is not None:
            try:
                lead = enrich(dict(lead))
            except Exception:
                pass

        business_name = lead.get("business", "Unknown")
        email = lead.get("email", "")
        linkedin_url = lead.get("linkedin", "")
        company_linkedin_url = lead.get("company_linkedin", "")

        ok_precheck, precheck_reason = precheck_lead(lead)
        if not ok_precheck:
            print(f"Precheck fail: {business_name} ({precheck_reason})")
            fail_path = fail_dir / f"{path.stem}.precheck_failed.json"
            payload = {"source_file": path.name, "reason": precheck_reason, "lead": lead}
            fail_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2))
            path.unlink(missing_ok=True)
            precheck_fail_count += 1
            continue

        if check_email_duplicate(email):
            print(f"Duplicate email: {business_name} ({email})")
            duplicate_count += 1
            path.rename(failed_dir / path.name)
            continue

        if linkedin_url and company_linkedin_url and check_linkedin_combo_duplicate(
            linkedin_url, company_linkedin_url
        ):
            print(f"Duplicate person+company LinkedIn: {business_name}")
            duplicate_count += 1
            path.rename(failed_dir / path.name)
            continue

        if args.enqueue_pending:
            dest = pending_dir / path.name
            if dest.exists():
                print(f"Already in pending; dropping duplicate from collected_pass: {path.name}")
                path.unlink(missing_ok=True)
            else:
                dest.write_text(json.dumps(lead, ensure_ascii=True, indent=2))
                path.unlink(missing_ok=True)
                enqueued_count += 1
                print(f"Enqueued: {business_name} -> pending/{path.name}")
            continue

        assert wallet is not None
        presign_result = gateway_get_presigned_url(wallet, lead)
        if not presign_result:
            print(
                f"Presign failed for {business_name}; leaving {path.name} in collected_pass. "
                "If the gateway said 'Hotkey not registered on subnet' but btcli shows you on netuid 71, "
                "the hosted metagraph may lag—retry later or run with --enqueue-pending then ./run-miner.sh."
            )
            continue

        if not gateway_upload_lead(presign_result["s3_url"], lead):
            print(f"S3 upload failed for {business_name}; leaving {path.name} in collected_pass.")
            continue

        if gateway_verify_submission(wallet, presign_result["lead_id"]):
            verified_count += 1
            print(f"Verified: {business_name} -> submitted/{path.name}")
            path.rename(submitted_dir / path.name)
        else:
            print(f"Verify failed for {business_name}; leaving {path.name} in collected_pass.")

    print(
        f"Done. verified={verified_count} enqueued_pending={enqueued_count} "
        f"precheck_fail={precheck_fail_count} duplicates_to_failed={duplicate_count} "
        f"skipped_already_submitted={skipped_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
