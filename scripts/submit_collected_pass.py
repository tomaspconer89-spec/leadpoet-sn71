#!/usr/bin/env python3
"""
Re-precheck JSON leads in lead_queue/collected_pass/, submit those that pass via the
gateway, and move verified files to lead_queue/submitted/.

- Precheck failure: written to lead_queue/collected_precheck_fail/<stem>.precheck_failed.json,
  original removed from collected_pass.
- Duplicate email / person+company LinkedIn: moved to lead_queue/failed/ (same as submit_queued_leads).
- Gateway HTTP 409 (e.g. duplicate_email_processing, duplicate_linkedin_combo): removed from
  collected_pass; lead_queue/failed/<stem>.gateway_rejected.json records error + lead snapshot.
- Presign / S3 / non-409 verify failure: file stays in collected_pass for retry.
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
import re
import sys
import time
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
    attach_gateway_attestation_fields,
    check_email_duplicate,
    check_linkedin_combo_duplicate,
    gateway_get_presigned_url,
    gateway_upload_lead,
    gateway_verify_submission_outcome,
)
from miner_models.lead_precheck import precheck_lead
from miner_models.minimal_lead_blob import minimal_gateway_lead


def _load_enrich():
    conv = _REPO_ROOT / "scripts" / "convert_raw_to_pending.py"
    if not conv.is_file():
        return None
    spec = importlib.util.spec_from_file_location("_crtp", conv)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return getattr(mod, "enrich_linkedin_fields", None)


def _trim_description_for_gateway(lead: dict, max_len: int = 2000) -> tuple[dict, int]:
    """Trim oversize description to satisfy gateway max length."""
    desc = lead.get("description")
    if not isinstance(desc, str):
        return lead, 0
    if len(desc) <= max_len:
        return lead, 0
    trimmed = dict(lead)
    trimmed["description"] = desc[:max_len].rstrip()
    return trimmed, len(desc) - len(trimmed["description"])


def _normalize_linkedin_url(raw: str, kind: str) -> str:
    if not isinstance(raw, str):
        return ""
    val = raw.strip().rstrip(").,;")
    if not val:
        return ""
    if val.startswith("/"):
        val = f"https://www.linkedin.com{val}"
    elif "linkedin.com" in val.lower() and not val.lower().startswith(("http://", "https://")):
        val = f"https://{val.lstrip('/')}"
    m = re.match(
        r"^(https?://(?:[a-z0-9-]+\.)?linkedin\.com)/(in|company)/([^/?#]+)",
        val,
        flags=re.I,
    )
    if not m:
        return ""
    bucket = m.group(2).lower()
    slug = m.group(3).strip()
    if kind == "person" and bucket != "in":
        return ""
    if kind == "company" and bucket != "company":
        return ""
    return f"https://www.linkedin.com/{bucket}/{slug}"


def _normalize_linkedin_fields(lead: dict) -> dict:
    out = dict(lead)
    p = _normalize_linkedin_url(out.get("linkedin", ""), kind="person")
    if p:
        out["linkedin"] = p
    c = _normalize_linkedin_url(out.get("company_linkedin", ""), kind="company")
    if c:
        out["company_linkedin"] = c
    socials = out.get("socials")
    if isinstance(socials, dict):
        sc = _normalize_linkedin_url(socials.get("linkedin", ""), kind="company")
        socials = dict(socials)
        socials["linkedin"] = sc or None
        out["socials"] = socials
    return out


def _submitted_path_for_attempt(submitted_dir: Path, src_name: str, attempt_idx: int) -> Path:
    """Return a collision-safe path in submitted/ for this attempt."""
    base = Path(src_name).stem
    suffix = Path(src_name).suffix or ".json"
    candidate = submitted_dir / src_name
    if not candidate.exists():
        return candidate
    i = max(2, int(attempt_idx))
    while True:
        candidate = submitted_dir / f"{base}.attempt{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def _write_failed_outcome(
    failed_dir: Path,
    src_name: str,
    status: str,
    lead: dict,
    *,
    lead_id: str | None = None,
    error_code: str | None = None,
    message: str | None = None,
    http_status: int | None = None,
) -> Path:
    out = failed_dir / f"{Path(src_name).stem}.submission_outcome.json"
    payload = {
        "source_file": src_name,
        "submission_status": status,
        "lead_id": lead_id,
        "http_status": http_status,
        "error": error_code,
        "message": message,
        "lead": lead,
    }
    out.write_text(json.dumps(payload, ensure_ascii=True, indent=2, default=str))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Precheck + submit leads from collected_pass")
    parser.add_argument("--wallet-name", default="YOUR_COLDKEY_NAME")
    parser.add_argument("--wallet-hotkey", default="veil")
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
    parser.add_argument(
        "--submit-delay-seconds",
        type=float,
        default=18.0,
        help="Pause before each gateway submit after the first (reduces anti-spam cooldown errors).",
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
    rejected_count = 0
    attempted_unverified_count = 0
    precheck_fail_count = 0
    enqueued_count = 0
    moved_to_submitted = 0
    submitted_collision_count = 0

    print(f"Processing up to {len(files)} file(s) from {collected}")
    if args.enqueue_pending:
        print("Mode: --enqueue-pending (precheck → lead_queue/pending/, no gateway submit)")

    gateway_submit_serial = 0
    for attempt_idx, path in enumerate(files, 1):

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

        lead = _normalize_linkedin_fields(lead)
        lead = minimal_gateway_lead(lead)
        business_name = lead.get("business", "Unknown")
        email = lead.get("email", "")
        lead, trimmed_chars = _trim_description_for_gateway(lead, max_len=2000)
        if trimmed_chars > 0:
            print(
                f"Trimmed description for gateway: {business_name} (-{trimmed_chars} chars)"
            )
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
            _write_failed_outcome(
                failed_dir,
                path.name,
                "duplicate_email",
                lead,
                error_code="duplicate_email",
                message="Duplicate email already exists",
            )
            target = _submitted_path_for_attempt(submitted_dir, path.name, attempt_idx)
            if target.name != path.name:
                submitted_collision_count += 1
            path.rename(target)
            moved_to_submitted += 1
            continue

        if linkedin_url and company_linkedin_url and check_linkedin_combo_duplicate(
            linkedin_url, company_linkedin_url
        ):
            print(f"Duplicate person+company LinkedIn: {business_name}")
            duplicate_count += 1
            _write_failed_outcome(
                failed_dir,
                path.name,
                "duplicate_linkedin_combo",
                lead,
                error_code="duplicate_linkedin_combo",
                message="Duplicate LinkedIn person+company combo already exists",
            )
            target = _submitted_path_for_attempt(submitted_dir, path.name, attempt_idx)
            if target.name != path.name:
                submitted_collision_count += 1
            path.rename(target)
            moved_to_submitted += 1
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
                f"Presign failed for {business_name}; archiving attempt in submitted. "
                "If the gateway said 'Hotkey not registered on subnet' but btcli shows you on netuid 71, "
                "the hosted metagraph may lag."
            )
            _write_failed_outcome(
                failed_dir,
                path.name,
                "attempted_unverified",
                lead,
                error_code="presign_failed",
                message="Gateway presign failed",
            )
            target = _submitted_path_for_attempt(submitted_dir, path.name, attempt_idx)
            if target.name != path.name:
                submitted_collision_count += 1
            path.rename(target)
            moved_to_submitted += 1
            attempted_unverified_count += 1
            continue

        if not gateway_upload_lead(presign_result["s3_url"], lead):
            print(f"S3 upload failed for {business_name}; archiving attempt in submitted.")
            _write_failed_outcome(
                failed_dir,
                path.name,
                "attempted_unverified",
                lead,
                lead_id=presign_result.get("lead_id"),
                error_code="s3_upload_failed",
                message="Gateway S3 upload failed",
            )
            target = _submitted_path_for_attempt(submitted_dir, path.name, attempt_idx)
            if target.name != path.name:
                submitted_collision_count += 1
            path.rename(target)
            moved_to_submitted += 1
            attempted_unverified_count += 1
            continue

        verify_out = gateway_verify_submission_outcome(wallet, presign_result["lead_id"])
        if verify_out.ok:
            verified_count += 1
            target = _submitted_path_for_attempt(submitted_dir, path.name, attempt_idx)
            if target.name != path.name:
                submitted_collision_count += 1
            print(f"Verified: {business_name} -> submitted/{target.name}")
            path.rename(target)
            moved_to_submitted += 1
        elif verify_out.terminal_conflict:
            rejected_count += 1
            out = _write_failed_outcome(
                failed_dir,
                path.name,
                "rejected",
                lead,
                lead_id=presign_result.get("lead_id"),
                http_status=verify_out.http_status,
                error_code=verify_out.error_code,
                message=verify_out.message,
            )
            target = _submitted_path_for_attempt(submitted_dir, path.name, attempt_idx)
            if target.name != path.name:
                submitted_collision_count += 1
            path.rename(target)
            moved_to_submitted += 1
            print(
                f"Gateway conflict (terminal): {business_name} -> submitted/{target.name} "
                f"(outcome: failed/{out.name})"
            )
        else:
            attempted_unverified_count += 1
            out = _write_failed_outcome(
                failed_dir,
                path.name,
                "attempted_unverified",
                lead,
                lead_id=presign_result.get("lead_id"),
                http_status=verify_out.http_status,
                error_code=verify_out.error_code,
                message=verify_out.message,
            )
            target = _submitted_path_for_attempt(submitted_dir, path.name, attempt_idx)
            if target.name != path.name:
                submitted_collision_count += 1
            path.rename(target)
            moved_to_submitted += 1
            print(
                f"Verify failed (non-terminal): {business_name} -> submitted/{target.name} "
                f"(outcome: failed/{out.name})"
            )

    print(
        f"Done. moved_to_submitted={moved_to_submitted} verified={verified_count} "
        f"attempted_unverified={attempted_unverified_count} rejected={rejected_count} "
        f"duplicates={duplicate_count} submitted_name_collisions={submitted_collision_count} "
        f"enqueued_pending={enqueued_count} precheck_fail={precheck_fail_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
