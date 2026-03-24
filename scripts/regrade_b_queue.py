#!/usr/bin/env python3
"""
Re-run normalization + title + person confidence + precheck + route_lead on leads in
lead_queue/B_retry_enrichment/. Updates JSON in place when still B; moves to A_ready_submit
(or other buckets) when routing changes. Updates lead_queue/collected_pass when precheck passes.

Do not raw-mv B -> A: folder placement must match computed route_lead(...).

Usage (repo root):
  python3 scripts/regrade_b_queue.py
  python3 scripts/regrade_b_queue.py --enrich-linkedin 1
  python3 scripts/regrade_b_queue.py --dry-run
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv

from miner_models.lead_normalization import apply_email_classification, normalize_legacy_lead_shape
from miner_models.minimal_lead_blob import minimal_gateway_lead
from miner_models.person_confidence import score_person_confidence
from miner_models.title_normalizer import normalize_title
from scripts.queue_router import route_lead
from scripts.retry_enrichment import targeted_retry_enrichment

load_dotenv(_REPO / ".env")


def _load_enrich():
    conv = _REPO / "scripts" / "convert_raw_to_pending.py"
    if not conv.is_file():
        return None
    spec = importlib.util.spec_from_file_location("_crtp", conv)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return getattr(mod, "enrich_linkedin_fields", None)


def _queue_key(lead: dict) -> str:
    import hashlib

    parts = [
        str(lead.get("email", "")).strip().lower(),
        str(lead.get("business", "")).strip().lower(),
        str(lead.get("linkedin", "")).strip().lower(),
        str(lead.get("website", "")).strip().lower(),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _process_one(
    lead: dict,
    *,
    enrich_fn,
) -> tuple[dict, bool, str | None, str]:
    from miner_models.lead_precheck import precheck_lead

    lead = dict(lead)
    normalize_legacy_lead_shape(lead)
    if enrich_fn is not None:
        try:
            lead = enrich_fn(dict(lead))
        except Exception:
            pass
    normalize_legacy_lead_shape(lead)
    apply_email_classification(lead)

    title_meta = normalize_title(str(lead.get("role", "")))
    lead["title_normalized"] = title_meta["normalized_title"]
    lead["seniority"] = title_meta["seniority"]
    lead["persona_bucket"] = title_meta["persona_bucket"]
    lead["target_fit"] = title_meta["target_fit"]

    conf = score_person_confidence(
        lead, title_matches_persona=title_meta["target_fit"] in ("high", "medium")
    )
    lead.update(conf)
    apply_email_classification(lead)

    pre_ok, reason = precheck_lead(lead)
    if (not pre_ok) and reason:
        if "name_not_in_email" in (reason or "") or "email_domain_mismatch" in (reason or ""):
            lead["identity_conflict"] = True
        lead, _, retry_attempts = targeted_retry_enrichment(
            lead, reason, enrich_linkedin=enrich_fn
        )
        if retry_attempts > 0:
            normalize_legacy_lead_shape(lead)
            apply_email_classification(lead)
            conf2 = score_person_confidence(
                lead,
                title_matches_persona=title_meta["target_fit"] in ("high", "medium"),
            )
            lead.update(conf2)
            pre_ok, reason = precheck_lead(lead)

    bucket = route_lead(lead, precheck_ok=pre_ok, precheck_reason=reason)
    return lead, pre_ok, reason, bucket


def main() -> int:
    p = argparse.ArgumentParser(description="Re-grade B_retry_enrichment leads; promote to A when eligible")
    p.add_argument("--enrich-linkedin", type=int, default=0, choices=(0, 1))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    queue = _REPO / "lead_queue"
    b_dir = queue / "B_retry_enrichment"
    graded_dirs = {
        "A_ready_submit": queue / "A_ready_submit",
        "B_retry_enrichment": queue / "B_retry_enrichment",
        "C_good_account_needs_person": queue / "C_good_account_needs_person",
        "D_low_confidence_hold": queue / "D_low_confidence_hold",
        "E_reject": queue / "E_reject",
    }
    pass_dir = queue / "collected_pass"
    fail_dir = queue / "collected_precheck_fail"
    for d in (*graded_dirs.values(), pass_dir, fail_dir):
        d.mkdir(parents=True, exist_ok=True)

    enrich_fn = _load_enrich() if args.enrich_linkedin == 1 else None
    files = sorted(b_dir.glob("*.json"))
    if not files:
        print(f"No JSON in {b_dir}")
        return 0

    promoted = updated = failed = 0
    for old_path in files:
        business = "?"
        try:
            lead_in = json.loads(old_path.read_text(encoding="utf-8"))
            business = str(lead_in.get("business", "?"))
            lead, pre_ok, reason, bucket = _process_one(lead_in, enrich_fn=enrich_fn)
            key = _queue_key(lead)
            new_graded = graded_dirs[bucket] / f"{key}.json"

            if args.dry_run:
                print(
                    f"[dry-run] {old_path.name} -> {bucket} precheck={'OK' if pre_ok else 'FAIL'} "
                    f"{reason or ''} business={business[:50]!r}"
                )
                continue

            if old_path.resolve() != new_graded.resolve():
                old_path.unlink(missing_ok=True)

            store_graded = (
                minimal_gateway_lead(lead)
                if (pre_ok and bucket == "A_ready_submit")
                else lead
            )
            new_graded.write_text(json.dumps(store_graded, ensure_ascii=True, indent=2), encoding="utf-8")

            if pre_ok:
                out_pass = store_graded if bucket == "A_ready_submit" else lead
                (pass_dir / f"{key}.json").write_text(
                    json.dumps(out_pass, ensure_ascii=True, indent=2), encoding="utf-8"
                )
            else:
                payload = {
                    "reason": reason,
                    "business": business,
                    "lead": lead,
                    "queue_bucket": bucket,
                }
                (fail_dir / f"{key}.precheck_failed.json").write_text(
                    json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8"
                )

            if bucket == "A_ready_submit":
                promoted += 1
                print(f"  PROMOTE -> A: {business[:60]!r} ({key[:16]}…)")
            elif pre_ok:
                updated += 1
                print(f"  still B: {business[:60]!r} ({key[:16]}…)")
            else:
                failed += 1
                print(f"  precheck FAIL: {business[:60]!r} ({reason})")
        except Exception as e:
            print(f"  ERROR {old_path.name}: {type(e).__name__}: {e}")
            return 1

    if not args.dry_run:
        print(f"Done. promoted_to_A={promoted} updated_still_B={updated} precheck_fail={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
