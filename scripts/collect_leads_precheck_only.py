#!/usr/bin/env python3
"""
Generate N leads via Lead Sorcerer, run precheck only (no gateway submit).
Writes passes to lead_queue/collected_pass/ and failures to
lead_queue/collected_precheck_fail/.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv
from miner_models.lead_normalization import (
    apply_email_classification,
    normalize_legacy_lead_shape,
)
from miner_models.minimal_lead_blob import minimal_gateway_lead
from miner_models.person_confidence import score_person_confidence
from miner_models.title_normalizer import normalize_title
from scripts.retry_enrichment import should_run_targeted_retry, targeted_retry_enrichment

load_dotenv(_REPO / ".env")


def _miner_log_path() -> Path:
    raw = (os.environ.get("MINER_LOG_FILE") or "").strip()
    if not raw:
        return _REPO / "miner.log"
    p = Path(raw)
    return p if p.is_absolute() else (_REPO / p)


def append_work_status(message: str) -> None:
    """Append one JSON line to miner.log (same shape as Lead Sorcerer logs) for tail -f."""
    log_path = _miner_log_path()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    line = json.dumps(
        {"ts": ts, "level": "INFO", "tool": "work_status", "msg": message},
        ensure_ascii=True,
    )
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
    except OSError:
        pass


def _queue_key(lead: dict) -> str:
    parts = [
        str(lead.get("email", "")).strip().lower(),
        str(lead.get("business", "")).strip().lower(),
        str(lead.get("linkedin", "")).strip().lower(),
        str(lead.get("website", "")).strip().lower(),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


_BLOCK_REASONS = ("general_purpose_email:", "free_email_domain:")


def _domain_from_url(raw: str) -> str:
    if not raw:
        return ""
    val = str(raw).strip()
    if not val:
        return ""
    parsed = urlparse(val if "://" in val else f"https://{val}")
    host = (parsed.hostname or "").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _lead_domain(lead: dict) -> str:
    return (
        _domain_from_url(lead.get("website", ""))
        or _domain_from_url(lead.get("source_url", ""))
        or _domain_from_url(lead.get("business_website", ""))
    )


async def _run(
    num: int,
    industry: str | None,
    *,
    target_pass: int | None,
    max_runs: int,
) -> int:
    # Force Lead Sorcerer tool logs into miner.log unless caller overrides explicitly.
    os.environ.setdefault("LEADSORCERER_LOG_FILE", str(_miner_log_path()))

    append_work_status(
        "collect_leads_precheck_only START "
        f"num={num} industry={industry!r} target_pass={target_pass} "
        f"max_runs={max_runs} "
        f"log={_miner_log_path()} leadsorcerer_log={os.environ.get('LEADSORCERER_LOG_FILE')}"
    )

    try:
        from miner_models.lead_sorcerer_main.main_leads import get_leads
        from miner_models.lead_precheck import precheck_lead
    except Exception as e:
        msg = (
            "collect_leads_precheck_only ERROR "
            f"dependency_import_failed={type(e).__name__}: {e}"
        )
        append_work_status(msg)
        print(f"Dependency import failed: {e}")
        return 1

    pass_dir = _REPO / "lead_queue" / "collected_pass"
    fail_dir = _REPO / "lead_queue" / "collected_precheck_fail"
    pass_dir.mkdir(parents=True, exist_ok=True)
    fail_dir.mkdir(parents=True, exist_ok=True)

    ok_n = 0
    bad_n = 0
    skipped_blocked_n = 0
    runs = 0
    existing_pass = len(list(pass_dir.glob("*.json")))
    blocked_domains: set[str] = set()

    for path in fail_dir.glob("*.precheck_failed.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            reason = str(payload.get("reason", "")).strip()
            if not reason.startswith(_BLOCK_REASONS):
                continue
            lead_obj = payload.get("lead", {}) if isinstance(payload, dict) else {}
            if not isinstance(lead_obj, dict):
                continue
            dom = _lead_domain(lead_obj)
            if dom:
                blocked_domains.add(dom)
        except Exception:
            continue

    def _process_batch(leads: list) -> None:
        nonlocal ok_n, bad_n, skipped_blocked_n
        for lead in leads:
            dom = _lead_domain(lead)
            if dom and dom in blocked_domains:
                skipped_blocked_n += 1
                print(f"  SKIP blocked domain: {dom}")
                continue
            lead = dict(lead)
            normalize_legacy_lead_shape(lead)
            normalize_legacy_lead_shape(lead)
            apply_email_classification(lead)

            key = _queue_key(lead)
            business = lead.get("business", "?")
            out_ok = pass_dir / f"{key}.json"
            if out_ok.exists():
                continue

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

            # Targeted retry only for high-profit allowlist (LinkedIn / last / domain match).
            retry_attempts = 0
            recovered_fields: list = []
            if (not pre_ok) and reason:
                if "name_not_in_email" in (reason or "") or "email_domain_mismatch" in (
                    reason or ""
                ):
                    lead["identity_conflict"] = True
                if should_run_targeted_retry(reason):
                    lead, recovered_fields, retry_attempts = targeted_retry_enrichment(
                        lead, reason, enrich_linkedin=None
                    )
                if retry_attempts > 0:
                    lead["retry_reason"] = reason
                    lead["retry_attempts"] = retry_attempts
                    if recovered_fields:
                        lead["recovered_fields"] = recovered_fields
                    normalize_legacy_lead_shape(lead)
                    apply_email_classification(lead)
                    conf2 = score_person_confidence(
                        lead,
                        title_matches_persona=title_meta["target_fit"] in ("high", "medium"),
                    )
                    lead.update(conf2)
                    pre_ok, reason = precheck_lead(lead)

            store_graded = minimal_gateway_lead(lead)

            if pre_ok:
                out_ok.write_text(json.dumps(store_graded, ensure_ascii=True, indent=2))
                print(f"  PASS precheck: {business} -> {out_ok.name}")
                ok_n += 1
            else:
                path = fail_dir / f"{key}.precheck_failed.json"
                payload = {
                    "reason": reason,
                    "business": business,
                    "lead": store_graded,
                }
                path.write_text(json.dumps(payload, ensure_ascii=True, indent=2))
                print(f"  FAIL precheck: {business} ({reason}) -> {path.name}")
                if str(reason).startswith(_BLOCK_REASONS):
                    dom = _lead_domain(lead)
                    if dom:
                        blocked_domains.add(dom)
                bad_n += 1

    if target_pass is None:
        print(f"Generating up to {num} leads (Sorcerer)...")
        leads = await get_leads(num, industry=industry, region=None)
        print(f"Sorcerer returned {len(leads)} legacy lead(s).")
        append_work_status(
            f"collect_leads_precheck_only Sorcerer returned {len(leads)} legacy lead(s) (single batch)"
        )
        _process_batch(leads)
    else:
        while runs < max_runs:
            runs += 1
            need = max(target_pass - existing_pass - ok_n, num)
            batch = max(need, num)
            print(f"Pipeline run {runs}/{max_runs}: generating up to {batch} leads...")
            leads = await get_leads(batch, industry=industry, region=None)
            print(f"Sorcerer returned {len(leads)} legacy lead(s).")
            append_work_status(
                f"collect_leads_precheck_only run {runs}/{max_runs}: "
                f"Sorcerer returned {len(leads)} legacy leads (batch_size={batch})"
            )
            _process_batch(leads)
            total_pass = existing_pass + ok_n
            if total_pass >= target_pass:
                print(
                    f"Reached target of {target_pass} precheck-pass leads in pass dir."
                )
                append_work_status(
                    f"collect_leads_precheck_only reached target_pass={target_pass} "
                    f"(total_pass_files≈{total_pass})"
                )
                break

    print(
        f"Done. New precheck pass this session: {ok_n}, "
        f"new fail: {bad_n}, skipped_blocked_domain: {skipped_blocked_n}"
    )
    print(f"Pass dir: {pass_dir}")
    print(f"Fail dir: {fail_dir}")
    append_work_status(
        "collect_leads_precheck_only DONE "
        f"new_pass={ok_n} new_fail={bad_n} "
        f"pass_dir={pass_dir} fail_dir={fail_dir}"
    )
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("-n", "--num", type=int, default=10)
    p.add_argument("--industry", default=None, help="Optional industry hint for Sorcerer")
    p.add_argument(
        "--target-pass",
        type=int,
        default=0,
        help="Stop after this many total *.json files in collected_pass (0 = no target)",
    )
    p.add_argument(
        "--max-runs",
        type=int,
        default=5,
        help="Max Sorcerer pipeline runs when using --target-pass",
    )
    args = p.parse_args()
    tgt = args.target_pass if args.target_pass > 0 else None
    return asyncio.run(
        _run(
            args.num,
            args.industry,
            target_pass=tgt,
            max_runs=args.max_runs,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
