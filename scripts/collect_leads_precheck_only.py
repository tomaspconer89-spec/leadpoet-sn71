#!/usr/bin/env python3
"""
Generate N leads via Lead Sorcerer, run precheck only (no gateway submit).
Writes passes to lead_queue/collected_pass/ and failures to lead_queue/collected_precheck_fail/.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv

load_dotenv(_REPO / ".env")


def _miner_log_path() -> Path:
    raw = (os.environ.get("MINER_LOG_FILE") or "").strip()
    return Path(raw) if raw else (_REPO / "miner.log")


def append_work_status(message: str) -> None:
    """Append one JSON line to miner.log (same shape as Lead Sorcerer logs) for tail -f."""
    log_path = _miner_log_path()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    line = json.dumps(
        {"ts": ts, "level": "INFO", "tool": "work_status", "msg": message},
        ensure_ascii=True,
    )
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
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


async def _run(
    num: int,
    industry: str | None,
    *,
    target_pass: int | None,
    max_runs: int,
) -> int:
    from miner_models.lead_sorcerer_main.main_leads import get_leads
    from miner_models.lead_precheck import precheck_lead

    _conv = _REPO / "scripts" / "convert_raw_to_pending.py"
    if _conv.is_file():
        _spec = importlib.util.spec_from_file_location("_crtp", _conv)
        _mod = importlib.util.module_from_spec(_spec)
        assert _spec.loader is not None
        _spec.loader.exec_module(_mod)
        enrich_linkedin_fields = getattr(_mod, "enrich_linkedin_fields", None)
    else:
        enrich_linkedin_fields = None

    pass_dir = _REPO / "lead_queue" / "collected_pass"
    fail_dir = _REPO / "lead_queue" / "collected_precheck_fail"
    pass_dir.mkdir(parents=True, exist_ok=True)
    fail_dir.mkdir(parents=True, exist_ok=True)

    append_work_status(
        "collect_leads_precheck_only START "
        f"num={num} industry={industry!r} target_pass={target_pass} max_runs={max_runs} "
        f"log={_miner_log_path()}"
    )

    ok_n = 0
    bad_n = 0
    runs = 0
    existing_pass = len(list(pass_dir.glob("*.json")))

    def _process_batch(leads: list) -> None:
        nonlocal ok_n, bad_n
        for lead in leads:
            if enrich_linkedin_fields is not None:
                try:
                    lead = enrich_linkedin_fields(dict(lead))
                except Exception:
                    pass
            key = _queue_key(lead)
            business = lead.get("business", "?")
            out_ok = pass_dir / f"{key}.json"
            if out_ok.exists():
                continue
            pre_ok, reason = precheck_lead(lead)
            if pre_ok:
                out_ok.write_text(json.dumps(lead, ensure_ascii=True, indent=2))
                print(f"  PASS precheck: {business} -> {out_ok.name}")
                ok_n += 1
            else:
                path = fail_dir / f"{key}.precheck_failed.json"
                payload = {"reason": reason, "business": business, "lead": lead}
                path.write_text(json.dumps(payload, ensure_ascii=True, indent=2))
                print(f"  FAIL precheck: {business} ({reason}) -> {path.name}")
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

    print(f"Done. New precheck pass this session: {ok_n}, new fail: {bad_n}")
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
        _run(args.num, args.industry, target_pass=tgt, max_runs=args.max_runs)
    )


if __name__ == "__main__":
    raise SystemExit(main())
