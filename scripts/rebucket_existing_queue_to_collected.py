#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from miner_models.lead_precheck import precheck_lead
from miner_models.minimal_lead_blob import minimal_gateway_lead


def _queue_key(lead: dict) -> str:
    parts = [
        str(lead.get("email", "")).strip().lower(),
        str(lead.get("business", "")).strip().lower(),
        str(lead.get("linkedin", "")).strip().lower(),
        str(lead.get("website", "")).strip().lower(),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def main() -> int:
    q = _REPO / "lead_queue"
    pass_dir = q / "collected_pass"
    fail_dir = q / "collected_precheck_fail"
    pass_dir.mkdir(parents=True, exist_ok=True)
    fail_dir.mkdir(parents=True, exist_ok=True)

    source_dirs = [
        q / "A_ready_submit",
        q / "B_retry_enrichment",
        q / "C_good_account_needs_person",
        q / "D_low_confidence_hold",
        q / "E_reject",
    ]

    total = 0
    wrote_pass = 0
    wrote_fail = 0
    skipped_bad = 0

    for d in source_dirs:
        if not d.exists():
            continue
        for p in d.glob("*.json"):
            total += 1
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                skipped_bad += 1
                continue
            if not isinstance(obj, dict):
                skipped_bad += 1
                continue

            lead = obj.get("lead") if isinstance(obj.get("lead"), dict) else obj
            if not isinstance(lead, dict):
                skipped_bad += 1
                continue

            slim = minimal_gateway_lead(lead)
            key = _queue_key(slim)
            pre_ok, reason = precheck_lead(slim)

            if pre_ok:
                out = pass_dir / f"{key}.json"
                if not out.exists():
                    out.write_text(json.dumps(slim, ensure_ascii=True, indent=2))
                    wrote_pass += 1
            else:
                out = fail_dir / f"{key}.precheck_failed.json"
                if not out.exists():
                    payload = {
                        "reason": reason,
                        "business": str(slim.get("business", "")).strip() or p.stem,
                        "lead": slim,
                    }
                    out.write_text(json.dumps(payload, ensure_ascii=True, indent=2))
                    wrote_fail += 1

    print(
        "rebucket_existing_queue_to_collected done: "
        f"scanned={total} wrote_pass={wrote_pass} wrote_fail={wrote_fail} skipped_bad={skipped_bad}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
