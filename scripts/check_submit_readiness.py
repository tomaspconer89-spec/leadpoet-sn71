#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from miner_models.lead_precheck import precheck_lead

QUEUE = REPO / "lead_queue"


def _load_lead(path: Path) -> Dict[str, Any] | None:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(obj, dict) and isinstance(obj.get("lead"), dict):
        return obj["lead"]
    if isinstance(obj, dict):
        return obj
    return None


def _validator_like_checks(lead: Dict[str, Any]) -> List[str]:
    """
    Add a few explicit readiness checks on top of precheck reason.
    Keeps checks lightweight and local (no paid/external API calls).
    """
    reasons: List[str] = []

    linkedin = str(lead.get("linkedin") or "").strip().lower()
    company_linkedin = str(lead.get("company_linkedin") or "").strip().lower()
    email = str(lead.get("email") or "").strip().lower()
    website = str(lead.get("website") or "").strip().lower()

    if "/in/" not in linkedin:
        reasons.append("invalid_linkedin_url")
    if "/company/" not in company_linkedin or "/in/" in company_linkedin:
        reasons.append("invalid_company_linkedin")
    if "@" not in email:
        reasons.append("invalid_email_format")
    if not website.startswith("http"):
        reasons.append("invalid_website")

    # US readiness sanity (common mismatch source)
    country = str(lead.get("country") or "").strip().lower()
    state = str(lead.get("state") or "").strip()
    city = str(lead.get("city") or "").strip()
    if country in {"united states", "usa", "us", "u.s.", "u.s.a."}:
        if not state:
            reasons.append("state_empty_for_usa")
        if not city:
            reasons.append("city_empty_for_usa")

    return reasons


def evaluate_file(path: Path) -> Tuple[str, str, str, str]:
    lead = _load_lead(path)
    if not lead:
        return (path.name, "FAIL", "invalid_json_or_shape", "")

    business = str(lead.get("business") or "").strip()
    ok, reason = precheck_lead(lead)
    extra_reasons = _validator_like_checks(lead)

    reasons: List[str] = []
    if not ok:
        reasons.append(reason or "precheck_failed")
    reasons.extend(extra_reasons)
    # de-dup keep order
    seen = set()
    deduped: List[str] = []
    for r in reasons:
        if not r:
            continue
        if r not in seen:
            seen.add(r)
            deduped.append(r)

    if deduped:
        return (path.name, "FAIL", "; ".join(deduped), business)
    return (path.name, "PASS", "", business)


def main() -> int:
    p = argparse.ArgumentParser(
        description="Validator-like readiness report for A_ready_submit + collected_pass"
    )
    p.add_argument(
        "--dirs",
        nargs="+",
        default=["A_ready_submit", "collected_pass"],
        help="Queue subdirs under lead_queue to scan",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Print JSON report instead of text table",
    )
    args = p.parse_args()

    files: List[Path] = []
    for d in args.dirs:
        qd = QUEUE / d
        if not qd.exists():
            continue
        files.extend(sorted(qd.glob("*.json")))

    rows = [evaluate_file(fp) for fp in files]
    total = len(rows)
    passed = sum(1 for _, status, _, _ in rows if status == "PASS")
    failed = total - passed

    if args.json:
        payload = {
            "summary": {"total": total, "pass": passed, "fail": failed},
            "results": [
                {"file": f, "status": s, "reason": r, "business": b}
                for f, s, r, b in rows
            ],
        }
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print("=== Submit Readiness Report ===")
    print(f"Total: {total} | PASS: {passed} | FAIL: {failed}")
    for f, s, r, b in rows:
        if s == "PASS":
            print(f"[PASS] {f} | {b}")
        else:
            print(f"[FAIL] {f} | {b} | {r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

