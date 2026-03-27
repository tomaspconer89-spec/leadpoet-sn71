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
from scripts.convert_raw_to_pending import validate_and_fix_with_scrapingdog

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


def evaluate_file(
    path: Path, *, scrapingdog_fix: bool, write_fixes: bool
) -> Tuple[str, str, str, str, List[str]]:
    raw = _load_lead(path)
    lead = raw
    if not lead:
        return (path.name, "FAIL", "invalid_json_or_shape", "", [])

    fixed_fields: List[str] = []
    if scrapingdog_fix:
        fixed_lead, changed = validate_and_fix_with_scrapingdog(dict(lead))
        if changed:
            fixed_fields = sorted(changed.keys())
            lead = fixed_lead
            if write_fixes:
                try:
                    obj = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(obj, dict) and isinstance(obj.get("lead"), dict):
                        obj["lead"] = fixed_lead
                    else:
                        obj = fixed_lead
                    path.write_text(
                        json.dumps(obj, ensure_ascii=True, indent=2), encoding="utf-8"
                    )
                except Exception:
                    pass

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
        return (path.name, "FAIL", "; ".join(deduped), business, fixed_fields)
    return (path.name, "PASS", "", business, fixed_fields)


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
    p.add_argument(
        "--scrapingdog-fix",
        type=int,
        default=0,
        choices=(0, 1),
        help="1 = use ScrapingDog-based check to detect/fix wrong fields",
    )
    p.add_argument(
        "--write-fixes",
        action="store_true",
        help="Write ScrapingDog-detected corrections back to files",
    )
    args = p.parse_args()

    files: List[Path] = []
    for d in args.dirs:
        qd = QUEUE / d
        if not qd.exists():
            continue
        files.extend(sorted(qd.glob("*.json")))

    rows = [
        evaluate_file(
            fp,
            scrapingdog_fix=bool(args.scrapingdog_fix),
            write_fixes=bool(args.write_fixes),
        )
        for fp in files
    ]
    total = len(rows)
    passed = sum(1 for _, status, _, _, _ in rows if status == "PASS")
    failed = total - passed

    if args.json:
        payload = {
            "summary": {"total": total, "pass": passed, "fail": failed},
            "results": [
                {
                    "file": f,
                    "status": s,
                    "reason": r,
                    "business": b,
                    "fixed_fields": ff,
                }
                for f, s, r, b, ff in rows
            ],
        }
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print("=== Submit Readiness Report ===")
    print(f"Total: {total} | PASS: {passed} | FAIL: {failed}")
    for f, s, r, b, ff in rows:
        fixed_note = f" | FIXED: {', '.join(ff)}" if ff else ""
        if s == "PASS":
            print(f"[PASS] {f} | {b}{fixed_note}")
        else:
            print(f"[FAIL] {f} | {b} | {r}{fixed_note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

