#!/usr/bin/env python3
"""
Scan queue JSON leads and report README/gateway field coverage + precheck outcome.

Compares each lead to the minimal gateway shape (see README.md Lead JSON Structure and
miner_models/minimal_lead_blob.py). Runs miner_models.lead_precheck.precheck_lead when available.

Usage (repo root):
  python3 scripts/lead_readme_compliance_report.py
  python3 scripts/lead_readme_compliance_report.py --dirs lead_queue/B_retry_enrichment
  python3 scripts/lead_readme_compliance_report.py --no-precheck
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# README sample + gateway: required strings for a "complete" row (hq_* required per README/gateway checks).
_README_STRING_FIELDS = (
    "business",
    "full_name",
    "first",
    "last",
    "email",
    "role",
    "website",
    "industry",
    "sub_industry",
    "country",
    "city",
    "linkedin",
    "company_linkedin",
    "source_url",
    "description",
    "employee_count",
    "hq_country",
)
# US policy in lead_precheck: state required for US contact location.
_OPTIONAL_BUT_TRACKED = ("state", "hq_state", "hq_city", "source_type")


def _unwrap_lead(obj: dict) -> dict:
    if isinstance(obj.get("lead"), dict):
        return dict(obj["lead"])
    return dict(obj)


def _nonempty_str(lead: dict, key: str) -> bool:
    v = lead.get(key)
    if v is None:
        return False
    if isinstance(v, str):
        return bool(v.strip())
    return True


def _missing_readme_fields(lead: dict) -> list[str]:
    missing: list[str] = []
    for k in _README_STRING_FIELDS:
        if not _nonempty_str(lead, k):
            missing.append(k)
    for k in _OPTIONAL_BUT_TRACKED:
        if not _nonempty_str(lead, k):
            missing.append(f"empty:{k}")
    return missing


def _warnings(lead: dict) -> list[str]:
    w: list[str] = []
    su = str(lead.get("source_url") or "").lower()
    if "linkedin" in su:
        w.append("linkedin_in_source_url")
    desc = str(lead.get("description") or "")
    if len(desc) < 70:
        w.append(f"description_short:{len(desc)}")
    li = str(lead.get("linkedin") or "")
    cli = str(lead.get("company_linkedin") or "")
    if li and "/in/" not in li.lower():
        w.append("linkedin_not_person_url")
    if cli and "/company/" not in cli.lower():
        w.append("company_linkedin_not_company_url")
    return w


def _iter_json_files(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(root.glob("*.json"))


def main() -> int:
    p = argparse.ArgumentParser(description="README/gateway compliance report for queue leads")
    p.add_argument(
        "--dirs",
        nargs="*",
        default=[
            "lead_queue/A_ready_submit",
            "lead_queue/B_retry_enrichment",
            "lead_queue/C_good_account_needs_person",
            "lead_queue/D_low_confidence_hold",
            "lead_queue/E_reject",
            "lead_queue/collected_pass",
        ],
        help="Directories to scan (relative to repo root)",
    )
    p.add_argument(
        "--output-dir",
        default="reports",
        help="Where to write JSON + CSV (relative to repo root)",
    )
    p.add_argument(
        "--no-precheck",
        action="store_true",
        help="Skip lead_precheck.precheck_lead (structure-only report)",
    )
    args = p.parse_args()

    try:
        from miner_models.lead_normalization import normalize_legacy_lead_shape
        from miner_models.lead_precheck import precheck_lead
    except Exception as e:
        print(f"Import error: {e}", file=sys.stderr)
        return 1

    out_dir = _REPO / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = out_dir / f"readme_compliance_{ts}.json"
    csv_path = out_dir / f"readme_compliance_{ts}.csv"

    rows_out: list[dict] = []
    by_bucket: Counter[str] = Counter()
    precheck_fail_reasons: Counter[str] = Counter()
    missing_field_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()

    for rel in args.dirs:
        bucket = Path(rel).name
        dir_path = _REPO / rel
        for path in _iter_json_files(dir_path):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as e:
                rows_out.append(
                    {
                        "bucket": bucket,
                        "file": path.name,
                        "error": f"{type(e).__name__}: {e}",
                    }
                )
                continue
            lead = _unwrap_lead(raw)
            normalize_legacy_lead_shape(lead)

            missing = _missing_readme_fields(lead)
            for m in missing:
                if m.startswith("empty:"):
                    missing_field_counts[m] += 1
                else:
                    missing_field_counts[m] += 1

            warns = _warnings(lead)
            for w in warns:
                warning_counts[w.split(":")[0]] += 1

            pre_ok: bool | None = None
            reason: str | None = None
            if not args.no_precheck:
                pre_ok, reason = precheck_lead(lead)
                if not pre_ok and reason:
                    precheck_fail_reasons[str(reason).split(":")[0]] += 1

            by_bucket[bucket] += 1
            rows_out.append(
                {
                    "bucket": bucket,
                    "file": path.name,
                    "precheck_ok": pre_ok,
                    "precheck_reason": reason,
                    "missing_fields": ";".join(missing),
                    "warnings": ";".join(warns),
                }
            )

    summary = {
        "generated_at_utc": ts,
        "files_scanned": len(rows_out),
        "by_bucket": dict(by_bucket),
        "missing_field_counts": dict(missing_field_counts.most_common()),
        "warning_counts": dict(warning_counts.most_common()),
        "precheck_fail_reason_prefix_counts": dict(precheck_fail_reasons.most_common(40))
        if not args.no_precheck
        else {},
        "outputs": {"json": str(json_path), "csv": str(csv_path)},
    }

    payload = {"summary": summary, "rows": rows_out}
    json_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "bucket",
                "file",
                "precheck_ok",
                "precheck_reason",
                "missing_fields",
                "warnings",
                "error",
            ],
            extrasaction="ignore",
        )
        w.writeheader()
        for r in rows_out:
            w.writerow(r)

    print(json.dumps(summary, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
