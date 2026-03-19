#!/usr/bin/env python3
"""
Summarize lead_queue sourced/attempted audit artifacts.

Usage:
  python scripts/lead_queue_audit_summary.py
  python scripts/lead_queue_audit_summary.py --root lead_queue
  python scripts/lead_queue_audit_summary.py --latest-failures 20
"""

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


def load_json_files(paths: List[Path]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in paths:
        try:
            data = json.loads(p.read_text())
            if isinstance(data, dict):
                data["_file"] = str(p)
                out.append(data)
        except Exception:
            continue
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize lead queue audit artifacts.")
    parser.add_argument("--root", default="lead_queue", help="Lead queue root directory")
    parser.add_argument(
        "--latest-failures",
        type=int,
        default=10,
        help="How many latest failure events to print",
    )
    args = parser.parse_args()

    root = Path(args.root)
    sourced_dir = root / "sourced"
    attempted_dir = root / "attempted"

    sourced_files = sorted(sourced_dir.glob("*.json")) if sourced_dir.exists() else []
    attempted_files = sorted(attempted_dir.glob("*.json")) if attempted_dir.exists() else []

    sourced_rows = load_json_files(sourced_files)
    attempted_rows = load_json_files(attempted_files)

    event_counts = Counter()
    for row in attempted_rows:
        event = row.get("event", "unknown")
        event_counts[event] += 1

    verified = event_counts.get("attempt_verified", 0)
    started = event_counts.get("attempt_started", 0)
    verify_rate = (verified / started * 100.0) if started else 0.0

    print("=" * 72)
    print("LEAD QUEUE AUDIT SUMMARY")
    print("=" * 72)
    print(f"Root:                     {root.resolve()}")
    print(f"Sourced leads saved:      {len(sourced_rows)}")
    print(f"Attempt audit rows:       {len(attempted_rows)}")
    print(f"Attempted leads started:  {started}")
    print(f"Verified submissions:     {verified}")
    print(f"Verify rate:              {verify_rate:.1f}%")
    print("")

    print("Attempt events:")
    if not event_counts:
        print("  (none)")
    else:
        for event, count in event_counts.most_common():
            print(f"  - {event}: {count}")
    print("")

    failure_events = {
        "attempt_presign_failed",
        "attempt_upload_failed",
        "attempt_verification_failed",
        "attempt_duplicate_email",
        "attempt_duplicate_linkedin_combo",
    }
    failures = [
        row for row in attempted_rows if row.get("event") in failure_events
    ]
    failures_sorted = sorted(
        failures, key=lambda r: str(r.get("ts", "")), reverse=True
    )

    print(f"Latest failure events (up to {args.latest_failures}):")
    if not failures_sorted:
        print("  (none)")
    else:
        for row in failures_sorted[: args.latest_failures]:
            event = row.get("event", "unknown")
            ts = row.get("ts", "unknown-ts")
            extra = row.get("extra", {}) if isinstance(row.get("extra"), dict) else {}
            business = extra.get("business") or row.get("lead", {}).get("business", "")
            lead_key = row.get("lead_key", "")
            print(f"  - {ts} | {event} | {business} | {lead_key[:16]}...")

    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

