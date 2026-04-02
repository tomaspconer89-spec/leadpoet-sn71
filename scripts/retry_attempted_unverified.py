#!/usr/bin/env python3
"""
Requeue failed submit attempts for retry.

Moves lead JSON files referenced by
`lead_queue/failed/*.submission_outcome.json` where:
  submission_status == "attempted_unverified"

From:
  lead_queue/failed/<source_file>
To:
  lead_queue/collected_pass/<source_file>

Default mode is dry-run. Pass --apply to execute moves.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Move attempted_unverified leads from failed -> collected_pass"
    )
    p.add_argument(
        "--failed-dir",
        default="lead_queue/failed",
        help="Folder containing *.submission_outcome.json and failed lead json files",
    )
    p.add_argument(
        "--collected-dir",
        default="lead_queue/collected_pass",
        help="Destination folder for retry submit queue",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually move files (default: dry-run)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    failed_dir = (repo_root / args.failed_dir).resolve()
    collected_dir = (repo_root / args.collected_dir).resolve()
    collected_dir.mkdir(parents=True, exist_ok=True)

    outcomes = sorted(failed_dir.glob("*.submission_outcome.json"))
    eligible = 0
    moved = 0
    skipped_missing_source = 0
    skipped_existing_dest = 0

    for outcome_path in outcomes:
        try:
            obj = json.loads(outcome_path.read_text(encoding="utf-8"))
        except Exception:
            continue

        status = str(obj.get("submission_status") or "").strip().lower()
        if status != "attempted_unverified":
            continue
        eligible += 1

        source_file = str(obj.get("source_file") or "").strip()
        if not source_file:
            continue

        src = failed_dir / source_file
        dest = collected_dir / source_file

        if not src.exists():
            skipped_missing_source += 1
            print(f"SKIP missing source: {source_file}")
            continue
        if dest.exists():
            skipped_existing_dest += 1
            print(f"SKIP destination exists: {source_file}")
            continue

        if args.apply:
            src.rename(dest)
        moved += 1
        action = "MOVE" if args.apply else "WOULD_MOVE"
        print(f"{action} {source_file}")

    mode = "APPLY" if args.apply else "DRY_RUN"
    print(f"\nMode: {mode}")
    print(f"Eligible attempted_unverified outcomes: {eligible}")
    print(f"{'Moved' if args.apply else 'Would move'}: {moved}")
    print(f"Skipped missing source: {skipped_missing_source}")
    print(f"Skipped existing destination: {skipped_existing_dest}")
    print(f"Failed dir: {failed_dir}")
    print(f"Collected dir: {collected_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

