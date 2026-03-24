#!/usr/bin/env python3
"""
Strip leads to the gateway minimal blob and move lead_queue/A_ready_submit/*.json → pending/.

Does not submit. Run submit_queued_leads.py afterward.

Usage (repo root):
  python3 scripts/a_ready_to_pending_minimal.py
  python3 scripts/a_ready_to_pending_minimal.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from miner_models.minimal_lead_blob import minimal_gateway_lead


def main() -> int:
    p = argparse.ArgumentParser(description="Minimal blob + A_ready_submit → pending")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    q = _REPO / "lead_queue"
    a_dir = q / "A_ready_submit"
    pending_dir = q / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(a_dir.glob("*.json"))
    if not files:
        print(f"No JSON in {a_dir}")
        return 0

    n = 0
    for src in files:
        try:
            raw = json.loads(src.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Skip invalid JSON {src.name}: {e}")
            continue
        slim = minimal_gateway_lead(raw)
        if args.dry_run:
            print(f"[dry-run] {src.name} keys_out={len(slim)}")
            n += 1
            continue
        dest = pending_dir / src.name
        if dest.exists():
            print(f"Skip: pending already has {dest.name}")
            continue
        dest.write_text(json.dumps(slim, ensure_ascii=True, indent=2), encoding="utf-8")
        src.unlink(missing_ok=True)
        n += 1
        print(f"Moved {src.name} -> pending/ ({len(slim)} fields)")
    print(f"Done. moved={n}" + (" (dry-run)" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
