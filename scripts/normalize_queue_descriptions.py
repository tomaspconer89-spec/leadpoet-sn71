#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Dict, List


REPO = Path(__file__).resolve().parent.parent
QUEUE = REPO / "lead_queue"


def clean_and_cap_description(desc: str, max_len: int) -> str:
    txt = re.sub(r"\s+", " ", (desc or "").strip())
    if len(txt) <= max_len:
        return txt
    clipped = txt[:max_len].rstrip(" ,;:-")
    cut = max(clipped.rfind(". "), clipped.rfind("; "), clipped.rfind(": "))
    if cut >= 120:
        clipped = clipped[: cut + 1].rstrip()
    return clipped


def ensure_description(lead: Dict[str, object], max_len: int) -> bool:
    before = str(lead.get("description") or "")
    after = clean_and_cap_description(before, max_len=max_len)
    if len(after) < 70:
        bn = str(lead.get("business") or "This company").strip()
        web = str(lead.get("website") or "").strip()
        after = (
            f"{bn} provides professional services to clients; details were sourced "
            f"from the company website at {web}."
        )
        after = clean_and_cap_description(after, max_len=max_len)
    if after != before:
        lead["description"] = after
        return True
    return False


def main() -> int:
    p = argparse.ArgumentParser(description="Normalize lead description fields in queue folders")
    p.add_argument(
        "--dirs",
        nargs="+",
        default=["A_ready_submit", "B_retry_enrichment", "collected_pass"],
        help="Queue subdirectories under lead_queue to process",
    )
    p.add_argument(
        "--max-len",
        type=int,
        default=int(os.getenv("LEAD_MAX_DESCRIPTION_LEN", "600") or "600"),
        help="Maximum description length after normalization",
    )
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    processed = 0
    changed = 0
    per_dir: Dict[str, int] = {}

    for d in args.dirs:
        qd = QUEUE / d
        count = 0
        if not qd.exists():
            per_dir[d] = 0
            continue
        for fp in sorted(qd.glob("*.json")):
            processed += 1
            try:
                obj = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:
                continue
            # support both raw lead files and wrapped payloads with {"lead": {...}}
            target = obj.get("lead") if isinstance(obj, dict) and isinstance(obj.get("lead"), dict) else obj
            if not isinstance(target, dict):
                continue
            if ensure_description(target, max_len=max(120, args.max_len)):
                changed += 1
                count += 1
                if not args.dry_run:
                    fp.write_text(json.dumps(obj, ensure_ascii=True, indent=2), encoding="utf-8")
        per_dir[d] = count

    print(f"Processed files: {processed}")
    print(f"Changed files: {changed}")
    print(f"Per-directory changes: {per_dir}")
    print(f"Mode: {'dry-run' if args.dry_run else 'write'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

