#!/usr/bin/env python3
"""
Collect all generated lead artifacts from /tmp into one project folder.

This does NOT submit leads. It centralizes generated files for inspection.
Submitted-ready queue files are maintained by neurons/miner.py in lead_queue/pending.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def collect(src_glob: str, dst_dir: Path) -> int:
    count = 0
    for src in Path("/tmp").glob(src_glob):
        if not src.is_file():
            continue
        dst = dst_dir / src.name
        if dst.exists():
            stem = src.stem
            suffix = src.suffix
            idx = 1
            while True:
                candidate = dst_dir / f"{stem}__{idx}{suffix}"
                if not candidate.exists():
                    dst = candidate
                    break
                idx += 1
        shutil.copy2(src, dst)
        count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect generated leads into one folder")
    parser.add_argument(
        "--out",
        default="lead_queue/raw_generated",
        help="Output folder relative to project root",
    )
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    crawl_count = collect("tmp*/crawl_artifacts/*.json", out_dir)
    evidence_count = collect("tmp*/evidence/domain/*.json", out_dir)

    print(f"Collected crawl artifacts: {crawl_count}")
    print(f"Collected evidence artifacts: {evidence_count}")
    print(f"Output folder: {out_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
