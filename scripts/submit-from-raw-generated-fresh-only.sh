#!/usr/bin/env bash
# Submit from lead_queue/raw_generated_fresh_only without moving or writing any file
# in that folder (read-only JSON). Does not use lead_queue/pending or submitted/.
#
# Usage (from repo root):
#   export WALLET_NAME=YOUR_COLDKEY_NAME WALLET_HOTKEY=culture
#   ./scripts/submit-from-raw-generated-fresh-only.sh
#
# Optional env:
#   RAW_DIR=lead_queue/raw_generated_fresh_only
#   SUBMIT_MAX=100
#   ENRICH_LINKEDIN=0   — disable Serper/Brave LinkedIn enrichment

set -euo pipefail
cd "$(dirname "$0")/.."

: "${WALLET_NAME:?Set WALLET_NAME (coldkey)}"
: "${WALLET_HOTKEY:?Set WALLET_HOTKEY}"

RAW_DIR="${RAW_DIR:-lead_queue/raw_generated_fresh_only}"

echo "==> Read-only submit from: $RAW_DIR (no queue file moves)"
python3 scripts/submit_raw_generated_readonly.py \
  --in-dir "$RAW_DIR" \
  --wallet-name "$WALLET_NAME" \
  --wallet-hotkey "$WALLET_HOTKEY" \
  --max "${SUBMIT_MAX:-100}" \
  --enrich-linkedin "${ENRICH_LINKEDIN:-1}"
