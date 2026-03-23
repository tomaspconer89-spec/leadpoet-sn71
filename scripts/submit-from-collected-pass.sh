#!/usr/bin/env bash
# Precheck lead_queue/collected_pass/*.json, submit passes, move verified to lead_queue/submitted/.
# Uses WALLET_NAME / WALLET_HOTKEY from environment or .env (via Python dotenv).
#
# If gateway /presign returns "Hotkey not registered on subnet" temporarily, stage only:
#   ./scripts/submit-from-collected-pass.sh --enqueue-pending
#
# Usage (repo root):
#   ./scripts/submit-from-collected-pass.sh
#   ./scripts/submit-from-collected-pass.sh --max 5 --enrich-linkedin 1

set -euo pipefail
cd "$(dirname "$0")/.."
PY=python3
for c in venv312/bin/python venv/bin/python; do
  if [ -x "$c" ]; then PY="$c"; break; fi
done
exec "$PY" scripts/submit_collected_pass.py "$@"
