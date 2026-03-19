#!/usr/bin/env bash
# Run miner and write output to miner.log (so you can watch with: tail -f miner.log)
# Same env as run-miner.sh; pass FRONTIER=1 etc. as needed.

set -e
cd "$(dirname "$0")"

LOG_FILE="${MINER_LOG_FILE:-miner.log}"
export ACCEPT_TERMS="${ACCEPT_TERMS:-1}"
export USE_LEAD_PRECHECK="${USE_LEAD_PRECHECK:-1}"
export FRONTIER="${FRONTIER:-1}"
export USE_HF_INDUSTRY="${USE_HF_INDUSTRY:-1}"
export USE_HF_EMAIL_INTENT_FILTER="${USE_HF_EMAIL_INTENT_FILTER:-1}"

echo "Logging to $LOG_FILE (watch with: tail -f $LOG_FILE)"
exec ./run-miner.sh 2>&1 | tee -a "$LOG_FILE"
