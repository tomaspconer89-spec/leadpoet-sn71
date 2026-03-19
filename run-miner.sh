#!/usr/bin/env bash
# Start the LeadPoet miner (Subnet 71).
# Called by run-miner-with-log.sh, scripts/run-miner-screen.sh, scripts/run-multi-miners.sh
# Set WALLET_NAME, WALLET_HOTKEY (and optionally NETUID, SUBTENSOR_NETWORK) before running.

set -e
cd "$(dirname "$0")"

# Prefer venv312, then venv
if [ -d "venv312" ]; then
  source venv312/bin/activate
elif [ -d "venv" ]; then
  source venv/bin/activate
else
  echo "No venv or venv312 found. Create one and install deps (e.g. pip install -e .)" >&2
  exit 1
fi

export WALLET_NAME="${WALLET_NAME:-YOUR_COLDKEY_NAME}"
export WALLET_HOTKEY="${WALLET_HOTKEY:-YOUR_HOTKEY_NAME}"
export NETUID="${NETUID:-71}"
export SUBTENSOR_NETWORK="${SUBTENSOR_NETWORK:-finney}"
export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"

# Load .env (if present) so gateway/API keys are available.
# This repo's .env uses KEY=VALUE lines (not "export KEY=VALUE"), so we auto-export on source.
if [ -f ".env" ]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

# Optional: non-interactive and precheck (often set by run-miner-with-log.sh / run-miner-screen.sh)
# ACCEPT_TERMS, USE_LEAD_PRECHECK, FRONTIER, etc. are passed through from caller

echo "Starting LeadPoet miner (Subnet 71)"
echo "  Wallet: $WALLET_NAME / $WALLET_HOTKEY"
echo "  NetUID: $NETUID  Network: $SUBTENSOR_NETWORK"
echo ""

# Use installed CLI if available, otherwise run neurons/miner.py directly
if command -v leadpoet &>/dev/null; then
  exec leadpoet \
    --wallet_name "$WALLET_NAME" \
    --wallet_hotkey "$WALLET_HOTKEY" \
    --netuid "$NETUID" \
    --subtensor_network "$SUBTENSOR_NETWORK" \
    "$@"
else
  exec python -u neurons/miner.py \
    --wallet_name "$WALLET_NAME" \
    --wallet_hotkey "$WALLET_HOTKEY" \
    --netuid "$NETUID" \
    --subtensor_network "$SUBTENSOR_NETWORK" \
    "$@"
fi
