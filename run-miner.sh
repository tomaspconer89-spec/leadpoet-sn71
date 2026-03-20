#!/usr/bin/env bash
# Start the LeadPoet miner (Subnet 71).
# Called by run-miner-with-log.sh, scripts/run-miner-screen.sh, scripts/run-multi-miners.sh
# Set WALLET_NAME, WALLET_HOTKEY (and optionally NETUID, SUBTENSOR_NETWORK) before running.

set -e
cd "$(dirname "$0")"

# Prefer venv312, then venv (only if activate script exists)
if [ -f "venv312/bin/activate" ]; then
  source venv312/bin/activate
elif [ -f "venv/bin/activate" ]; then
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
# Ensure local packages (Leadpoet/, miner_models/, neurons/) are importable.
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

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

# Run miner module directly to avoid stale/broken entrypoint shebangs.
exec python -u neurons/miner.py \
  --wallet_name "$WALLET_NAME" \
  --wallet_hotkey "$WALLET_HOTKEY" \
  --netuid "$NETUID" \
  --subtensor_network "$SUBTENSOR_NETWORK" \
  "$@"
