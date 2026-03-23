#!/usr/bin/env bash
# Run the miner in a screen session named sn71, with output also in miner.log.
# Requires: sudo apt install screen
# Then: ./scripts/run-miner-screen.sh
# Watch: screen -r sn71   (detach: Ctrl+A then D)
# Or:    tail -f miner.log

set -e
cd "$(dirname "$0")/.."

if ! command -v screen &>/dev/null; then
  echo "Screen is not installed. Install with: sudo apt install screen"
  exit 1
fi

# Kill any existing miner in this session name
screen -S sn71 -X quit 2>/dev/null || true

# Wallet must be set (run-miner.sh uses these)
if [ -z "$WALLET_NAME" ] || [ -z "$WALLET_HOTKEY" ]; then
  echo "Set WALLET_NAME and WALLET_HOTKEY before running, e.g.:"
  echo "  export WALLET_NAME=YOUR_COLDKEY_NAME WALLET_HOTKEY=culture"
  echo "  ./scripts/run-miner-screen.sh"
  exit 1
fi

LOG_FILE="${MINER_LOG_FILE:-miner.log}"
export ACCEPT_TERMS=1
export USE_LEAD_PRECHECK=1
export FRONTIER=1
export USE_HF_INDUSTRY=1
export USE_HF_EMAIL_INTENT_FILTER=1

echo "Starting miner in screen session 'sn71' (log: $LOG_FILE)"
# tee -a buffers file I/O (often ~4KB), so miner.log can look "empty" in editors for a long time.
# tee >(stdbuf -oL cat >> file) duplicates to the terminal and appends each line to the log immediately.
screen -dmS sn71 bash -c "cd '$PWD' && ./run-miner.sh 2>&1 | tee >(stdbuf -oL cat >> \"$LOG_FILE\"); echo 'Miner exited. Press Enter.'; read"
echo "Attach to see live output: screen -r sn71"
echo "Watch log from another terminal: tail -f $PWD/$LOG_FILE"
