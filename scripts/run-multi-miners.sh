#!/usr/bin/env bash
# Run multiple LeadPoet miners (one process per hotkey) in background or in screen sessions.
# Run from repo root: ./scripts/run-multi-miners.sh

set -e
cd "$(dirname "$0")/.."

WALLET_NAME="${WALLET_NAME:-YOUR_COLDKEY_NAME}"
# Space-separated list of hotkey names to run
HOTKEYS="${HOTKEYS:-culture}"
NETUID="${NETUID:-71}"
SUBTENSOR_NETWORK="${SUBTENSOR_NETWORK:-finney}"
FRONTIER="${FRONTIER:-0}"
# Use "screen" to run each miner in a named screen session, or "nohup" for background, or "foreground" to run one only
MODE="${MODE:-screen}"

echo "=============================================="
echo " Run multiple miners (Subnet 71)"
echo "=============================================="
echo " Coldkey: $WALLET_NAME"
echo " Hotkeys: $HOTKEYS"
echo " Mode: $MODE (screen | nohup | foreground)"
echo ""

if [ "$MODE" = "foreground" ]; then
  # Run single miner in foreground (first hotkey in list)
  hk=$(echo "$HOTKEYS" | awk '{print $1}')
  export WALLET_NAME WALLET_HOTKEY="$hk" NETUID SUBTENSOR_NETWORK FRONTIER
  exec ./run-miner.sh
fi

for hotkey in $HOTKEYS; do
  session_name="sn71-${hotkey}"
  log_file="logs/miner_${hotkey}.log"
  mkdir -p logs

  if [ "$MODE" = "screen" ]; then
    if screen -list 2>/dev/null | grep -q "\.${session_name}[[:space:]]"; then
      echo "Screen session $session_name already running. Attach with: screen -r $session_name"
    else
      echo "Starting miner in screen: $session_name (hotkey: $hotkey)"
      REPO_ROOT="$(pwd)"
      screen -dmS "$session_name" bash -c "cd '$REPO_ROOT' && export WALLET_NAME='$WALLET_NAME' WALLET_HOTKEY='$hotkey' NETUID='$NETUID' SUBTENSOR_NETWORK='$SUBTENSOR_NETWORK' FRONTIER='$FRONTIER' && ./run-miner.sh 2>&1 | tee -a $log_file; exec bash"
    fi
  elif [ "$MODE" = "nohup" ]; then
    echo "Starting miner in background: $hotkey (log: $log_file)"
    nohup env WALLET_NAME="$WALLET_NAME" WALLET_HOTKEY="$hotkey" NETUID="$NETUID" SUBTENSOR_NETWORK="$SUBTENSOR_NETWORK" FRONTIER="$FRONTIER" ./run-miner.sh >> "$log_file" 2>&1 &
  fi
done

if [ "$MODE" = "screen" ]; then
  echo ""
  echo "List screens: screen -ls"
  echo "Attach to a miner: screen -r sn71-<hotkey>"
  echo "Detach from screen: Ctrl+A then D"
fi
