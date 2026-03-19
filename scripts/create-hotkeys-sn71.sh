#!/usr/bin/env bash
# Create multiple hotkeys under one coldkey for running several miners on Subnet 71.
# Run from repo root: ./scripts/create-hotkeys-sn71.sh
# You will be prompted for your coldkey password for each new hotkey.

set -e
cd "$(dirname "$0")/.."

WALLET_NAME="${WALLET_NAME:-miner}"
NUM_HOTKEYS="${NUM_HOTKEYS:-3}"

echo "=============================================="
echo " Create hotkeys for multiple miners (SN71)"
echo "=============================================="
echo " Coldkey (wallet name): $WALLET_NAME"
echo " Number of hotkeys to create: $NUM_HOTKEYS"
echo ""
echo " Hotkeys will be named: default, miner_2, miner_3, ..."
echo " (If 'default' already exists, we skip it and create miner_2, miner_3, ...)"
echo ""

# Ensure coldkey exists
if ! btcli wallet list 2>/dev/null | grep -q "$WALLET_NAME"; then
  echo "Coldkey '$WALLET_NAME' not found. Create it first:"
  echo "  btcli wallet create --wallet.name $WALLET_NAME"
  exit 1
fi

# Create hotkey 'default' if not present
if ! btcli wallet list --wallet.name "$WALLET_NAME" 2>/dev/null | grep -q "default"; then
  echo "Creating hotkey: default"
  btcli wallet create --wallet.name "$WALLET_NAME" --wallet.hotkey default
else
  echo "Hotkey 'default' already exists, skipping."
fi

# Create miner_2, miner_3, ...
for i in $(seq 2 "$NUM_HOTKEYS"); do
  hk="miner_$i"
  if btcli wallet list --wallet.name "$WALLET_NAME" 2>/dev/null | grep -q "$hk"; then
    echo "Hotkey '$hk' already exists, skipping."
  else
    echo "Creating hotkey: $hk"
    btcli wallet create --wallet.name "$WALLET_NAME" --wallet.hotkey "$hk"
  fi
done

echo ""
echo "Done. List hotkeys: btcli wallet list --wallet.name $WALLET_NAME"
echo "Next: register each hotkey on subnet 71 with scripts/register-hotkeys-sn71.sh"
