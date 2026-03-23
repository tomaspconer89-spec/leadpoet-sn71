#!/usr/bin/env bash
# Register one or more hotkeys on Subnet 71 (LeadPoet).
# Each registration costs TAO (dynamic). Ensure your coldkey has enough balance.
# Run from repo root: ./scripts/register-hotkeys-sn71.sh

set -e
cd "$(dirname "$0")/.."

WALLET_NAME="${WALLET_NAME:-YOUR_COLDKEY_NAME}"
# Space-separated list of hotkey names to register
HOTKEYS="${HOTKEYS:-culture}"
NETUID="${NETUID:-71}"
SUBTENSOR_NETWORK="${SUBTENSOR_NETWORK:-finney}"

echo "=============================================="
echo " Register hotkeys on Subnet 71 (LeadPoet)"
echo "=============================================="
echo " Coldkey: $WALLET_NAME"
echo " Hotkeys: $HOTKEYS"
echo " NetUID: $NETUID  Network: $SUBTENSOR_NETWORK"
echo ""
echo " Each registration costs TAO (check current cost: subnet71.com or TAO.app)"
echo " You will be prompted for your coldkey password for each registration."
echo ""

for hotkey in $HOTKEYS; do
  echo "----------------------------------------------"
  echo " Registering hotkey: $hotkey"
  echo "----------------------------------------------"
  btcli subnet register \
    --netuid "$NETUID" \
    --subtensor.network "$SUBTENSOR_NETWORK" \
    --wallet.name "$WALLET_NAME" \
    --wallet.hotkey "$hotkey"
  echo ""
done

echo "Done. Check UIDs: btcli wallet overview --netuid $NETUID"
