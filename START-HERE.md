# LeadPoet Subnet 71 — Mining in This Folder

This folder contains the **LeadPoet (Subnet 71)** miner setup. Use it to mine on Bittensor SN71 from here.

**This checkout’s wallets:** coldkey name **`YOUR_COLDKEY_NAME`** (literal btcli `wallet.name`), hotkey **`culture`**, SS58 **`5Ek4PGqroRd5JmyDNu22VLVViPd5FLJ94W2WroLXP49qf4Yj`**. These are set in `.env` as `WALLET_NAME` / `WALLET_HOTKEY`.

---

## Quick start

### 1. Register (one-time, needs TAO)

```bash
btcli subnet register \
  --netuid 71 \
  --subtensor.network finney \
  --wallet.name YOUR_COLDKEY_NAME \
  --wallet.hotkey culture
```

If you use a different coldkey name in btcli, change `WALLET_NAME` in `.env`. Hotkey name must match the wallet entry you created (here: `culture`).

### 2. Run the miner

From this folder (`leadpoet-sn71/`):

```bash
./run-miner.sh
```

**High-performance (frontier) mode** — use this if you have good hardware and want more rewards (shorter interval, more leads per cycle, parallel submission):

```bash
FRONTIER=1 ./run-miner.sh
```

Or with custom wallet/network:

```bash
WALLET_NAME=YOUR_COLDKEY_NAME WALLET_HOTKEY=culture ./run-miner.sh
FRONTIER=1 WALLET_NAME=YOUR_COLDKEY_NAME WALLET_HOTKEY=culture ./run-miner.sh
```

Or run Python directly:

```bash
source venv/bin/activate
python neurons/miner.py \
  --wallet_name YOUR_COLDKEY_NAME \
  --wallet_hotkey culture \
  --netuid 71 \
  --subtensor_network finney
```

---

## What’s in this folder

| Item            | Description                          |
|-----------------|--------------------------------------|
| `venv/`         | Python virtualenv (already set up)   |
| `neurons/miner.py` | LeadPoet miner entrypoint         |
| `run-miner.sh`  | Script to start the miner            |
| `README.md`     | Full LeadPoet repo documentation     |
| `env.example`   | Example env vars (optional)           |

---

## First run

On first run the miner will ask you to **accept the Contributor Terms**. You must accept to mine.

---

## Full guide

For prerequisites, TAO, lead quality rules, and rate limits, see:

**`../SN71-LeadPoet-Mining-Guide.md`** (in the parent Work_shop folder)

---

## Multiple miners (several hotkeys)

To run **several miners** on the same subnet (one coldkey, multiple hotkeys):

1. **Create hotkeys:** `NUM_HOTKEYS=3 ./scripts/create-hotkeys-sn71.sh`  
2. **Register each on SN71:** `HOTKEYS="culture miner_2 miner_3" ./scripts/register-hotkeys-sn71.sh` (or override the list)  
3. **Run all:** `HOTKEYS="culture miner_2 miner_3" ./scripts/run-multi-miners.sh`  

See **`docs/MULTI-MINER-SETUP.md`** for the full step-by-step.

---

## Watch logs

To run the miner and save output to a log file (so you can watch from another terminal):

```bash
./run-miner-with-log.sh
```

In another terminal, watch live output:

```bash
cd /media/bsai2/Software/Work_shop/leadpoet-sn71
tail -f miner.log
```

## Run in screen (keeps miner running after disconnect)

Install screen once: `sudo apt install screen`

Then start the miner in a named session (with log file):

```bash
./scripts/run-miner-screen.sh
```

Attach to see live output: `screen -r sn71`  
Detach: **Ctrl+A** then **D**  
Watch log without attaching: `tail -f miner.log`

---

## Avoid rejections

To reduce gateway and validator rejections (and save your 200 rejections/day quota), enable miner-side pre-validation:

```bash
USE_LEAD_PRECHECK=1 ./run-miner.sh
```

See **`docs/AVOID-REJECTIONS.md`** for why leads are rejected and what is checked.

For **qualification vs lead mining**, **what the accepted terms mean**, and **fixing "0 leads" (missing GSE/FIRECRAWL/OPENROUTER)**, see **`docs/QUALIFICATION-AND-TERMS.md`**.

Before running the miner, check sourcing keys: **`./scripts/check-sourcing-env.sh`** (reports missing or placeholder keys).

**Pipeline diagram (models + tools, Mermaid):** **`docs/MINER_PIPELINE-DIAGRAM.md`**. Step-by-step with code pointers: **`docs/WORKFLOW-WITH-CODE.md`**.

---

## Useful links

- Dashboard: https://www.subnet71.com  
- LeadPoet: https://leadpoet.com  
- Repo: https://github.com/leadpoet/leadpoet  
