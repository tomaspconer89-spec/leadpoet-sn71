# Running Multiple Miners on Subnet 71 (LeadPoet)

Use **one coldkey** and **multiple hotkeys**. Each hotkey registers as a separate miner (separate UID) on the subnet. You run one miner process per hotkey.

**This repo’s single-miner defaults** use coldkey **`YOUR_COLDKEY_NAME`** (literal `wallet.name` in btcli) and hotkey **`culture`** (SS58 `5Ek4PGqroRd5JmyDNu22VLVViPd5FLJ94W2WroLXP49qf4Yj`). `run-miner.sh`, `register-hotkeys-sn71.sh`, and `run-multi-miners.sh` pick these up from `.env` / env unless you override them.

---

## 1. Prerequisites

- **Coldkey** already created (holds TAO for registration and payouts).
- **Enough TAO** to register each hotkey (registration cost is dynamic; check [subnet71.com](https://www.subnet71.com) or TAO.app).
- **LeadPoet repo** set up in `leadpoet-sn71/` with venv and `pip install -e .` done.

---

## 2. Create multiple hotkeys

One coldkey can have many hotkeys. Create as many as you want miners.

**Option A – Script (recommended)**

From repo root (`leadpoet-sn71/`):

```bash
# Create 3 hotkeys: default, miner_2, miner_3 (you'll be prompted for coldkey password)
NUM_HOTKEYS=3 ./scripts/create-hotkeys-sn71.sh
```

With a different coldkey name:

```bash
WALLET_NAME=my_coldkey NUM_HOTKEYS=4 ./scripts/create-hotkeys-sn71.sh
```

**Option B – Manual**

```bash
# Create each hotkey (prompted for coldkey password)
btcli wallet create --wallet.name miner --wallet.hotkey default
btcli wallet create --wallet.name miner --wallet.hotkey miner_2
btcli wallet create --wallet.name miner --wallet.hotkey miner_3
```

List hotkeys:

```bash
btcli wallet list --wallet.name miner
```

---

## 3. Register each hotkey on Subnet 71

Each hotkey must be **registered once** on netuid 71. Each registration costs TAO (from the coldkey).

**Option A – Script**

Register the hotkeys you created (edit the list if needed):

```bash
# Register default, miner_2, miner_3 on subnet 71
HOTKEYS="default miner_2 miner_3" ./scripts/register-hotkeys-sn71.sh
```

You will be prompted for your **coldkey password** for each registration.

**Option B – Manual (one by one)**

```bash
btcli subnet register --netuid 71 --subtensor.network finney --wallet.name miner --wallet.hotkey default
btcli subnet register --netuid 71 --subtensor.network finney --wallet.name miner --wallet.hotkey miner_2
btcli subnet register --netuid 71 --subtensor.network finney --wallet.name miner --wallet.hotkey miner_3
```

Check UIDs:

```bash
btcli wallet overview --netuid 71
```

You should see one row per hotkey (one UID per miner).

---

## 4. Run multiple miners

Run **one miner process per hotkey**. Each process uses the same coldkey and a different hotkey.

**Option A – Each miner in its own screen session (recommended)**

From repo root:

```bash
# Start miners for default, miner_2, miner_3 in separate screen sessions
HOTKEYS="default miner_2 miner_3" ./scripts/run-multi-miners.sh
```

List and attach:

```bash
screen -ls
screen -r sn71-default    # attach to first miner
screen -r sn71-miner_2    # attach to second miner
# Detach: Ctrl+A then D
```

**Option B – Custom hotkey list / frontier mode**

```bash
WALLET_NAME=YOUR_COLDKEY_NAME HOTKEYS="miner_2 miner_3 miner_4" FRONTIER=1 ./scripts/run-multi-miners.sh
```

**Option C – Run one miner in foreground (single hotkey)**

```bash
MODE=foreground WALLET_HOTKEY=miner_2 ./scripts/run-multi-miners.sh
```

Or use the existing run-miner.sh:

```bash
WALLET_HOTKEY=miner_2 ./run-miner.sh
WALLET_HOTKEY=miner_3 ./run-miner.sh   # in another terminal
```

**Option D – Background with nohup (no screen)**

```bash
MODE=nohup HOTKEYS="default miner_2 miner_3" ./scripts/run-multi-miners.sh
# Logs: logs/miner_default.log, logs/miner_miner_2.log, ...
```

---

## 5. Summary

| Step | What to do |
|------|------------|
| 1 | Create hotkeys: `NUM_HOTKEYS=3 ./scripts/create-hotkeys-sn71.sh` (or manual `btcli wallet create`) |
| 2 | Register each on SN71: `HOTKEYS="default miner_2 miner_3" ./scripts/register-hotkeys-sn71.sh` (or manual `btcli subnet register`) |
| 3 | Run miners: `HOTKEYS="default miner_2 miner_3" ./scripts/run-multi-miners.sh` (or run `./run-miner.sh` per hotkey in separate terminals) |

**Important**

- **One coldkey** can have many hotkeys; **one hotkey** = one UID on the subnet = one miner process.
- Each registration costs **TAO**; ensure the coldkey has enough balance before registering.
- Keep each miner process running (e.g. in screen or as a service) so validators can reach it.
