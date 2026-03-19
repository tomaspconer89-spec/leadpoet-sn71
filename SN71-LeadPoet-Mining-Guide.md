# Step-by-Step Guide: Mining on Bittensor Subnet 71 (LeadPoet)

This guide walks you through mining on **Subnet 71 (LeadPoet)** — the decentralized AI sales intelligence platform on Bittensor where miners earn TAO by sourcing and qualifying high-quality sales leads.

---

## What You Need to Know

- **LeadPoet (SN71)** = Miners find/qualify sales leads; validators score them; you earn TAO for approved, quality leads.
- **Platform**: Linux or macOS only (mining is **not** supported on Windows).
- **Costs**: You need **TAO** for registration (amount varies; check current cost before registering).
- **Rewards**: Based on **quality and validity** of leads (rolling 30-epoch history). Higher reputation-score leads = more reward.

---

## Step 1 — Prerequisites

### 1.1 Hardware & OS

- **OS**: Linux or macOS.
- **Miners**: No strict minimum; depends on your sourcing approach (APIs, scraping, etc.).
- **Network**: Stable internet.

### 1.2 Software

- **Python**: 3.9–3.12.
- **Bittensor CLI**:
  ```bash
  pip install bittensor>=9.10
  ```

### 1.3 Bittensor Wallet

Create a wallet if you don’t have one:

```bash
# Create coldkey (holds TAO)
btcli wallet create --wallet.name miner

# Create hotkey (used for mining)
btcli wallet create --wallet.name miner --wallet.hotkey default
```

Keep coldkey seed phrase **offline and safe**. You need TAO in this wallet for registration and fees.

---

## Step 2 — Get TAO for Registration

1. Buy or transfer **TAO** to the coldkey you’ll use (e.g. `miner`).
2. Check registration cost for subnet 71 (it changes):
   - Use [subnet71.com](https://www.subnet71.com) or TAO.app for current cost.
3. Ensure balance is enough for **registration + a small buffer** for fees.

---

## Step 3 — Clone and Install LeadPoet

```bash
# 1. Clone the repo (note: capital L in Leadpoet)
git clone https://github.com/leadpoet/Leadpoet.git
cd Leadpoet

# 2. Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate   # Linux/macOS

# 3. Install
pip install --upgrade pip
pip install -e .
```

---

## Step 4 — Register Your Miner on Subnet 71

Register your hotkey on netuid 71 (Finney mainnet):

```bash
btcli subnet register \
    --netuid 71 \
    --subtensor.network finney \
    --wallet.name miner \
    --wallet.hotkey default
```

- Replace `miner` and `default` with your wallet name and hotkey if different.
- When prompted, use your **coldkey** password to pay the registration cost in TAO.
- After success, your hotkey has a **UID** on subnet 71.

Check registration:

```bash
btcli wallet overview --netuid 71
```

---

## Step 5 — Run the Miner

From the `Leadpoet` repo directory, with your venv activated:

```bash
python neurons/miner.py \
    --wallet_name miner \
    --wallet_hotkey default \
    --netuid 71 \
    --subtensor_network finney
```

Optional: custom wallet path (if not using default `~/.bittensor/wallets`):

```bash
python neurons/miner.py \
    --wallet_name miner \
    --wallet_hotkey default \
    --wallet_path /path/to/your/wallets \
    --netuid 71 \
    --subtensor_network finney
```

Keep this process running (e.g. in `screen` or `tmux`) so your miner stays online.

---

## Step 6 — How Mining Works (What Your Miner Does)

1. **Sourcing**: Your miner finds prospects (e.g. via web scraping, APIs, databases).
2. **Submission**: It gets a pre-signed S3 URL, hashes lead data, signs with your hotkey, and uploads.
3. **Validation**: Several validators check each lead (email, domain, LinkedIn, reputation, etc.).
4. **Rewards**: Only **consensus-approved** leads count. Your share of emissions is based on the **sum of reputation scores** of your approved leads over a rolling 30-epoch window.

---

## Step 7 — Lead Quality Rules (Avoid Rejections)

- **Email**: Must be **valid** (no catch-all, disposable, or generic like `info@`, `hello@`).  
- **Name–email match**: First or last name should appear in the email (e.g. `john.doe@company.com`).
- **Required fields**: All required fields in the lead JSON must be present and correct (see repo for exact schema).
- **Industry**: Use exact values from `validator_models/industry_taxonomy.py`.
- **Source URL**: Use the real URL where the lead was found, or `"proprietary_database"` if applicable (see repo rules).

**Rate limits (per day, UTC):**

- 1,000 submission attempts.
- 200 rejections (including consensus rejections). After 200 rejections, submissions are blocked until midnight UTC.

---

## Step 8 — Optional: Qualification Model (Curating Leads)

Beyond **sourcing** leads, you can run a **qualification model** that picks leads from the approved pool that match an Ideal Customer Profile (ICP):

- You implement a `find_leads(icp)` (or `qualify`) function that queries the leads DB (using env-injected config, no hardcoded credentials).
- You submit the model to the gateway; validators score it on ~100 ICPs.
- Good performance can earn extra rewards. See the LeadPoet README for the exact interface, return schema, and time/cost limits.

---

## Step 9 — Monitor Your Miner

- **Dashboard**: [subnet71.com](https://www.subnet71.com) — leads, miners, incentives.
- **Wallet/UID**: `btcli wallet overview --netuid 71`
- **Rejection feedback**: Rejection reasons are logged (e.g. in transparency logs); use them to improve lead quality and approval rate.

---

## Quick Reference Commands

| Task              | Command |
|-------------------|--------|
| Register on SN71 | `btcli subnet register --netuid 71 --subtensor.network finney --wallet.name miner --wallet.hotkey default` |
| Run miner         | `python neurons/miner.py --wallet_name miner --wallet_hotkey default --netuid 71 --subtensor_network finney` |
| Check wallet/UID  | `btcli wallet overview --netuid 71` |

---

## Useful Links

- **LeadPoet repo**: https://github.com/leadpoet/Leadpoet  
- **Subnet 71 dashboard**: https://www.subnet71.com  
- **Subnet info**: https://subnetalpha.ai/subnet/subnet-71/  
- **Bittensor docs**: https://docs.bittensor.com  
- **TAO.app (subnets)**: https://tao.app  

---

## Summary Checklist

- [ ] Linux or macOS; Python 3.9–3.12; `bittensor>=9.10` installed  
- [ ] Bittensor wallet created (coldkey + hotkey)  
- [ ] TAO in wallet for registration and fees  
- [ ] LeadPoet repo cloned, venv created, `pip install -e .`  
- [ ] Registered on subnet 71: `btcli subnet register --netuid 71 ...`  
- [ ] Miner running: `python neurons/miner.py ...`  
- [ ] Lead quality and rate limits understood to maximize approvals and rewards  

Once your miner is registered and running, focus on **high-quality, valid leads** and staying within rate limits to maximize your share of SN71 emissions.
