# Qualification, Accepted Terms & Fixing Sourcing

## 1. What is the qualification (model competition)?

**Qualification** is an **optional** separate track on Subnet 71:

- You build an AI/ML **model** that, given an Ideal Customer Profile (ICP) description, finds the best matching lead from the approved lead pool.
- You **submit that model** (as a tarball) to the gateway with a TAO payment.
- Validators **evaluate** your model against 100 ICPs; the **champion** model earns **5% of subnet emissions**.

So there are two ways to earn on SN71:

1. **Lead mining** (default) — source and submit leads; get rewarded when validators approve them.
2. **Qualification** — submit a curation model; get rewarded if it wins the evaluation.

Your model must expose `find_leads(icp)` and follow the rules in the README (time/cost limits, no forbidden libraries, etc.).

---

## 2. Why do we “skip” qualification when running non-interactively?

When you run the miner with **`ACCEPT_TERMS=1`** (e.g. in this window or in background):

- The miner does **not** prompt for “Submit a qualification model? (Y/N)”.
- It assumes **N** and goes straight to **normal lead mining**.

So we “skip” only in the sense of **not asking**. You can still submit a qualification model later by running the miner **without** `ACCEPT_TERMS=1`, then choosing **Y** when prompted, or by using the qualification submission flow separately (see README).

**Summary:** Skipping = “don’t ask, just mine leads.” You can do qualification anytime you want by running interactively and choosing Y.

---

## 3. Accepted terms (what you agreed to)

When you accepted the contributor terms (by typing **Y** or via **`ACCEPT_TERMS=1`**), you agreed to the **Leadpoet Contributor Terms of Service**. Here is a short summary; the full text is in the repo.

**Full terms (same as when you accepted):**  
`docs/contributor_terms.md` in this repo (and the canonical version is fetched from GitHub at runtime; the hash is shown in the miner log).

**Summary of the 10 sections:**

| # | Section | You agree to |
|---|--------|--------------|
| 1 | **Lawful data collection** | Only submit data from public, first-party, or licensed resale sources; no breach of ToS or use of paid DBs without resale rights. |
| 2 | **Ownership & license** | You own or have rights to the data; you grant Leadpoet an irrevocable, worldwide license to store, validate, sell, and distribute it. |
| 3 | **Accuracy & integrity** | Submit accurate, non-fraudulent, non-duplicative data; cooperate with audits. |
| 4 | **Restricted sources** | No ZoomInfo, Apollo, PDL, RocketReach, Hunter, Snov, Lusha, Clearbit, LeadIQ unless you have a resale agreement and provide a license hash. |
| 5 | **Resale rights** | Leadpoet and buyers may resell, enrich, and redistribute approved leads. |
| 6 | **Compliance & takedowns** | Respond to compliance/takedown requests; rewards may be frozen if your submission causes issues. |
| 7 | **Indemnification** | You accept responsibility; if your data causes a legal claim against Leadpoet, you indemnify. |
| 8 | **Terms version** | Your acceptance is tied to a terms_version_hash; if terms change, you must re-accept to continue mining. |
| 9 | **Privacy** | No personal KYC; wallet address is your identity; only wallet, timestamp, and version hash are logged. |
| 10 | **Termination** | Leadpoet may suspend contributors who violate terms or fail audits; suspended miners lose rewards. |

**Contact:** hello@leadpoet.com | https://leadpoet.com

---

## 4. Solving issues: why “0 leads” and how to fix it

If the miner starts but logs something like **“Sourced 0 new leads”** and **“Lead Sorcerer missing required environment variables”**, the **sourcing** step is missing API keys.

**Required for Lead Sorcerer (sourcing):**

| Variable | Purpose | Where to get |
|----------|--------|---------------|
| **GSE_API_KEY** | Google Programmable Search API key | [Programmable Search](https://programmablesearchengine.google.com/) — create a custom search engine, then get an API key from Google Cloud (Custom Search JSON API). |
| **GSE_CX** | Search engine ID | Same place — it’s the “Search engine ID” of your custom search engine. |
| **OPENROUTER_KEY** | LLM for lead generation | [OpenRouter](https://openrouter.ai/) |
| **FIRECRAWL_KEY** | Web crawl/extract | [Firecrawl](https://firecrawl.dev/) |

**What to do:**

1. Copy the example env and add these in the miner repo root:
   ```bash
   cp env.example .env
   ```
2. Edit **`.env`** and set (uncomment and fill if they’re commented):
   ```bash
   GSE_API_KEY=your_actual_google_api_key
   GSE_CX=your_actual_search_engine_id
   OPENROUTER_KEY=your_actual_openrouter_key
   FIRECRAWL_KEY=your_actual_firecrawl_key
   ```
3. Restart the miner so it loads the new `.env`.

After that, the miner should be able to source leads. If you still see errors, check the miner log for the exact missing variable or error message.

For **submission** and **validation**, the gateway/validators also expect Supabase, TrueList, and ScrapingDog keys to be configured where applicable (see `env.example`). This doc focuses on fixing the **sourcing** (“0 leads”) issue.

---

## 5. Go on — checklist

1. **Check sourcing keys:** `./scripts/check-sourcing-env.sh` (must pass before leads will source).
2. **Edit `.env`:** Replace placeholders for `GSE_API_KEY`, `GSE_CX`, `OPENROUTER_KEY`, `FIRECRAWL_KEY` with your real API keys.
3. **Start miner:** `ACCEPT_TERMS=1 ./run-miner.sh` (or add `USE_LEAD_PRECHECK=1 FRONTIER=1` for better throughput and fewer rejections).
4. **Optional:** Run in `screen` or `tmux` so the miner keeps running after you close the terminal.
