# LeadPoet SN71 System Workflow (with code locations)

This document walks through the **miner workflow** from startup to lead submission, with file paths and function names so you can jump to the code.

---

## Where is the miner?

| What | Location |
|------|----------|
| **Sourcing miner** | **`neurons/miner.py`** — `main()`, sourcing loop, `sanitize_prospect()`, gateway submit. Restored from upstream `origin/main`. |
| **Base miner class** | **`Leadpoet/base/miner.py`** — `BaseMinerNeuron`: axon, metagraph, run loop. No sourcing logic. |
| **What exists under `neurons/`** | Only `neurons/validator.py`, `neurons/auditor_validator.py`. No `neurons/miner.py`. |

Discovery/crawl/industry/intent live in `miner_models/` and Lead Sorcerer; validation/sanitization use `Leadpoet/utils/source_provenance.py`, `miner_models/lead_precheck.py`, and validator taxonomy. The **orchestrator** that would call all of that (get_leads → validate → sanitize → precheck → submit) is the missing **`neurons/miner.py`**.

---

## 1. Startup: how the miner is run

| Step | What happens | File / code |
|------|----------------|-------------|
| You run the miner | e.g. `./scripts/run-miner-screen.sh` or `./run-miner-with-log.sh` | **`scripts/run-miner-screen.sh`** (screen wrapper), **`run-miner-with-log.sh`** (log tee) |
| Wrapper sets env and calls runner | Sets `ACCEPT_TERMS`, `USE_LEAD_PRECHECK`, `FRONTIER`, etc.; then runs `./run-miner.sh` | **`run-miner-with-log.sh`** lines 8–15, **`scripts/run-miner-screen.sh`** line 27 |
| Runner activates venv and starts Python | Activates `venv312` or `venv`, exports wallet/netuid, then runs the miner entry point | **`run-miner.sh`** lines 9–21, 32–46 |
| Python entry point | Either `leadpoet` CLI or `python -u neurons/miner.py` (see `setup.py`) | **`run-miner.sh`** lines 33–46; **`setup.py`** entry_points: `leadpoet=neurons.miner:main` |

So the **intended** first Python file is **`neurons/miner.py`** with **`main()`** as the process entry.

**`neurons/miner.py`** has been restored from upstream (`git show origin/main:neurons/miner.py`). It contains the full sourcing loop, sanitize, precheck, and gateway submit logic.

---

## 2. Miner process: initialization

| Step | What happens | File / code |
|------|----------------|-------------|
| Parse args, load config | Wallet name/hotkey, netuid, subtensor network, sourcing interval, etc. | **`neurons/miner.py`** – `main()`, argparse / config |
| Build miner neuron | Subclass of base miner; connect to subtensor, register wallet, set UID | **`Leadpoet/base/miner.py`** – `BaseMinerNeuron`, `__init__` (e.g. axon, metagraph) |
| Start sourcing loop | Loop that periodically generates leads and submits them | **`neurons/miner.py`** – `sourcing_loop()` (or equivalent) called from `main()` |

Base miner behavior (axon serve, metagraph sync) is in **`Leadpoet/base/miner.py`** (`run()`, axon attach). The **sourcing-specific loop** (get leads → validate → submit) lives in **`neurons/miner.py`**.

---

## 3. Sourcing loop (one cycle)

Each cycle of the sourcing loop typically does: **get leads → validate (source provenance) → sanitize → precheck → dedupe → submit to gateway**.

### 3.1 Generate leads (Lead Sorcerer)

| Step | What happens | File / code |
|------|----------------|-------------|
| Call Lead Sorcerer | Miner asks for N leads (and optional industry/region) | **`neurons/miner.py`** – calls `get_leads(num_leads, industry, region)` from miner_models |
| Entry to Lead Sorcerer | Check env (GSE, OPENROUTER, FIRECRAWL keys), then run pipeline | **`miner_models/lead_sorcerer_main/main_leads.py`** – `get_leads()` ~410–502 |
| Run pipeline | Build ICP config, run Domain → Crawl, return lead records | **`main_leads.py`** – `run_lead_sorcerer_pipeline()` ~281–361; calls **`src/orchestrator.py`** – `LeadSorcererOrchestrator.run_pipeline()` |
| Orchestrator: Domain → Crawl | Load `icp_config.json`, run domain tool then crawl tool | **`miner_models/lead_sorcerer_main/src/orchestrator.py`** – `run_pipeline()` ~363; `_run_traditional_pipeline()` ~399; `domain_tool.run()`, `crawl_tool.run()` |
| Domain tool | Discover domains (GSE/DDG from queries), score with LLM, output candidate lead records | **`miner_models/lead_sorcerer_main/src/domain.py`** – `DomainTool.run()` |
| Crawl tool | For each domain, scrape with Firecrawl; extract company + contact (name, email, role, LinkedIn, etc.) | **`miner_models/lead_sorcerer_main/src/crawl.py`** – `CrawlTool.run()` |
| Convert to legacy format | Map Lead Sorcerer records to the dict shape the miner expects (business, email, website, industry, role, linkedin, etc.) | **`main_leads.py`** – `convert_lead_record_to_legacy_format()` ~163–278 |
| Return to miner | List of legacy-format leads (with email + business) | **`main_leads.py`** – `get_leads()` returns `legacy_leads` |

ICP (queries, caps, specific_urls) is read from **`miner_models/lead_sorcerer_main/icp_config.json`**.  
If `specific_urls` is non-empty, the orchestrator can **bypass domain discovery** and create lead records from those URLs (see **`orchestrator.py`** – `should_bypass_domain_discovery()`, `create_lead_records_from_specific_urls()`).

---

### 3.2 Source provenance validation (miner-side)

| Step | What happens | File / code |
|------|----------------|-------------|
| Filter by source provenance | For each lead, validate `source_url` / `source_type`: denylist, domain age, URL reachability | **`neurons/miner.py`** – logic that uses `Leadpoet.utils.source_provenance` (e.g. `validate_source_url()`) to keep only valid leads |
| Denylist and URL checks | Denylist check, domain age ≥7 days, HEAD request for reachability | **`Leadpoet/utils/source_provenance.py`** – `validate_source_url()` ~86; `is_restricted_source()` ~38 |

Only leads that pass this validation should be passed to sanitize/precheck/submit.

---

### 3.3 Sanitize and normalize

| Step | What happens | File / code |
|------|----------------|-------------|
| Sanitize prospect | Fill/normalize required fields: country, city, employee_count, company_linkedin, etc., so gateway is less likely to reject | **`neurons/miner.py`** – `sanitize_prospect(lead, miner_hotkey)` |
| Log produced leads | After sanitization, log each lead (business, email, website) for debugging | **`neurons/miner.py`** – after building `sanitized` list, `bt.logging.info("Lead Sorcerer produced lead: ...")` |

---

### 3.4 Precheck (optional, before gateway)

| Step | What happens | File / code |
|------|----------------|-------------|
| Precheck leads | If `USE_LEAD_PRECHECK=1`, run local checks that mirror gateway/validator rules (required fields, email format, free email, role/description length, industry taxonomy, etc.) | **`miner_models/lead_precheck.py`** – `filter_leads_by_precheck(leads)` ~303; `precheck_lead(lead)` and helpers (`_check_required_fields`, `_check_source_url`, etc.) |
| Split passed / failed | Returns list to submit and list of (lead, reason) for failures | **`lead_precheck.py`** – `filter_leads_by_precheck()` returns `(passed, failed)` |

Constants (e.g. `REQUIRED_FIELDS`, `VALID_EMPLOYEE_COUNTS`) are in **`miner_models/lead_precheck.py`** at the top.

---

### 3.5 Deduplication and submit to gateway

| Step | What happens | File / code |
|------|----------------|-------------|
| Dedupe | Avoid re-submitting the same email (e.g. check Supabase or local set) | **`neurons/miner.py`** – logic that uses **`Leadpoet/utils/cloud_db.py`** (e.g. duplicate check by email) before adding to `to_submit` |
| Get presigned URL | For each lead, call gateway to get presigned S3 URL and lead_id | **`Leadpoet/utils/cloud_db.py`** – `gateway_get_presigned_url(wallet, lead)` (referenced by scripts; exact name in that module) |
| Upload lead | PUT lead JSON to the presigned S3 URL | **`Leadpoet/utils/cloud_db.py`** – `gateway_upload_lead(s3_url, lead)` |
| Verify submission | Tell gateway the upload is done; gateway records SUBMISSION / VALIDATION_FAILED | **`Leadpoet/utils/cloud_db.py`** – `gateway_verify_submission(wallet, lead_id)` |
| Log failure reason | If a step fails (presign, upload, verify), log which step failed for which business | **`neurons/miner.py`** – after `_submit_one_lead()`, `bt.logging.warning("Submit failed for %s: %s", business, status)` |

Gateway base URL is configured in **`Leadpoet/utils/cloud_db.py`** (e.g. `GATEWAY_URL`).  
Events (SUBMISSION_REQUEST, SUBMISSION, VALIDATION_FAILED, CONSENSUS_RESULT) are written to the **Supabase transparency log**; the miner reads/writes via the same cloud_db / Supabase client.

---

## 4. Validator and gateway (high level)

| Component | Role | File / code |
|-----------|------|-------------|
| Gateway | Accepts miner uploads (presign → upload → verify), runs initial validation (required fields, sanity checks). Rejects with VALIDATION_FAILED and reason. | Gateway service (e.g. **`gateway/`**); miner talks to it via **`Leadpoet/utils/cloud_db.py`** |
| Validator | Fetches leads from queue, runs automated checks (including **source provenance** again), consensus, rewards. | **`neurons/validator.py`** – validator loop; **`validator_models/automated_checks.py`** – `run_stage0_2_checks()` / `run_automated_checks()`; **`validator_models/checks_repscore.py`** – `check_source_provenance()` |
| Source provenance (validator) | Same rules: source_url, source_type, denylist, domain age, licensed resale if needed | **`Leadpoet/utils/source_provenance.py`** – shared; **`validator_models/checks_repscore.py`** – `check_source_provenance()` ~921 |

---

## 5. Quick reference: important files

| Purpose | File |
|--------|------|
| Start miner (screen / log) | `run-miner-with-log.sh`, `scripts/run-miner-screen.sh` |
| Run miner (venv + Python) | `run-miner.sh` |
| Miner entry + sourcing loop | `neurons/miner.py` (main, sourcing_loop, sanitize_prospect, submit loop) |
| Base miner (axon, metagraph) | `Leadpoet/base/miner.py` |
| Get leads (Lead Sorcerer) | `miner_models/lead_sorcerer_main/main_leads.py` (get_leads, run_lead_sorcerer_pipeline, convert_lead_record_to_legacy_format) |
| Sorcerer pipeline (Domain → Crawl) | `miner_models/lead_sorcerer_main/src/orchestrator.py` |
| Domain discovery + scoring | `miner_models/lead_sorcerer_main/src/domain.py` |
| Crawl (Firecrawl) | `miner_models/lead_sorcerer_main/src/crawl.py` |
| ICP config | `miner_models/lead_sorcerer_main/icp_config.json` |
| Source provenance (shared) | `Leadpoet/utils/source_provenance.py` |
| Precheck (miner-side) | `miner_models/lead_precheck.py` |
| Gateway + Supabase | `Leadpoet/utils/cloud_db.py` |
| Validator checks (source provenance, etc.) | `validator_models/checks_repscore.py`, `validator_models/automated_checks.py` |
| Lead stats (transparency log) | `scripts/lead_stats.py` |

---

## 6. Data flow summary

```
run-miner.sh
  → neurons/miner.py main()
    → sourcing_loop():
      → get_leads()                    [main_leads.py]
        → run_lead_sorcerer_pipeline() [main_leads.py]
          → orchestrator.run_pipeline() [orchestrator.py]
            → domain_tool.run()         [domain.py]
            → crawl_tool.run()          [crawl.py]
        → convert_lead_record_to_legacy_format() [main_leads.py]
      → source provenance filter       [source_provenance.py]
      → sanitize_prospect()            [neurons/miner.py]
      → filter_leads_by_precheck()     [lead_precheck.py]  (if USE_LEAD_PRECHECK=1)
      → dedupe                         [cloud_db / miner]
      → for each lead: gateway_get_presigned_url → gateway_upload_lead → gateway_verify_submission [cloud_db.py]
```

This is the workflow of the system with the part of the code where each step lives.

---

## System check before running

From the repo root, run:

```bash
python scripts/check-system.py
```

This verifies: critical files exist, run scripts are executable, Python import chain (Leadpoet, miner_models, neurons.miner), `.env` presence, and optional WALLET_NAME/WALLET_HOTKEY. Fix any reported errors before starting the miner.

**Headless / screen runs:** Set `ACCEPT_TERMS=1` so the miner does not block on "Do you accept these terms?" or "Submit qualification model?". The run scripts (`run-miner-with-log.sh`, `scripts/run-miner-screen.sh`) set this by default so the whole flow runs without interactive prompts.
