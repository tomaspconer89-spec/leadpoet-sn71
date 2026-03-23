# Miner pipeline — visual workflow (models & tools)

Diagrams below render on GitHub/GitLab and in many Markdown previewers that support **Mermaid**.  
For the same flow with file/function references, see **[WORKFLOW-WITH-CODE.md](./WORKFLOW-WITH-CODE.md)**.

---

## 1. End-to-end flow

```mermaid
flowchart LR
  subgraph run["Launch"]
    A[run-miner.sh] --> B[neurons/miner.py]
    S[scripts/run-miner-screen.sh] --> A
    L[run-miner-with-log.sh] --> A
  end

  subgraph net["Subnet"]
    B --> C[Bittensor: wallet / metagraph / axon]
  end

  subgraph loop["Sourcing loop"]
    C --> D[get_leads — Lead Sorcerer]
    D --> E[Source provenance]
    E --> F[Sanitize & normalize]
    F --> G[lead_precheck]
    G --> H[Intent: rank / industry / roles]
    H --> I[Dedupe & lead_queue]
    I --> J[Gateway: presign → S3 upload → verify]
    J --> D
  end
```

---

## 2. Lead Sorcerer (discovery → crawl → legacy shape)

```mermaid
flowchart TB
  subgraph cfg["Config"]
    ICP[icp_config.json]
  end

  subgraph domain["Domain discovery"]
    Q[Search APIs: Serper / Brave / Google CSE]
    LLM[OpenRouter LLMs — scoring & extraction]
    Q --> DOM[domain.py — DomainTool]
    LLM --> DOM
    ICP --> DOM
  end

  subgraph crawl["Crawl & extract"]
    C4[Crawl4AI API — crawler_4ai_api.py / CRAWL4AI_API_URL]
    FC[Firecrawl — FIRECRAWL_KEY]
    TR[trafilatura fallback]
    DOM --> CRAWL[crawl.py — CrawlTool]
    CRAWL --> C4
    CRAWL --> FC
    CRAWL --> TR
  end

  subgraph out["To miner"]
    CRAWL --> CONV[main_leads.py — convert_lead_record_to_legacy_format]
    TM[team_members → contacts fallback]
    CONV --> TM
    TM --> LEG[Legacy lead dicts → miner]
  end
```

---

## 3. Models & classifiers (after generation)

```mermaid
flowchart LR
  subgraph hf["Optional local / HF"]
    H1[USE_HF_INDUSTRY — swarupt/industry-classification via hf_models]
    H2[USE_HF_EMAIL_INTENT_FILTER — email intent classifier]
  end

  subgraph api["API fallback"]
    OR[OpenRouter — industry / roles / ranking when enabled]
  end

  subgraph heur["Heuristics"]
    KW[Keyword taxonomy — miner_models/taxonomy.py]
  end

  LEG2[Leads from Sorcerer] --> H1
  LEG2 --> H2
  H1 -.->|if off or fail| OR
  H2 -.->|if off or fail| OR
  OR -.->|if no key| KW
  H1 -.->|map fail| KW
```

---

## 4. Validation & submission stack

```mermaid
flowchart TB
  P[Lead dict] --> SP[Leadpoet.utils.source_provenance — URL / source_type]
  SP --> PC[miner_models/lead_precheck.py]
  PC --> DD[Duplicate checks — email / LinkedIn / company]
  DD --> GW[HTTP: presigned URL + S3 + verification]
  GW --> BT[On-chain / subnet activity via Bittensor]
```

---

## ASCII sketch (no Mermaid needed)

```
  screen / run-miner.sh
           │
           ▼
  neurons/miner.py ◄──────────────────────────────┐
           │                                        │
           │  get_leads()                            │
           ▼                                        │
  main_leads.py + orchestrator                      │
     │ domain (Serper/Brave/GSE + OpenRouter)       │
     │ crawl (Crawl4AI / Firecrawl / trafilatura)   │
     └► legacy leads (+ team_members→contacts)     │
           │                                        │
           ├► source_provenance                     │
           ├► sanitize                             │
           ├► lead_precheck                         │
           ├► intent_model (HF +/or OpenRouter)    │
           ├► lead_queue / dedupe                  │
           └► gateway upload ────────────────────────┘
```

---

## Environment flags (common)

| Flag / key | Role |
|------------|------|
| `SERPER_API_KEY` / `BRAVE_API_KEY` / `GSE_*` | Domain search |
| `OPENROUTER_KEY` | LLM calls in domain/crawl path & intent (unless disabled) |
| `FIRECRAWL_KEY` | Managed scrape/extract |
| `CRAWL4AI_API_URL`, `USE_CRAWL4AI_FIRST` | Local Crawl4AI service |
| `USE_HF_INDUSTRY`, `USE_HF_EMAIL_INTENT_FILTER` | HF classifiers in `miner_models/` |
| `USE_LEAD_PRECHECK` | Local gateway-aligned checks |
| `WALLET_NAME`, `WALLET_HOTKEY` | Bittensor wallet |

---

*Last updated: aligns with Lead Sorcerer + `neurons/miner.py` sourcing path in this repo.*
