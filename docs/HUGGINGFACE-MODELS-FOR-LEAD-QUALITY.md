# Hugging Face Models for Lead Quality (LeadPoet SN71)

This doc lists **Hugging Face models** that can improve lead quality in this project. Use them to **augment or replace** OpenRouter LLM calls for classification and scoring (faster, cheaper, local).

---

## 1. Industry / company classification

**Use case:** Map company/lead descriptions to industry and sub-industry (your `industry` / `sub_industry` must match `validator_models/industry_taxonomy.py`).

### jmurray10/naics-aof-classifier (recommended)

- **Task:** Company description → NAICS code (1,012 codes) + Area of Focus (59 tech categories).
- **Base:** BAAI/bge-small-en-v1.5 (384-dim embeddings), cosine similarity.
- **Performance:** ~70% top-1 NAICS, ~85% top-1 AoF; **<40ms** inference, no GPU.
- **Fit:** Good for tech/B2B. You’d add a **mapping layer** from NAICS/AoF → LeadPoet `industry` / `sub_industry` (see `miner_models/taxonomy.py`).
- **URL:** https://huggingface.co/jmurray10/naics-aof-classifier  
- **Note:** Requires accepting conditions on the model page (login).

**Example usage (from model card):**
```python
from transformers import AutoModel, AutoTokenizer
import torch
# Load: jmurray10/naics-aof-classifier (encoder + naics_index.pkl)
# Input: "Represent this business for industry classification: {description}"
# Output: naics code/title, aof class, confidence
```

### swarupt/industry-classification

- **Task:** Business description → one of 20 industries (DistilBERT).
- **Fit:** Simpler than NAICS; may need mapping to LeadPoet’s taxonomy (e.g. Tech & AI, Finance & Fintech, Health & Wellness, Media & Education).
- **URL:** https://huggingface.co/swarupt/industry-classification

### sampathkethineedi/industry-classification-api

- **Task:** Industry classification API-style model.
- **URL:** https://huggingface.co/sampathkethineedi/industry-classification-api

---

## 2. Email / intent classification (lead quality signal)

**Use case:** Score or filter leads by “intent” (e.g. sales vs support) from email or text.

### Shoriful025/Email-Intent-Classifier (recommended)

- **Task:** Email text → 5 intent categories (multi-class).
- **Base:** distilbert-base-uncased.
- **Data:** 100K enterprise emails (Sales, HR, IT, Marketing).
- **Performance:** **95.1% accuracy**, macro F1 0.94.
- **Fit:** Use to tag or filter leads by email intent (e.g. keep “sales” / “inquiry” intents only).
- **URL:** https://huggingface.co/Shoriful025/Email-Intent-Classifier

**Usage:**
```python
from transformers import pipeline
pipe = pipeline("text-classification", model="Shoriful025/Email-Intent-Classifier")
out = pipe("I'd like to schedule a demo of your platform.")
# Returns label + score
```

---

## 3. Intent / lead scoring (generic)

**Use case:** Pre-score “how well does this lead match an ICP?” before or alongside OpenRouter.

### mindpadi/intent_classifier

- **Task:** Intent detection (e.g. emotional support, scheduling).
- **Performance:** 91.3% accuracy, 89.8% F1.
- **Fit:** More “conversation intent”; could be repurposed or used as a feature for lead scoring.
- **URL:** https://huggingface.co/mindpadi/intent_classifier

### learn-abc/banking77-intent-classifier

- **Task:** 77 intents → 12 categories (BERT).
- **Performance:** 96.04% accuracy, 0.956 F1.
- **Fit:** Domain-specific (banking); useful if your leads are finance-heavy.
- **URL:** https://huggingface.co/learn-abc/banking77-intent-classifier

---

## 4. Datasets (for fine-tuning your own model)

- **shawhin/lead-scoring-x:** 5,688 samples, 7 features, binary conversion prediction.  
  https://huggingface.co/datasets/shawhin/lead-scoring-x  
- **Hridhi/B2B-Sales-Acceleration-Intelligence:** 500+ instruction-response pairs for B2B sales AI (gated).  
  https://huggingface.co/datasets/Hridhi/B2B-Sales-Acceleration-Intelligence  
- **rankfor/PersonaGen-Enterprise:** 5K buyer personas, 15 industries, 42 roles, 47K+ search queries.  
  https://huggingface.co/datasets/rankfor/PersonaGen-Enterprise  

---

## 5. Where to plug into this repo

| Goal | Current (OpenRouter) | Hugging Face option | Where in code |
|------|----------------------|----------------------|----------------|
| Industry from description | `classify_industry()` in intent_model.py | jmurray10/naics-aof-classifier or swarupt/industry-classification | `miner_models/intent_model.py` – add a local path that maps HF output → LeadPoet industry/sub_industry |
| Email/intent filter | (none) | Shoriful025/Email-Intent-Classifier | Before or after `sanitize_prospect()` in miner sourcing loop – filter or tag by email intent |
| Lead vs ICP score | `rank_leads()` in intent_model.py (OpenRouter) | Fine-tune on lead-scoring-x or use as extra feature | `miner_models/intent_model.py` – optional local scorer used before or after OpenRouter |

---

## 6. Integrated in this repo (optional)

The miner can use two HF models via env flags (no code change):

1. **Industry (local):** `USE_HF_INDUSTRY=1`  
   - Uses `swarupt/industry-classification` in `classify_industry()` before OpenRouter.  
   - Fast, no API cost; falls back to OpenRouter/heuristic if HF fails or is off.

2. **Email intent filter (no spam):** `USE_HF_EMAIL_INTENT_FILTER=1`  
   - Uses `Shoriful025/Email-Intent-Classifier` in the sourcing loop.  
   - Drops leads classified as `PROMOTION` (marketing spam).

**Install (required for HF):**
```bash
pip install transformers torch
```

**Enable in `.env` or shell:**
```bash
export USE_HF_INDUSTRY=1
export USE_HF_EMAIL_INTENT_FILTER=1
./run-miner.sh
```

Implementation: `miner_models/hf_models.py`; wiring in `miner_models/intent_model.py` and `neurons/miner.py`.

---

## 7. Quick start: add a local industry classifier (manual)

1. Install:
   ```bash
   pip install transformers torch
   ```
2. For **NAICS/AoF** (best for tech companies):
   - Accept conditions: https://huggingface.co/jmurray10/naics-aof-classifier  
   - Use the model card’s `predict(description)` snippet.
   - Build a small mapping: NAICS or AoF → `industry` / `sub_industry` from `miner_models/taxonomy.py` (and `validator_models/industry_taxonomy.py` if you want validator alignment).
3. For **email intent** (filter low-intent leads):
   - Load `Shoriful025/Email-Intent-Classifier` with `pipeline("text-classification", model="...")`.
   - Run on lead’s `description` or a synthetic “email” field; drop or down-rank low-intent labels.

---

## Summary

- **Industry:** **jmurray10/naics-aof-classifier** (NAICS + AoF, fast, no GPU) + mapping to LeadPoet taxonomy.
- **Email/intent:** **Shoriful025/Email-Intent-Classifier** (95.1% accuracy, 5 classes) to filter or tag leads.
- **Lead scoring:** Keep OpenRouter for rich ICP matching; optionally add **shawhin/lead-scoring-x** or a small HF classifier as a pre-filter or extra feature.

All of these run locally with `transformers` + PyTorch, so they can reduce OpenRouter cost and latency while improving consistency of industry and intent signals.
