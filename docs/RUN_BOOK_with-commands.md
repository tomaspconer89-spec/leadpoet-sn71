## Runbook (Copy/Paste)

Use this sequence every time.

### 1) Start Crawl4AI API
From repo root:

```bash
python3 -m uvicorn crawler_4ai_api:app --host 127.0.0.1 --port 11235
```

Keep it running in one terminal.

---

### 2) Health-check providers + crawler
In another terminal:

```bash
python3 - << 'PY'
import os, json, requests
from pathlib import Path
from dotenv import load_dotenv

repo = Path(".")
load_dotenv(repo / ".env")

print("=== Health Check ===")

# Crawl4AI
try:
    r = requests.get("http://127.0.0.1:11235/health", timeout=8)
    print("Crawl4AI:", r.status_code, r.text[:120])
except Exception as e:
    print("Crawl4AI: ERROR", type(e).__name__, e)

# Serper
key = (os.getenv("SERPER_API_KEY") or "").strip()
if key:
    try:
        r = requests.get(
            "https://google.serper.dev/account",
            headers={"X-API-KEY": key, "Content-Type": "application/json"},
            timeout=12,
        )
        print("Serper:", r.status_code, r.text[:160])
    except Exception as e:
        print("Serper: ERROR", type(e).__name__, e)
else:
    print("Serper: KEY_MISSING")

# GSE
gk = (os.getenv("GSE_API_KEY") or "").strip()
cx = (os.getenv("GSE_CX") or "").strip()
if gk and cx:
    try:
        r = requests.get(
            "https://customsearch.googleapis.com/customsearch/v1",
            params={"key": gk, "cx": cx, "q": "b2b saas", "num": 1},
            timeout=12,
        )
        print("GSE:", r.status_code)
    except Exception as e:
        print("GSE: ERROR", type(e).__name__, e)
else:
    print("GSE: KEY_OR_CX_MISSING")

print("=== End ===")
PY
```

**Proceed only if**:
- Crawl4AI is `200`
- Serper is `200`
- (GSE can be degraded if Serper is healthy)

---

### 3) Collect leads (target pass count)
Single batch of 10:
```bash
USE_CRAWL4AI_FIRST=1 python3 scripts/collect_leads_precheck_only.py -n 10
```

Better production loop (until pass target):
```bash
USE_CRAWL4AI_FIRST=1 python3 scripts/collect_leads_precheck_only.py -n 12 --target-pass 10 --max-runs 10
```

---

### 4) Submit pass queue to gateway
```bash
python3 scripts/submit_collected_pass.py --max 100
```

Optional with linkedin enrichment on submit path:
```bash
python3 scripts/submit_collected_pass.py --max 100 --enrich-linkedin 1
```

---

### 5) Fast monitoring commands
Watch run status:
```bash
tail -f miner.log
```

Check queue counts:
```bash
ls -1 lead_queue/collected_pass/*.json 2>/dev/null | wc -l
ls -1 lead_queue/collected_precheck_fail/*.precheck_failed.json 2>/dev/null | wc -l
ls -1 lead_queue/A_ready_submit/*.json 2>/dev/null | wc -l
```

---

## Quick troubleshooting

- **`ConnectError` on search**  
  Network/provider issue. Re-run health check first. If Serper `200`, retry collection.

- **Crawl4AI refused connection**  
  Start uvicorn service again (step 1).

- **Many fails like `missing_required_fields: linkedin`**  
  Expected B-bucket behavior; run retry/regrade flow later:
  ```bash
  python3 scripts/regrade_b_queue.py --enrich-linkedin 1
  ```

If you want, next I can run this exact runbook now step-by-step and report outputs live.