## Lead Pipeline Command Runbook

This document is a command-first workflow for daily operations.

## Core Commands

Collect new leads (target 10 pass):
```bash
python3 scripts/collect_leads_precheck_only.py --target-pass 10
```

Collect while allowing company-anchor records without contacts (so downstream enrichment can still run):
```bash
LEAD_SORCERER_RELAX_CONTACT_FILTER=1 python3 scripts/collect_leads_precheck_only.py --target-pass 10
```

Regrade retry queue (`B_retry_enrichment`):
```bash
python3 scripts/regrade_b_queue.py
```

Readiness check (validator-like) for ready folders:
```bash
python3 scripts/check_submit_readiness.py
```

Readiness check with JSON output:
```bash
python3 scripts/check_submit_readiness.py --json
```

Submit from pending/ready queue to gateway:
```bash
python3 scripts/submit_queued_leads.py
```

Capture ScrapingDog raw responses + derived lead report for a specific LinkedIn URL:
```bash
python3 scripts/scrapingdog_lead_report.py --linkedin-url "https://www.linkedin.com/in/example"
```

## Useful Monitoring

Watch live logs:
```bash
tail -f miner.log
```

Inspect latest saved Sorcerer artifacts snapshot:
```bash
ls -1dt reports/sorcerer_artifacts/* | head -1
cat "$(ls -1dt reports/sorcerer_artifacts/* | head -1)/manifest.json"
```

Count current queue sizes quickly:
```bash
for d in lead_queue/A_ready_submit lead_queue/B_retry_enrichment lead_queue/C_good_account_needs_person lead_queue/D_low_confidence_hold lead_queue/E_reject lead_queue/collected_pass lead_queue/collected_precheck_fail lead_queue/submitted; do printf "%-45s %5s\n" "$d" "$(ls -1 "$d" 2>/dev/null | wc -l)"; done
```

## Recommended Daily Flow

1) Collect:
```bash
python3 scripts/collect_leads_precheck_only.py --target-pass 10
```

2) Regrade retries:
```bash
python3 scripts/regrade_b_queue.py
```

3) Check readiness:
```bash
python3 scripts/check_submit_readiness.py
```

4) Submit:
```bash
python3 scripts/submit_queued_leads.py
```

5) Re-check queue counts:
```bash
for d in lead_queue/A_ready_submit lead_queue/B_retry_enrichment lead_queue/C_good_account_needs_person lead_queue/D_low_confidence_hold lead_queue/E_reject lead_queue/collected_pass lead_queue/collected_precheck_fail lead_queue/submitted; do printf "%-45s %5s\n" "$d" "$(ls -1 "$d" 2>/dev/null | wc -l)"; done
```

## Optional One-Liner (All Steps)

```bash
python3 scripts/collect_leads_precheck_only.py --target-pass 10 && python3 scripts/regrade_b_queue.py && python3 scripts/check_submit_readiness.py && python3 scripts/submit_queued_leads.py
```