#!/usr/bin/env python3
"""
LeadPoet SN71 system check: verify all elements are in place to run the miner.
Run from repo root: python scripts/check-system.py
"""
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
os.chdir(REPO_ROOT)
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

errors = []
warnings = []

def ok(msg):
    print(f"  \033[92m\u2713\033[0m {msg}")

def fail(msg):
    print(f"  \033[91m\u2717\033[0m {msg}")
    errors.append(msg)

def warn(msg):
    print(f"  \033[93m!\033[0m {msg}")
    warnings.append(msg)

print("LeadPoet SN71 system check\n" + "=" * 50)

# 1. Critical files
print("\n1. Critical files")
files_required = [
    "neurons/miner.py",
    "neurons/validator.py",
    "Leadpoet/base/miner.py",
    "Leadpoet/base/neuron.py",
    "Leadpoet/protocol.py",
    "Leadpoet/utils/cloud_db.py",
    "Leadpoet/utils/source_provenance.py",
    "Leadpoet/utils/contributor_terms.py",
    "miner_models/lead_sorcerer_main/main_leads.py",
    "miner_models/lead_sorcerer_main/icp_config.json",
    "miner_models/lead_sorcerer_main/src/orchestrator.py",
    "miner_models/lead_sorcerer_main/src/domain.py",
    "miner_models/lead_sorcerer_main/src/crawl.py",
    "miner_models/intent_model.py",
    "miner_models/lead_precheck.py",
    "run-miner.sh",
    "env.example",
]
for f in files_required:
    if (REPO_ROOT / f).exists():
        ok(f)
    else:
        fail(f"Missing: {f}")

# 2. Run scripts executable
print("\n2. Run scripts")
for script in ["run-miner.sh", "run-miner-with-log.sh"]:
    p = REPO_ROOT / script
    if p.exists():
        if os.access(p, os.X_OK):
            ok(f"{script} executable")
        else:
            warn(f"{script} not executable (chmod +x {script})")
    else:
        if script == "run-miner.sh":
            fail(f"Missing: {script}")
        else:
            warn(f"Optional missing: {script}")
screen_script = REPO_ROOT / "scripts" / "run-miner-screen.sh"
if screen_script.exists() and os.access(screen_script, os.X_OK):
    ok("scripts/run-miner-screen.sh executable")
elif screen_script.exists():
    warn("scripts/run-miner-screen.sh not executable")

# 3. Python import chain
print("\n3. Python imports")
try:
    from Leadpoet.base.neuron import BaseNeuron
    ok("Leadpoet.base.neuron")
except Exception as e:
    fail(f"Leadpoet.base.neuron: {e}")

try:
    from Leadpoet.base.miner import BaseMinerNeuron
    ok("Leadpoet.base.miner")
except Exception as e:
    fail(f"Leadpoet.base.miner: {e}")

try:
    from Leadpoet.protocol import LeadRequest
    ok("Leadpoet.protocol")
except Exception as e:
    fail(f"Leadpoet.protocol: {e}")

try:
    from Leadpoet.utils.cloud_db import (
        gateway_get_presigned_url,
        gateway_upload_lead,
        gateway_verify_submission,
        push_prospects_to_cloud,
        check_linkedin_combo_duplicate,
    )
    ok("Leadpoet.utils.cloud_db (gateway + push/check)")
except Exception as e:
    fail(f"Leadpoet.utils.cloud_db: {e}")

try:
    from miner_models.lead_sorcerer_main.main_leads import get_leads
    ok("miner_models.lead_sorcerer_main.main_leads.get_leads")
except Exception as e:
    fail(f"miner_models.lead_sorcerer_main.main_leads: {e}")

try:
    from miner_models.intent_model import rank_leads, classify_industry, classify_roles
    ok("miner_models.intent_model")
except Exception as e:
    fail(f"miner_models.intent_model: {e}")

try:
    from neurons import miner
    assert hasattr(miner, "main") and hasattr(miner, "Miner")
    ok("neurons.miner (main, Miner)")
except Exception as e:
    fail(f"neurons.miner: {e}")

# 4. Environment
print("\n4. Environment")
if (REPO_ROOT / ".env").exists():
    ok(".env exists")
    # Check for unfilled env.example template values (avoid matching literal wallet name YOUR_COLDKEY_NAME)
    env_path = REPO_ROOT / ".env"
    content = env_path.read_text()
    template_markers = (
        "your_supabase_anon_key_here",
        "your_truelist_api_key_here",
        "your_scrapingdog_api_key_here",
        "your_openrouter_key_here",
        "your_google_api_key_here",
        "your_search_engine_id_here",
        "your_firecrawl_key_here",
    )
    if any(m in content for m in template_markers):
        warn(".env may still contain env.example placeholders (replace template API key lines)")
else:
    warn(".env missing — copy env.example to .env and set GSE_API_KEY, GSE_CX, OPENROUTER_KEY, FIRECRAWL_KEY")

# Wallet (for run-miner.sh) — read .env if vars not exported
def _wallet_from_dotenv():
    path = REPO_ROOT / ".env"
    if not path.exists():
        return "", ""
    wn, wh = "", ""
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k == "WALLET_NAME":
            wn = v
        elif k == "WALLET_HOTKEY":
            wh = v
    return wn, wh


ewn, ewh = _wallet_from_dotenv()
wallet_name = (os.environ.get("WALLET_NAME") or ewn).strip()
wallet_hotkey = (os.environ.get("WALLET_HOTKEY") or ewh).strip()


def _wallet_configured(name: str, hotkey: str) -> bool:
    if not name or not hotkey:
        return False
    if "YOUR_" in hotkey:
        return False
    if name == "YOUR_COLDKEY_NAME":
        return True
    return "YOUR_" not in name


if _wallet_configured(wallet_name, wallet_hotkey):
    ok("WALLET_NAME and WALLET_HOTKEY set (coldkey + hotkey)")
else:
    warn(
        "Set WALLET_NAME and WALLET_HOTKEY before running "
        "(e.g. export WALLET_NAME=YOUR_COLDKEY_NAME WALLET_HOTKEY=culture)"
    )

# 5. Venv
print("\n5. Virtual environment")
venv_dir = REPO_ROOT / "venv312" if (REPO_ROOT / "venv312").exists() else REPO_ROOT / "venv"
if venv_dir.exists():
    ok(f"{venv_dir.name} found")
else:
    warn("No venv312 or venv — create with: python3 -m venv venv312 && source venv312/bin/activate && pip install -e .")

# 6. Optional
print("\n6. Optional")
if (REPO_ROOT / "scripts" / "lead_stats.py").exists():
    ok("scripts/lead_stats.py (lead stats)")
else:
    warn("scripts/lead_stats.py not found")

print("\n" + "=" * 50)
if errors:
    print(f"\n\033[91mFAILED: {len(errors)} error(s)\033[0m")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)
if warnings:
    print(f"\n\033[93mWarnings: {len(warnings)}\033[0m")
    for w in warnings:
        print(f"  - {w}")
print("\n\033[92mSystem check passed. You can run: ./run-miner.sh (with WALLET_NAME/WALLET_HOTKEY set)\033[0m")
sys.exit(0)
