#!/usr/bin/env python3
"""
Miner Status Dashboard — check your miner's essential stats.

Usage:
    python scripts/miner_status.py --hotkey veil
    python scripts/miner_status.py --hotkey veil --days 30
    python scripts/miner_status.py --hotkey veil --hours 6
    python scripts/miner_status.py --hotkey 5CAhh...Z
"""

import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = "https://qplwoislplkcegvdmbim.supabase.co"
SUPABASE_ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFwbHdvaXNscGxrY2VndmRtYmltIiwi"
    "cm9sZSI6ImFub24iLCJpYXQiOjE3NDQ4NDcwMDUsImV4cCI6MjA2MDQyMzAwNX0."
    "5E0WjAthYDXaCWY6qjzXm2k20EhadWfigak9hleKZk8"
)
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://52.91.135.79:8000")


def _headers():
    return {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
    }


def query_transparency_log(hotkey: str, event_type: str = None, hours: int = 24, limit: int = 1000) -> List[Dict]:
    import requests
    url = f"{SUPABASE_URL}/rest/v1/transparency_log"
    params = {
        "select": "*",
        "actor_hotkey": f"eq.{hotkey}",
        "order": "ts.desc",
        "limit": str(limit),
    }
    if event_type:
        params["event_type"] = f"eq.{event_type}"
    if hours > 0:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        params["ts"] = f"gte.{since}"

    try:
        resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"  (Query failed: {e})")
    return []


def query_all_submissions_for_hotkey(hotkey: str, limit: int = 1000) -> List[Dict]:
    """Query ALL submissions ever (no time filter) for a hotkey."""
    return query_transparency_log(hotkey, "SUBMISSION", hours=0, limit=limit)


def query_all_consensus_for_hotkey(hotkey: str, limit: int = 1000) -> List[Dict]:
    """Query ALL consensus results ever (no time filter) for a hotkey."""
    return query_transparency_log(hotkey, "CONSENSUS_RESULT", hours=0, limit=limit)


def query_rate_limits(hotkey: str) -> Optional[Dict]:
    import requests
    url = f"{SUPABASE_URL}/rest/v1/rate_limits"
    params = {
        "select": "*",
        "miner_hotkey": f"eq.{hotkey}",
        "limit": "1",
    }
    try:
        resp = requests.get(url, headers=_headers(), params=params, timeout=15)
        if resp.status_code == 200 and resp.json():
            return resp.json()[0]
    except Exception:
        pass
    return None


def get_metagraph_info(hotkey: str) -> Optional[Dict]:
    try:
        import bittensor as bt
        sub = bt.subtensor(network="finney")
        meta = sub.metagraph(71)
        if hotkey in meta.hotkeys:
            idx = meta.hotkeys.index(hotkey)
            info = {"uid": idx}
            for attr, key in [("S", "stake"), ("T", "trust"), ("I", "incentive"), ("E", "emission")]:
                try:
                    info[key] = float(getattr(meta, attr)[idx])
                except Exception:
                    info[key] = 0.0
            try:
                info["is_active"] = bool(meta.active[idx])
            except Exception:
                info["is_active"] = True
            return info
        else:
            return {"uid": "NOT REGISTERED", "stake": 0, "trust": 0, "incentive": 0, "emission": 0, "is_active": False}
    except Exception as e:
        print(f"  (Could not fetch metagraph: {e})")
    return None


def resolve_hotkey_from_wallet(wallet_name: str, hotkey_name: str) -> Optional[str]:
    try:
        from bittensor_wallet import Wallet
        w = Wallet(name=wallet_name, hotkey=hotkey_name)
        return w.hotkey.ss58_address
    except Exception:
        pass
    try:
        import bittensor as bt
        w = bt.wallet(name=wallet_name, hotkey=hotkey_name)
        return w.hotkey.ss58_address
    except Exception:
        pass
    return None


def parse_miner_log(log_path: str) -> Dict[str, Any]:
    """Parse miner.log for lead sourcing, duplicates, submissions, errors."""
    stats = {
        "sorcerer_produced": 0,
        "sourced_total": 0,
        "sourced_leads": [],
        "duplicate_email": 0,
        "duplicate_linkedin": 0,
        "duplicate_details": [],
        "all_duplicates_cycles": 0,
        "submitted_ok": 0,
        "presign_fail": 0,
        "gateway_fail": 0,
        "errors": [],
        "last_lines": [],
    }
    if not os.path.exists(log_path):
        return stats

    try:
        with open(log_path, "r", errors="replace") as f:
            lines = f.readlines()
    except Exception:
        return stats

    re_produced = re.compile(r"Lead Sorcerer produced (\\d+) valid leads")
    re_sourced = re.compile(r"Sourced (\\d+) new leads")
    re_sourced_item = re.compile(r"^\\s+\\d+\\.\\s+(.+)$")
    re_dup_email = re.compile(r"Skipping duplicate email:\\s*(.+)")
    re_dup_linkedin = re.compile(r"Skipping duplicate person\\+company")
    re_dup_detected = re.compile(r"DUPLICATE DETECTED:\\s*(.+)")
    re_dup_time = re.compile(r"Submission time:\\s*(.+)")
    re_dup_miner = re.compile(r"Original miner:\\s*(.+)")
    re_all_dups = re.compile(r"All \\d+ lead\\(s\\) were duplicates")

    current_dup = {}
    for line in lines:
        m = re_produced.search(line)
        if m:
            stats["sorcerer_produced"] += int(m.group(1))
            continue
        m = re_sourced.search(line)
        if m:
            stats["sourced_total"] += int(m.group(1))
            continue
        m = re_sourced_item.match(line)
        if m:
            stats["sourced_leads"].append(m.group(1).strip())
            continue
        m = re_dup_detected.search(line)
        if m:
            current_dup = {"reason": m.group(1).strip()}
            continue
        m = re_dup_time.search(line)
        if m:
            current_dup["submitted_at"] = m.group(1).strip()
            continue
        m = re_dup_miner.search(line)
        if m:
            current_dup["original_miner"] = m.group(1).strip()
            continue
        m = re_dup_email.search(line)
        if m:
            stats["duplicate_email"] += 1
            current_dup["lead"] = m.group(1).strip()
            if current_dup:
                stats["duplicate_details"].append(current_dup.copy())
            current_dup = {}
            continue
        if re_dup_linkedin.search(line):
            stats["duplicate_linkedin"] += 1
            continue
        if re_all_dups.search(line):
            stats["all_duplicates_cycles"] += 1
            continue
        if "Verified:" in line or "Lead uploaded to S3" in line:
            stats["submitted_ok"] += 1
        elif "Failed to get presigned URL" in line:
            stats["presign_fail"] += 1
        elif "Gateway submission exception" in line:
            stats["gateway_fail"] += 1

    stats["last_lines"] = [l.strip() for l in lines[-15:] if l.strip()]
    return stats


def print_header(title: str):
    print(f"\\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def parse_args():
    """Custom arg parser that ignores bittensor's --logging.* args."""
    hotkey = None
    days = None
    hours_arg = None
    logfile = None
    wallet_name = "YOUR_COLDKEY_NAME"

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        a = args[i]
        if a.startswith("--logging"):
            i += 1
            if i < len(args) and not args[i].startswith("--"):
                i += 1
            continue
        if a in ("--config", "--strict", "--no_version_checking"):
            i += 1
            if a == "--config" and i < len(args) and not args[i].startswith("--"):
                i += 1
            continue

        if a == "--hotkey" and i + 1 < len(args):
            hotkey = args[i + 1]; i += 2; continue
        if a == "--days" and i + 1 < len(args):
            days = int(args[i + 1]); i += 2; continue
        if a == "--hours" and i + 1 < len(args):
            hours_arg = int(args[i + 1]); i += 2; continue
        if a == "--logfile" and i + 1 < len(args):
            logfile = args[i + 1]; i += 2; continue
        if a == "--wallet" and i + 1 < len(args):
            wallet_name = args[i + 1]; i += 2; continue
        if a in ("-h", "--help"):
            print("Miner Status Dashboard")
            print()
            print("  --hotkey NAME   hotkey name (e.g. veil) or SS58 address")
            print("  --days N        look back N days (default: 7, use 0 for all time)")
            print("  --hours N       look back N hours (overrides --days)")
            print("  --wallet NAME   coldkey wallet name (default: YOUR_COLDKEY_NAME)")
            print("  --logfile PATH  path to miner.log for duplicate/error analysis")
            print()
            print("Examples:")
            print("  python scripts/miner_status.py --hotkey veil")
            print("  python scripts/miner_status.py --hotkey veil --days 30")
            print("  python scripts/miner_status.py --hotkey veil --hours 6")
            print("  python scripts/miner_status.py --hotkey 5CAhh...Z --days 0")
            sys.exit(0)
        i += 1

    if not hotkey:
        print("Error: --hotkey is required")
        print("  python scripts/miner_status.py --hotkey veil")
        sys.exit(1)

    return hotkey, days, hours_arg, logfile, wallet_name


def main():
    hotkey_arg, days, hours_arg, log_path, wallet_name = parse_args()

    if hotkey_arg.startswith("5") and len(hotkey_arg) > 40:
        hotkey = hotkey_arg
    else:
        hotkey = resolve_hotkey_from_wallet(wallet_name, hotkey_arg)
        if not hotkey:
            print(f"Could not load wallet {wallet_name}/{hotkey_arg}")
            print(f"Try: --hotkey <SS58_ADDRESS> or --wallet <COLDKEY> --hotkey <HOTKEY>")
            sys.exit(1)

    if hours_arg is not None:
        hours = hours_arg
        all_time = False
        time_label = f"last {hours}h"
    elif days is not None:
        all_time = (days == 0)
        hours = days * 24
        time_label = "ALL TIME" if all_time else f"last {days}d"
    else:
        days = 7
        hours = 168
        all_time = False
        time_label = "last 7d"

    print_header("LEADPOET SN71 MINER STATUS")
    print(f"  Hotkey:     {hotkey_arg} -> {hotkey}")
    print(f"  Time range: {time_label}")
    print(f"  Checked at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")

    # --- 1. Metagraph info ---
    print_header("NETWORK STATUS")
    meta = get_metagraph_info(hotkey)
    if meta:
        uid = meta['uid']
        print(f"  UID:        {uid}")
        if uid == "NOT REGISTERED":
            print("  ** This hotkey is NOT registered on SN71 **")
        else:
            print(f"  Stake:      {meta['stake']:.4f} TAO")
            print(f"  Trust:      {meta['trust']:.4f}")
            print(f"  Incentive:  {meta['incentive']:.6f}")
            print(f"  Emission:   {meta['emission']:.6f}")
            print(f"  Active:     {meta['is_active']}")
    else:
        print("  (Could not fetch metagraph info)")

    # --- 2. Rate limits ---
    print_header("RATE LIMITS (today)")
    rl = query_rate_limits(hotkey)
    if rl:
        subs = rl.get("submissions", 0)
        rejs = rl.get("rejections", 0)
        reset = rl.get("reset_at", "midnight UTC")
        print(f"  Submissions:  {subs} / 1000")
        print(f"  Rejections:   {rejs} / 250")
        print(f"  Reset at:     {reset}")
        if subs > 0:
            rej_rate = (rejs / subs * 100) if subs else 0
            print(f"  Reject rate:  {rej_rate:.1f}%")
    else:
        print("  No rate limit data found (no submissions yet today?)")

    # --- 3. Submissions from transparency log ---
    use_hours = 0 if all_time else hours
    label = time_label
    print_header(f"SUBMISSIONS ({label})")

    submissions = query_transparency_log(hotkey, "SUBMISSION", use_hours)
    sub_requests = query_transparency_log(hotkey, "SUBMISSION_REQUEST", use_hours)
    consensus = query_transparency_log(hotkey, "CONSENSUS_RESULT", use_hours)

    approved = [c for c in consensus if c.get("payload", {}).get("final_decision") == "approve"]
    denied = [c for c in consensus if c.get("payload", {}).get("final_decision") == "deny"]
    pending = len(submissions) - len(consensus)

    print(f"  Presign requests:   {len(sub_requests)}")
    print(f"  Submitted (S3):     {len(submissions)}")
    print(f"  Consensus results:  {len(consensus)}")
    print(f"    Approved:         {len(approved)}")
    print(f"    Rejected:         {len(denied)}")
    print(f"    Pending:          {max(pending, 0)}")

    if len(submissions) > 0:
        approval_rate = len(approved) / len(consensus) * 100 if consensus else 0
        print(f"  Approval rate:      {approval_rate:.1f}%")

    # Date range of submissions
    if submissions:
        first_ts = submissions[-1].get("ts", "?")[:10]
        last_ts = submissions[0].get("ts", "?")[:10]
        print(f"  Date range:         {first_ts} to {last_ts}")

    # --- 4. Rejection reasons ---
    if denied:
        print_header("REJECTION REASONS")
        reasons: Dict[str, int] = {}
        for d in denied:
            payload = d.get("payload", {})
            reason = payload.get("primary_rejection_reason", "unknown")
            reasons[reason] = reasons.get(reason, 0) + 1

        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            bar = "#" * min(count, 40)
            print(f"  {count:>3}x  {reason}")
            print(f"       {bar}")

    # --- 5. Recent leads ---
    print_header("RECENT LEADS (last 10)")
    recent = submissions[:10]
    if recent:
        for i, s in enumerate(recent, 1):
            payload = s.get("payload", {})
            ts = s.get("ts", "?")
            lead_id = payload.get("lead_id", "?")
            email_hash = payload.get("email_hash", "?")[:16]
            print(f"  {i}. [{ts[:19]}] lead={lead_id[:12]}... email_hash={email_hash}...")
    else:
        print("  (none)")

    # --- 6. Storage proofs ---
    storage = query_transparency_log(hotkey, "STORAGE_PROOF", use_hours)
    if storage:
        matched = sum(1 for s in storage if s.get("payload", {}).get("hash_match") == True)
        print_header("STORAGE PROOFS")
        print(f"  Total:    {len(storage)}")
        print(f"  Matched:  {matched}")
        print(f"  Failed:   {len(storage) - matched}")

    # --- 7. Miner log analysis (duplicates, errors) ---
    if not log_path:
        for candidate in ["miner.log", "../miner.log"]:
            if os.path.exists(candidate):
                log_path = candidate
                break

    if log_path and os.path.exists(log_path):
        log_stats = parse_miner_log(log_path)

        print_header(f"LEAD SOURCING ({log_path})")
        print(f"  Sorcerer produced:      {log_stats['sorcerer_produced']} valid leads")
        print(f"  Sourced total:          {log_stats['sourced_total']} leads")
        print(f"  Uploaded to S3 (OK):    {log_stats['submitted_ok']}")

        total_dups = log_stats['duplicate_email'] + log_stats['duplicate_linkedin']
        print(f"  Duplicate skips:        {total_dups}")
        print(f"    Email duplicates:     {log_stats['duplicate_email']}")
        print(f"    LinkedIn duplicates:  {log_stats['duplicate_linkedin']}")
        print(f"  All-duplicate cycles:   {log_stats['all_duplicates_cycles']}")
        print(f"  Presign failures:       {log_stats['presign_fail']}")
        print(f"  Gateway errors:         {log_stats['gateway_fail']}")

        if log_stats['sourced_total'] > 0:
            ok_rate = log_stats['submitted_ok'] / log_stats['sourced_total'] * 100
            dup_rate = total_dups / log_stats['sourced_total'] * 100
            print(f"\\n  OK rate:       {ok_rate:.1f}%")
            print(f"  Duplicate rate: {dup_rate:.1f}%")

        if log_stats['duplicate_details']:
            print_header("DUPLICATE DETAILS (why skipped)")
            shown = log_stats['duplicate_details'][-10:]
            for i, d in enumerate(shown, 1):
                lead = d.get("lead", "?")
                reason = d.get("reason", "?")
                ts = d.get("submitted_at", "?")
                miner = d.get("original_miner", "?")
                print(f"  {i}. {lead}")
                print(f"     Reason: {reason}")
                print(f"     Originally submitted: {ts}")
                print(f"     By miner: {miner}")

        if log_stats['sourced_leads']:
            print_header("SOURCED LEADS (recent)")
            for lead in log_stats['sourced_leads'][-10:]:
                print(f"  - {lead}")

        if log_stats['last_lines']:
            print_header("LAST LOG LINES")
            for line in log_stats['last_lines']:
                print(f"  {line[:120]}")
    else:
        print_header("MINER LOG")
        print("  No miner.log found. Run miner with logging:")
        print("    python neurons/miner.py ... 2>&1 | tee -a miner.log")
        print("  Then: python scripts/miner_status.py --hotkey veil --logfile miner.log")

    # --- Summary ---
    print_header("SUMMARY")
    has_log = log_path and os.path.exists(log_path)
    if len(submissions) == 0 and not has_log:
        print(f"  No submissions in {time_label}.")
        print("  Is your miner running? Check: pgrep -f neurons/miner.py")
        print("  Try --days 0 for full history, or --logfile miner.log")
    elif len(submissions) == 0 and has_log:
        print(f"  No submissions reached gateway yet.")
        if log_stats.get("all_duplicates_cycles", 0) > 0:
            print(f"  All sourced leads were duplicates ({log_stats['duplicate_email']} email dups).")
            print("  The miner is working but all leads it finds were already submitted.")
        else:
            print("  Miner may still be sourcing. Check log sections above.")
    elif len(denied) == 0 and len(approved) > 0:
        print(f"  Looking good! {len(approved)} approved, 0 rejected.")
    elif len(approved) > 0:
        rate = len(approved) / (len(approved) + len(denied)) * 100
        print(f"  {len(approved)} approved, {len(denied)} rejected ({rate:.0f}% approval)")
        if rate < 70:
            print("  TIP: High rejection rate. Check rejection reasons above.")
    else:
        print(f"  {len(submissions)} submitted, waiting for consensus...")

    print(f"\\n{'='*70}\\n")


if __name__ == "__main__":
    main()

