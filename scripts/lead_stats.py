#!/usr/bin/env python3
"""
LeadPoet Miner Lead Stats

Query the public Supabase transparency_log to show:
  - attempted: SUBMISSION_REQUEST (leads you sent to the gateway)
  - gateway_rejected: VALIDATION_FAILED (gateway said no, with reason)
  - submitted: SUBMISSION (accepted into queue)
  - accepted / rejected: CONSENSUS_RESULT (validator final decision)
  - pending: submitted - accepted - rejected

Event flow:
  1. SUBMISSION_REQUEST = miner sent a lead to the gateway (attempted).
  2. Gateway validates → if it fails: VALIDATION_FAILED (gateway rejected, reason in payload).
  3. If it passes → SUBMISSION = lead accepted into queue (submitted).
  4. Later, validators decide → CONSENSUS_RESULT (accepted / rejected).

Usage:
  ./scripts/lead-stats.sh                    # your miner, last 7 days (uses MINER_HOTKEY or wallet)
  ./scripts/lead-stats.sh --hours 24         # last 24 hours
  ./scripts/lead-stats.sh --days 1           # last 1 day
  ./scripts/lead-stats.sh --hotkey 5Gn3...   # specific miner hotkey

  Set MINER_HOTKEY=your_ss58 to filter to your miner, or use --hotkey.
  Set WALLET_NAME and WALLET_HOTKEY (as for the miner) to auto-detect hotkey.

Data source: public Supabase transparency_log (read-only anon key).
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone, timedelta

# Public read-only credentials (same as cloud_db.py)
SUPABASE_URL = "https://qplwoislplkcegvdmbim.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFwbHdvaXNscGxrY2VndmRtYmltIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ4NDcwMDUsImV4cCI6MjA2MDQyMzAwNX0.5E0WjAthYDXaCWY6qjzXm2k20EhadWfigak9hleKZk8"


def get_supabase():
    try:
        from supabase import create_client
        return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    except ImportError:
        print("supabase not installed. Either:", file=sys.stderr)
        print("  Run with project venv:  ./scripts/lead-stats.sh", file=sys.stderr)
        print("  Or: source venv312/bin/activate && python3 scripts/lead_stats.py", file=sys.stderr)
        print("  Or: pip install supabase", file=sys.stderr)
        sys.exit(1)


# Tail query: most recent rows. Small limit to reduce chance of DB statement timeout.
# Supabase may have a short timeout; if all limits timeout, the table may need an index on created_at.
TAIL_LIMIT = 1000


def fetch_recent_tail(supabase, limit: int = TAIL_LIMIT):
    """Fetch most recent rows. Tries smaller limits on timeout."""
    for try_limit in [limit, 200, 50]:
        try:
            q = supabase.table("transparency_log").select("id, event_type, created_at, actor_hotkey, payload").order("created_at", desc=True).limit(try_limit)
            r = q.execute()
            return r.data or []
        except Exception as e:
            err = str(e)
            if "57014" in err or "timeout" in err.lower():
                continue
            print(f"Warning: fetch failed: {e}", file=sys.stderr)
            return []
    print("Database queries timed out. The transparency_log table may be very large.", file=sys.stderr)
    print("Try again later or use --days 1. Subnet maintainers may need to add an index on created_at.", file=sys.stderr)
    return []


def filter_by_window(rows, since_iso: str, actor_hotkey: str = None):
    """Filter rows to created_at >= since_iso and optionally actor_hotkey."""
    out = []
    for row in rows:
        if row.get("created_at", "") < since_iso:
            continue
        if actor_hotkey and row.get("actor_hotkey") != actor_hotkey:
            continue
        out.append(row)
    return out


def parse_payload(payload):
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {}
    return {}


def extract_validation_reason(payload: dict) -> str:
    """Get human-readable reason from VALIDATION_FAILED payload."""
    p = payload or {}
    reason = p.get("reason") or p.get("message") or ""
    if isinstance(reason, dict):
        reason = reason.get("message") or reason.get("reason") or str(reason)
    missing = p.get("missing_required_fields") or p.get("missing")
    if missing:
        reason = f"{reason} missing={missing}" if reason else f"missing_required_fields: {missing}"
    return reason or "unknown"


def extract_consensus_decision(payload: dict) -> str:
    p = payload or {}
    return p.get("final_decision") or p.get("decision") or "unknown"


def get_my_hotkey():
    """Resolve miner hotkey from env: MINER_HOTKEY, or load from wallet (WALLET_NAME, WALLET_HOTKEY)."""
    hotkey = (os.environ.get("MINER_HOTKEY") or "").strip()
    if hotkey:
        return hotkey
    wallet_name = (os.environ.get("WALLET_NAME") or "").strip()
    wallet_hotkey = (os.environ.get("WALLET_HOTKEY") or "").strip()
    if not wallet_name or not wallet_hotkey:
        return None
    try:
        import bittensor as bt
        wallet_path = os.path.expanduser(os.environ.get("WALLET_PATH", "~/.bittensor/wallets"))
        config = bt.Config()
        config.wallet = bt.Config()
        config.wallet.name = wallet_name
        config.wallet.hotkey = wallet_hotkey
        config.wallet.path = wallet_path
        wallet = bt.wallet(config=config)
        return wallet.hotkey.ss58_address
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(description="LeadPoet lead stats — your miner, by days or hours")
    ap.add_argument("--days", type=int, default=None, help="Look back N days (default 7 if --hours not set)")
    ap.add_argument("--hours", type=float, default=None, help="Look back N hours (overrides --days)")
    ap.add_argument("--hotkey", type=str, default=None, help="Miner hotkey SS58 (default: MINER_HOTKEY or wallet)")
    ap.add_argument("--all", action="store_true", help="Show all miners (do not filter by hotkey)")
    args = ap.parse_args()

    # Time window: --hours wins, else --days, else 7 days
    if args.hours is not None:
        delta = timedelta(hours=args.hours)
        window_label = f"last {args.hours} hours"
    else:
        days = args.days if args.days is not None else 7
        delta = timedelta(days=days)
        window_label = f"last {days} day(s)"

    since = datetime.now(timezone.utc) - delta
    since_iso = since.isoformat()

    supabase = get_supabase()
    hotkey = None if args.all else (args.hotkey or get_my_hotkey() or "").strip() or None

    # Single fast query: most recent TAIL_LIMIT rows (no date filter = no full scan = no timeout)
    all_recent = fetch_recent_tail(supabase, limit=TAIL_LIMIT)
    in_window = filter_by_window(all_recent, since_iso, hotkey)

    # Count by event_type (all in Python)
    attempted = sum(1 for r in in_window if r.get("event_type") == "SUBMISSION_REQUEST")
    gateway_rejected = sum(1 for r in in_window if r.get("event_type") == "VALIDATION_FAILED")
    submitted = sum(1 for r in in_window if r.get("event_type") == "SUBMISSION")
    accepted = sum(1 for r in in_window if r.get("event_type") == "CONSENSUS_RESULT" and extract_consensus_decision(parse_payload(r.get("payload"))) == "approve")
    rejected = sum(1 for r in in_window if r.get("event_type") == "CONSENSUS_RESULT" and extract_consensus_decision(parse_payload(r.get("payload"))) == "deny")
    pending = max(0, submitted - accepted - rejected)

    # Recent VALIDATION_FAILED for reasons (from same in_window)
    vf_events = [r for r in in_window if r.get("event_type") == "VALIDATION_FAILED"][:20]
    rl_events = [r for r in in_window if r.get("event_type") == "RATE_LIMIT"][:10]

    print("LeadPoet Lead Stats (transparency_log)")
    print("=" * 56)
    if hotkey:
        print(f"  Miner: YOUR MINER  ({hotkey[:16]}...{hotkey[-8:] if len(hotkey) > 28 else hotkey})")
    else:
        print("  Miner: all miners (use --hotkey or set MINER_HOTKEY for your miner only)")
    print(f"  Window: {window_label}  (since {since_iso[:19].replace('T', ' ')})")
    print(f"  (from last {TAIL_LIMIT} events — counts may be partial if very active)")
    print()
    print("  attempted        :", attempted, "  (SUBMISSION_REQUEST = leads you sent)")
    print("  gateway_rejected :", gateway_rejected, "  (VALIDATION_FAILED = failed gateway checks)")
    print("  submitted        :", submitted, "  (SUBMISSION = accepted into queue)")
    print("  accepted         :", accepted, "  (CONSENSUS_RESULT approve)")
    print("  rejected         :", rejected, "  (CONSENSUS_RESULT deny)")
    print("  pending          :", pending, "  (submitted - accepted - rejected)")
    print()

    if vf_events:
        print("Recent gateway rejections (VALIDATION_FAILED) with reasons:")
        print("-" * 56)
        for e in vf_events[:15]:
            ts = e.get("created_at", "")[:19].replace("T", " ")
            payload = parse_payload(e.get("payload"))
            reason = extract_validation_reason(payload)
            print(f"  {ts}  {reason}")
        print()

    if rl_events:
        print("Recent RATE_LIMIT events:")
        print("-" * 56)
        for e in rl_events[:5]:
            ts = e.get("created_at", "")[:19].replace("T", " ")
            payload = parse_payload(e.get("payload"))
            msg = payload.get("message") or payload.get("reason") or str(payload)[:80]
            print(f"  {ts}  {msg}")
        print()


if __name__ == "__main__":
    main()
