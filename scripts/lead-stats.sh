#!/usr/bin/env bash
# Run lead_stats.py with the project venv (so supabase and deps are available).
# Usage: ./scripts/lead-stats.sh [--days N] [--hotkey SS58]

set -e
cd "$(dirname "$0")/.."

if [ -d "venv312" ]; then
  source venv312/bin/activate
elif [ -d "venv" ]; then
  source venv/bin/activate
else
  echo "No venv312 or venv found. Install deps: pip install supabase" >&2
  exit 1
fi

exec python3 scripts/lead_stats.py "$@"
