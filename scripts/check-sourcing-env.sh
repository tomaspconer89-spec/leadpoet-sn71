#!/usr/bin/env bash
# Check that .env has real API keys for miner sourcing (Lead Sorcerer).
# Run from repo root: ./scripts/check-sourcing-env.sh

set -e
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo "❌ No .env file. Copy env.example to .env and add your keys:"
  echo "   cp env.example .env"
  echo "   Then edit .env and set: GSE_API_KEY, GSE_CX, OPENROUTER_KEY, FIRECRAWL_KEY"
  exit 1
fi

# Read each required var from .env (handles comments and empty lines)
get_val() { grep -E "^${1}=" .env 2>/dev/null | head -1 | cut -d= -f2- | sed 's/^["'\'']//;s/["'\'']$//' | tr -d '\r'; }

REQUIRED="GSE_API_KEY GSE_CX OPENROUTER_KEY FIRECRAWL_KEY"
MISSING=""
PLACEHOLDER=""

for var in $REQUIRED; do
  val="$(get_val "$var")"
  if [ -z "$val" ]; then
    MISSING="$MISSING $var"
  elif echo "$val" | grep -qE 'your_|_here|example|placeholder|your.*key'; then
    PLACEHOLDER="$PLACEHOLDER $var"
  fi
done

if [ -n "$MISSING" ]; then
  echo "❌ Missing in .env:$MISSING"
  echo "   Add these to .env (see env.example and docs/QUALIFICATION-AND-TERMS.md)."
  exit 1
fi

if [ -n "$PLACEHOLDER" ]; then
  echo "⚠️  Placeholder values in .env (replace with real API keys):$PLACEHOLDER"
  echo "   GSE_API_KEY, GSE_CX → https://programmablesearchengine.google.com/"
  echo "   OPENROUTER_KEY     → https://openrouter.ai/"
  echo "   FIRECRAWL_KEY     → https://firecrawl.dev/"
  exit 1
fi

echo "✅ Sourcing env looks good (GSE_API_KEY, GSE_CX, OPENROUTER_KEY, FIRECRAWL_KEY set)."
echo "   Run: ./run-miner.sh  (or ACCEPT_TERMS=1 ./run-miner.sh)"
