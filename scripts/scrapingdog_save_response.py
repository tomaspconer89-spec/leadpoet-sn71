#!/usr/bin/env python3
"""
Call ScrapingDog with a URL (same patterns as the miner) and save raw responses to JSON.

Loads SCRAPINGDOG_API_KEY from the repo root ``.env`` (via python-dotenv), then the
process environment. Copy ``env.example`` to ``.env`` and set the key there (see env.example).

Examples:
  python3 scripts/scrapingdog_save_response.py "https://www.linkedin.com/in/jim-bennett-nowcfo"
  python3 scripts/scrapingdog_save_response.py --url "..." --no-scrape
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

_REPO = Path(__file__).resolve().parent.parent
load_dotenv(_REPO / ".env")


def _google(key: str, query: str, *, results: int = 10) -> dict:
    url = (
        "https://api.scrapingdog.com/google"
        f"?api_key={urllib.parse.quote(key)}"
        f"&query={urllib.parse.quote(query)}"
        f"&results={results}&country=us&page=0"
    )
    req = urllib.request.Request(url=url, method="GET")
    with urllib.request.urlopen(req, timeout=45) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return {"_parse_error": True, "_raw_body_preview": body[:8000]}
    return data if isinstance(data, dict) else {"_unexpected_type": str(type(data)), "value": data}


def _scrape(key: str, page_url: str) -> tuple[dict, bytes]:
    q = urllib.parse.urlencode(
        {"api_key": key, "url": page_url, "dynamic": "false"}
    )
    full = f"https://api.scrapingdog.com/scrape?{q}"
    req = urllib.request.Request(url=full, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            text = raw.decode("utf-8", errors="replace")
            meta = {
                "http_ok": True,
                "status_code": getattr(resp, "status", 200),
                "body_length": len(text),
                "body_preview": text[:12000],
            }
            return meta, raw
    except urllib.error.HTTPError as e:
        raw = e.read() if e.fp else b""
        body = raw.decode("utf-8", errors="replace")
        meta = {
            "http_ok": False,
            "status_code": e.code,
            "error": str(e),
            "body_length": len(body),
            "body_preview": body[:8000],
        }
        return meta, raw
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return (
            {
                "http_ok": False,
                "status_code": None,
                "error": str(e),
                "body_length": 0,
                "body_preview": "",
            },
            b"",
        )


def _slug_from_url(u: str) -> str:
    s = u.rstrip("/").split("/")[-1] or "page"
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s)[:80]
    return s or "page"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("url", nargs="?", default="", help="Target URL (e.g. LinkedIn profile)")
    p.add_argument("--url", dest="url_opt", default="", help="Same as positional url")
    p.add_argument("--output-dir", default="reports", help="Relative to repo root")
    p.add_argument("--no-scrape", action="store_true", help="Only run Google API calls")
    args = p.parse_args()
    page_url = (args.url or args.url_opt or "").strip()
    if not page_url:
        print("Provide a URL as first argument or --url", file=sys.stderr)
        return 2

    key = os.getenv("SCRAPINGDOG_API_KEY", "").strip()
    if not key:
        env_path = _REPO / ".env"
        hint = (
            f"Set SCRAPINGDOG_API_KEY in {env_path} (or export it). "
            "See env.example."
        )
        if not env_path.is_file():
            hint = (
                f"No {env_path} — copy env.example to .env and set "
                "SCRAPINGDOG_API_KEY, or export the variable."
            )
        print(f"SCRAPINGDOG_API_KEY is not set. {hint}", file=sys.stderr)
        return 1

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = _slug_from_url(page_url)
    out_dir = _REPO / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / f"scrapingdog_{slug}_{ts}.json"
    out_html = out_dir / f"scrapingdog_{slug}_{ts}_scrape_body.html"

    payload: dict = {
        "saved_at_utc": ts,
        "input_url": page_url,
        "endpoints": {
            "google_quoted_url": {
                "query": f'"{page_url}"',
                "response": _google(key, f'"{page_url}"'),
            },
            "google_quoted_url_plus_location": {
                "query": f'"{page_url}" location',
                "response": _google(key, f'"{page_url}" location'),
            },
        },
    }

    if not args.no_scrape:
        scrape_meta, raw_bytes = _scrape(key, page_url)
        payload["endpoints"]["scrape"] = {"url": page_url, "result": scrape_meta}
        if raw_bytes:
            try:
                out_html.write_bytes(raw_bytes)
                payload["endpoints"]["scrape"]["full_body_saved_to"] = str(
                    out_html.relative_to(_REPO)
                )
            except OSError as e:
                payload["endpoints"]["scrape"]["full_body_save_error"] = str(e)

    out_json.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    print(out_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
