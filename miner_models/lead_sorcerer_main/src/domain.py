"""
Domain tool for Lead Sorcerer.

This tool generates qualified domains from ICP queries using cheap pre-crawl scoring.
Search backends: Serper, Brave, or GSE; results are normalized to a common shape before LLM scoring.

Authoritative specifications: BRD §226-280
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import httpx
from openai import AsyncOpenAI

from .common import (
    AsyncSemaphorePool,
    ErrorCode,
    PermitManager,
    append_audit,
    append_status,
    build_error,
    build_metrics,
    compute_next_revisit,
    generate_lead_id,
    load_costs_config,
    load_schema_checksum,
    load_visited_domains_from_lead_queue_dir,
    normalize_domain,
    now_z,
    round4,
    recompute_total_cost,
    setup_logging,
    validate_provider_config,
    validate_template_placeholders,
    write_with_lock,
    append_jsonl_with_lock,
)

# ============================================================================
# Constants and Configuration
# ============================================================================

INTERNAL_VERSION = "1.0.0"
TOOL_NAME = "domain"

# Default values from BRD
DEFAULT_REVISIT_AFTER_DAYS = 90
DEFAULT_FAILURE_REVISIT_DAYS = 1
DEFAULT_DOMAIN_TTL_DAYS = 180
DEFAULT_DOMAIN_SERP_TTL_HOURS = 24
DEFAULT_MAX_PAGES = 5
DEFAULT_MODE = "thorough"

# High-noise sources that frequently reduce useful ICP yield.
LOW_INTENT_DOMAIN_KEYWORDS = (
    "indeed.",
    "ziprecruiter.",
    "builtin.",
    "glassdoor.",
    "workable.",
    "workday.",
    "greenhouse.",
    "lever.",
    "job",
    "jobs",
    "careers",
    "career",
    "substack.",
    "medium.",
)

LOW_INTENT_TITLE_MARKERS = (
    "job search",
    "jobs",
    "careers",
    "hiring now",
    "salary",
    "resume",
    "cv",
)

# ============================================================================
# Google Search Engine (GSE) Integration
# ============================================================================


class GSESearchClient:
    """Client for Google Programmable Search Engine (GSE)."""

    def __init__(self, api_key: str, cx: str, semaphore_pool: AsyncSemaphorePool):
        self.api_key = api_key
        self.cx = cx
        self.semaphore_pool = semaphore_pool
        self.base_url = "https://www.googleapis.com/customsearch/v1"

    async def search(self, query: str, page: int = 1) -> Dict[str, Any]:
        """
        Perform Google search with pagination.

        Args:
            query: Search query
            page: Page number (1-based)

        Returns:
            Search results dictionary
        """
        async with self.semaphore_pool:
            params = {
                "key": self.api_key,
                "cx": self.cx,
                "q": query,
                "start": (page - 1) * 10 + 1,  # GSE uses 1-based indexing
                "num": 10,
            }

            async with httpx.AsyncClient(timeout=(3.0, 10.0)) as client:
                response = await client.get(self.base_url, params=params)
                response.raise_for_status()
                return response.json()


class SerperSearchClient:
    """Client for Serper.dev Google Search API. Returns results in GSE-compatible format."""

    def __init__(self, api_key: str, semaphore_pool: AsyncSemaphorePool):
        self.api_key = api_key
        self.semaphore_pool = semaphore_pool
        # Serper uses the google subdomain for web search.
        self.base_url = "https://google.serper.dev/search"

    async def search(self, query: str, page: int = 1) -> Dict[str, Any]:
        async with self.semaphore_pool:
            # Serper pagination uses 1-based page numbers.
            payload = {
                "q": query,
                "num": 10,
                "page": page,
            }
            headers = {
                "X-API-KEY": self.api_key,
                "Content-Type": "application/json",
            }

            async with httpx.AsyncClient(timeout=(5.0, 20.0)) as client:
                response = await client.post(
                    self.base_url, json=payload, headers=headers
                )
                response.raise_for_status()
                data = response.json()

            items = []
            for idx, result in enumerate(data.get("organic", []), start=1):
                items.append(
                    {
                        "title": result.get("title", ""),
                        "link": result.get("link", ""),
                        "snippet": result.get("snippet", ""),
                        "rank": idx,
                    }
                )

            return {"items": items}


class BraveSearchClient:
    """Client for Brave Search API. Returns results in GSE-compatible format."""

    def __init__(self, api_key: str, semaphore_pool: AsyncSemaphorePool):
        self.api_key = api_key
        self.semaphore_pool = semaphore_pool
        self.base_url = "https://api.search.brave.com/res/v1/web/search"

    async def search(self, query: str, page: int = 1) -> Dict[str, Any]:
        async with self.semaphore_pool:
            params = {
                "q": query,
                "count": 10,
                "offset": (page - 1) * 10,
            }
            headers = {
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "X-Subscription-Token": self.api_key,
            }

            async with httpx.AsyncClient(timeout=(5.0, 15.0)) as client:
                response = await client.get(
                    self.base_url, params=params, headers=headers
                )
                response.raise_for_status()
                data = response.json()

            items = []
            for idx, result in enumerate(
                data.get("web", {}).get("results", []), start=1
            ):
                items.append({
                    "title": result.get("title", ""),
                    "link": result.get("url", ""),
                    "snippet": result.get("description", ""),
                    "rank": idx,
                })

            return {"items": items}


class ApifySearchClient:
    """Client for Apify Google Search actor. Returns results in GSE-compatible format."""

    def __init__(
        self,
        api_token: str,
        actor_id: str,
        semaphore_pool: AsyncSemaphorePool,
    ):
        self.api_token = api_token
        self.actor_id = actor_id
        self.semaphore_pool = semaphore_pool

    async def search(self, query: str, page: int = 1) -> Dict[str, Any]:
        async with self.semaphore_pool:
            aid = self.actor_id
            if "/" in aid and "~" not in aid:
                aid = aid.replace("/", "~", 1)
            run_url = (
                "https://api.apify.com/v2/acts/"
                f"{aid}/run-sync-get-dataset-items"
                f"?token={self.api_token}"
            )
            payload = {
                "queries": query,
                "resultsPerPage": 10,
                "maxPagesPerQuery": max(1, int(page)),
                "mobileResults": False,
            }
            headers = {"Content-Type": "application/json", "Accept": "application/json"}

            async with httpx.AsyncClient(timeout=(8.0, 45.0)) as client:
                response = await client.post(run_url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()

            rows = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
            items = []
            for idx, row in enumerate(rows, start=1):
                if not isinstance(row, dict):
                    continue
                organic = row.get("organicResults")
                if isinstance(organic, list):
                    for j, r in enumerate(organic, start=1):
                        if not isinstance(r, dict):
                            continue
                        items.append(
                            {
                                "title": r.get("title", ""),
                                "link": r.get("url", ""),
                                "snippet": r.get("description", ""),
                                "rank": j,
                            }
                        )
                    if items:
                        break
                items.append(
                    {
                        "title": row.get("title", ""),
                        "link": row.get("url", ""),
                        "snippet": row.get("description", ""),
                        "rank": idx,
                    }
                )

            return {"items": items[:10]}


# ============================================================================
# LLM Scoring
# ============================================================================


class LLMScorer:
    """LLM-based domain scoring using OpenRouter."""

    def __init__(self, api_key: str, semaphore_pool: AsyncSemaphorePool):
        self.client = AsyncOpenAI(
            api_key=api_key, base_url="https://openrouter.ai/api/v1"
        )
        self.semaphore_pool = semaphore_pool
        self.primary_model = os.environ.get("OPENROUTER_PRIMARY_MODEL", "gpt-4o-mini")
        self.fallback_model = os.environ.get("OPENROUTER_FALLBACK_MODEL", "gpt-3.5-turbo")
        self.max_tokens = int(os.environ.get("OPENROUTER_MAX_TOKENS", "120"))
        self.disable_llm = os.environ.get("OPENROUTER_DISABLE", "0") == "1"

    def _heuristic_fallback(
        self, title: str, snippet: str, query: str, icp_text: str
    ) -> Tuple[float, str, List[str], str]:
        """
        Cheap fallback when OpenRouter credits are exhausted.
        """
        text = f"{title} {snippet}".lower()
        icp_terms = [t for t in re.findall(r"[a-z0-9]{4,}", icp_text.lower()) if len(t) >= 4]
        query_terms = [t for t in re.findall(r"[a-z0-9]{4,}", query.lower()) if len(t) >= 4]
        terms = set(icp_terms[:30] + query_terms[:20])
        if not terms:
            return 0.0, "Heuristic fallback: no terms available", ["scoring_fallback"], "heuristic_no_credit"

        hits = sum(1 for t in terms if t in text)
        ratio = hits / max(1, len(terms))
        score = round(max(0.0, min(1.0, ratio * 2.5)), 4)
        reason = f"Heuristic fallback used (credit-limited). Term hits={hits}/{len(terms)}"
        flags = ["scoring_fallback", "credit_limited"]
        return score, reason, flags, "heuristic_no_credit"

    def _is_timeout_error(self, err: Exception) -> bool:
        txt = f"{type(err).__name__}: {err}".lower()
        return (
            "timeout" in txt
            or "timed out" in txt
            or isinstance(err, asyncio.TimeoutError)
            or isinstance(err, httpx.TimeoutException)
        )

    async def _complete_with_retries(self, model: str, prompt: str):
        retries = max(0, int(os.environ.get("OPENROUTER_TIMEOUT_RETRIES", "1") or 1))
        backoff_s = max(0.0, float(os.environ.get("OPENROUTER_TIMEOUT_BACKOFF_S", "1.0") or 1.0))
        attempts = retries + 1
        last_err: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                return await self.client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    max_tokens=self.max_tokens,
                )
            except Exception as err:
                last_err = err
                if not self._is_timeout_error(err) or attempt >= attempts:
                    raise
                await asyncio.sleep(backoff_s * attempt)
        raise last_err if last_err else RuntimeError("openrouter_completion_failed")

    def _build_scoring_prompt(
        self, title: str, snippet: str, query: str, icp_text: str
    ) -> str:
        """
        Build scoring prompt for LLM.

        Args:
            title: SERP result title
            snippet: SERP result snippet
            query: Search query used
            icp_text: ICP description

        Returns:
            Formatted prompt string
        """
        return f"""You are evaluating whether a company matches an Ideal Customer Profile (ICP).

ICP Description: {icp_text}

Search Query: {query}

Company Information:
Title: {title}
Description: {snippet}

Rate this company from 0.0 to 1.0 based on how well it matches the ICP.
Consider factors like:
- Industry relevance
- Company size and stage
- Geographic location
- Technology focus
- Business model alignment

Return only a JSON object with this exact format:
{{
    "score": <float between 0.0 and 1.0>,
    "reason": "<brief explanation of score>",
    "flags": ["<any disqualifying flags like geo_mismatch, size_mismatch, etc.>"]
}}

Example flags: geo_mismatch, size_mismatch, industry_mismatch, stage_mismatch, technology_mismatch"""

    async def score_domain(
        self, title: str, snippet: str, query: str, icp_text: str
    ) -> Tuple[float, str, List[str], str, str]:
        """
        Score a domain using LLM.

        Args:
            title: SERP result title
            snippet: SERP result snippet
            query: Search query used
            icp_text: ICP description

        Returns:
            Tuple of (score, reason, flags, model_used, prompt_fingerprint)
        """
        async with self.semaphore_pool:
            prompt = self._build_scoring_prompt(title, snippet, query, icp_text)
            prompt_fingerprint = hashlib.sha256(prompt.encode()).hexdigest()

            if self.disable_llm:
                score, reason, flags, model_used = self._heuristic_fallback(
                    title, snippet, query, icp_text
                )
                return score, reason, flags, model_used, prompt_fingerprint

            try:
                # Try primary model first
                response = await self._complete_with_retries(self.primary_model, prompt)
                model_used = self.primary_model
            except Exception as primary_err:
                try:
                    # Fallback to secondary model
                    response = await self._complete_with_retries(self.fallback_model, prompt)
                    model_used = self.fallback_model
                except Exception as fallback_err:
                    error_text = f"{primary_err} | {fallback_err}".lower()
                    if "requires more credits" in error_text or "code: 402" in error_text:
                        score, reason, flags, model_used = self._heuristic_fallback(
                            title, snippet, query, icp_text
                        )
                        return score, reason, flags, model_used, prompt_fingerprint
                    if self._is_timeout_error(primary_err) or self._is_timeout_error(fallback_err):
                        score, reason, flags, model_used = self._heuristic_fallback(
                            title, snippet, query, icp_text
                        )
                        flags = list(flags) + ["llm_timeout_fallback"]
                        reason = (
                            f"{reason} (OpenRouter timeout after retries on primary+fallback)"
                        )
                        return score, reason, flags, model_used, prompt_fingerprint
                    raise

            content = response.choices[0].message.content.strip()

            # Parse JSON response
            try:
                result = json.loads(content)
                score = float(result.get("score", 0.0))
                reason = result.get("reason", "No reason provided")
                flags = result.get("flags", [])

                # Validate score range
                score = max(0.0, min(1.0, score))

                return score, reason, flags, model_used, prompt_fingerprint
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                # Fallback to default values if parsing fails
                logging.warning(f"Failed to parse LLM response: {e}")
                return (
                    0.0,
                    "Failed to parse response",
                    [],
                    model_used,
                    prompt_fingerprint,
                )


# ============================================================================
# Domain History Management
# ============================================================================


class DomainHistoryManager:
    """Manages domain history for cross-run deduplication and TTL enforcement."""

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.history_file = self.data_dir / "domain_history.jsonl"
        self.history_cache: Dict[str, Dict[str, Any]] = {}
        self._load_history()

    def _load_history(self) -> None:
        """Load existing domain history from disk."""
        if not self.history_file.exists():
            return

        try:
            with open(self.history_file, "r") as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line)
                        domain = record.get("domain")
                        if domain:
                            self.history_cache[domain] = record
        except Exception as e:
            logging.warning(f"Failed to load domain history: {e}")

    def _cleanup_expired_entries(self, ttl_days: int) -> None:
        """Remove expired domain history entries."""
        cutoff_time = datetime.utcnow() - timedelta(days=ttl_days)
        expired_domains = []

        for domain, record in self.history_cache.items():
            scored_at = record.get("provenance", {}).get("scored_at")
            if scored_at:
                try:
                    record_time = datetime.fromisoformat(
                        scored_at.replace("Z", "+00:00")
                    )
                    # Make both timezone-aware for comparison
                    cutoff_time_aware = cutoff_time.replace(tzinfo=record_time.tzinfo)
                    if record_time < cutoff_time_aware:
                        expired_domains.append(domain)
                except ValueError:
                    # Invalid timestamp, remove
                    expired_domains.append(domain)

        for domain in expired_domains:
            del self.history_cache[domain]

    def get_domain_record(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Get existing domain record if it exists and is not expired.

        Args:
            domain: Normalized domain

        Returns:
            Domain record or None if not found/expired
        """
        return self.history_cache.get(domain)

    def add_domain_record(self, record: Dict[str, Any]) -> None:
        """
        Add or update domain record in history.

        Args:
            record: Domain record to add/update
        """
        domain = record.get("domain")
        if domain:
            self.history_cache[domain] = record

    def save_history(self, ttl_days: int) -> None:
        """
        Save domain history to disk and cleanup expired entries.

        Args:
            ttl_days: TTL for domain history entries
        """
        self._cleanup_expired_entries(ttl_days)

        # Write updated history in JSONL format
        records = list(self.history_cache.values())
        if records:
            # Remove existing file if present to ensure clean JSONL format

            # Write to a temporary file first for atomic replacement
            temp_file = self.history_file.with_suffix(".tmp")
            append_jsonl_with_lock(str(temp_file), records)
            # Atomically replace the old file
            temp_file.replace(self.history_file)


# ============================================================================
# Search Cache Management
# ============================================================================


class SearchCache:
    """Manages SERP result caching with TTL."""

    def __init__(self):
        self.cache: Dict[str, Dict[str, Any]] = {}

    def _get_cache_key(self, query: str, page: int, backend: str) -> str:
        """Generate cache key for query, page, and search backend."""
        return f"{query}|{page}|{backend}"

    def get_cached_results(
        self, query: str, page: int, ttl_hours: int, backend: str = "google"
    ) -> Optional[Dict[str, Any]]:
        """
        Get cached search results if not expired.

        Args:
            query: Search query
            page: Page number
            ttl_hours: TTL in hours

        Returns:
            Cached results or None if expired/not found
        """
        cache_key = self._get_cache_key(query, page, backend)
        cached = self.cache.get(cache_key)

        if not cached:
            return None

        # Check TTL
        fetched_at = cached.get("fetched_at")
        if fetched_at:
            try:
                fetch_time = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
                if datetime.utcnow() - fetch_time.replace(tzinfo=None) < timedelta(
                    hours=ttl_hours
                ):
                    return cached
            except ValueError:
                pass

        # Expired, remove from cache
        del self.cache[cache_key]
        return None

    def cache_results(
        self,
        query: str,
        page: int,
        results: Dict[str, Any],
        backend: str = "google",
    ) -> None:
        """
        Cache search results.

        Args:
            query: Search query
            page: Page number
            results: Search results
        """
        cache_key = self._get_cache_key(query, page, backend)
        self.cache[cache_key] = {"results": results, "fetched_at": now_z()}


# ============================================================================
# Main Domain Tool
# ============================================================================


class DomainTool:
    """Main Domain tool implementation."""

    def __init__(self, icp_config: Dict[str, Any], data_dir: str):
        self.icp_config = icp_config
        self.data_dir = Path(data_dir)
        self.evidence_dir = self.data_dir / "evidence" / "domain"
        self.evidence_dir.mkdir(parents=True, exist_ok=True)

        # Load costs configuration
        self.costs_config = load_costs_config()
        validate_provider_config(["openrouter", "gse", "apify_crawl"], self.costs_config)

        # Initialize components
        self.permit_manager = PermitManager(
            max_permits=icp_config.get("concurrency", {}).get(
                "max_concurrent_requests", 3
            )
        )
        self.semaphore_pool = AsyncSemaphorePool(self.permit_manager)

        # Domain discovery: ordered chain; each query tries providers until one succeeds.
        # Serper -> Brave -> GSE -> Apify.
        use_brave = os.environ.get("DOMAIN_DISCOVERY_USE_BRAVE", "1").strip().lower() not in (
            "0",
            "false",
            "no",
        )
        use_serper = os.environ.get("DOMAIN_DISCOVERY_USE_SERPER", "1").strip().lower() not in (
            "0",
            "false",
            "no",
        )
        use_gse = os.environ.get("DOMAIN_DISCOVERY_USE_GSE", "0").strip().lower() not in (
            "0",
            "false",
            "no",
        )
        use_apify = os.environ.get("DOMAIN_DISCOVERY_USE_APIFY", "1").strip().lower() not in (
            "0",
            "false",
            "no",
        )
        serper_key = os.environ.get("SERPER_API_KEY", "").strip()
        brave_key = os.environ.get("BRAVE_API_KEY", "").strip()
        gse_key = os.environ.get("GSE_API_KEY", "").strip()
        gse_cx = os.environ.get("GSE_CX", "").strip()
        apify_token = os.environ.get("APIFY_API_TOKEN", "").strip()
        apify_actor_id = os.environ.get(
            "APIFY_SEARCH_ACTOR_ID", "apify/google-search-scraper"
        ).strip()

        # (cache_backend_id, billing key in costs.yaml, client)
        self._search_chain: List[Tuple[str, str, Any]] = []
        if serper_key and use_serper:
            self._search_chain.append(
                (
                    "serper",
                    "gse",
                    SerperSearchClient(
                        api_key=serper_key,
                        semaphore_pool=self.semaphore_pool,
                    ),
                )
            )
        if brave_key and use_brave:
            self._search_chain.append(
                (
                    "brave",
                    "gse",
                    BraveSearchClient(
                        api_key=brave_key,
                        semaphore_pool=self.semaphore_pool,
                    ),
                )
            )
        if gse_key and gse_cx and use_gse:
            self._search_chain.append(
                (
                    "gse",
                    "gse",
                    GSESearchClient(
                        api_key=gse_key,
                        cx=gse_cx,
                        semaphore_pool=self.semaphore_pool,
                    ),
                )
            )
        if apify_token and use_apify:
            self._search_chain.append(
                (
                    "apify",
                    "gse",
                    ApifySearchClient(
                        api_token=apify_token,
                        actor_id=apify_actor_id,
                        semaphore_pool=self.semaphore_pool,
                    ),
                )
            )

        if not self._search_chain:
            raise KeyError(
                "No domain search provider configured: set SERPER_API_KEY, "
                "BRAVE_API_KEY, and/or GSE_API_KEY+GSE_CX, and/or APIFY_API_TOKEN "
                "(and enable toggles DOMAIN_DISCOVERY_USE_SERPER / _BRAVE / _GSE / _APIFY)"
            )

        self._last_search_cost_key = self._search_chain[0][1]
        chain_desc = " -> ".join(bid for bid, _, _ in self._search_chain)
        logging.getLogger(TOOL_NAME).info(
            f"🔍 Domain discovery provider chain (first success wins per query): {chain_desc}"
        )
        logging.getLogger(TOOL_NAME).info(
            "🔧 Provider toggles: BRAVE=%s SERPER=%s GSE=%s APIFY=%s",
            use_brave,
            use_serper,
            use_gse,
            use_apify,
        )
        self._provider_health: Dict[str, Dict[str, float]] = {
            bid: {"fail_streak": 0.0, "cooldown_until": 0.0} for bid, _, _ in self._search_chain
        }
        self._provider_retry_attempts = max(
            1, int(os.environ.get("DOMAIN_PROVIDER_RETRY_ATTEMPTS", "2") or 2)
        )
        self._provider_cooldown_fail_streak = max(
            2, int(os.environ.get("DOMAIN_PROVIDER_COOLDOWN_FAIL_STREAK", "3") or 3)
        )
        self._provider_cooldown_s = max(
            5.0, float(os.environ.get("DOMAIN_PROVIDER_COOLDOWN_S", "45") or 45)
        )
        self._provider_retry_backoff_s = max(
            0.2, float(os.environ.get("DOMAIN_PROVIDER_RETRY_BACKOFF_S", "0.8") or 0.8)
        )

        openrouter_key = os.environ.get("OPENROUTER_KEY", "").strip()
        self.llm_scorer = LLMScorer(
            api_key=openrouter_key, semaphore_pool=self.semaphore_pool
        )
        if not openrouter_key:
            # Force deterministic heuristic scoring when no OpenRouter key is provided.
            self.llm_scorer.disable_llm = True
            logging.getLogger(TOOL_NAME).warning(
                "OPENROUTER_KEY missing; using heuristic scorer fallback."
            )
        blocked_default = (
            "linkedin.com,facebook.com,instagram.com,youtube.com,reddit.com,quora.com,"
            "zoominfo.com,pitchbook.com,rocketreach.co,yelp.com"
        )
        blocked_csv = os.environ.get("DOMAIN_BLOCKLIST", blocked_default)
        self.domain_blocklist = {
            d.strip().lower() for d in blocked_csv.split(",") if d.strip()
        }

        self.domain_history = DomainHistoryManager(data_dir)
        self.search_cache = SearchCache()

        self._visited_queue_domains: set[str] = set()
        vdir = os.environ.get("LEAD_QUEUE_VISITED_DIR", "").strip()
        skip_visited = os.environ.get(
            "LEADPOET_SKIP_VISITED_QUEUE_DOMAINS", "1"
        ).strip().lower() not in ("0", "false", "no")
        if vdir and skip_visited:
            self._visited_queue_domains = load_visited_domains_from_lead_queue_dir(
                Path(vdir)
            )
            if self._visited_queue_domains:
                logging.getLogger(TOOL_NAME).info(
                    "⏭️ Skip list: "
                    f"{len(self._visited_queue_domains)} domain(s) from lead_queue "
                    f"({vdir})"
                )

        # Load schema checksum
        self.schema_checksum = load_schema_checksum()

        # Setup logging (child tools inherit shared run log via env)
        self.logger = setup_logging(TOOL_NAME, data_dir=self.data_dir)

    def _validate_icp_config(self) -> List[Dict[str, Any]]:
        """
        Validate ICP configuration.

        Returns:
            List of validation errors
        """
        errors = []

        # Check required fields
        required_fields = ["name", "icp_text", "queries"]
        for field in required_fields:
            if field not in self.icp_config:
                errors.append(
                    build_error(
                        ErrorCode.SCHEMA_VALIDATION,
                        context={"missing_field": field, "stage": TOOL_NAME},
                    )
                )

        # Validate template placeholders
        if "queries" in self.icp_config:
            for query in self.icp_config["queries"]:
                missing_placeholders = validate_template_placeholders(query, {})
                if missing_placeholders:
                    errors.append(
                        build_error(
                            ErrorCode.SCHEMA_VALIDATION,
                            context={
                                "missing_placeholders": missing_placeholders,
                                "query": query,
                                "stage": TOOL_NAME,
                            },
                        )
                    )

        return errors

    def _get_search_parameters(self) -> Tuple[int, int, str]:
        """
        Get search parameters from ICP config.

        Returns:
            Tuple of (max_pages, ttl_hours, mode)
        """
        search_config = self.icp_config.get("search", {})
        max_pages = search_config.get("max_pages", DEFAULT_MAX_PAGES)

        refresh_config = self.icp_config.get("refresh_policy", {})
        ttl_hours = refresh_config.get(
            "domain_serp_ttl_hours", DEFAULT_DOMAIN_SERP_TTL_HOURS
        )

        mode = self.icp_config.get("mode", DEFAULT_MODE)
        if mode == "fast":
            max_pages = min(1, max_pages)

        return max_pages, ttl_hours, mode

    def _get_refresh_parameters(self) -> Tuple[int, int, int]:
        """
        Get refresh policy parameters.

        Returns:
            Tuple of (revisit_after_days, failure_revisit_days, domain_ttl_days)
        """
        refresh_config = self.icp_config.get("refresh_policy", {})
        revisit_after_days = refresh_config.get(
            "revisit_after_days", DEFAULT_REVISIT_AFTER_DAYS
        )
        failure_revisit_days = refresh_config.get(
            "failure_revisit_days", DEFAULT_FAILURE_REVISIT_DAYS
        )
        domain_ttl_days = refresh_config.get("domain_ttl_days", DEFAULT_DOMAIN_TTL_DAYS)

        return revisit_after_days, failure_revisit_days, domain_ttl_days

    def _create_lead_record(self, domain: str, query: str) -> Dict[str, Any]:
        """
        Create base lead record structure.

        Args:
            domain: Normalized domain
            query: Search query

        Returns:
            Base lead record
        """
        lead_id = generate_lead_id(domain)

        return {
            "lead_id": lead_id,
            "domain": domain,
            "company": {
                "name": None,
                "description": None,
                "industry": None,
                "naics_keywords": [],
                "hq_location": None,
                "locations": [],
                "size_hint": None,
                "socials": {
                    "linkedin": None,
                    "github": None,
                    "twitter": None,
                    "telegram": None,
                    "instagram": None,
                },
                "careers_url": None,
                "blog_url": None,
                "tech_stack": [],
                "evidence_urls": [],
            },
            "contacts": [],
            "icp": {
                "pre_score": None,
                "pre_reason": None,
                "pre_pass": None,
                "pre_flags": [],
                "scoring_meta": {
                    "method": None,
                    "model": None,
                    "prompt_fingerprint": None,
                    "temperature": None,
                },
                "crawl_score": None,
                "crawl_reason": None,
                "threshold": self.icp_config.get("threshold", 0.7),
                "filtering_strict": self.icp_config.get("filtering_strict", True),
            },
            "provenance": {
                "queries": [query],
                "discovery_evidence": [],
                "scored_at": None,
                "crawled_at": None,
                "enriched_at": None,
                "next_revisit_at": None,
                "tool_versions": {
                    "domain": {
                        "version": INTERNAL_VERSION,
                        "schema_version": self.schema_checksum,
                    },
                    "crawl": {"version": None, "schema_version": None},
                    "enrich": {"version": None, "schema_version": None},
                },
                "cache": {"domain_cache_hit": None},
                "evidence_paths": {"domain": None, "crawl": None, "enrich": None},
            },
            "best_contact_id": None,
            "status": "scored",
            "status_history": [],
            "audit": [],
            "cost": {
                "domain_usd": 0.0,
                "crawl_usd": 0.0,
                "enrich_usd": 0.0,
                "total_usd": 0.0,
            },
        }

    def _normalize_serp_result(
        self, item: Dict[str, Any], query: str, page_index: int
    ) -> Dict[str, Any]:
        """
        Normalize SERP result item.

        Args:
            item: Raw SERP result
            query: Search query
            page_index: Page index

        Returns:
            Normalized result
        """
        # Extract domain from URL
        url = item.get("link", "")
        domain = normalize_domain(url)

        return {
            "url": url,
            "title": item.get("title", ""),
            "snippet": item.get("snippet", ""),
            "query": query,
            "page_index": page_index,
            "rank_on_page": item.get("rank", 0),
            "fetched_at": now_z(),
            "domain": domain,
        }

    def _is_valid_domain_candidate(self, domain: str) -> bool:
        """
        Strict gate before scoring to avoid malformed "domains" and obvious non-host values.
        """
        d = (domain or "").strip().lower()
        if not d:
            return False
        if "@" in d or " " in d or "/" in d:
            return False
        if d.startswith("http://") or d.startswith("https://"):
            return False
        if len(d) > 253 or "." not in d:
            return False
        if d in {"localhost"}:
            return False
        # IPv4 values are not company domains.
        if re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", d):
            return False
        labels = d.split(".")
        if any(not part or len(part) > 63 for part in labels):
            return False
        if not re.fullmatch(r"[a-z]{2,24}", labels[-1]):
            return False
        label_re = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
        return all(label_re.fullmatch(part) for part in labels)

    def _is_low_intent_result(self, item: Dict[str, Any]) -> bool:
        """Skip common job-board/content results that rarely yield named decision-makers."""
        url = str(item.get("link", "") or "").lower()
        title = str(item.get("title", "") or "").lower()
        snippet = str(item.get("snippet", "") or "").lower()

        if any(k in url for k in LOW_INTENT_DOMAIN_KEYWORDS):
            return True
        if "/jobs" in url or "/careers" in url or "/career" in url:
            return True
        text = f"{title} {snippet}"
        if any(marker in text for marker in LOW_INTENT_TITLE_MARKERS):
            return True
        return False

    def _is_retryable_provider_error(self, err: Exception) -> bool:
        if isinstance(err, (asyncio.TimeoutError, httpx.TimeoutException, httpx.ConnectError)):
            return True
        if isinstance(err, httpx.HTTPStatusError):
            status = err.response.status_code if err.response is not None else 0
            return status in {408, 425, 429, 500, 502, 503, 504}
        txt = f"{type(err).__name__}: {err}".lower()
        return "timeout" in txt or "temporarily unavailable" in txt or "connection reset" in txt

    def _provider_on_success(self, bid: str) -> None:
        st = self._provider_health.get(bid)
        if not st:
            return
        st["fail_streak"] = 0.0
        st["cooldown_until"] = 0.0

    def _provider_on_failure(self, bid: str) -> None:
        st = self._provider_health.get(bid)
        if not st:
            return
        st["fail_streak"] += 1.0
        if st["fail_streak"] >= float(self._provider_cooldown_fail_streak):
            st["cooldown_until"] = time.time() + self._provider_cooldown_s

    def _provider_available(self, bid: str) -> bool:
        st = self._provider_health.get(bid)
        if not st:
            return True
        return time.time() >= float(st.get("cooldown_until", 0.0))

    async def _search_query(
        self, query: str, max_pages: int, ttl_hours: int
    ) -> Tuple[List[Dict[str, Any]], int, int]:
        """
        Search for domains using a single query.

        Args:
            query: Search query
            max_pages: Maximum pages to search
            ttl_hours: Cache TTL in hours

        Returns:
            Tuple of (results, cache_hits, cache_misses)
        """
        self.logger.info(f"🔍 Searching query: {query} (max_pages={max_pages})")

        normalized_results: List[Dict[str, Any]] = []
        cache_hits = 0
        cache_misses = 0
        skip_counts: Counter[str] = Counter()

        for page in range(1, max_pages + 1):
            # Cache: first hit along the configured chain
            cached = None
            hit_backend: Optional[str] = None
            hit_cost_key: Optional[str] = None
            for bid, ck, _ in self._search_chain:
                c = self.search_cache.get_cached_results(
                    query, page, ttl_hours, backend=bid
                )
                if c:
                    cached = c
                    hit_backend = bid
                    hit_cost_key = ck
                    break

            if cached:
                cache_hits += 1
                if hit_cost_key:
                    self._last_search_cost_key = hit_cost_key
                for item in cached["results"].get("items", []):
                    # Skip noisy/non-company domains before scoring to save API cost.
                    url = (item.get("link", "") or "").lower()
                    if any(domain in url for domain in self.domain_blocklist):
                        skip_counts["blocklist"] += 1
                        continue
                    if self._is_low_intent_result(item):
                        skip_counts["low_intent"] += 1
                        continue
                    normalized_item = self._normalize_serp_result(item, query, page)
                    if not self._is_valid_domain_candidate(
                        str(normalized_item.get("domain", "") or "")
                    ):
                        skip_counts["invalid_domain"] += 1
                        continue
                    normalized_results.append(normalized_item)
                continue

            # Fetch fresh: try each provider until one succeeds
            results: Optional[Dict[str, Any]] = None
            used_backend: Optional[str] = None
            used_cost_key: Optional[str] = None
            last_err: Optional[Exception] = None
            for bid, ck, client in self._search_chain:
                if not self._provider_available(bid):
                    self.logger.warning(
                        "⏸️ Provider '%s' in cooldown, skipping this page", bid
                    )
                    continue
                try:
                    provider_ok = False
                    for attempt in range(1, self._provider_retry_attempts + 1):
                        try:
                            results = await client.search(query, page)
                            used_backend = bid
                            used_cost_key = ck
                            self._provider_on_success(bid)
                            provider_ok = True
                            break
                        except Exception as e:
                            last_err = e
                            retryable = self._is_retryable_provider_error(e)
                            status_txt = ""
                            if isinstance(e, httpx.HTTPStatusError):
                                code = e.response.status_code if e.response is not None else "?"
                                status_txt = f"HTTP {code}"
                            else:
                                status_txt = type(e).__name__
                            if retryable and attempt < self._provider_retry_attempts:
                                self.logger.warning(
                                    "⚠️ Search provider '%s' transient failure for query %r page %s "
                                    "(attempt %s/%s): %s; retrying",
                                    bid,
                                    query,
                                    page,
                                    attempt,
                                    self._provider_retry_attempts,
                                    status_txt,
                                )
                                await asyncio.sleep(self._provider_retry_backoff_s * attempt)
                                continue
                            self._provider_on_failure(bid)
                            self.logger.warning(
                                "⚠️ Search provider '%s' failed for query %r page %s: %s",
                                bid,
                                query,
                                page,
                                status_txt,
                            )
                            break
                    if provider_ok:
                        break
                except Exception as e:
                    last_err = e
                    self._provider_on_failure(bid)
                    self.logger.warning(
                        f"⚠️ Search provider '{bid}' failed for "
                        f"query {query!r} page {page}: {type(e).__name__}"
                    )

            if results is None:
                err_note = (
                    f"HTTP {last_err.response.status_code}"
                    if isinstance(last_err, httpx.HTTPStatusError)
                    and last_err.response is not None
                    else type(last_err).__name__
                    if last_err
                    else "unknown"
                )
                self.logger.error(
                    f"❌ All search providers failed for query {query!r} page {page} "
                    f"({err_note})"
                )
                continue

            cache_misses += 1
            if used_cost_key:
                self._last_search_cost_key = used_cost_key
            if used_backend:
                self.search_cache.cache_results(
                    query, page, results, backend=used_backend
                )
            for item in results.get("items", []):
                url = (item.get("link", "") or "").lower()
                if any(domain in url for domain in self.domain_blocklist):
                    skip_counts["blocklist"] += 1
                    continue
                if self._is_low_intent_result(item):
                    skip_counts["low_intent"] += 1
                    continue
                normalized_item = self._normalize_serp_result(item, query, page)
                if not self._is_valid_domain_candidate(
                    str(normalized_item.get("domain", "") or "")
                ):
                    skip_counts["invalid_domain"] += 1
                    continue
                normalized_results.append(normalized_item)

        if skip_counts:
            self.logger.warning(
                "Filtered noisy SERP results for query %r: %s",
                query,
                dict(skip_counts),
            )
        return normalized_results, cache_hits, cache_misses

    async def _search_and_score_domains(
        self, queries: List[str], max_pages: int, ttl_hours: int
    ) -> Tuple[List[Dict[str, Any]], int, int]:
        """
        Search and score domains for given queries.

        Args:
            queries: List of search queries
            max_pages: Maximum pages to fetch
            ttl_hours: Cache TTL in hours

        Returns:
            Tuple of (scored_domains, cache_hits, cache_misses)
        """
        self.logger.info(
            f"🔍 _search_and_score_domains: {len(queries)} queries, {max_pages} pages, {ttl_hours}h TTL"
        )
        all_scored_domains: List[Dict[str, Any]] = []
        total_cache_hits = 0
        total_cache_misses = 0

        for query_idx, query in enumerate(queries, 1):
            self.logger.info(
                f"🔍 Processing query {query_idx}/{len(queries)}: '{query}'"
            )
            # Search for domains using the current query
            (
                query_results,
                query_cache_hits,
                query_cache_misses,
            ) = await self._search_query(query, max_pages, ttl_hours)
            total_cache_hits += query_cache_hits
            total_cache_misses += query_cache_misses
            # Score domains for this query
            self.logger.info(
                f"🤖 Starting LLM scoring for {len(query_results)} domains..."
            )
            if len(query_results) == 0:
                self.logger.warning(
                    f"No scoreable domains for query {query!r} after provider + noise filtering"
                )
            scored_domains = await self._score_domains(query_results)
            self.logger.info(
                f"✅ LLM scoring completed: {len(scored_domains)} scored domains"
            )
            all_scored_domains.extend(scored_domains)

        self.logger.info(
            f"📊 Search completed: {len(all_scored_domains)} total results, {total_cache_hits} cache hits, {total_cache_misses} cache misses"
        )
        if len(all_scored_domains) == 0:
            self.logger.error(
                "Search/scoring produced zero domains across all queries; check provider connectivity and query quality."
            )
        return all_scored_domains, total_cache_hits, total_cache_misses

    async def _score_domains(
        self, serp_results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Score domains using LLM.

        Args:
            serp_results: List of normalized SERP results

        Returns:
            List of scored domain records
        """
        self.logger.info(
            f"🤖 _score_domains: Processing {len(serp_results)} SERP results"
        )
        scored_domains = []
        domain_map = {}  # Track unique domains

        for idx, result in enumerate(serp_results, 1):
            domain = result["domain"]
            self.logger.info(
                f"🤖 Processing domain {idx}/{len(serp_results)}: {domain}"
            )

            if domain in self._visited_queue_domains:
                self.logger.info(
                    f"⏭️ Skipping domain already in lead_queue: {domain}"
                )
                continue

            # Skip if we already processed this domain
            if domain in domain_map:
                self.logger.info(
                    f"🔄 Domain {domain} already processed, adding to evidence"
                )
                # Add to existing record's discovery evidence
                existing_record = domain_map[domain]
                if len(existing_record["provenance"]["discovery_evidence"]) < 3:
                    existing_record["provenance"]["discovery_evidence"].append(result)
                continue

            # Create new lead record
            self.logger.info(f"📝 Creating new lead record for domain: {domain}")
            record = self._create_lead_record(domain, result["query"])

            # Score the domain
            self.logger.info(f"🤖 Scoring domain {domain} with LLM...")
            try:
                (
                    score,
                    reason,
                    flags,
                    model_used,
                    prompt_fingerprint,
                ) = await self.llm_scorer.score_domain(
                    result["title"],
                    result["snippet"],
                    result["query"],
                    self.icp_config["icp_text"],
                )

                # Update record with scoring results
                record["icp"]["pre_score"] = round4(score)
                record["icp"]["pre_reason"] = reason
                record["icp"]["pre_flags"] = flags
                record["icp"]["scoring_meta"] = {
                    "method": "llm",
                    "model": model_used,
                    "prompt_fingerprint": prompt_fingerprint,
                    "temperature": 0.0,
                }

                # Set pre_pass based on threshold
                threshold = record["icp"]["threshold"]
                record["icp"]["pre_pass"] = score >= threshold

                # Add discovery evidence
                record["provenance"]["discovery_evidence"].append(result)

                # Set scored_at and next_revisit_at
                record["provenance"]["scored_at"] = now_z()
                revisit_after_days, failure_revisit_days, _ = (
                    self._get_refresh_parameters()
                )
                record["provenance"]["next_revisit_at"] = compute_next_revisit(
                    None, revisit_after_days, failure_revisit_days, False
                )

                # Add status history entry
                append_status(
                    record["status_history"], "scored", f"Scored with {model_used}"
                )

                # Add audit entry
                append_audit(
                    record, "Domain", f"Scored domain {domain} with score {score}"
                )

                # Calculate costs
                search_unit_cost = self.costs_config[self._last_search_cost_key]
                openrouter_cost = self.costs_config["openrouter"]

                # Estimate token usage (rough approximation)
                prompt_tokens = (
                    len(
                        result["title"]
                        + result["snippet"]
                        + result["query"]
                        + self.icp_config["icp_text"]
                    )
                    // 4
                )
                response_tokens = 100  # Estimated response length
                total_tokens = prompt_tokens + response_tokens
                token_cost = (total_tokens / 1000) * openrouter_cost

                record["cost"]["domain_usd"] = round4(search_unit_cost + token_cost)
                recompute_total_cost(record)

            except Exception as e:
                self.logger.error(f"LLM scoring failed for domain {domain}: {e}")
                # Set default values on scoring failure
                record["icp"]["pre_score"] = 0.0
                record["icp"]["pre_reason"] = f"Scoring failed: {str(e)}"
                record["icp"]["pre_pass"] = False
                record["icp"]["pre_flags"] = ["scoring_error"]

                # Set failure revisit time
                _, failure_revisit_days, _ = self._get_refresh_parameters()
                record["provenance"]["next_revisit_at"] = compute_next_revisit(
                    None, 0, failure_revisit_days, True
                )

                # Add status history entry
                append_status(record["status_history"], "scored", "Scoring failed")

                # Add audit entry
                append_audit(
                    record, "Domain", f"Scoring failed for domain {domain}: {e}"
                )

                # Set minimal cost
                record["cost"]["domain_usd"] = round4(
                    self.costs_config[self._last_search_cost_key]
                )
                recompute_total_cost(record)

            # Store in domain map
            domain_map[domain] = record
            scored_domains.append(record)

        return scored_domains

    def _save_evidence(self, record: Dict[str, Any]) -> None:
        """
        Save evidence to disk and update record pointer.

        Args:
            record: Lead record
        """
        domain = record["domain"]
        evidence_file = self.evidence_dir / f"{domain}.json"

        evidence_data = {
            "domain": domain,
            "queries": record["provenance"]["queries"],
            "fetched_at": now_z(),
            "serp_results": record["provenance"]["discovery_evidence"],
            "scoring_prompt_fingerprint": record["icp"]["scoring_meta"][
                "prompt_fingerprint"
            ],
        }

        write_with_lock(str(evidence_file), evidence_data)
        record["provenance"]["evidence_paths"]["domain"] = str(evidence_file)

    async def run(self, icp_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Main entry point for Domain tool.

        Args:
            icp_config: ICP configuration (overrides instance config if provided)

        Returns:
            Tool output envelope
        """
        start_time = time.time()
        self.logger.info(f"🚀 Domain tool run() started at {start_time}")

        try:
            # Use provided config or instance config
            config = icp_config or self.icp_config
            self.logger.info(f"📋 Using config: {config.get('name', 'Unknown')}")

            # Validate configuration
            self.logger.info("🔍 Validating ICP configuration...")
            validation_errors = self._validate_icp_config()
            if validation_errors:
                self.logger.error(
                    f"❌ Configuration validation failed: {validation_errors}"
                )
                return {
                    "data": {"lead_records": []},
                    "errors": validation_errors,
                    "metrics": build_metrics(0, 0, 0),
                }
            self.logger.info("✅ Configuration validation passed")

            # Get search and refresh parameters
            self.logger.info("⚙️ Getting search and refresh parameters...")
            max_pages, ttl_hours, mode = self._get_search_parameters()
            revisit_after_days, failure_revisit_days, domain_ttl_days = (
                self._get_refresh_parameters()
            )
            self.logger.info(
                f"📊 Search params: max_pages={max_pages}, ttl_hours={ttl_hours}, mode={mode}"
            )

            # Get queries and validate placeholders
            queries = config.get("queries", [])
            self.logger.info(f"🔍 Found {len(queries)} queries to process")
            if not queries:
                self.logger.error("❌ No queries provided")
                return {
                    "data": {"lead_records": []},
                    "errors": [
                        build_error(
                            ErrorCode.SCHEMA_VALIDATION,
                            context={
                                "error": "No queries provided",
                                "stage": TOOL_NAME,
                            },
                        )
                    ],
                    "metrics": build_metrics(0, 0, 0),
                }

            # Search and score domains
            self.logger.info("🔍 Starting search and score domains...")
            (
                scored_domains,
                cache_hits,
                cache_misses,
            ) = await self._search_and_score_domains(queries, max_pages, ttl_hours)

            # Save evidence for each domain
            for record in scored_domains:
                self._save_evidence(record)

                # Update domain history
                self.domain_history.add_domain_record(record)

            # Save domain history
            self.domain_history.save_history(domain_ttl_days)

            # Calculate metrics
            count_in = len(queries)
            count_out = len(scored_domains)
            duration_ms = int((time.time() - start_time) * 1000)

            cache_hit_rate = None
            if cache_hits + cache_misses > 0:
                cache_hit_rate = cache_hits / (cache_hits + cache_misses)

            pass_rate = None
            if count_out > 0:
                pass_count = sum(1 for r in scored_domains if r["icp"]["pre_pass"])
                pass_rate = pass_count / count_out

            total_cost = sum(r["cost"]["domain_usd"] for r in scored_domains)

            metrics = build_metrics(
                count_in=count_in,
                count_out=count_out,
                duration_ms=duration_ms,
                cache_hit_rate=cache_hit_rate,
                pass_rate=pass_rate,
                cost_usd={
                    "domain": total_cost,
                    "crawl": 0.0,
                    "enrich": 0.0,
                    "total": total_cost,
                },
            )

            return {
                "data": {"lead_records": scored_domains},
                "errors": [],
                "metrics": metrics,
            }

        except Exception as e:
            self.logger.error(f"Domain tool failed: {e}")
            return {
                "data": {"lead_records": []},
                "errors": [build_error(ErrorCode.UNKNOWN, exc=e, tool=TOOL_NAME)],
                "metrics": build_metrics(0, 0, int((time.time() - start_time) * 1000)),
            }

    def truncate_evidence_arrays(self, record: Dict[str, Any]) -> None:
        """
        Enforce evidence array truncation limits.

        Args:
            record: Lead record to truncate
        """
        # Company evidence_urls ≤ 10
        if "company" in record and "evidence_urls" in record["company"]:
            if len(record["company"]["evidence_urls"]) > 10:
                record["company"]["evidence_urls"] = record["company"]["evidence_urls"][
                    :10
                ]

        # Contacts evidence_urls ≤ 5
        if "contacts" in record:
            for contact in record["contacts"]:
                if "evidence_urls" in contact and len(contact["evidence_urls"]) > 5:
                    contact["evidence_urls"] = contact["evidence_urls"][:5]


# ============================================================================
# CLI Interface
# ============================================================================


def main():
    """CLI entry point for Domain tool."""
    import sys

    try:
        # Read JSON from stdin
        input_data = json.load(sys.stdin)

        # Extract icp_config
        icp_config = input_data.get("icp_config", {})
        data_dir = input_data.get("config", {}).get("data_dir", "./data")

        # Create tool instance
        tool = DomainTool(icp_config, data_dir)

        # Run tool
        result = asyncio.run(tool.run(icp_config))

        # Write JSON to stdout
        json.dump(result, sys.stdout, indent=2)

    except json.JSONDecodeError as e:
        print(
            json.dumps(
                {
                    "data": {"lead_records": []},
                    "errors": [
                        build_error(ErrorCode.SCHEMA_VALIDATION, exc=e, tool=TOOL_NAME)
                    ],
                    "metrics": build_metrics(0, 0, 0),
                }
            ),
            file=sys.stderr,
        )
        sys.exit(1)
    except Exception as e:
        print(
            json.dumps(
                {
                    "data": {"lead_records": []},
                    "errors": [build_error(ErrorCode.UNKNOWN, exc=e, tool=TOOL_NAME)],
                    "metrics": build_metrics(0, 0, 0),
                }
            ),
            file=sys.stderr,
        )
        sys.exit(1)
