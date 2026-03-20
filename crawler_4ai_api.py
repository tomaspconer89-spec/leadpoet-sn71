from __future__ import annotations

from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

try:
    from crawl4ai import AsyncWebCrawler
except Exception as exc:  # pragma: no cover - import guard for runtime clarity
    AsyncWebCrawler = None
    _IMPORT_ERROR = str(exc)
else:
    _IMPORT_ERROR = ""


app = FastAPI(title="LeadPoet Crawl4AI API", version="1.0.0")


class CrawlerRequest(BaseModel):
    url: str
    timeout_ms: Optional[int] = 45000


class CrawlerResponse(BaseModel):
    success: bool
    url: str
    title: str
    markdown: str
    links: List[str]
    metadata: dict


@app.get("/health")
async def health() -> dict:
    return {
        "ok": AsyncWebCrawler is not None,
        "crawler4ai_available": AsyncWebCrawler is not None,
        "error": _IMPORT_ERROR or None,
    }


@app.post("/scrape", response_model=CrawlerResponse)
async def scrape(req: CrawlerRequest) -> CrawlerResponse:
    if AsyncWebCrawler is None:
        raise HTTPException(
            status_code=500,
            detail=f"crawl4ai import failed: {_IMPORT_ERROR}",
        )

    try:
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url=req.url)

        title = ""
        metadata = getattr(result, "metadata", {}) or {}
        if isinstance(metadata, dict):
            title = str(metadata.get("title") or "")

        markdown = str(getattr(result, "markdown", "") or "")
        links_raw = getattr(result, "links", []) or []
        if isinstance(links_raw, dict):
            links = [str(v) for v in links_raw.values() if v]
        elif isinstance(links_raw, list):
            links = [str(v) for v in links_raw if v]
        else:
            links = []

        return CrawlerResponse(
            success=True,
            url=req.url,
            title=title,
            markdown=markdown,
            links=links,
            metadata=metadata if isinstance(metadata, dict) else {},
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"crawl failed: {exc}")