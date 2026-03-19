"""
Optional Hugging Face models for lead quality (industry + email intent).
Enable with env: USE_HF_INDUSTRY=1, USE_HF_EMAIL_INTENT_FILTER=1.

- Industry: swarupt/industry-classification (20 classes) -> mapped to LeadPoet taxonomy.
- Email intent: Shoriful025/Email-Intent-Classifier (5 classes); filter out PROMOTION (spam).
"""
from __future__ import annotations

import os
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Swarupt industry model (20 classes) -> LeadPoet ALLOWED_INDUSTRIES
# ---------------------------------------------------------------------------
SWARUPT_TO_LEADPOET = {
    "Technology, Information and Media": "Technology",
    "Financial Services": "Finance",
    "Hospitals and Health Care": "Healthcare",
    "Manufacturing": "Manufacturing",
    "Retail": "Retail",
    "Education": "Education",
    "Real Estate and Equipment Rental Services": "Real Estate",
    "Oil, Gas, and Mining": "Energy & Utilities",
    "Utilities": "Energy & Utilities",
    "Transportation, Logistics and Storage": "Transportation & Logistics",
    "Entertainment Providers": "Media & Entertainment",
    "Professional Services": "Technology",
    "Administrative and Support Services": "Marketing",
    "Consumer Services": "Retail",
    "Accomodation Services": "Retail",
    "Construction": "Manufacturing",
    "Farming, Ranching, Forestry": "Manufacturing",
    "Government Administration": "Education",
    "Holding Companies": "Finance",
    "Wholesale": "Retail",
}

# Email intent: labels we consider low-quality for B2B lead (filter out when USE_HF_EMAIL_INTENT_FILTER=1)
EMAIL_INTENT_EXCLUDE = {"PROMOTION"}  # marketing spam

_industry_pipeline = None
_email_intent_pipeline = None


def _get_industry_pipeline():
    global _industry_pipeline
    if _industry_pipeline is not None:
        return _industry_pipeline
    if os.getenv("USE_HF_INDUSTRY", "0") != "1":
        return None
    try:
        from transformers import pipeline
        _industry_pipeline = pipeline(
            "text-classification",
            model="swarupt/industry-classification",
            top_k=1,
        )
        logger.info("Loaded HF industry model: swarupt/industry-classification")
        return _industry_pipeline
    except Exception as e:
        logger.warning("Could not load HF industry model: %s", e)
        return None


def _get_email_intent_pipeline():
    global _email_intent_pipeline
    if _email_intent_pipeline is not None:
        return _email_intent_pipeline
    if os.getenv("USE_HF_EMAIL_INTENT_FILTER", "0") != "1":
        return None
    try:
        from transformers import pipeline
        _email_intent_pipeline = pipeline(
            "text-classification",
            model="Shoriful025/Email-Intent-Classifier",
            top_k=1,
        )
        logger.info("Loaded HF email intent model: Shoriful025/Email-Intent-Classifier")
        return _email_intent_pipeline
    except Exception as e:
        logger.warning("Could not load HF email intent model: %s", e)
        return None


def classify_industry_hf(description: str) -> Optional[str]:
    """
    Classify company/lead description into LeadPoet industry using HF model.
    Returns one of ALLOWED_INDUSTRIES (lowercase), or None if model unavailable / no mapping.
    """
    pipe = _get_industry_pipeline()
    if not pipe or not description or not description.strip():
        return None
    try:
        text = (description.strip()[:512] or "company")  # model max length
        out = pipe(text)
        if not out or not out[0]:
            return None
        label = out[0][0].get("label", "")
        if not label:
            return None
        mapped = SWARUPT_TO_LEADPOET.get(label)
        if mapped:
            return mapped.lower()
        return None
    except Exception as e:
        logger.debug("HF industry classification failed: %s", e)
        return None


def predict_email_intent(text: str) -> Tuple[Optional[str], float]:
    """
    Predict email/lead intent (URGENT, INFO, MEETING, TASK, PROMOTION).
    Returns (label, score) or (None, 0.0) if model unavailable.
    """
    pipe = _get_email_intent_pipeline()
    if not pipe or not text or not text.strip():
        return None, 0.0
    try:
        snippet = (text.strip()[:512] or "message")
        out = pipe(snippet)
        if not out or not out[0]:
            return None, 0.0
        item = out[0][0]
        return item.get("label"), float(item.get("score", 0.0))
    except Exception as e:
        logger.debug("HF email intent failed: %s", e)
        return None, 0.0


def should_filter_lead_by_intent(lead_description: str, business_name: str = "") -> bool:
    """
    Return True if lead should be filtered out based on email intent (e.g. PROMOTION = spam).
    Only active when USE_HF_EMAIL_INTENT_FILTER=1 and model loaded.
    """
    pipe = _get_email_intent_pipeline()
    if not pipe:
        return False
    text = f"{business_name} {lead_description}".strip() or "lead"
    label, _ = predict_email_intent(text)
    if label and label in EMAIL_INTENT_EXCLUDE:
        return True
    return False
