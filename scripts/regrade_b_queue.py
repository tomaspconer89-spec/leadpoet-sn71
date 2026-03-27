#!/usr/bin/env python3
"""
Re-run normalization + title + person confidence + precheck + route_lead on leads in
lead_queue/B_retry_enrichment/. Updates JSON in place when still B; moves to A_ready_submit
(or other buckets) when routing changes. Updates lead_queue/collected_pass when precheck passes.

Do not raw-mv B -> A: folder placement must match computed route_lead(...).

Usage (repo root):
  python3 scripts/regrade_b_queue.py
  python3 scripts/regrade_b_queue.py --enrich-linkedin 1
  python3 scripts/regrade_b_queue.py --dry-run
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv

from miner_models.lead_normalization import apply_email_classification, normalize_legacy_lead_shape
from miner_models.minimal_lead_blob import minimal_gateway_lead
from miner_models.person_confidence import score_person_confidence
from miner_models.title_normalizer import normalize_title
from scripts.queue_router import route_lead
from scripts.retry_enrichment import should_run_targeted_retry, targeted_retry_enrichment

load_dotenv(_REPO / ".env")


def _load_enrich():
    conv = _REPO / "scripts" / "convert_raw_to_pending.py"
    if not conv.is_file():
        return None
    spec = importlib.util.spec_from_file_location("_crtp", conv)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return getattr(mod, "enrich_linkedin_fields", None)


def _load_scrapingdog_fixer():
    conv = _REPO / "scripts" / "convert_raw_to_pending.py"
    if not conv.is_file():
        return None
    spec = importlib.util.spec_from_file_location("_crtp_fix", conv)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return getattr(mod, "validate_and_fix_with_scrapingdog", None)


def _load_location_helpers():
    conv = _REPO / "scripts" / "convert_raw_to_pending.py"
    if not conv.is_file():
        return None, None
    spec = importlib.util.spec_from_file_location("_crtp_loc", conv)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return (
        getattr(mod, "parse_hq_location", None),
        getattr(mod, "normalize_country_name", None),
    )


_US_STATE_ABBR = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
}


def _scrapingdog_google_json(query: str) -> dict:
    key = os.getenv("SCRAPINGDOG_API_KEY", "").strip()
    if not key:
        return {}
    url = (
        "https://api.scrapingdog.com/google"
        f"?api_key={urllib.parse.quote(key)}"
        f"&query={urllib.parse.quote(query)}"
        "&results=10&country=us&page=0"
    )
    req = urllib.request.Request(url=url, method="GET")
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8")
    data = json.loads(body)
    return data if isinstance(data, dict) else {}


def _collect_text_blobs(obj, out: list[str]) -> None:
    if isinstance(obj, dict):
        for v in obj.values():
            _collect_text_blobs(v, out)
        return
    if isinstance(obj, list):
        for v in obj:
            _collect_text_blobs(v, out)
        return
    if isinstance(obj, str):
        s = obj.strip()
        if s:
            out.append(s)


def _extract_us_location(text: str) -> tuple[str, str, str]:
    s = (text or "").strip()
    if not s:
        return "", "", ""
    m = re.search(r"\b([A-Z][A-Za-z .'-]{1,60}),\s*([A-Z]{2})\b", s)
    if m:
        city = m.group(1).strip(" ,")
        st = m.group(2).upper()
        if st in _US_STATE_ABBR:
            return city, st, "United States"
    m = re.search(
        r"\b([A-Z][A-Za-z .'-]{1,60}),\s*"
        r"(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming)\b",
        s,
    )
    if m:
        return m.group(1).strip(" ,"), m.group(2).strip(), "United States"
    return "", "", ""


def _enrich_location_from_linkedin(
    lead: dict,
    parse_hq_location,
    normalize_country_name,
) -> tuple[dict, list[str]]:
    """
    Use LinkedIn URLs + ScrapingDog SERP snippets to infer city/state/country.
    """
    changed: list[str] = []
    candidate = dict(lead)
    urls = [
        str(candidate.get("linkedin") or "").strip(),
        str(candidate.get("company_linkedin") or "").strip(),
    ]
    urls = [u for u in urls if "linkedin.com/" in u.lower()]
    if not urls:
        return candidate, changed

    need_loc = any(
        not str(candidate.get(k) or "").strip()
        for k in ("city", "state", "country", "hq_city", "hq_state", "hq_country")
    )
    if not need_loc:
        return candidate, changed

    found_city = found_state = found_country = ""
    for u in urls:
        try:
            data = _scrapingdog_google_json(f"\"{u}\" location")
        except Exception:
            continue
        blobs: list[str] = []
        _collect_text_blobs(data, blobs)
        for b in blobs:
            c, st, co = _extract_us_location(b)
            if c and st:
                found_city, found_state, found_country = c, st, co
                break
            if parse_hq_location is not None:
                c2, s2, co2 = parse_hq_location(b)
                if c2 and (s2 or co2):
                    found_city = c2.strip()
                    found_state = (s2 or "").strip()
                    found_country = (co2 or "").strip()
                    if normalize_country_name is not None:
                        found_country = normalize_country_name(found_country)
                    break
        if found_city:
            break

    if not found_city:
        return candidate, changed

    def _set_if_empty(key: str, value: str):
        if value and not str(candidate.get(key) or "").strip():
            candidate[key] = value
            changed.append(key)

    _set_if_empty("city", found_city)
    _set_if_empty("state", found_state)
    _set_if_empty("country", found_country or "United States")
    _set_if_empty("hq_city", found_city)
    _set_if_empty("hq_state", found_state)
    _set_if_empty("hq_country", found_country or "United States")
    return candidate, changed


_NORMAL_GATEWAY_FIELDS = (
    "business",
    "full_name",
    "first",
    "last",
    "email",
    "role",
    "website",
    "industry",
    "sub_industry",
    "country",
    "state",
    "city",
    "linkedin",
    "company_linkedin",
    "source_url",
    "description",
    "employee_count",
    "hq_country",
    "hq_state",
    "hq_city",
    "source_type",
)


def _ensure_normal_fields(lead: dict) -> dict:
    """
    Keep SN71-normal fields present in queue files even when value is empty.
    This avoids disappearing keys like state/city/hq_state/hq_city in B queue.
    """
    # Strip extras, then rebuild in exact README order.
    base = minimal_gateway_lead(dict(lead))
    out: dict = {}
    for k in _NORMAL_GATEWAY_FIELDS:
        v = base.get(k, "")
        out[k] = "" if v is None else v
    out["phone_numbers"] = (
        base.get("phone_numbers")
        if isinstance(base.get("phone_numbers"), list)
        else []
    )
    out["socials"] = (
        base.get("socials")
        if isinstance(base.get("socials"), dict)
        else {}
    )
    return out


def _queue_key(lead: dict) -> str:
    import hashlib

    parts = [
        str(lead.get("email", "")).strip().lower(),
        str(lead.get("business", "")).strip().lower(),
        str(lead.get("linkedin", "")).strip().lower(),
        str(lead.get("website", "")).strip().lower(),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _process_one(
    lead: dict,
    *,
    enrich_fn,
    scrapingdog_fixer=None,
    parse_hq_location=None,
    normalize_country_name=None,
) -> tuple[dict, bool, str | None, str, list[str]]:
    from miner_models.lead_precheck import precheck_lead

    lead = dict(lead)
    normalize_legacy_lead_shape(lead)
    fixed_fields: list[str] = []
    if scrapingdog_fixer is not None:
        try:
            fixed_lead, changed = scrapingdog_fixer(dict(lead))
            if changed:
                lead = fixed_lead
                fixed_fields = sorted(changed.keys())
        except Exception:
            pass
    if enrich_fn is not None:
        try:
            lead = enrich_fn(dict(lead))
        except Exception:
            pass
    try:
        lead, loc_changed = _enrich_location_from_linkedin(
            lead,
            parse_hq_location=parse_hq_location,
            normalize_country_name=normalize_country_name,
        )
        for k in loc_changed:
            if k not in fixed_fields:
                fixed_fields.append(k)
    except Exception:
        pass
    normalize_legacy_lead_shape(lead)
    apply_email_classification(lead)

    title_meta = normalize_title(str(lead.get("role", "")))
    lead["title_normalized"] = title_meta["normalized_title"]
    lead["seniority"] = title_meta["seniority"]
    lead["persona_bucket"] = title_meta["persona_bucket"]
    lead["target_fit"] = title_meta["target_fit"]

    conf = score_person_confidence(
        lead, title_matches_persona=title_meta["target_fit"] in ("high", "medium")
    )
    lead.update(conf)
    apply_email_classification(lead)

    pre_ok, reason = precheck_lead(lead)
    retry_attempts = 0
    if (not pre_ok) and reason:
        if "name_not_in_email" in (reason or "") or "email_domain_mismatch" in (reason or ""):
            lead["identity_conflict"] = True
        if should_run_targeted_retry(reason):
            lead, _, retry_attempts = targeted_retry_enrichment(
                lead, reason, enrich_linkedin=enrich_fn
            )
        if retry_attempts > 0:
            normalize_legacy_lead_shape(lead)
            apply_email_classification(lead)
            conf2 = score_person_confidence(
                lead,
                title_matches_persona=title_meta["target_fit"] in ("high", "medium"),
            )
            lead.update(conf2)
            pre_ok, reason = precheck_lead(lead)

    bucket = route_lead(lead, precheck_ok=pre_ok, precheck_reason=reason)
    return lead, pre_ok, reason, bucket, fixed_fields


def main() -> int:
    p = argparse.ArgumentParser(description="Re-grade B_retry_enrichment leads; promote to A when eligible")
    p.add_argument("--enrich-linkedin", type=int, default=1, choices=(0, 1))
    p.add_argument("--scrapingdog-fix", type=int, default=1, choices=(0, 1))
    p.add_argument(
        "--route",
        type=int,
        default=0,
        choices=(0, 1),
        help="When 1, move failed leads to routed bucket; when 0, keep failed leads in B_retry_enrichment",
    )
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    queue = _REPO / "lead_queue"
    b_dir = queue / "B_retry_enrichment"
    graded_dirs = {
        "A_ready_submit": queue / "A_ready_submit",
        "B_retry_enrichment": queue / "B_retry_enrichment",
        "C_good_account_needs_person": queue / "C_good_account_needs_person",
        "D_low_confidence_hold": queue / "D_low_confidence_hold",
        "E_reject": queue / "E_reject",
    }
    pass_dir = queue / "collected_pass"
    fail_dir = queue / "collected_precheck_fail"
    for d in (*graded_dirs.values(), pass_dir, fail_dir):
        d.mkdir(parents=True, exist_ok=True)

    enrich_fn = _load_enrich() if args.enrich_linkedin == 1 else None
    scrapingdog_fixer = _load_scrapingdog_fixer() if args.scrapingdog_fix == 1 else None
    parse_hq_location, normalize_country_name = _load_location_helpers()
    files = sorted(b_dir.glob("*.json"))
    if not files:
        print(f"No JSON in {b_dir}")
        return 0

    promoted = updated = failed = 0
    for old_path in files:
        business = "?"
        try:
            lead_in = json.loads(old_path.read_text(encoding="utf-8"))
            business = str(lead_in.get("business", "?"))
            lead, pre_ok, reason, bucket, fixed_fields = _process_one(
                lead_in,
                enrich_fn=enrich_fn,
                scrapingdog_fixer=scrapingdog_fixer,
                parse_hq_location=parse_hq_location,
                normalize_country_name=normalize_country_name,
            )
            key = _queue_key(lead)
            # Always promote precheck-passing leads to A_ready_submit.
            if pre_ok:
                dest_bucket = "A_ready_submit"
            else:
                dest_bucket = bucket if args.route == 1 else "B_retry_enrichment"
            new_graded = graded_dirs[dest_bucket] / f"{key}.json"

            if args.dry_run:
                print(
                    f"[dry-run] {old_path.name} -> {dest_bucket} (suggested={bucket}) "
                    f"precheck={'OK' if pre_ok else 'FAIL'} {reason or ''} "
                    f"fixed_fields={fixed_fields} business={business[:50]!r}"
                )
                continue

            if old_path.resolve() != new_graded.resolve():
                old_path.unlink(missing_ok=True)

            # Keep all SN71-normal fields present (including location keys),
            # and update with any ScrapingDog corrections.
            store_graded = _ensure_normal_fields(lead)
            new_graded.write_text(json.dumps(store_graded, ensure_ascii=True, indent=2), encoding="utf-8")

            if pre_ok:
                (pass_dir / f"{key}.json").write_text(
                    json.dumps(store_graded, ensure_ascii=True, indent=2), encoding="utf-8"
                )
            else:
                payload = {
                    "reason": reason,
                    "business": business,
                    "lead": store_graded,
                    "queue_bucket": bucket,
                }
                (fail_dir / f"{key}.precheck_failed.json").write_text(
                    json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8"
                )

            if pre_ok:
                promoted += 1
                print(f"  PROMOTE -> A: {business[:60]!r} ({key[:16]}…)")
            else:
                failed += 1
                print(
                    f"  precheck FAIL ({'routed' if args.route == 1 else 'kept in B'}): "
                    f"{business[:60]!r} ({reason}) fixed_fields={fixed_fields}"
                )
                if fixed_fields:
                    print("    changed:", ", ".join(fixed_fields))
        except Exception as e:
            print(f"  ERROR {old_path.name}: {type(e).__name__}: {e}")
            return 1

    if not args.dry_run:
        print(f"Done. promoted_to_A={promoted} updated_still_B={updated} precheck_fail={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
