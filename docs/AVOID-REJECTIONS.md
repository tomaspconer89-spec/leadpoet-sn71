# How to Avoid Lead Rejections (Subnet 71)

This doc summarizes **where and why** leads are rejected (gateway + validators) and the **best way to avoid** them: **pre-validate on the miner** before submission.

---

## 1. Validation pipeline (research summary)

Rejections happen in two places:

### A. Gateway (POST /submit)

Before a lead enters the validator queue, the gateway runs:

| Check | Rejection reason | Fix |
|-------|------------------|-----|
| **Required fields** | `missing_required_fields` | Send all of: business, full_name, first, last, email, role, website, industry, sub_industry, country, city, linkedin, company_linkedin, source_url, description, employee_count. For US: also state. |
| **Name sanity** | `name_invalid_chars`, `name_credential` | No commas, digits, parentheses in first/last/full_name. No credentials (MBA, PhD, III, Jr, Sr) in name fields. |
| **Role sanity** | `role_too_short`, `role_invalid_format`, etc. | Role 2–80 chars, no URLs, no company name in role, no degree abbreviations (MBA, CPA). |
| **Description** | `desc_too_short` | description ≥ 20 chars. |
| **Industry / sub_industry** | `invalid_sub_industry`, `invalid_industry_pairing` | sub_industry must be a key in `validator_models/industry_taxonomy.py`; industry must be in that sub_industry’s `industries` list. |
| **Location** | `country_empty`, `state_empty_for_usa`, `city_invalid_*` | US: country + state + city required; city/state must validate (geo). Non-US: country + city. Use standard names (e.g. "United States"). |
| **HQ** | `invalid_hq_location` | hq_country required; for US companies hq_state required. |
| **employee_count** | `invalid_employee_count` | Must be exactly one of: `"0-1"`, `"2-10"`, `"11-50"`, `"51-200"`, `"201-500"`, `"501-1,000"`, `"1,001-5,000"`, `"5,001-10,000"`, `"10,001+"`. |
| **Email format** | `invalid_email_format` | Valid `name@domain.com`; no free domains (gmail, yahoo, etc.). |
| **Email vs website** | `email_domain_mismatch` | Email domain must match website domain (same root). |
| **LinkedIn** | `invalid_linkedin_url`, `invalid_company_linkedin` | Person URL must contain `/in/`; company URL must contain `/company/` and not `/in/`. |
| **source_url** | — | If source_type is `proprietary_database`, source_url must be `proprietary_database`. No LinkedIn URL in source_url. |
| **Attestation** | — | wallet_ss58, terms_version_hash present; terms version current. |

### B. Validators (after gateway accepts)

Validators run automated checks (Stages 0–5). Consensus rejections count toward your **200 rejections/day** limit.

| Stage | What’s checked | Typical rejection |
|-------|-----------------|--------------------|
| **0** | Email regex, **name–email match**, **general-purpose email** (info@, hello@, support@…), **free/disposable domain** | Name not in email; info@/hello@; gmail/yahoo; disposable. |
| **1** | Domain age (&lt;7 days fails), MX, SPF/DMARC | Domain too new; no MX; weak DMARC. |
| **2** | DNSBL (blacklist) | Domain blacklisted. |
| **3** | TrueList email verification | Email invalid / catch-all / unknown. |
| **4** | LinkedIn (person): URL found, name/company/location/role match | url_not_found, name_not_found, company_not_found, location/role mismatch. |
| **5** | Company: LinkedIn company name, employee count, HQ, industry/description | company_name, employee_count, hq_country mismatch. |

---

## 2. Best solution: miner-side pre-validation

**Run the same checks you can do locally before submitting.** That way you:

- Don’t waste **submission attempts** (1000/day) on leads that will fail at the gateway.
- Don’t burn **rejection quota** (200/day) on validator consensus failures you could have avoided.

### What you can pre-check (no external API)

- Required fields present and non-empty.
- **Email format** (regex), **name–email match** (first or last name in email local part).
- **General-purpose prefixes**: reject info@, hello@, support@, contact@, team@, etc.
- **Free email domains**: reject gmail, yahoo, outlook, etc.
- **employee_count** in the exact allowed list.
- **industry** / **sub_industry** in `INDUSTRY_TAXONOMY` (sub_industry as key, industry in its `industries` list).
- **Role**: length 2–80, no URLs, no company name in role.
- **Name**: no commas, digits, parentheses; no credentials (MBA, PhD, Jr, Sr, etc.) in name fields.
- **Description** minimum length (e.g. ≥20 chars).
- **LinkedIn**: person URL contains `/in/`, company URL contains `/company/` and not `/in/`.
- **Location**: US has country + state + city; non-US has country + city.
- **Email domain vs website**: same root domain (if you have both).

### What you cannot pre-check (need validator/APIs)

- Domain age, MX, SPF/DMARC, DNSBL.
- TrueList result (valid/catch-all/invalid).
- LinkedIn scrape (person/company match).
- Stage 5 company name / employee count / HQ vs LinkedIn.

So: **pre-validate everything in the first list**; for the rest, only submit leads that already look like real B2B contacts (corporate email, plausible role, good description).

---

## 3. Using the built-in precheck (this repo)

The module **`miner_models/lead_precheck.py`** runs the checks above before submission. When **`USE_LEAD_PRECHECK=1`**, the miner filters out leads that fail so they are never sent to the gateway.

**Enable:**

```bash
export USE_LEAD_PRECHECK=1
./run-miner.sh
```

**Effect:** Fewer gateway rejections and fewer validator consensus rejections → fewer hits on the 200 rejections/day limit and better approval rate.

---

## 4. Quick reference: avoid these

| Avoid | Reason |
|-------|--------|
| Missing any required field | Gateway: missing_required_fields |
| info@, hello@, support@, contact@, team@ | Validator: general-purpose email |
| gmail.com, yahoo.com, outlook.com, etc. | Gateway / validator: free email domain |
| First or last name not in email | Validator: name_email_match |
| employee_count not in the 9 allowed values | Gateway: invalid_employee_count |
| sub_industry not in industry_taxonomy.py | Gateway: invalid_sub_industry |
| industry not in sub_industry’s industries list | Gateway: invalid_industry_pairing |
| US lead without state | Gateway: state_empty_for_usa |
| City/state/country invalid or inconsistent | Gateway: invalid_region_format |
| Role &lt; 2 chars or &gt; 80, or contains URL/company | Gateway: role_* |
| Name with digits, commas, credentials (MBA, Jr) | Gateway: name_* |
| Description &lt; 20 chars | Gateway: desc_too_short |
| LinkedIn person URL without /in/ or company URL without /company/ | Gateway: invalid_linkedin_url |
| source_url with "linkedin" in it | Gateway: LinkedIn not allowed in source_url |
| Email domain ≠ website domain | Gateway: email_domain_mismatch |

---

## 5. Rate limits (reminder)

- **1000 submission attempts/day** (all attempts, including duplicates and invalid).
- **200 rejections/day** (missing required fields + validator consensus rejections). When you hit 200, submissions are blocked until midnight UTC.

Pre-validation reduces both attempts on bad leads and rejections, so you stay under the limit and improve reward share.
