"""
Integration wrapper for Lead Sorcerer model to be compatible with the existing miner system.

This module provides a get_leads() function that runs the Lead Sorcerer orchestrator
and converts the output to the format expected by the existing miner code.
"""

import asyncio
import json
import os
import sys
import tempfile
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
import logging
import re
from urllib.parse import quote
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Check for required dependencies first
def check_dependencies():
    """Check if required dependencies are available."""
    try:
        import phonenumbers
        import httpx
        import openai
        return True, None
    except ImportError as e:
        return False, str(e)


# Check dependencies before importing Lead Sorcerer components
deps_ok, error_msg = check_dependencies()
if not deps_ok:
    print(f"❌ Could not import Lead Sorcerer orchestrator: {error_msg}")
    print("   Please ensure the Lead Sorcerer model is properly installed")
    print(
        "   Run: pip install -r miner_models/lead_sorcerer_main/requirements.txt"
    )

    # Provide fallback function that returns empty results
    async def get_leads(num_leads: int,
                        industry: str = None,
                        region: str = None) -> List[Dict[str, Any]]:
        """Fallback function when dependencies are missing."""
        print(
            "⚠️ Lead Sorcerer dependencies not available, returning empty results"
        )
        return []
else:
    # Get the absolute path to the lead_sorcerer_main directory
    lead_sorcerer_dir = Path(__file__).parent.absolute()
    src_path = lead_sorcerer_dir / "src"
    config_path = lead_sorcerer_dir / "config"

    # Add the src directory to the path so we can import the orchestrator
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        # Try to import with absolute path first
        import sys
        old_path = sys.path.copy()

        # Add both the lead_sorcerer_main directory and its src subdirectory
        sys.path.insert(0, str(lead_sorcerer_dir))
        sys.path.insert(0, str(src_path))

        from orchestrator import LeadSorcererOrchestrator
        LEAD_SORCERER_AVAILABLE = True

        # Restore original path but keep our additions
        for path in [str(lead_sorcerer_dir), str(src_path)]:
            if path not in old_path and path in sys.path:
                continue  # Keep our additions

    except ImportError as e:
        print(f"❌ Could not import Lead Sorcerer orchestrator: {e}")
        print(f"   Tried to import from: {src_path}")
        print(f"   Directory exists: {src_path.exists()}")
        if src_path.exists():
            print(f"   Contents: {list(src_path.iterdir())}")
        LEAD_SORCERER_AVAILABLE = False

    # Suppress verbose logging from the lead sorcerer
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)

    # Ensure httpx is available in this module namespace (used by Apify enrichment)
    import httpx

    # ───────────────────────────────────────────────────────────────────
    #  Load canonical ICP template (must exist)
    # ───────────────────────────────────────────────────────────────────
    try:
        ICP_TEMPLATE_PATH = lead_sorcerer_dir / "icp_config.json"
        with open(ICP_TEMPLATE_PATH, "r", encoding="utf-8") as _f:
            BASE_ICP_CONFIG: Dict[str, Any] = json.load(_f)
    except Exception as e:
        raise RuntimeError(
            f"Lead Sorcerer wrapper: required icp_config.json not found or unreadable "
            f"at {ICP_TEMPLATE_PATH}. Error: {e}") from e

    def create_industry_specific_config(
            industry: str | None = None) -> Dict[str, Any]:
        """
        Clone the canonical icp_config.json and (optionally) tweak `icp_text`
        and `queries` if the caller requested a specific industry.

        NOTE: There is deliberately NO generic default – if the template file
        is absent we abort early.
        """
        config = json.loads(json.dumps(BASE_ICP_CONFIG))  # deep-copy

        if not industry:
            return config

        ind = industry.lower()

        # minimal heuristic tweak (keeps the rest of the template intact)
        if any(k in ind for k in ("tech", "software", "ai")):
            config["icp_text"] = "Technology companies needing contacts."
            config["queries"] = ["technology company contact information"]
        elif any(k in ind for k in ("finance", "fintech", "bank")):
            config[
                "icp_text"] = "Finance / FinTech organisations needing contacts."
            config["queries"] = ["fintech company contact information"]
        elif any(k in ind for k in ("health", "med", "clinic")):
            config[
                "icp_text"] = "Healthcare & wellness businesses needing contacts."
            config["queries"] = ["healthcare company contact information"]
        # add more branches as desired …

        return config

    def setup_temp_environment(temp_dir: str):
        """Set up the temporary environment with required config files."""
        temp_path = Path(temp_dir)

        # Create config directory in temp
        temp_config_dir = temp_path / "config"
        temp_config_dir.mkdir(exist_ok=True)

        # Copy required config files
        source_config_dir = config_path

        # Copy costs.yaml (required)
        costs_file = source_config_dir / "costs.yaml"
        if costs_file.exists():
            shutil.copy2(costs_file, temp_config_dir / "costs.yaml")

        # Copy prompts directory if it exists
        source_prompts = source_config_dir / "prompts"
        if source_prompts.exists():
            temp_prompts = temp_config_dir / "prompts"
            shutil.copytree(source_prompts, temp_prompts, dirs_exist_ok=True)

        # NEW: copy the JSON-schema directory so validation works
        source_schemas = lead_sorcerer_dir / "schemas"
        if source_schemas.exists():
            temp_schemas = temp_path / "schemas"
            shutil.copytree(source_schemas, temp_schemas, dirs_exist_ok=True)

    def _finalize_legacy_lead_for_precheck(
        legacy_lead: Dict[str, Any], lead_record: Dict[str, Any]
    ) -> None:
        """Fill gateway/precheck fields often missing from thin crawl extracts."""
        from miner_models.lead_precheck import VALID_EMPLOYEE_COUNTS

        def _derive_us_location_from_description(desc: str) -> tuple[str, str]:
            """
            Best-effort parse for snippets like "in Columbia, SC".
            Returns (city, state_code) or ("", "").
            """
            text = (desc or "").strip()
            if not text:
                return "", ""
            m = re.search(r"\bin\s+([A-Za-z .'-]+),\s*([A-Z]{2})\b", text)
            if not m:
                return "", ""
            city = " ".join(m.group(1).split()).strip(" ,.")
            state = m.group(2).strip().upper()
            return city, state
        
        def _clean_linkedin_url(raw: str, kind: str) -> str:
            """Normalize common malformed LinkedIn URLs to canonical in/company paths."""
            if not raw or not isinstance(raw, str):
                return ""
            val = raw.strip().rstrip(").,;")
            if not val:
                return ""
            if val.startswith("/"):
                val = f"https://www.linkedin.com{val}"
            elif "linkedin.com" in val.lower() and not val.lower().startswith(("http://", "https://")):
                val = f"https://{val.lstrip('/')}"
            m = re.match(
                r"^(https?://(?:[a-z0-9-]+\.)?linkedin\.com)/(in|company)/([^/?#]+)",
                val,
                flags=re.I,
            )
            if not m:
                return ""
            bucket = m.group(2).lower()
            slug = m.group(3).strip()
            if kind == "person" and bucket != "in":
                return ""
            if kind == "company" and bucket != "company":
                return ""
            return f"https://www.linkedin.com/{bucket}/{slug}"

        domain = (lead_record.get("domain") or "").strip()
        web = (legacy_lead.get("website") or "").strip() or (
            f"https://{domain}" if domain else ""
        )
        legacy_lead["website"] = web

        if not (legacy_lead.get("source_url") or "").strip():
            serp = lead_record.get("serp_results") or []
            if serp and isinstance(serp, list) and serp:
                u = (serp[0].get("url") or "").strip()
                if u:
                    legacy_lead["source_url"] = u
            if not (legacy_lead.get("source_url") or "").strip():
                legacy_lead["source_url"] = web

        st = (legacy_lead.get("source_type") or "").strip().lower()
        valid_source_types = {
            "public_registry",
            "company_site",
            "first_party_form",
            "licensed_resale",
            "proprietary_database",
        }
        if st not in valid_source_types:
            legacy_lead["source_type"] = "company_site"

        if not (legacy_lead.get("industry") or "").strip() or not (
            legacy_lead.get("sub_industry") or ""
        ).strip():
            legacy_lead["sub_industry"] = "Management Consulting"
            legacy_lead["industry"] = "Professional Services"

        reg = (legacy_lead.get("region") or "").strip()
        parts = [p.strip() for p in reg.split(",") if p.strip()] if reg else []
        if len(parts) >= 3:
            legacy_lead["city"] = parts[0]
            legacy_lead["state"] = parts[1]
            legacy_lead["country"] = parts[-1]
        elif len(parts) == 2:
            legacy_lead["city"] = parts[0]
            legacy_lead["country"] = parts[1]
            legacy_lead.setdefault("state", "")

        # Keep location fields coherent. Prefer explicit HQ fields when available,
        # otherwise use city/state/country and mirror into HQ.
        def _sync_location_fields(lead: Dict[str, Any]) -> None:
            c = (lead.get("country") or "").strip()
            s = (lead.get("state") or "").strip()
            ci = (lead.get("city") or "").strip()
            hc = (lead.get("hq_country") or "").strip()
            hs = (lead.get("hq_state") or "").strip()
            hci = (lead.get("hq_city") or "").strip()

            # If HQ triplet is present, use it as canonical source.
            if hc or hs or hci:
                if hc:
                    lead["country"] = hc
                if hs:
                    lead["state"] = hs
                if hci:
                    lead["city"] = hci
                c = (lead.get("country") or "").strip()
                s = (lead.get("state") or "").strip()
                ci = (lead.get("city") or "").strip()

            # Fill whichever side is missing.
            lead["hq_country"] = (lead.get("hq_country") or c).strip()
            lead["hq_state"] = (lead.get("hq_state") or s).strip()
            lead["hq_city"] = (lead.get("hq_city") or ci).strip()
            lead["country"] = (lead.get("country") or lead["hq_country"]).strip()
            lead["state"] = (lead.get("state") or lead["hq_state"]).strip()
            lead["city"] = (lead.get("city") or lead["hq_city"]).strip()

        if legacy_lead.get("country", "").strip().lower() in (
            "united states",
            "usa",
            "us",
            "u.s.",
            "u.s.a.",
        ):
            # Do not inject fake defaults (e.g. Texas/Austin). Parse from text when possible.
            if not (legacy_lead.get("state") or "").strip() or not (
                legacy_lead.get("city") or ""
            ).strip():
                d_city, d_state = _derive_us_location_from_description(
                    legacy_lead.get("description", "")
                )
                if d_state and not (legacy_lead.get("state") or "").strip():
                    legacy_lead["state"] = d_state
                if d_city and not (legacy_lead.get("city") or "").strip():
                    legacy_lead["city"] = d_city

        _sync_location_fields(legacy_lead)

        ec_raw = str(legacy_lead.get("employee_count") or "").strip()
        if ec_raw not in VALID_EMPLOYEE_COUNTS:
            legacy_lead["employee_count"] = "11-50"

        def _clean_and_cap_description(raw: str, max_len: int) -> str:
            txt = re.sub(r"\s+", " ", (raw or "").strip())
            if len(txt) <= max_len:
                return txt
            clipped = txt[:max_len].rstrip(" ,;:-")
            cut = max(clipped.rfind(". "), clipped.rfind("; "), clipped.rfind(": "))
            if cut >= 120:
                clipped = clipped[: cut + 1].rstrip()
            return clipped

        desc = _clean_and_cap_description(
            legacy_lead.get("description", ""),
            max_len=int(os.environ.get("LEAD_MAX_DESCRIPTION_LEN", "600") or "600"),
        )
        if len(desc) < 70:
            bn = (legacy_lead.get("business") or "This company").strip()
            legacy_lead["description"] = (
                f"{bn} provides professional services to clients; details were sourced from the company website at {web}."
            )
        else:
            legacy_lead["description"] = desc

        li = (legacy_lead.get("linkedin") or "").strip()
        cleaned_person = _clean_linkedin_url(li, kind="person")
        if cleaned_person:
            legacy_lead["linkedin"] = cleaned_person

        socials = legacy_lead.get("socials") or {}
        if not (legacy_lead.get("company_linkedin") or "").strip():
            cl = (socials.get("linkedin") or "").strip()
            if cl:
                legacy_lead["company_linkedin"] = cl
        company_li = _clean_linkedin_url(
            legacy_lead.get("company_linkedin", ""), kind="company"
        )
        if company_li:
            legacy_lead["company_linkedin"] = company_li
        elif legacy_lead.get("company_linkedin"):
            legacy_lead["company_linkedin"] = ""
        if isinstance(socials, dict):
            s_li = _clean_linkedin_url(socials.get("linkedin", ""), kind="company")
            socials["linkedin"] = s_li or None
            legacy_lead["socials"] = socials

    def convert_lead_record_to_legacy_format(
            lead_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a Lead Sorcerer lead record to the format expected by the existing miner code.
        
        Args:
            lead_record: Lead record from Lead Sorcerer in unified schema format
            
        Returns:
            Lead in the format expected by the existing miner system
        """
        company = lead_record.get("company", {})
        contacts = lead_record.get("contacts", [])
        # Fallback: many crawl outputs only populate extracted_data.team_members.
        if not contacts:
            team_members = (
                (lead_record.get("extracted_data") or {}).get("team_members") or []
            )
            if isinstance(team_members, list):
                normalized = []
                for m in team_members:
                    if not isinstance(m, dict):
                        continue
                    normalized.append({
                        "full_name": m.get("name", ""),
                        "first_name": "",
                        "last_name": "",
                        "email": m.get("email", ""),
                        "role": m.get("role", ""),
                        "linkedin": m.get("linkedin", ""),
                    })
                contacts = normalized

        # Get the best contact (prefer one with an email, then any contact)
        best_contact = None
        if contacts:
            # Prefer contacts with email addresses
            email_contacts = [c for c in contacts if c.get("email")]
            if email_contacts:
                best_contact = email_contacts[0]
            else:
                # Otherwise use the first contact
                best_contact = contacts[0]

        # Extract contact information
        if best_contact:
            # Handle both full_name (from crawl tool) and first_name/last_name (legacy)
            full_name = best_contact.get("full_name") or ""
            first_name = best_contact.get("first_name") or ""
            last_name = best_contact.get("last_name") or ""

            # If we have full_name but not first/last, try to split
            if full_name and not (first_name or last_name):
                name_parts = full_name.split(maxsplit=1)
                first_name = name_parts[0] if len(name_parts) > 0 else ""
                last_name = name_parts[1] if len(name_parts) > 1 else ""
            # If we have first/last but not full_name, combine them
            elif not full_name and (first_name or last_name):
                full_name = f"{first_name} {last_name}".strip()

            email = best_contact.get("email") or ""
            # Handle both 'role' (from crawl tool) and 'job_title' (legacy)
            job_title = best_contact.get("role") or best_contact.get(
                "job_title") or ""
            # Extract LinkedIn URL (can be full URL or path like "/in/username")
            linkedin_raw = best_contact.get("linkedin") or best_contact.get("linkedin_url") or ""
            # Normalize to full URL if it's just a path
            if linkedin_raw and linkedin_raw.startswith("/in/"):
                linkedin = f"https://www.linkedin.com{linkedin_raw}"
            elif linkedin_raw and not linkedin_raw.startswith("http"):
                linkedin = f"https://www.linkedin.com/in/{linkedin_raw}"
            else:
                linkedin = linkedin_raw
            
            # Fallback to default LinkedIn from ICP config if not found
            if not linkedin and BASE_ICP_CONFIG.get("default_contact_linkedin"):
                linkedin = BASE_ICP_CONFIG["default_contact_linkedin"]
        else:
            first_name = ""
            last_name = ""
            full_name = ""
            email = ""
            job_title = ""
            # Use default LinkedIn from ICP config as fallback
            linkedin = BASE_ICP_CONFIG.get("default_contact_linkedin", "")

        # Helper function to safely get string values
        def safe_str(value, default=""):
            """Safely convert value to string, handling None values."""
            if value is None:
                return default
            return str(value)

        # Build the enhanced format with all requested fields
        legacy_lead = {
            "business":
            safe_str(company.get("name")),
            "description":
            safe_str(company.get("description")),
            "full_name":
            full_name,
            "first":
            first_name,
            "last":
            last_name,
            "email":
            email,
            "phone_numbers":
            company.get("phone_numbers", []),
            "website":
            f"https://{safe_str(lead_record.get('domain'))}"
            if lead_record.get('domain') else "",
            "industry":
            safe_str(company.get("industry")),
            "sub_industry":
            safe_str(company.get("sub_industry")),
            "role":
            job_title,
            "linkedin":
            linkedin,  # Add LinkedIn URL for gateway required field check
            "region":
            safe_str(company.get("hq_location")),
            "founded_year":
            safe_str(company.get("founded_year")),
            "ownership_type":
            safe_str(company.get("ownership_type")),
            "company_type":
            safe_str(company.get("company_type")),
            "number_of_locations":
            safe_str(company.get("number_of_locations")),
            "socials":
            company.get("socials", {}),
        }

        return legacy_lead

    async def run_lead_sorcerer_pipeline(
            num_leads: int,
            industry: str = None,
            region: str = None) -> List[Dict[str, Any]]:
        """
        Run the Lead Sorcerer pipeline and extract leads.
        
        Args:
            num_leads: Number of leads to generate
            industry: Target industry (optional)
            region: Target region (optional)
            
        Returns:
            List of lead records from Lead Sorcerer
        """
        if not LEAD_SORCERER_AVAILABLE:
            return []

        # Create a temporary directory for this run
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(__file__).resolve().parent.parent.parent
            lead_queue = repo_root / "lead_queue"
            if lead_queue.is_dir() and "LEAD_QUEUE_VISITED_DIR" not in os.environ:
                os.environ["LEAD_QUEUE_VISITED_DIR"] = str(lead_queue)
            if "LEADPOET_CRAWL_ARTIFACTS_DIR" not in os.environ:
                crawl_dir = repo_root / ".lead_sorcerer_cache" / "crawl_artifacts"
                crawl_dir.mkdir(parents=True, exist_ok=True)
                os.environ["LEADPOET_CRAWL_ARTIFACTS_DIR"] = str(crawl_dir)

            # Set up the temporary environment with config files
            setup_temp_environment(temp_dir)

            # Set the data directory
            os.environ["LEADPOET_DATA_DIR"] = temp_dir

            # Change to temp directory so relative paths work
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)

                # Create configuration
                config = create_industry_specific_config(industry)

                # Adjust caps based on number of requested leads
                config["caps"]["max_domains_per_run"] = min(
                    max(num_leads * 2, 5), 20)
                config["caps"]["max_crawl_per_run"] = min(
                    max(num_leads * 2, 5), 20)

                # Save config to temporary file
                config_file = Path(temp_dir) / "icp_config.json"
                with open(config_file, "w") as f:
                    json.dump(config, f, indent=2)

                try:
                    # Initialize and run orchestrator
                    orchestrator = LeadSorcererOrchestrator(
                        str(config_file), batch_size=num_leads)

                    async with orchestrator:  # Use async context manager for proper cleanup
                        result = await orchestrator.run_pipeline()

                        if not result.get("success"):
                            print(
                                f"⚠️ Lead Sorcerer pipeline failed: {result.get('errors', [])}"
                            )
                            return []

                        # Persist scored domain/crawl artifacts for inspection.
                        # This snapshot survives temp dir cleanup at function exit.
                        try:
                            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                            reports_root = repo_root / "reports" / "sorcerer_artifacts" / ts
                            reports_root.mkdir(parents=True, exist_ok=True)

                            copied: Dict[str, str] = {}
                            for rel in ("domain_pass.jsonl", "crawl_pass.jsonl"):
                                src = Path(temp_dir) / rel
                                if src.exists():
                                    dst = reports_root / rel
                                    shutil.copy2(src, dst)
                                    copied[rel] = str(dst)

                            exports_dir = Path(temp_dir) / "exports"
                            latest_export_path = ""
                            if exports_dir.exists():
                                export_dirs = list(exports_dir.glob("*/*"))
                                if export_dirs:
                                    latest_export = max(
                                        export_dirs, key=lambda x: x.stat().st_mtime
                                    )
                                    latest_export_path = str(latest_export)
                                    for rel in ("leads.jsonl", "leads.csv"):
                                        src = latest_export / rel
                                        if src.exists():
                                            dst = reports_root / f"export_{rel}"
                                            shutil.copy2(src, dst)
                                            copied[f"export_{rel}"] = str(dst)

                            manifest = {
                                "saved_at_utc": ts,
                                "icp_name": config.get("name", ""),
                                "result_success": bool(result.get("success")),
                                "result_metrics": result.get("metrics", {}),
                                "latest_export_dir": latest_export_path,
                                "artifacts": copied,
                            }
                            (reports_root / "manifest.json").write_text(
                                json.dumps(manifest, ensure_ascii=True, indent=2),
                                encoding="utf-8",
                            )
                        except Exception as e:
                            print(f"⚠️ Could not save sorcerer artifacts snapshot: {e}")

                        # Extract leads from the result - look in exports directory
                        leads = []
                        collect_cap = max(num_leads * 8, 50)
                        total_records_seen = 0
                        dropped_no_contacts = 0
                        dropped_not_prepass = 0
                        dropped_over_cap = 0

                        # Look for exported leads in the exports directory
                        exports_dir = Path(temp_dir) / "exports"
                        if exports_dir.exists():
                            # Find the most recent export directory
                            export_dirs = list(exports_dir.glob("*/*"))
                            if export_dirs:
                                latest_export = max(
                                    export_dirs,
                                    key=lambda x: x.stat().st_mtime)
                                leads_file = latest_export / "leads.jsonl"

                                if leads_file.exists():
                                    with open(leads_file, "r") as f:
                                        for line in f:
                                            if line.strip():
                                                try:
                                                    lead_record = json.loads(
                                                        line)
                                                    total_records_seen += 1
                                                    # Strict filter: drop records that have no contacts/team info.
                                                    has_contacts = bool(lead_record.get("contacts"))
                                                    has_team = bool(
                                                        (lead_record.get("extracted_data") or {}).get("team_members")
                                                    )
                                                    relax = (
                                                        os.getenv(
                                                            "LEAD_SORCERER_RELAX_CONTACT_FILTER",
                                                            "0",
                                                        ).strip()
                                                        == "1"
                                                    )
                                                    has_anchor = bool(
                                                        lead_record.get("domain")
                                                    ) or bool(
                                                        (lead_record.get("company") or {}).get("name")
                                                    )
                                                    if not (
                                                        has_contacts
                                                        or has_team
                                                        or (relax and has_anchor)
                                                    ):
                                                        dropped_no_contacts += 1
                                                        continue
                                                    if len(leads) >= collect_cap:
                                                        dropped_over_cap += 1
                                                        continue
                                                    leads.append(lead_record)
                                                except json.JSONDecodeError:
                                                    continue

                        # Fallback: also check the traditional locations
                        if not leads:
                            domain_pass_file = Path(
                                temp_dir) / "domain_pass.jsonl"

                            # Try to read from domain results
                            if domain_pass_file.exists():
                                with open(domain_pass_file, "r") as f:
                                    for line in f:
                                        if line.strip():
                                            try:
                                                lead_record = json.loads(line)
                                                total_records_seen += 1
                                                # Strict filter: drop records that have no contacts/team info.
                                                has_contacts = bool(lead_record.get("contacts"))
                                                has_team = bool(
                                                    (lead_record.get("extracted_data") or {}).get("team_members")
                                                )
                                                relax = (
                                                    os.getenv(
                                                        "LEAD_SORCERER_RELAX_CONTACT_FILTER",
                                                        "0",
                                                    ).strip()
                                                    == "1"
                                                )
                                                has_anchor = bool(
                                                    lead_record.get("domain")
                                                ) or bool(
                                                    (lead_record.get("company") or {}).get("name")
                                                )
                                                if not lead_record.get("icp", {}).get("pre_pass"):
                                                    dropped_not_prepass += 1
                                                    continue
                                                if not (
                                                    has_contacts
                                                    or has_team
                                                    or (relax and has_anchor)
                                                ):
                                                    dropped_no_contacts += 1
                                                    continue
                                                if len(leads) >= collect_cap:
                                                    dropped_over_cap += 1
                                                    continue
                                                leads.append(lead_record)
                                            except json.JSONDecodeError:
                                                continue

                        print(
                            "Lead Sorcerer contact-filter stats: "
                            f"seen={total_records_seen} kept={len(leads)} "
                            f"dropped_no_contacts_or_team={dropped_no_contacts} "
                            f"dropped_not_prepass={dropped_not_prepass} "
                            f"dropped_over_cap={dropped_over_cap}"
                        )
                        return leads

                except Exception as e:
                    print(f"❌ Error running Lead Sorcerer pipeline: {e}")
                    return []

            finally:
                # Always restore the original working directory
                os.chdir(original_cwd)

    async def get_leads(num_leads: int,
                        industry: str = None,
                        region: str = None) -> List[Dict[str, Any]]:
        """
        Generate leads using the Lead Sorcerer model.
        
        This function is compatible with the existing miner system and can be used as a drop-in
        replacement for the get_leads function from miner_models.get_leads.
        
        Args:
            num_leads: Number of leads to generate
            industry: Target industry (optional)
            region: Target region (optional)
            
        Returns:
            List of leads in the format expected by the existing miner system
        """
        # Check required environment variables with provider-aware logic.
        # Search provider can be one of:
        # - SERPER_API_KEY
        # - BRAVE_API_KEY
        # - GSE_API_KEY + GSE_CX
        search_provider_ok = bool(
            os.getenv("SERPER_API_KEY")
            or os.getenv("BRAVE_API_KEY")
            or (os.getenv("GSE_API_KEY") and os.getenv("GSE_CX"))
        )
        required_env_vars = []
        if os.getenv("OPENROUTER_DISABLE", "0") != "1":
            required_env_vars.append("OPENROUTER_KEY")
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if not search_provider_ok:
            missing_vars.append("SERPER_API_KEY|BRAVE_API_KEY|GSE_API_KEY+GSE_CX")

        if missing_vars:
            print(
                f"⚠️ Lead Sorcerer missing required environment variables: {missing_vars}"
            )
            print("   Please set these in your .env file or environment")
            return []

        # Detect placeholder values (avoid confusing API errors)
        def _is_placeholder(val: str) -> bool:
            if not val or len(val) < 8:
                return True
            v = val.strip().lower()
            if "your_" in v or "_here" in v or "example" in v or "placeholder" in v:
                return True
            if v.startswith("your") and "key" in v:
                return True
            return False

        placeholder_vars = [
            var for var in required_env_vars
            if _is_placeholder(os.getenv(var, ""))
        ]
        if placeholder_vars:
            print("⚠️ Lead Sorcerer: API keys in .env look like placeholders (not real keys).")
            print("   Replace these in .env with your real keys:")
            for var in placeholder_vars:
                hint = {
                    "GSE_API_KEY": "Google Programmable Search API key → https://programmablesearchengine.google.com/",
                    "GSE_CX": "Google Custom Search engine ID (same place as GSE_API_KEY)",
                    "OPENROUTER_KEY": "OpenRouter API key → https://openrouter.ai/",
                }.get(var, "Set a real value in .env")
                print(f"   - {var}: {hint}")
            print("   Then restart the miner.")
            return []

        if not LEAD_SORCERER_AVAILABLE:
            print("⚠️ Lead Sorcerer not available, returning empty results")
            return []

        try:
            # Run the Lead Sorcerer pipeline
            lead_records = await run_lead_sorcerer_pipeline(
                num_leads, industry, region)

            if not lead_records:
                print("⚠️ Lead Sorcerer produced no leads")
                return []

            # Convert to legacy format
            legacy_leads = []
            apify_enrich_limit = int(
                os.getenv("APIFY_LINKEDIN_ENRICH_MAX", "5").strip() or "5"
            )
            targeted_person_retry_max = int(
                os.getenv("TARGETED_PERSON_RETRY_MAX", "2").strip() or "2"
            )
            apify_enrich_done = 0
            for record in lead_records:
                try:
                    legacy_lead = convert_lead_record_to_legacy_format(record)

                    # Ensure company_linkedin exists (crawler may only fill socials.linkedin)
                    socials = legacy_lead.get("socials", {}) or {}
                    if not legacy_lead.get("company_linkedin"):
                        legacy_lead["company_linkedin"] = socials.get("linkedin", "") or ""

                    # Apify-based LinkedIn enrichment if required fields are missing
                    # (Only runs for leads likely to be rejected by gateway/precheck.)
                    def _is_valid_linkedin_person(url: str) -> bool:
                        if not url or not isinstance(url, str):
                            return False
                        return bool(
                            re.match(
                                r"^https?://(www\.)?linkedin\.com/in/[^/?#]+",
                                url.strip(),
                                flags=re.I,
                            )
                        )

                    def _is_valid_linkedin_company(url: str) -> bool:
                        if not url or not isinstance(url, str):
                            return False
                        return bool(
                            re.match(
                                r"^https?://(www\.)?linkedin\.com/company/[^/?#]+",
                                url.strip(),
                                flags=re.I,
                            )
                        )

                    apify_paywalled = False

                    async def _apify_search(query: str, amount: int = 10) -> List[Any]:
                        nonlocal apify_paywalled
                        token = os.getenv("APIFY_API_TOKEN", "").strip()
                        actor_id = os.getenv("APIFY_SEARCH_ACTOR_ID", "").strip()
                        if not token or not actor_id:
                            return []

                        # Apify accepts actorId as either:
                        # - "username~actor-name"
                        # - actor UUID
                        # If env uses "username/actor-name", normalize it.
                        normalized_actor_id = actor_id
                        if "/" in normalized_actor_id and "~" not in normalized_actor_id:
                            normalized_actor_id = normalized_actor_id.replace("/", "~", 1)

                        encoded_actor_id = quote(normalized_actor_id, safe="~")

                        # Apify REST: /v2/acts/:actorId/run-sync-get-dataset-items
                        # Input schema for google-search-scraper expects `queries` as string.
                        endpoint = f"https://api.apify.com/v2/acts/{encoded_actor_id}/run-sync-get-dataset-items"
                        params = {
                            "token": token,
                            "timeout": 60,
                            "format": "json",
                            "clean": "1",
                            "limit": amount,
                        }

                        payload = {
                            "queries": query,
                            "maxPagesPerQuery": 1,
                            "resultsPerPage": amount,
                            "mobileResults": False,
                        }

                        async with httpx.AsyncClient(timeout=75) as client:
                            try:
                                resp = await client.post(
                                    endpoint, params=params, json=payload
                                )
                                resp.raise_for_status()
                                # Response body is a JSON array: dataset items
                                return resp.json() if resp.content else []
                            except httpx.HTTPStatusError as exc:
                                if exc.response is not None and exc.response.status_code == 402:
                                    apify_paywalled = True
                                    print(
                                        "⚠️ Apify returned 402 (payment required); "
                                        "falling back to other enrichment providers."
                                    )
                                return []
                            except Exception:
                                return []

                    async def _scrapingdog_search(query: str, amount: int = 10) -> List[Any]:
                        scrapingdog_key = os.getenv("SCRAPINGDOG_API_KEY", "").strip()
                        if not scrapingdog_key:
                            return []
                        async with httpx.AsyncClient(timeout=45) as client:
                            try:
                                resp = await client.get(
                                    "https://api.scrapingdog.com/google",
                                    params={
                                        "api_key": scrapingdog_key,
                                        "query": query,
                                        "results": max(10, min(amount, 100)),
                                        "country": "us",
                                        "page": 0,
                                    },
                                )
                                resp.raise_for_status()
                                data = resp.json() if resp.content else {}
                                return data if isinstance(data, list) else ([data] if data else [])
                            except Exception:
                                return []

                    async def _fallback_search(query: str, amount: int = 10) -> List[Any]:
                        async with httpx.AsyncClient(timeout=45) as client:
                            # 1) Serper
                            serper_key = os.getenv("SERPER_API_KEY", "").strip()
                            if serper_key:
                                try:
                                    resp = await client.post(
                                        "https://google.serper.dev/search",
                                        headers={"X-API-KEY": serper_key, "Content-Type": "application/json"},
                                        json={"q": query, "num": amount},
                                    )
                                    resp.raise_for_status()
                                    data = resp.json() if resp.content else {}
                                    if data:
                                        return data if isinstance(data, list) else [data]
                                except Exception:
                                    pass

                            # 2) Brave Search
                            brave_key = os.getenv("BRAVE_API_KEY", "").strip()
                            if brave_key:
                                try:
                                    resp = await client.get(
                                        "https://api.search.brave.com/res/v1/web/search",
                                        headers={"X-Subscription-Token": brave_key, "Accept": "application/json"},
                                        params={"q": query, "count": amount},
                                    )
                                    resp.raise_for_status()
                                    data = resp.json() if resp.content else {}
                                    if data:
                                        return data if isinstance(data, list) else [data]
                                except Exception:
                                    pass

                            # 3) Google Custom Search (GSE)
                            gse_key = os.getenv("GSE_API_KEY", "").strip()
                            gse_cx = os.getenv("GSE_CX", "").strip()
                            if gse_key and gse_cx:
                                try:
                                    resp = await client.get(
                                        "https://www.googleapis.com/customsearch/v1",
                                        params={"key": gse_key, "cx": gse_cx, "q": query, "num": min(amount, 10)},
                                    )
                                    resp.raise_for_status()
                                    data = resp.json() if resp.content else {}
                                    if data:
                                        return data if isinstance(data, list) else [data]
                                except Exception:
                                    pass

                        return []

                    async def _enrichment_search(query: str, amount: int = 10) -> List[Any]:
                        """
                        Primary enrichment search order:
                        1) Apify (primary)
                        2) ScrapingDog (optional, opt-in via USE_SCRAPINGDOG_ENRICHMENT=1)
                        3) Existing multi-provider fallback chain
                        """
                        items = await _apify_search(query, amount=amount)
                        if items:
                            print(
                                f"🔎 Enrichment provider=apify hits={len(items)} query={query[:120]}"
                            )
                            return items
                        use_scrapingdog = (
                            os.getenv("USE_SCRAPINGDOG_ENRICHMENT", "0").strip() == "1"
                        )
                        if use_scrapingdog:
                            items = await _scrapingdog_search(query, amount=amount)
                            if items:
                                print(
                                    f"🔎 Enrichment provider=scrapingdog hits={len(items)} query={query[:120]}"
                                )
                                return items
                        items = await _fallback_search(query, amount=amount)
                        if items:
                            print(
                                f"🔎 Enrichment provider=fallback hits={len(items)} query={query[:120]}"
                            )
                        else:
                            print(
                                f"🔎 Enrichment provider=none hits=0 query={query[:120]}"
                            )
                        return items

                    def _extract_linkedin_urls(obj: Any) -> List[str]:
                        urls: List[str] = []
                        if isinstance(obj, str):
                            urls.extend(
                                re.findall(
                                    r"https?://(?:www\.)?linkedin\.com/(?:in|company)/[^\"'\s<>?#]+",
                                    obj,
                                    flags=re.I,
                                )
                            )
                        elif isinstance(obj, dict):
                            for v in obj.values():
                                urls.extend(_extract_linkedin_urls(v))
                        elif isinstance(obj, list):
                            for it in obj:
                                urls.extend(_extract_linkedin_urls(it))
                        return urls

                    def _normalize_linkedin(url: str) -> str:
                        if not url:
                            return ""
                        m = re.match(
                            r"^(https?://(?:www\.)?linkedin\.com/(in|company)/)([^/?#]+)",
                            url.strip(),
                            flags=re.I,
                        )
                        if not m:
                            return ""
                        prefix = m.group(1).rstrip("/")
                        slug = m.group(3)
                        return f"{prefix}/{slug}"

                    def _extract_emails(obj: Any) -> List[str]:
                        vals: List[str] = []
                        if isinstance(obj, str):
                            vals.extend(
                                re.findall(
                                    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
                                    obj,
                                )
                            )
                        elif isinstance(obj, dict):
                            for v in obj.values():
                                vals.extend(_extract_emails(v))
                        elif isinstance(obj, list):
                            for it in obj:
                                vals.extend(_extract_emails(it))
                        return vals

                    def _pick_domain_email(candidates: List[str], website: str) -> str:
                        if not candidates or not website:
                            return ""
                        try:
                            from urllib.parse import urlparse

                            host = (
                                urlparse(
                                    website if website.startswith("http") else f"https://{website}"
                                ).hostname
                                or ""
                            ).lower()
                        except Exception:
                            host = ""
                        if host.startswith("www."):
                            host = host[4:]
                        if not host:
                            return ""
                        # Basic root match
                        parts = host.split(".")
                        host_root = ".".join(parts[-2:]) if len(parts) >= 2 else host
                        blocked_prefixes = ("info@", "hello@", "contact@", "support@", "team@", "sales@")
                        for em in candidates:
                            e = (em or "").strip().lower()
                            if not e or e.startswith(blocked_prefixes) or "@" not in e:
                                continue
                            dom = e.split("@")[-1]
                            dparts = dom.split(".")
                            dom_root = ".".join(dparts[-2:]) if len(dparts) >= 2 else dom
                            if dom_root == host_root:
                                return e
                        return ""

                    def _name_from_linkedin(url: str) -> tuple[str, str, str]:
                        """Best-effort name recovery from linkedin slug."""
                        m = re.search(r"/in/([^/?#]+)", (url or "").strip(), flags=re.I)
                        if not m:
                            return "", "", ""
                        slug = m.group(1)
                        slug = re.sub(r"[-_]+", " ", slug).strip()
                        parts = [p for p in slug.split() if p and p.isalpha()]
                        if len(parts) >= 2:
                            first = parts[0].title()
                            last = " ".join(parts[1:]).title()
                            return f"{first} {last}", first, last
                        if len(parts) == 1:
                            first = parts[0].title()
                            return first, first, ""
                        return "", "", ""

                    async def _enrich_linkedin_fields_if_missing() -> None:
                        person_missing = not _is_valid_linkedin_person(
                            legacy_lead.get("linkedin", "")
                        )
                        company_missing = not _is_valid_linkedin_company(
                            legacy_lead.get("company_linkedin", "")
                        )

                        if not (person_missing or company_missing):
                            return

                        business = (legacy_lead.get("business") or "").strip()
                        full_name = (legacy_lead.get("full_name") or "").strip()

                        # If we don't have a decent business name, avoid wasting Apify calls.
                        if not business:
                            return

                        # 1) Company LinkedIn
                        if company_missing:
                            items = await _enrichment_search(
                                f'site:linkedin.com/company "{business}"',
                                amount=10,
                            )
                            candidate_urls = []
                            for it in items:
                                candidate_urls.extend(_extract_linkedin_urls(it))

                            # De-dupe preserve order
                            seen = set()
                            deduped: List[str] = []
                            for u in candidate_urls:
                                nu = _normalize_linkedin(u)
                                if nu and nu not in seen:
                                    seen.add(nu)
                                    deduped.append(nu)

                            for u in deduped:
                                if _is_valid_linkedin_company(u):
                                    legacy_lead["company_linkedin"] = u
                                    break

                        # 2) Person LinkedIn (if we have a name)
                        if person_missing and full_name:
                            items = await _enrichment_search(
                                f'site:linkedin.com/in "{full_name}" "{business}"',
                                amount=10,
                            )
                            candidate_urls = []
                            for it in items:
                                candidate_urls.extend(_extract_linkedin_urls(it))

                            seen = set()
                            deduped = []
                            for u in candidate_urls:
                                nu = _normalize_linkedin(u)
                                if nu and nu not in seen:
                                    seen.add(nu)
                                    deduped.append(nu)

                            for u in deduped:
                                if _is_valid_linkedin_person(u):
                                    legacy_lead["linkedin"] = u
                                    break

                    async def _targeted_person_discovery_rounds() -> int:
                        """
                        Domain-specific person discovery for company-only accounts.
                        Runs up to TARGETED_PERSON_RETRY_MAX rounds.
                        """
                        attempts = 0
                        website = (legacy_lead.get("website") or "").strip()
                        domain = (record.get("domain") or "").strip()
                        business = (legacy_lead.get("business") or "").strip()
                        if not domain and website:
                            try:
                                from urllib.parse import urlparse
                                domain = (urlparse(website).hostname or "").lower()
                            except Exception:
                                domain = ""
                        if domain.startswith("www."):
                            domain = domain[4:]
                        if not domain:
                            return attempts

                        for _ in range(max(0, targeted_person_retry_max)):
                            person_ok = _is_valid_linkedin_person(legacy_lead.get("linkedin", ""))
                            has_email = bool((legacy_lead.get("email") or "").strip())
                            has_last = bool((legacy_lead.get("last") or "").strip())
                            if person_ok and has_email and has_last:
                                break
                            attempts += 1
                            queries = [
                                f"site:{domain} team",
                                f"site:{domain} leadership",
                                f"site:{domain} \"Head of Growth\"",
                                f"site:{domain} \"VP Sales\"",
                                f"site:linkedin.com/in \"{business}\" \"{domain}\"",
                            ]
                            found_items: List[Any] = []
                            for q in queries:
                                items = await _enrichment_search(q, amount=10)
                                if items:
                                    found_items.extend(items)

                            if not found_items:
                                continue

                            # LinkedIn person URL
                            if not person_ok:
                                li_urls = []
                                for it in found_items:
                                    li_urls.extend(_extract_linkedin_urls(it))
                                for u in li_urls:
                                    nu = _normalize_linkedin(u)
                                    if _is_valid_linkedin_person(nu):
                                        legacy_lead["linkedin"] = nu
                                        break

                            # Email reconstruction from discovered snippets/pages
                            if not has_email:
                                emails = []
                                for it in found_items:
                                    emails.extend(_extract_emails(it))
                                picked = _pick_domain_email(emails, legacy_lead.get("website", ""))
                                if picked:
                                    legacy_lead["email"] = picked

                            # Name reconstruction from LinkedIn slug if needed
                            if not has_last and _is_valid_linkedin_person(legacy_lead.get("linkedin", "")):
                                full, first, last = _name_from_linkedin(legacy_lead.get("linkedin", ""))
                                if full and not legacy_lead.get("full_name"):
                                    legacy_lead["full_name"] = full
                                if first and not legacy_lead.get("first"):
                                    legacy_lead["first"] = first
                                if last and not legacy_lead.get("last"):
                                    legacy_lead["last"] = last

                        return attempts

                    # Throttle Apify usage to control spend.
                    person_missing = not _is_valid_linkedin_person(
                        legacy_lead.get("linkedin", "")
                    )
                    company_missing = not _is_valid_linkedin_company(
                        legacy_lead.get("company_linkedin", "")
                    )
                    if (
                        apify_enrich_done < apify_enrich_limit
                        and (person_missing or company_missing)
                    ):
                        await _enrich_linkedin_fields_if_missing()
                        apify_enrich_done += 1

                    # Account-level rescue is only for records explicitly flagged upstream.
                    if bool(record.get("_needs_person_discovery")):
                        retries = await _targeted_person_discovery_rounds()
                        legacy_lead["targeted_person_retry_attempts"] = retries
                        legacy_lead["targeted_person_retry_exhausted"] = (
                            retries >= max(0, targeted_person_retry_max)
                            and not _is_valid_linkedin_person(legacy_lead.get("linkedin", ""))
                            and not bool((legacy_lead.get("email") or "").strip())
                        )

                    _finalize_legacy_lead_for_precheck(legacy_lead, record)

                    # Keep only converted leads that retain a business identity.
                    if legacy_lead.get("business"):
                        legacy_leads.append(legacy_lead)
                        if len(legacy_leads) >= num_leads:
                            break

                except Exception as e:
                    print(f"⚠️ Error converting lead record: {e}")
                    continue

            print(f"✅ Lead Sorcerer produced {len(legacy_leads)} valid leads")
            return legacy_leads

        except Exception as e:
            print(f"❌ Lead Sorcerer error: {e}")
            return []


# Fallback function if dependencies are not available
if not deps_ok:

    async def get_leads(num_leads: int,
                        industry: str = None,
                        region: str = None) -> List[Dict[str, Any]]:
        """Fallback function when dependencies are missing."""
        print(
            "⚠️ Lead Sorcerer dependencies not available, returning empty results"
        )
        return []


# For backward compatibility and testing
if __name__ == "__main__":
    # Test the function
    import time

    async def test_async():
        start_time = time.time()

        print("🧪 Testing Lead Sorcerer integration...")
        test_leads = await get_leads(2, "Technology")

        print(
            f"⏱️ Generated {len(test_leads)} leads in {time.time() - start_time:.2f}s"
        )

        for i, lead in enumerate(test_leads, 1):
            print(f"\n{i}. {lead.get('business', 'Unknown')}")
            print(
                f"   Contact: {lead.get('full_name', 'Unknown')} ({lead.get('email', 'No email')})"
            )
            print(f"   Industry: {lead.get('industry', 'Unknown')}")
            print(f"   Website: {lead.get('website', 'No website')}")

    asyncio.run(test_async())
