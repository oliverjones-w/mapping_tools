"""
BBG Extraction — Advanced Matcher
----------------------------------
Reads Bloomberg CSV exports from new/, compares against hf_map via API,
and writes results (confirmed, discrepancies, additions) to bbg_results.db.

Data sources (via API):
  - HF map records:  GET {MAPPING_API_BASE}/hf/records
  - Firm aliases:    GET {BANKST_API_BASE}/firms?include=aliases

Output:
  - bbg_results.db (project root) — full run history per firm
  - Source CSVs archived to {BBG_EXTRACTION_ROOT}/{firm_id}/archive/

Usage:
    python scripts/bbg_extraction_advanced.py

Config (env vars, all optional):
    BANKST_GATEWAY   — API gateway base URL (default: http://100.82.94.80:7842)
    BBG_ROOT         — path to BBG extraction folder (default: C:\\data_extractions\\bbg_extraction)
"""

import csv
import glob
import json
import os
import shutil
import sys
import warnings
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ---------------------------------------------------------------------------
# Path setup — add src/ so bbg_db is importable
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import bbg_db

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_GATEWAY      = os.environ.get("BANKST_GATEWAY", "http://100.82.94.80:7842")
BANKST_API_BASE  = f"{API_GATEWAY}/api/core"
MAPPING_API_BASE = f"{API_GATEWAY}/api/mapping"

BBG_EXTRACTION_ROOT = os.environ.get("BBG_ROOT", r"C:\data_extractions\bbg_extraction")
NEW_DATA_DIRECTORY  = os.path.join(BBG_EXTRACTION_ROOT, "new")
BBG_DB_PATH         = PROJECT_ROOT / "bbg_results.db"

# BBG CSV column → hf_map field (hf_map uses lowercase keys from SQLite)
COLUMN_MAPPING = {
    "Company":  "firm",
    "Title":    "title",
    "Location": "location",
    "Focus":    "focus",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_string(s: Optional[str]) -> str:
    if s is None:
        return ""
    return s.strip().lower()


def api_get(url: str) -> Any:
    resp = requests.get(url, headers={"Accept": "application/json"}, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Data loaders (API-backed)
# ---------------------------------------------------------------------------

def load_firm_aliases_map() -> Tuple[
    Optional[Dict[str, str]],
    Optional[Dict[str, str]],
    Optional[Dict[str, Set[str]]],
    Optional[Dict[str, str]],   # firm_id → canonical_name (for DB writes)
]:
    """
    Fetches firm data from BankSt API (/firms?include=aliases) and builds:
      alias_map          — normalized name/alias/platform → canonical name
      id_map             — normalized name/alias/platform → firm_id
      firm_blacklist_map — firm_id → set of normalized blacklisted names
      firm_name_map      — firm_id → canonical name (for storing in bbg_runs)
    """
    url = f"{BANKST_API_BASE}/firms?include=aliases"
    print(f"Loading firm aliases from {url} ...")
    try:
        firm_list = api_get(url)
    except Exception as e:
        print(f"Error fetching firm aliases: {e}")
        return None, None, None, None

    alias_map:          Dict[str, str]       = {}
    id_map:             Dict[str, str]       = {}
    firm_blacklist_map: Dict[str, Set[str]]  = {}
    firm_name_map:      Dict[str, str]       = {}
    total_blacklist_entries = 0

    for firm_obj in firm_list:
        canonical_name = firm_obj.get("name")      # was "canonical" in firm_aliases.json
        firm_id        = firm_obj.get("firm_id")   # was "id" in firm_aliases.json

        if not canonical_name or not firm_id:
            print(f"Warning: Skipping firm with missing 'name' or 'firm_id'.")
            continue

        firm_name_map[firm_id] = canonical_name

        # Per-firm blacklist
        firm_blacklist_map[firm_id] = set()
        for name in firm_obj.get("blacklist", []):
            if name:
                firm_blacklist_map[firm_id].add(normalize_string(name))
                total_blacklist_entries += 1

        # All names this firm is known by
        all_names = [canonical_name] + firm_obj.get("aliases", []) + firm_obj.get("platforms", [])

        for name in all_names:
            if not name:
                continue
            norm = normalize_string(name)

            if norm not in alias_map:
                alias_map[norm] = canonical_name
            elif alias_map[norm] != canonical_name:
                print(f"Warning (alias_map): '{name}' maps to multiple canonicals. Keeping '{alias_map[norm]}'.")

            if norm not in id_map:
                id_map[norm] = firm_id
            elif id_map[norm] != firm_id:
                print(f"Warning (id_map): '{name}' maps to multiple firm IDs. Keeping '{id_map[norm]}'.")

    print(
        f"Loaded {len(alias_map)} aliases, "
        f"{total_blacklist_entries} blacklist entries across {len(firm_blacklist_map)} firms."
    )
    return alias_map, id_map, firm_blacklist_map, firm_name_map


def load_hf_persons_map() -> Tuple[Optional[Dict[str, List[Dict]]], Optional[List[Dict]]]:
    """
    Fetches all active HF map records via the mapping API and builds a
    name → [records] lookup dict.

    Records have lowercase keys: id, firm, name, title, location,
    function, strategy, products, reports_to
    """
    url = f"{MAPPING_API_BASE}/hf/records"
    print(f"Loading HF map records from {url} ...")
    try:
        persons_list = api_get(url)
    except Exception as e:
        print(f"Error fetching HF map records: {e}")
        return None, None

    person_map: Dict[str, List[Dict]] = {}
    for p in persons_list:
        p["source_found"] = False
        name = p.get("name")
        if name:
            norm = normalize_string(name)
            if norm not in person_map:
                person_map[norm] = []
            person_map[norm].append(p)

    print(f"Loaded {len(persons_list)} HF records, {len(person_map)} unique names.")
    return person_map, persons_list


# ---------------------------------------------------------------------------
# Discrepancy helpers
# ---------------------------------------------------------------------------

def get_discrepancy_key(row: Dict) -> tuple:
    return (
        row.get("name"),
        row.get("master_record_uids"),
        row.get("discrepancy_field"),
        row.get("new_file_value"),
    )


def flatten_discrepancies(discrepancies_json: List[Dict]) -> Dict[tuple, Dict]:
    """
    Converts the structured discrepancy output from process_one_file into
    flat rows ready for DB insertion, deduplicating on (name, uids, field, value).
    """
    flat_map = {}
    today = str(date.today())

    for d in discrepancies_json:
        base_name = d["name"]
        base_uids = ", ".join(d["master_record_uids"])
        for field, details in d["discrepancies"].items():
            flat_row = {
                "name":               base_name,
                "master_record_uids": base_uids,
                "discrepancy_field":  field,
                "new_file_value":     details.get("new_file_value", "N/A"),
                "master_file_values": ", ".join(details.get("master_file_values", [])),
                "alias_check_info":   details.get("alias_check", "N/A"),
                "source_file":        details.get("source_file", "N/A"),
                "status":             "Active",
                "first_seen":         today,
            }
            key = get_discrepancy_key(flat_row)
            if key not in flat_map:
                flat_map[key] = flat_row

    return flat_map


# ---------------------------------------------------------------------------
# Core processing — comparison logic
# ---------------------------------------------------------------------------

def process_one_file(
    filepath: str,
    person_map: Dict[str, List[Dict]],
    alias_map: Dict[str, str],
    blacklist_set: Set[str],
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Processes a single BBG CSV against the HF map.
    Returns (confirmed_matches, discrepancies_json, additions).

    confirmed_matches  — list of hf_map record dicts (lowercase keys)
    discrepancies_json — structured list: [{name, master_record_uids, discrepancies:{field:{...}}}]
    additions          — list of dicts for people not in hf_map
    """
    if not os.path.exists(filepath):
        print(f"Error: File not found: {filepath}")
        return [], [], []

    print(f"Processing {filepath} ...")

    found_matches:       List[Dict] = []
    found_discrepancies: List[Dict] = []
    found_additions:     List[Dict] = []
    processed_count    = 0
    match_count        = 0
    discrepancy_count  = 0

    try:
        with open(filepath, "r", encoding="utf-8-sig", errors="replace") as f:
            reader  = csv.reader(f)
            headers = [h.strip() for h in next(reader)]
            reader  = csv.DictReader(f, fieldnames=headers)

            for row in reader:
                processed_count += 1

                new_company_name = str(row.get("Company", "")).strip()

                # Firm-specific blacklist check
                if new_company_name and normalize_string(new_company_name) in blacklist_set:
                    continue

                # Name resolution: prefer First Name + Last Name columns
                first = str(row.get("First Name", "")).strip()
                last  = str(row.get("Last Name",  "")).strip()
                new_name = (
                    f"{first} {last}".strip()
                    if (first or last)
                    else str(row.get("Name", "")).strip()
                )

                if not new_name:
                    print(f"Warning: Skipping row {processed_count} — name fields empty.")
                    continue

                normalized_name     = normalize_string(new_name)
                master_records_list = person_map.get(normalized_name)

                if master_records_list:
                    # ── Person exists in HF map — check firm matches ──────────
                    person_discrepancies: Dict = {}
                    has_discrepancy = False

                    normalized_new_company = normalize_string(new_company_name)
                    canonical_new_company  = alias_map.get(normalized_new_company)
                    confirmed_firm_matches = []

                    for master_record in master_records_list:
                        master_firm      = master_record.get("firm")   # lowercase key
                        norm_master_firm = normalize_string(master_firm)
                        firm_matches     = False

                        if canonical_new_company:
                            if normalize_string(canonical_new_company) == norm_master_firm:
                                firm_matches = True
                        else:
                            if normalized_new_company == norm_master_firm:
                                firm_matches = True

                        if firm_matches:
                            confirmed_firm_matches.append(master_record)

                    if confirmed_firm_matches:
                        found_matches.extend(confirmed_firm_matches)
                        match_count += 1
                        for record in confirmed_firm_matches:
                            record["source_found"] = True
                    else:
                        has_discrepancy  = True
                        all_master_firms = list(set(
                            mr.get("firm", "N/A") for mr in master_records_list
                        ))
                        alias_check_msg = (
                            f"'{new_company_name}' (canonical: '{canonical_new_company}') "
                            f"matched none of: {all_master_firms}"
                            if canonical_new_company
                            else f"'{new_company_name}' (no alias found) matched none of: {all_master_firms}"
                        )
                        person_discrepancies["firm"] = {
                            "new_file_value":    new_company_name,
                            "master_file_values": all_master_firms,
                            "alias_check":        alias_check_msg,
                            "source_file":        os.path.basename(filepath),
                        }

                    if has_discrepancy:
                        discrepancy_count += 1
                        first_record   = master_records_list[0]
                        all_master_ids = [mr.get("id", "N/A") for mr in master_records_list]
                        found_discrepancies.append({
                            "name":               first_record["name"],
                            "master_record_uids": all_master_ids,
                            "discrepancies":      person_discrepancies,
                        })

                else:
                    # ── Person not in HF map — addition if firm is known ─────
                    normalized_new_company = normalize_string(new_company_name)
                    canonical_new_company  = alias_map.get(normalized_new_company)

                    if canonical_new_company:
                        found_additions.append({
                            "name":              new_name,
                            "company":           new_company_name,
                            "canonical_company": canonical_new_company,
                            "title":             row.get("Title"),
                            "location":          row.get("Location"),
                            "focus":             row.get("Focus"),
                            "source_file":       os.path.basename(filepath),
                        })

    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return [], [], []

    print(
        f"  → {processed_count} rows: "
        f"{match_count} confirmed, "
        f"{discrepancy_count} discrepancies, "
        f"{len(found_additions)} additions"
    )
    return found_matches, found_discrepancies, found_additions


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    # Initialise DB (creates tables if they don't exist)
    bbg_db.init_db(BBG_DB_PATH)

    # Load reference data from APIs
    alias_map, id_map, firm_blacklist_map, firm_name_map = load_firm_aliases_map()
    person_map, _all_persons                              = load_hf_persons_map()

    if not alias_map or not id_map or firm_blacklist_map is None or person_map is None:
        print("Critical Error: Could not load reference data from API. Exiting.")
        return 1

    if not os.path.exists(NEW_DATA_DIRECTORY):
        print(f"Error: 'new' directory not found: {NEW_DATA_DIRECTORY}")
        return 1

    # ── File discovery ────────────────────────────────────────────────────────

    files_to_process:    List[str] = []
    firms_with_new_files: Set[str] = set()

    for file_path in glob.glob(os.path.join(NEW_DATA_DIRECTORY, "*.csv")):
        files_to_process.append(file_path)
        base            = os.path.splitext(os.path.basename(file_path))[0]
        firm_identifier = base.split("_")[0]
        firm_id         = id_map.get(normalize_string(firm_identifier), firm_identifier)
        if firm_id:
            firms_with_new_files.add(firm_id)

    # For firms with no new CSV, queue their latest archived CSV for a re-run
    all_firm_folders = [
        d for d in os.listdir(BBG_EXTRACTION_ROOT)
        if os.path.isdir(os.path.join(BBG_EXTRACTION_ROOT, d)) and d != "new"
    ]

    for firm_id in all_firm_folders:
        if firm_id in firms_with_new_files:
            print(f"Skipping archive check for {firm_id} (new file queued).")
            continue
        archive_folder = os.path.join(BBG_EXTRACTION_ROOT, firm_id, "archive")
        if not os.path.exists(archive_folder):
            continue
        archive_files = glob.glob(os.path.join(archive_folder, "*.csv"))
        if archive_files:
            latest = max(archive_files, key=os.path.getmtime)
            files_to_process.append(latest)
            print(f"Queueing latest archive for {firm_id}: {os.path.basename(latest)}")

    if not files_to_process:
        print("No CSV files found to process. Exiting.")
        return 0

    print(f"\nFound {len(files_to_process)} file(s) to process...")

    total_files        = 0
    total_confirmed    = 0
    total_discrepancies = 0
    total_additions    = 0

    # ── Main processing loop ──────────────────────────────────────────────────

    for csv_file_path in files_to_process:
        csv_filename = os.path.basename(csv_file_path)
        base_no_ext  = os.path.splitext(csv_filename)[0]
        is_new_file  = (
            os.path.normpath(os.path.dirname(csv_file_path))
            == os.path.normpath(NEW_DATA_DIRECTORY)
        )

        if is_new_file:
            firm_identifier = base_no_ext.split("_")[0]
        else:
            # Archive path: .../BBG_ROOT/{firm_id}/archive/filename.csv
            firm_identifier = os.path.basename(
                os.path.dirname(os.path.dirname(csv_file_path))
            )

        firm_id   = id_map.get(normalize_string(firm_identifier), firm_identifier)
        firm_name = firm_name_map.get(firm_id, firm_id.replace("_", " ").title())

        if not firm_id:
            print(f"Warning: No firm ID resolved for '{firm_identifier}'. Skipping '{csv_filename}'.")
            continue

        current_blacklist = firm_blacklist_map.get(firm_id, set())
        print(f"\n--- {csv_filename} (firm_id: {firm_id}, name: {firm_name}) ---")

        matches, discrepancies_json, additions_json = process_one_file(
            csv_file_path, person_map, alias_map, current_blacklist
        )

        discrepancy_rows = list(flatten_discrepancies(discrepancies_json).values())
        today_str = str(date.today())

        # Write run to DB
        run_id = bbg_db.create_run(
            db_path           = BBG_DB_PATH,
            firm_id           = firm_id,
            firm_name         = firm_name,
            csv_filename      = csv_filename,
            source_type       = "new" if is_new_file else "archive",
            rows_processed    = len(matches) + len(discrepancy_rows) + len(additions_json),
            confirmed_count   = len(matches),
            discrepancy_count = len(discrepancy_rows),
            addition_count    = len(additions_json),
        )

        # Confirmed records
        bbg_db.insert_confirmed(BBG_DB_PATH, run_id, [
            {
                "run_id":       run_id,
                "firm_id":      firm_id,
                "hf_record_id": r.get("id"),
                "name":         r.get("name"),
                "firm":         r.get("firm"),
                "title":        r.get("title"),
                "location":     r.get("location"),
                "function":     r.get("function"),
                "strategy":     r.get("strategy"),
                "products":     r.get("products"),
                "reports_to":   r.get("reports_to"),
            }
            for r in matches
        ])

        # Discrepancies
        bbg_db.insert_discrepancies(BBG_DB_PATH, run_id, [
            {**row, "run_id": run_id, "firm_id": firm_id}
            for row in discrepancy_rows
        ])

        # Additions
        bbg_db.insert_additions(BBG_DB_PATH, run_id, [
            {
                "run_id":           run_id,
                "firm_id":          firm_id,
                "name":             row["name"],
                "company":          row["company"],
                "canonical_company": row["canonical_company"],
                "title":            row.get("title"),
                "location":         row.get("location"),
                "focus":            row.get("focus"),
                "source_file":      row["source_file"],
                "first_seen":       today_str,
            }
            for row in additions_json
        ])

        total_confirmed     += len(matches)
        total_discrepancies += len(discrepancy_rows)
        total_additions     += len(additions_json)

        # Archive source CSV (new files only)
        firm_archive_folder = os.path.join(BBG_EXTRACTION_ROOT, firm_id, "archive")
        os.makedirs(firm_archive_folder, exist_ok=True)

        if is_new_file:
            try:
                base, ext = os.path.splitext(csv_filename)
                firm_part = base.split("_")[0]
                date_part = base.replace(f"{firm_part}_", "")
                try:
                    parsed           = datetime.strptime(date_part, "%Y%m%d")
                    new_archive_name = f"{parsed.strftime('%Y-%m-%d')}{ext}"
                except ValueError:
                    new_archive_name = f"{date_part or datetime.now().strftime('%Y-%m-%d')}{ext}"

                dest = os.path.join(firm_archive_folder, new_archive_name)
                if os.path.exists(dest):
                    print(f"Warning: Archive '{new_archive_name}' already exists. Skipping move.")
                else:
                    shutil.move(csv_file_path, dest)
                    print(f"Archived: {csv_filename} → {new_archive_name}")
            except Exception as e:
                print(f"Error archiving {csv_filename}: {e}")
        else:
            print(f"Re-run from archive — no move needed.")

        total_files += 1

    print(f"\n--- Grand Total ---")
    print(f"Files processed:  {total_files}")
    print(f"Confirmed:        {total_confirmed}")
    print(f"Discrepancies:    {total_discrepancies}")
    print(f"Additions:        {total_additions}")
    print(f"DB:               {BBG_DB_PATH}")
    return 0


if __name__ == "__main__":
    import traceback
    try:
        sys.exit(main() or 0)
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
    except Exception as e:
        print(f"\nUnhandled error: {e}")
        traceback.print_exc()
        sys.exit(1)
