"""
BBG Pipeline — reusable extraction logic.

Decoupled from the filesystem so the API can call it directly with raw
CSV bytes (drag-and-drop upload) without touching the disk for the source.

Used by:
  - src/api.py          (POST /api/bbg/upload)
  - scripts/bbg_extraction_advanced.py  (unchanged — still works standalone)
"""

from __future__ import annotations

import csv
import io
import os
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_GATEWAY      = os.environ.get("BANKST_GATEWAY", "http://100.82.94.80:7842")
BANKST_API_BASE  = f"{API_GATEWAY}/api/core"
MAPPING_API_BASE = f"{API_GATEWAY}/api/mapping"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize(s: Optional[str]) -> str:
    if s is None:
        return ""
    return s.strip().lower()


def _api_get(url: str) -> Any:
    resp = requests.get(url, headers={"Accept": "application/json"}, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Reference data loaders
# ---------------------------------------------------------------------------

def load_firm_aliases() -> Tuple[Dict[str, str], Dict[str, str], Dict[str, Set[str]], Dict[str, str]]:
    """
    Fetches firm data from BankSt API and builds lookup maps.

    Returns:
      alias_map     — normalized name/alias -> canonical name
      id_map        — normalized name/alias -> firm_id
      blacklist_map — firm_id -> set of normalized blacklisted names
      name_map      — firm_id -> canonical name
    """
    url       = f"{BANKST_API_BASE}/firms?include=aliases"
    firm_list = _api_get(url)

    alias_map:     Dict[str, str]      = {}
    id_map:        Dict[str, str]      = {}
    blacklist_map: Dict[str, Set[str]] = {}
    name_map:      Dict[str, str]      = {}

    for firm in firm_list:
        canonical = firm.get("name")
        firm_id   = firm.get("firm_id")
        if not canonical or not firm_id:
            continue

        name_map[firm_id]      = canonical
        blacklist_map[firm_id] = {normalize(n) for n in firm.get("blacklist", []) if n}

        all_names = [canonical] + firm.get("aliases", []) + firm.get("platforms", [])
        for name in all_names:
            if not name:
                continue
            norm = normalize(name)
            if norm not in alias_map:
                alias_map[norm] = canonical
            if norm not in id_map:
                id_map[norm] = firm_id

    return alias_map, id_map, blacklist_map, name_map


def load_hf_persons() -> Tuple[Dict[str, List[Dict]], List[Dict]]:
    """
    Fetches all active HF map records via HTTP and returns a name-keyed lookup.
    Use load_hf_persons_from_db() inside the API to avoid a self-call deadlock.
    """
    url     = f"{MAPPING_API_BASE}/hf/records"
    persons = _api_get(url)

    person_map: Dict[str, List[Dict]] = {}
    for p in persons:
        p["source_found"] = False
        name = p.get("name")
        if name:
            person_map.setdefault(normalize(name), []).append(p)

    return person_map, persons


def load_hf_persons_from_db(db_path: Path) -> Tuple[Dict[str, List[Dict]], List[Dict]]:
    """
    Reads all active HF map records directly from SQLite.

    Drop-in replacement for load_hf_persons() that avoids the HTTP round-trip
    (and the self-call deadlock when called from within the mapping-tools API).
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM records WHERE is_active = 1").fetchall()
    conn.close()

    person_map:  Dict[str, List[Dict]] = {}
    all_persons: List[Dict] = []
    for row in rows:
        p = dict(row)
        p["source_found"] = False
        all_persons.append(p)
        name = p.get("name")
        if name:
            person_map.setdefault(normalize(name), []).append(p)

    return person_map, all_persons


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_csv_columns(content: bytes) -> Tuple[bool, str]:
    """
    Checks that the CSV has the columns the pipeline requires.
    Returns (ok, error_message).
    """
    try:
        text    = content.decode("utf-8-sig", errors="replace")
        reader  = csv.reader(io.StringIO(text))
        headers = {h.strip() for h in next(reader)}
    except Exception as exc:
        return False, f"Could not parse CSV headers: {exc}"

    if "Company" not in headers:
        return False, f"Missing required column 'Company'. Found: {sorted(headers)}"

    has_name = "Name" in headers or ("First Name" in headers and "Last Name" in headers)
    if not has_name:
        return False, "Missing name column(s). Need 'Name' or both 'First Name' and 'Last Name'."

    return True, ""


def resolve_firm_from_filename(filename: str, id_map: Dict[str, str]) -> Optional[str]:
    """
    Derives a firm_id from a CSV filename.

    Rules (in order):
      1. Strip .csv extension
      2. Take the part before the first '_' (e.g. "alphadyne_20240318" -> "alphadyne")
      3. Normalize and look up in id_map

    Returns firm_id string, or None if not found.
    """
    stem     = Path(filename).stem        # "alphadyne_20240318" or "alphadyne"
    firm_key = stem.split("_")[0]         # "alphadyne"
    return id_map.get(normalize(firm_key))


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_csv(
    content:      bytes,
    csv_filename:  str,
    person_map:   Dict[str, List[Dict]],
    alias_map:    Dict[str, str],
    blacklist_set: Set[str],
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Runs the BBG matching logic against raw CSV bytes.

    Returns:
      confirmed      — list of hf_map record dicts (confirmed matches)
      disc_json      — structured discrepancy list (pre-flatten)
      additions      — list of addition dicts
    """
    text    = content.decode("utf-8-sig", errors="replace")
    lines   = text.splitlines()
    headers = [h.strip() for h in next(csv.reader(io.StringIO(lines[0])))]
    body    = "\n".join(lines[1:])
    rows    = csv.DictReader(io.StringIO(body), fieldnames=headers)

    confirmed: List[Dict] = []
    disc_json: List[Dict] = []
    additions: List[Dict] = []

    for row in rows:
        company = str(row.get("Company", "")).strip()
        if company and normalize(company) in blacklist_set:
            continue

        first = str(row.get("First Name", "")).strip()
        last  = str(row.get("Last Name",  "")).strip()
        name  = (
            f"{first} {last}".strip()
            if (first or last)
            else str(row.get("Name", "")).strip()
        )
        if not name:
            continue

        norm_name    = normalize(name)
        norm_company = normalize(company)
        canonical    = alias_map.get(norm_company)
        masters      = person_map.get(norm_name)

        if masters:
            firm_matches = [
                rec for rec in masters
                if (normalize(canonical) if canonical else norm_company)
                   == normalize(rec.get("firm", ""))
            ]

            if firm_matches:
                bbg_title    = row.get("Title", "").strip() or None
                bbg_location = row.get("Location", "").strip() or None
                bbg_focus    = row.get("Focus", "").strip() or None
                for r in firm_matches:
                    r["source_found"] = True
                    r["bbg_title"]    = bbg_title
                    r["bbg_location"] = bbg_location
                    r["bbg_focus"]    = bbg_focus
                confirmed.extend(firm_matches)
            else:
                all_firms = list({r.get("firm", "N/A") for r in masters})
                alias_msg = (
                    f"'{company}' (canonical: '{canonical}') matched none of: {all_firms}"
                    if canonical
                    else f"'{company}' (no alias found) matched none of: {all_firms}"
                )
                disc_json.append({
                    "name":               masters[0]["name"],
                    "master_record_uids": [r.get("id", "N/A") for r in masters],
                    "discrepancies": {"firm": {
                        "new_file_value":     company,
                        "master_file_values": all_firms,
                        "alias_check":        alias_msg,
                        "source_file":        csv_filename,
                    }},
                })
        elif canonical:
            additions.append({
                "name":              name,
                "company":           company,
                "canonical_company": canonical,
                "title":             row.get("Title"),
                "location":          row.get("Location"),
                "focus":             row.get("Focus"),
                "source_file":       csv_filename,
            })

    return confirmed, disc_json, additions


def flatten_discrepancies(disc_json: List[Dict]) -> List[Dict]:
    """Flattens structured discrepancy list into DB-ready rows, deduplicating."""
    today = str(date.today())
    seen: Dict[tuple, Dict] = {}

    for d in disc_json:
        uids = ", ".join(d["master_record_uids"])
        for field, details in d["discrepancies"].items():
            row = {
                "name":               d["name"],
                "master_record_uids": uids,
                "discrepancy_field":  field,
                "new_file_value":     details.get("new_file_value", "N/A"),
                "master_file_values": ", ".join(details.get("master_file_values", [])),
                "alias_check_info":   details.get("alias_check", "N/A"),
                "source_file":        details.get("source_file", "N/A"),
                "status":             "Active",
                "first_seen":         today,
            }
            key = (row["name"], row["master_record_uids"], row["discrepancy_field"], row["new_file_value"])
            if key not in seen:
                seen[key] = row

    return list(seen.values())
