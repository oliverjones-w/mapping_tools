import os
import glob
import json
import pandas as pd
import streamlit as st
from datetime import datetime

# --- Configuration ---

# === Hardcoded Paths (Active) ===
# These paths point to your central data store, just like your
# processing script. This ensures they are always looking at the same files.

# === Windows Paths (Commented Out) ===
CONFIG_ROOT = r"C:\obsidian-vault\config"
BBG_EXTRACTION_ROOT = r"C:\data_extractions\bbg_extraction"

FUNCTIONS_JSON_FILE = os.path.join(CONFIG_ROOT, "functions.json")

# --- File Definitions ---
# We build all file paths from the roots defined above

# --- REMOVED ALL_MATCHES_FILE ---

# Your other config files
FIRM_ALIASES_FILE = os.path.join(CONFIG_ROOT, 'firm_aliases.json')
MASTER_PERSONS_FILE = os.path.join(CONFIG_ROOT, 'master_names.json')


# --- Shared Helper Functions ---

@st.cache_data
def load_json_data(filepath: str):
    """Loads a JSON file."""
    if not os.path.exists(filepath):
        # We'll return None and let the page handle the error
        return None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Error loading {filepath}: {e}")
        return None

@st.cache_data
def get_id_to_canonical_map(aliases_file_path: str) -> dict:
    """Loads the firm aliases and builds a map of {id: canonical_name}."""
    aliases_data = load_json_data(aliases_file_path)
    if not isinstance(aliases_data, list):
        st.error(f"Aliases file at {aliases_file_path} is not a list.")
        return {}
    
    id_map = {}
    for firm_obj in aliases_data:
        firm_id = firm_obj.get("id")
        canonical_name = firm_obj.get("canonical")
        if firm_id and canonical_name:
            id_map[firm_id] = canonical_name
    return id_map

# --- REMOVED get_confirmed_counts_by_firm function ---

@st.cache_data
def get_all_firm_ids() -> list:
    """Scans the extraction root for all firm sub-folders."""
    try:
        entries = os.listdir(BBG_EXTRACTION_ROOT)
        firm_folders = [
            entry for entry in entries
            if os.path.isdir(os.path.join(BBG_EXTRACTION_ROOT, entry)) and entry != 'new'
        ]
        return sorted(firm_folders)
    except FileNotFoundError:
        return []

@st.cache_data
# --- UPDATED: Removed firm_count_map from arguments ---
def get_all_firm_metrics(id_to_name_map: dict) -> list:
    """
    Calculates metrics for ALL firms.
    This is for the main dashboard.
    """
    all_metrics = []
    firm_ids = get_all_firm_ids()
    
    for firm_id in firm_ids:
        canonical_name = id_to_name_map.get(firm_id, firm_id.replace('_', ' ').title())

        metrics = {
            "Firm": canonical_name,
            "Firm ID": firm_id, # Store the ID for lookups
            "Confirmed Headcount": 0, # Initialize to 0
            "Total Additions": 0,
            "Total Headcount": 0,
            "Active Discrepancies": 0,
            "Last Processed": "N/A"
        }
        
        # --- NEW LOGIC: Count rows from the new _matches.csv file ---
        confirmed_matches_file = os.path.join(BBG_EXTRACTION_ROOT, firm_id, "confirmed_matches", f"{firm_id}_matches.csv")
        if os.path.exists(confirmed_matches_file):
            try:
                df_c = pd.read_csv(confirmed_matches_file)
                metrics["Confirmed Headcount"] = df_c.shape[0]
            except pd.errors.EmptyDataError:
                pass # Keep 0 if file is empty
            except Exception as e:
                st.warning(f"Could not read matches file for {firm_id}: {e}")
        
        # Get Discrepancy Count
        discrepancy_file = os.path.join(BBG_EXTRACTION_ROOT, firm_id, "discrepancies", f"{firm_id}_discrepancies.csv")
        if os.path.exists(discrepancy_file):
            try:
                df_d = pd.read_csv(discrepancy_file)
                metrics["Active Discrepancies"] = df_d[df_d['Status'] == 'Active'].shape[0]
            except pd.errors.EmptyDataError: pass
        
        # Get Additions Count
        additions_file = os.path.join(BBG_EXTRACTION_ROOT, firm_id, "additions", f"{firm_id}_additions.csv")
        if os.path.exists(additions_file):
            try:
                df_a = pd.read_csv(additions_file)
                metrics["Total Additions"] = df_a.shape[0]
            except pd.errors.EmptyDataError: pass

        # Calculate Total Headcount
        metrics["Total Headcount"] = metrics["Confirmed Headcount"] + metrics["Total Additions"]

        # Get Last Processed Time
        archive_folder = os.path.join(BBG_EXTRACTION_ROOT, firm_id, "archive")
        if os.path.exists(archive_folder):
            archive_files = glob.glob(os.path.join(archive_folder, "*.csv"))
            if archive_files:
                latest_file = max(archive_files, key=os.path.getmtime)
                timestamp = os.path.getmtime(latest_file)
                metrics["Last Processed"] = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %I:%M:%S %p")
        
        all_metrics.append(metrics)
    
    return all_metrics

# -------- Functions mapping (Risk Taker classification) --------
import json, os, re
import pandas as pd
import streamlit as st

# Make sure this exists near your other paths:
FUNCTIONS_JSON_FILE = os.path.join(CONFIG_ROOT, "functions.json")

@st.cache_data(show_spinner=False)
def load_functions_map(path: str = FUNCTIONS_JSON_FILE) -> dict:
    """
    Load functions.json and return a dict keyed by canonical function (lowercased).
    Accepts:
      - list of objects: [{"Function":"Portfolio Manager","Risk Taker":true,...}, ...]
      - list of objects: [{"function":"PM","risk_taker":true,...}, ...]
      - dict: {"pm": {...}, "trader": {...}} (also {"functions":[...]})
    """
    if not path or not os.path.exists(path):
        return {}

    try:
        # allow // and /* */ comments if present
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read()
        txt = re.sub(r"/\*.*?\*/", "", txt, flags=re.S)
        txt = re.sub(r"//.*?$", "", txt, flags=re.M)
        raw = json.loads(txt)
    except Exception as e:
        st.error(f"Error reading functions.json: {e}")
        return {}

    # Unwrap common containers
    if isinstance(raw, dict) and "functions" in raw and isinstance(raw["functions"], list):
        raw = raw["functions"]

    out = {}
    if isinstance(raw, dict):
        # dict form: {"pm": {...}, "trader": {...}}
        for k, v in raw.items():
            out[str(k).strip().lower()] = v
    elif isinstance(raw, list):
        # list form with possibly capitalized keys
        for obj in raw:
            # accept Function / function / name
            name = (obj.get("Function") or obj.get("function") or obj.get("name") or "").strip()
            if not name:
                continue
            out[name.lower()] = obj
    return out

def attach_risk_flag(df: pd.DataFrame, func_map: dict) -> pd.DataFrame:
    """
    Add 'Risk Taker' and 'Function Order' columns based on functions.json.
    """
    if df is None or df.empty or "Function" not in df.columns:
        return df

    def lookup_meta(func_val):
        key = str(func_val or "").strip().lower()
        meta = func_map.get(key)
        if not isinstance(meta, dict):
            return None, None
        # Risk flag (any capitalization)
        risk_v = meta.get("risk_taker", meta.get("Risk Taker", meta.get("risk taker")))
        if isinstance(risk_v, str):
            risk_v = risk_v.strip().lower() in ("true", "1", "yes", "y")
        # Order (float or int)
        order_v = meta.get("Order") or meta.get("order")
        try:
            order_v = float(order_v)
        except Exception:
            order_v = None
        return risk_v, order_v

    out = df.copy()
    risk_vals, order_vals = zip(*[lookup_meta(v) for v in out["Function"]])
    out["Risk Taker"] = risk_vals
    out["Function Order"] = order_vals
    return out