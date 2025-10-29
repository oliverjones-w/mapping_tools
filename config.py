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

# Root for config files
CONFIG_ROOT = r"/mnt/c/obsidian-vault/config"

# Root for data extractions
BBG_EXTRACTION_ROOT = r"/mnt/c/data_extractions/bbg_extraction"

# === Windows Paths (Commented Out) ===
# CONFIG_ROOT = r"C:\obsidian-vault\config"
# BBG_EXTRACTION_ROOT = r"C:\data_extractions\bbg_extraction"


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