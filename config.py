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

# The matches file you just moved
ALL_MATCHES_FILE = os.path.join(CONFIG_ROOT, 'all_bbg_master_records.json')

# Your other config files
FIRM_ALIASES_FILE = os.path.join(CONFIG_ROOT, 'firm_aliases.json')
MASTER_PERSONS_FILE = os.path.join(CONFIG_ROOT, 'master_names.json')


# --- Shared Helper Functions ---
# (The rest of your file stays exactly the same)
# ...