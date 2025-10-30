from __future__ import annotations
import os, re, json
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import boto3
import pandas as pd
import streamlit as st

# =========================================================
# 0) Paths (local folders)
# =========================================================
BBG_EXTRACTION_ROOT = Path(r"/mnt/c/data_extractions/bbg_extraction")

# =========================================================
# 1) S3 Config
# =========================================================
def _s3():
    """Create an S3 client using credentials from Streamlit secrets."""
    return boto3.client(
        "s3",
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
        region_name=st.secrets["AWS_REGION"],
    )

@st.cache_data(show_spinner=False)
def load_json_from_s3(key: str):
    """Download and parse a JSON file from S3."""
    s3 = _s3()
    bucket = st.secrets["S3_BUCKET"]
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

# === Load JSONs from S3 ===
FIRM_ALIASES_DATA   = load_json_from_s3(st.secrets["S3_KEY_FIRM_ALIASES"])
MASTER_NAMES_DATA   = load_json_from_s3(st.secrets["S3_KEY_MASTER_NAMES"])
FUNCTIONS_JSON_DATA = load_json_from_s3(st.secrets["S3_KEY_FUNCTIONS"])

# =========================================================
# 2) Constants
# =========================================================
SUBDIRS: Dict[str, str] = {
    "confirmed": "confirmed_matches",
    "discrepancies": "discrepancies",
    "additions": "additions",
}
FILE_PATTERNS: Dict[str, str] = {
    "confirmed": "{fid}_matches.csv",
    "discrepancies": "{fid}_discrepancies.csv",
    "additions": "{fid}_additions.csv",
}
TABS = ("Confirmed", "Discrepancies", "Additions")
DATAFRAME_HEIGHT = 1200

COLUMN_CONFIG = {
    "ID": {"label": "ID", "help": "Primary identifier", "width": "small"},
    "Title": {"label": "Title", "width": "medium"},
    "Products": {"label": "Products", "width": "large"},
}

# =========================================================
# 3) UI Helpers
# =========================================================
URLS = {
    "linkedin": "https://www.google.com/search?q={q}+site%3Alinkedin.com",
    "news": "https://news.google.com/search?q={q}",
    "sec": "https://www.sec.gov/edgar/search/#/q={q}",
    "ukco": "https://find-and-update.company-information.service.gov.uk/search?q={q}",
    "google": "https://www.google.com/search?q={q}",
}

def link_button(href: str, label: str) -> str:
    return f'<a href="{href}" target="_blank" class="btn-link">{label}</a>'

def pill(text: str) -> str:
    return (
        '<span style="display:inline-block;padding:.25rem .55rem;border-radius:999px;'
        'border:1px solid var(--muted,rgba(0,0,0,.12));margin:0 .25rem .25rem 0;'
        f'font-size:.85rem;">{text}</span>'
    )

# =========================================================
# 4) Aliases & Functions
# =========================================================
@st.cache_data(show_spinner=False)
def get_id_to_canonical_map(data: List[dict] | None = None) -> Dict[str, str]:
    """Build {firm_id: canonical_name} from firm_aliases.json"""
    if data is None:
        data = FIRM_ALIASES_DATA
    if not isinstance(data, list):
        st.error("Firm aliases JSON is malformed.")
        return {}
    out = {}
    for firm in data:
        fid = firm.get("id")
        canonical = firm.get("canonical")
        if fid and canonical:
            out[str(fid)] = str(canonical)
    return out

@st.cache_data(show_spinner=False)
def load_functions_map(data=None) -> Dict:
    """Parse functions.json into a dict keyed by lowercase function name."""
    if data is None:
        data = FUNCTIONS_JSON_DATA
    if isinstance(data, dict) and "functions" in data:
        data = data["functions"]

    out = {}
    if isinstance(data, dict):
        for k, v in data.items():
            out[str(k).lower()] = v
    elif isinstance(data, list):
        for obj in data:
            name = (obj.get("Function") or obj.get("function") or obj.get("name") or "").strip()
            if not name:
                continue
            out[name.lower()] = obj
    return out

# =========================================================
# 5) Firm discovery & metrics
# =========================================================
@st.cache_data(show_spinner=False)
def get_all_firm_ids() -> List[str]:
    """Scan BBG_EXTRACTION_ROOT for firm folders (ignores 'new')."""
    try:
        entries = os.listdir(BBG_EXTRACTION_ROOT)
        firm_folders = [
            e for e in entries
            if (BBG_EXTRACTION_ROOT / e).is_dir() and e != "new"
        ]
        return sorted(firm_folders)
    except FileNotFoundError:
        return []

@st.cache_data(show_spinner=False)
def get_all_firm_metrics(id_to_name_map: Dict[str, str]) -> List[Dict]:
    """Compute headcount metrics across firms."""
    results = []
    for fid in get_all_firm_ids():
        canonical_name = id_to_name_map.get(fid, fid.replace("_", " ").title())
        metrics = {
            "Firm": canonical_name,
            "Firm ID": fid,
            "Confirmed Headcount": 0,
            "Total Additions": 0,
            "Active Discrepancies": 0,
            "Total Headcount": 0,
            "Last Processed": "N/A",
        }

        try:
            confirmed = BBG_EXTRACTION_ROOT / fid / SUBDIRS["confirmed"] / FILE_PATTERNS["confirmed"].format(fid=fid)
            if confirmed.exists():
                df_c = pd.read_csv(confirmed)
                metrics["Confirmed Headcount"] = df_c.shape[0]
        except Exception:
            pass

        try:
            discrepancies = BBG_EXTRACTION_ROOT / fid / SUBDIRS["discrepancies"] / FILE_PATTERNS["discrepancies"].format(fid=fid)
            if discrepancies.exists():
                df_d = pd.read_csv(discrepancies)
                status_col = next((c for c in df_d.columns if c.lower() == "status"), None)
                if status_col:
                    metrics["Active Discrepancies"] = df_d[df_d[status_col].astype(str).str.lower() == "active"].shape[0]
        except Exception:
            pass

        try:
            additions = BBG_EXTRACTION_ROOT / fid / SUBDIRS["additions"] / FILE_PATTERNS["additions"].format(fid=fid)
            if additions.exists():
                df_a = pd.read_csv(additions)
                metrics["Total Additions"] = df_a.shape[0]
        except Exception:
            pass

        metrics["Total Headcount"] = metrics["Confirmed Headcount"] + metrics["Total Additions"]

        archive_folder = BBG_EXTRACTION_ROOT / fid / "archive"
        if archive_folder.exists():
            archive_files = list(archive_folder.glob("*.csv"))
            if archive_files:
                latest = max(archive_files, key=lambda p: p.stat().st_mtime)
                ts = latest.stat().st_mtime
                metrics["Last Processed"] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %I:%M:%S %p")

        results.append(metrics)

    return results

# =========================================================
# 5) Risk-taker flagging helper
# =========================================================

def attach_risk_flag(df: pd.DataFrame, func_map: Dict[str, Dict]) -> pd.DataFrame:
    """
    Add 'Risk Taker' and optional 'Function Order' columns to a dataframe
    using mapping loaded from functions.json.
    """
    if df is None or df.empty or "Function" not in df.columns:
        return df

    df = df.copy()
    df["Risk Taker"] = df["Function"].map(
        lambda f: func_map.get(str(f).lower(), {}).get("Risk Taker", False)
    )
    df["Function Order"] = df["Function"].map(
        lambda f: func_map.get(str(f).lower(), {}).get("Order", None)
    )

    return df
