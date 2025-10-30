# utils.py
from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from pathlib import Path
import streamlit as st
from typing import Dict, Iterable, Optional, Tuple, Union

import pandas as pd


# ================================
# 0) Dataclasses for config inputs
# ================================
@dataclass(frozen=True)
class FirmIOLayout:
    """
    Describes where firm data lives on disk and how files are named.
    Example:
        root = Path("data/bbg_extraction")
        subdirs = {"confirmed":"confirmed_matches","discrepancies":"discrepancies","additions":"additions"}
        patterns = {"confirmed":"{fid}_matches.csv", "discrepancies":"{fid}_discrepancies.csv", "additions":"{fid}_additions.csv"}
    """
    root: Path
    subdirs: Dict[str, str]
    patterns: Dict[str, str]


# ================================
# 1) Path tooling
# ================================
def firm_logo_path(root: Path, fid: str) -> Optional[str]:
    """
    Look for a logo file within a firm's directory.
    Returns a string path if found, else None.
    """
    base = root / fid
    for name in ("logo.png", "logo.jpg", "logo.jpeg", "logo.svg", "Logo.png"):
        p = base / name
        if p.exists():
            return str(p)
    return None


def build_firm_paths(fid: str, layout: FirmIOLayout) -> Dict[str, Path]:
    """
    Build canonical file paths for a firm based on a layout.
    Keys: 'confirmed', 'discrepancies', 'additions' (or any you define).
    """
    base = layout.root / fid
    out: Dict[str, Path] = {}
    for key, sub in layout.subdirs.items():
        patt = layout.patterns.get(key)
        if patt is None:
            continue
        out[key] = base / sub / patt.format(fid=fid)
    return out


# ================================
# 2) CSV I/O (safe)
# ================================
def read_csv_safe(path: Union[str, Path]) -> Optional[pd.DataFrame]:
    """
    Read a CSV if present and non-empty.
    - Drops a leading unnamed index column if detected.
    - Returns None on empty/missing/unreadable files.
    """
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return None
    try:
        df = pd.read_csv(p)
        if len(df.columns) and df.columns[0].lower().startswith("unnamed"):
            df = df.drop(columns=[df.columns[0]])
        return df
    except pd.errors.EmptyDataError:
        return None
    except Exception:
        # Intentionally swallow exceptions to keep pages resilient;
        # let callers decide how to warn/log.
        return None


def write_zip_of_csvs(
    files: Iterable[Path],
    arc_root: Optional[Path] = None,
) -> bytes:
    """
    Create a ZIP (as bytes) of provided CSV file paths.
    If arc_root is provided, arc names are made relative to it; otherwise, use filename only.
    Non-existent files are skipped silently.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in files:
            p = Path(p)
            if not p.exists() or p.suffix.lower() != ".csv":
                continue
            arcname = str(p.relative_to(arc_root)) if arc_root and _is_relative_to(p, arc_root) else p.name
            z.write(p, arcname=arcname)
    buf.seek(0)
    return buf.read()


def zip_firm_files(fid: str, layout: FirmIOLayout) -> bytes:
    """
    Convenience: bundle all CSVs from the firm's standard subfolders into a ZIP.
    """
    base = layout.root / fid
    csvs: list[Path] = []
    for key, sub in layout.subdirs.items():
        subdir = base / sub
        if subdir.exists():
            csvs.extend(sorted(subdir.glob("*.csv")))
    return write_zip_of_csvs(csvs, arc_root=base)


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


# ================================
# 3) Time helpers
# ================================
def human_delta(ts: Union[str, pd.Timestamp, None]) -> str:
    """
    Return a human-friendly age like 'just now', '5 min ago', '3 h ago', '10 d ago'.
    If input has tz, compute with that tz; otherwise compute in local system time.
    """
    if ts is None:
        return "—"
    try:
        dt = pd.to_datetime(ts)
        now = pd.Timestamp.now(tz=dt.tz) if dt.tzinfo else pd.Timestamp.now()
        secs = (now - dt).total_seconds()
        if secs < 60:
            return "just now"
        if secs < 3600:
            return f"{int(secs // 60)} min ago"
        if secs < 86400:
            return f"{int(secs // 3600)} h ago"
        return f"{int(secs // 86400)} d ago"
    except Exception:
        return "—"


# ================================
# 4) DataFrame helpers
# ================================
def apply_quick_filters(
    df: pd.DataFrame,
    text_query: Optional[str] = None,
    strategies: Optional[Iterable[str]] = None,
    functions: Optional[Iterable[str]] = None,
    locations: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Filter a people/roles DataFrame using common columns:
      - 'Strategy', 'Function', 'Location' (if present)
      - text query across ['Name','Title','Firm','Function','Strategy','Location'] (if present)
    Returns a filtered copy (original is not modified).
    """
    out = df.copy()

    if "Strategy" in out.columns and strategies:
        out = out[out["Strategy"].isin(list(strategies))]
    if "Function" in out.columns and functions:
        out = out[out["Function"].isin(list(functions))]
    if "Location" in out.columns and locations:
        out = out[out["Location"].isin(list(locations))]

    if text_query:
        q = str(text_query).lower()
        cols = [c for c in ["Name", "Title", "Firm", "Function", "Strategy", "Location"] if c in out.columns]
        if cols:
            mask = out[cols].astype(str).apply(lambda s: s.str.lower().str.contains(q)).any(axis=1)
            out = out[mask]

    return out


def default_sort(df: pd.DataFrame, by: str = "Function Order", ascending: bool = True) -> pd.DataFrame:
    """
    Sort if the column exists; otherwise return the df unchanged.
    """
    if by in df.columns:
        return df.sort_values(by=by, ascending=ascending, na_position="last")
    return df


# ================================
# 5) Lightweight metrics accessors
# ================================
def extract_metric_counts(df: Optional[pd.DataFrame], column: str, top_n: int = 10) -> pd.Series:
    """
    Return a value_counts() Series for `column` (or empty Series if df/column missing).
    Uses 'Unknown' for NaNs and truncates to top_n.
    """
    if df is None or df.empty or column not in df.columns:
        return pd.Series(dtype="int64")
    return (
        df[column].fillna("Unknown").value_counts().sort_values(ascending=False).head(top_n)
    )


# ================================
# 6) Functions-map helpers (optional)
# ================================
def attach_risk_flag(df: pd.DataFrame, functions_map: Dict[str, Dict]) -> pd.DataFrame:
    """
    Adds:
      - 'Risk Taker' (bool)
      - 'Function Order' (float)  # taken from 'Order' or 'order' in functions.json
    Case-insensitive match on Function names.
    """
    out = df.copy()
    if out is None or out.empty or "Function" not in out.columns or not functions_map:
        return out

    # Normalize mapping to lowercase keys
    norm_map = {str(k).strip().lower(): (v or {}) for k, v in functions_map.items()}

    # Normalize function column to lowercase for the map, keep original column unchanged
    func_norm = out["Function"].astype(str).str.strip().str.lower()

    # Build vectorized lookups
    risk_series = func_norm.map(
        lambda k: _coerce_bool(norm_map.get(k, {}).get("Risk Taker",
                         norm_map.get(k, {}).get("risk_taker",
                         norm_map.get(k, {}).get("risk taker"))))
    )
    order_series = func_norm.map(
        lambda k: _coerce_float(norm_map.get(k, {}).get("Order",
                           norm_map.get(k, {}).get("order")))
    )

    out["Risk Taker"] = risk_series
    out["Function Order"] = order_series
    return out


def _coerce_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"true", "1", "yes", "y", "t"}
    return None


def _coerce_float(v):
    try:
        return float(v)
    except Exception:
        return None


# ================================
# 7) Metrics map helpers
# ================================
def list_to_id_map(rows: Iterable[Dict]) -> Dict[str, Dict]:
    """
    Convert a list of metric dicts to a dict keyed by firm id.
    Accepts any of: 'Firm ID', 'Firm_ID', 'firm_id'.
    """
    as_map: Dict[str, Dict] = {}
    for row in rows:
        fid = row.get("Firm ID") or row.get("Firm_ID") or row.get("firm_id")
        if fid:
            as_map[str(fid)] = row
    return as_map


def pick_canonical_firm_name(
    selected_firm_id: str,
    id_to_name_map: Dict[str, str],
    metrics_row: Dict[str, str],
) -> str:
    """
    Best-effort canonical firm name selection.
    """
    return (
        metrics_row.get("Firm")
        or metrics_row.get("Firm Name")
        or id_to_name_map.get(selected_firm_id, selected_firm_id.replace("_", " ").title())
    )

# ===========
# Autosize data frames with max height component

def autosized_df(df, base_height_per_row=28, max_height=1400, **kwargs):
    """
    Display a dataframe with automatic height sizing.

    Args:
        df (pd.DataFrame): The dataframe to display.
        base_height_per_row (int): Approximate pixel height per row.
        max_height (int): Maximum pixel height allowed.
        **kwargs: Passed directly to st.dataframe() (e.g. use_container_width=True).
    """
    # Estimate height: header (~60px) + rows * base_height_per_row
    est_height = min(60 + len(df) * base_height_per_row, max_height)
    return st.dataframe(df, height=est_height, **kwargs)


@st.cache_data(show_spinner=False)
def read_csv_safe(path: str | Path):
    """
    Safe CSV loader for firm-level data.
    - Returns None if file missing or empty
    - Drops a leading 'Unnamed' index column if present
    """
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return None
    try:
        df = pd.read_csv(p)
        # drop default index column if present
        if len(df.columns) and str(df.columns[0]).lower().startswith("unnamed"):
            df = df.drop(columns=[df.columns[0]])
        return df
    except pd.errors.EmptyDataError:
        return None
    except Exception as e:
        st.warning(f"Could not read {p.name}: {e}")
        return None
