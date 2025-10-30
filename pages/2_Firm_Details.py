# app.py (or pages/2_Firm_Details.py)
from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from config import load_functions_map, attach_risk_flag
import pandas as pd
import streamlit as st

import config  # your shared helpers/constants

# =========================================================
# 0) Page config
# =========================================================
st.set_page_config(
    page_title="Firm Data Overview",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================================================
# 1) macOS-inspired, theme-aware CSS (light + dark)
# =========================================================
st.markdown("""
<style>
/* ---------- Base font ---------- */
html, body, [class*="css"] {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI",
               Roboto, "Helvetica Neue", Arial, sans-serif !important;
}

/* ---------- LIGHT THEME ---------- */
[data-theme="light"] div[data-testid="stAppViewContainer"] > .main {
  background-color: #F5F5F7; /* macOS-like gray */
}
[data-theme="light"] {
  --text: #1D1D1F;
  --muted: rgba(0,0,0,0.10);
  --divider: rgba(0,0,0,0.08);
  --card-bg: #FFFFFF;
  --metric: #000000;
}
[data-theme="light"] .card {
  background: var(--card-bg);
  color: var(--text);
  border: 1px solid var(--muted);
  border-radius: 12px;
  padding: 18px;
  margin-bottom: 10px;
  box-shadow: 0 1px 2px rgba(0,0,0,0.05);
}
[data-theme="light"] .card h4,
[data-theme="light"] h1, [data-theme="light"] h2, [data-theme="light"] h3, [data-theme="light"] h4 {
  color: var(--text);
}
[data-theme="light"] .metric-value { color: var(--metric); }
[data-theme="light"] hr { border: 0; border-top: 1px solid var(--divider); }
[data-theme="light"] section[data-testid="stSidebar"] { background: #FFFFFF; }

/* ---------- DARK THEME ---------- */
[data-theme="dark"] div[data-testid="stAppViewContainer"] > .main {
  background-color: #1C1C1E; /* macOS "Pro" dark */
}
[data-theme="dark"] {
  --text: #F2F2F7;          /* Apple label color on dark */
  --muted: rgba(255,255,255,0.14);
  --divider: rgba(255,255,255,0.12);
  --card-bg: #2C2C2E;       /* Apple secondary background on dark */
  --metric: #FFFFFF;
}
[data-theme="dark"] .card {
  background: var(--card-bg);
  color: var(--text);
  border: 1px solid var(--muted);
  border-radius: 12px;
  padding: 18px;
  margin-bottom: 10px;
  box-shadow: 0 1px 2px rgba(0,0,0,0.20);
}
[data-theme="dark"] .card h4,
[data-theme="dark"] h1, [data-theme="dark"] h2, [data-theme="dark"] h3, [data-theme="dark"] h4 {
  color: var(--text);
}
[data-theme="dark"] .metric-value { color: var(--metric); }
[data-theme="dark"] hr { border: 0; border-top: 1px solid var(--divider); }
[data-theme="dark"] section[data-testid="stSidebar"] { background: #1E1E1F; }

/* ---------- Shared tweaks ---------- */
.card h4 { margin: 0 0 6px 0; font-weight: 600; }
.metric-value { font-size: 1.6rem; font-weight: 700; line-height: 1.2; }

/* Header link buttons */
.btn-row { display:flex; gap:.5rem; flex-wrap:wrap; }
a.btn-link {
  display:inline-block; padding:.45rem .7rem; border-radius:10px;
  text-decoration:none; font-weight:600; border:1px solid var(--muted, rgba(0,0,0,.12));
}
[data-theme="light"] a.btn-link { color:#1D1D1F; background:#fff; }
[data-theme="dark"]  a.btn-link { color:#F2F2F7; background:#2C2C2E; }
</style>
""", unsafe_allow_html=True)

# =========================================================
# 2) Cached loaders + safe CSV read
# =========================================================
@st.cache_data(show_spinner=False)
def load_id_to_name_map(aliases_path: str | Path) -> Dict[str, str]:
    return config.get_id_to_canonical_map(aliases_path)

@st.cache_data(show_spinner=False)
def load_firm_ids() -> List[str]:
    return config.get_all_firm_ids()

@st.cache_data(show_spinner=False)
def load_all_metrics(id_to_name: Dict[str, str]) -> Tuple[List[Dict], Dict[str, Dict]]:
    lst = config.get_all_firm_metrics(id_to_name)
    # Be tolerant to slight key naming diffs
    as_map = {row.get("Firm ID") or row.get("Firm_ID") or row.get("firm_id"): row for row in lst}
    return lst, as_map

@st.cache_data(show_spinner=False)
def read_csv_safe(path: str | Path) -> Optional[pd.DataFrame]:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return None
    try:
        df = pd.read_csv(p)
        # Normalize unnamed first column if it's just an index
        if len(df.columns) and df.columns[0].lower().startswith("unnamed"):
            df = df.drop(columns=[df.columns[0]])
        return df
    except pd.errors.EmptyDataError:
        return None
    except Exception as e:
        st.warning(f"Could not read {p.name}: {e}")
        return None

# =========================================================
# 3) Helpers (logo path, link buttons, freshness, zipping)
# =========================================================
def get_firm_logo_path(firm_id: str) -> Optional[str]:
    """Looks for a logo image inside the firm folder."""
    root = Path(config.BBG_EXTRACTION_ROOT) / firm_id
    for name in ("logo.png", "logo.jpg", "logo.jpeg", "logo.svg", "Logo.png"):
        p = root / name
        if p.exists():
            return str(p)
    return None

def link_button(href: str, label: str) -> str:
    return f'<a href="{href}" target="_blank" class="btn-link">{label}</a>'

def human_delta(ts: str) -> str:
    try:
        dt = pd.to_datetime(ts)
        now = pd.Timestamp.now(tz=dt.tz) if dt.tzinfo else pd.Timestamp.now()
        secs = (now - dt).total_seconds()
        if secs < 60: return "just now"
        if secs < 3600: return f"{int(secs//60)} min ago"
        if secs < 86400: return f"{int(secs//3600)} h ago"
        return f"{int(secs//86400)} d ago"
    except Exception:
        return "—"

def pill(text: str) -> str:
    return f'<span style="display:inline-block;padding:.25rem .55rem;border-radius:999px;border:1px solid var(--muted,rgba(0,0,0,.12));margin:0 .25rem .25rem 0;font-size:.85rem;">{text}</span>'

def zip_firm_files(fid: str) -> bytes:
    root = Path(config.BBG_EXTRACTION_ROOT) / fid
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for sub in ["confirmed_matches", "discrepancies", "additions"]:
            subdir = root / sub
            if subdir.exists():
                for p in subdir.glob("*.csv"):
                    z.write(p, arcname=str(p.relative_to(root)))
    buf.seek(0)
    return buf.read()

# =========================================================
# 4) Config + basic presence checks
# =========================================================
id_to_name_map = config.get_id_to_canonical_map()
if not id_to_name_map:
    st.error("Failed to load firm aliases. Check `config.FIRM_ALIASES_FILE` path/format.")
    st.stop()

firm_ids = load_firm_ids()
if not firm_ids:
    st.error(f"No firm folders found in {config.BBG_EXTRACTION_ROOT}. Run your processing script first.")
    st.stop()

_, all_metrics_map = load_all_metrics(id_to_name_map)

# =========================================================
# 5) Sidebar
# =========================================================
st.sidebar.markdown("### Data Source")
st.sidebar.caption(f"Root: `{config.BBG_EXTRACTION_ROOT}`")

selected_firm_id = st.sidebar.selectbox(
    "Select Firm",
    firm_ids,
    index=0,
    format_func=lambda fid: id_to_name_map.get(fid, fid.replace("_", " ").title()),
)

metrics = all_metrics_map.get(selected_firm_id, {})
if not metrics:
    st.error(f"Could not find metrics for `{selected_firm_id}`.")
    st.stop()

canonical_firm_name = (
    metrics.get("Firm")
    or metrics.get("Firm Name")
    or id_to_name_map.get(selected_firm_id, selected_firm_id.replace("_", " ").title())
)

# =========================================================
# 6) Profile header (logo + link buttons) and headline
# =========================================================

logo_path = get_firm_logo_path(selected_firm_id)  # <-- ensure this is BEFORE the columns

header_col_left, header_col_right = st.columns([6, 1], gap="large")

with header_col_left:
    st.markdown(f"# {canonical_firm_name}")
    q = id_to_name_map.get(selected_firm_id, selected_firm_id.replace("_", " "))
    btns = [
        link_button(f"https://www.google.com/search?q={q}+site%3Alinkedin.com", "LinkedIn"),
        link_button(f"https://news.google.com/search?q={q}", "News"),
        link_button(f"https://www.sec.gov/edgar/search/#/q={q}", "SEC Filings"),
        link_button(f"https://find-and-update.company-information.service.gov.uk/search?q={q}", "UK Filings"),
        link_button(f"https://www.google.com/search?q={q}", "Google"),
    ]
    st.markdown(f'<div class="btn-row">{"".join(btns)}</div>', unsafe_allow_html=True)

with header_col_right:
    if logo_path and Path(logo_path).exists():
        st.image(logo_path, use_container_width=True)
    else:
        st.empty()

st.write("---")

# =========================================================
# 7) Top metric cards
# =========================================================
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(
        '<div class="card"><h4>Active Discrepancies</h4>'
        f'<div class="metric-value">{metrics.get("Active Discrepancies", 0)}</div></div>',
        unsafe_allow_html=True
    )
with col2:
    st.markdown(
        '<div class="card"><h4>New Additions (Pending)</h4>'
        f'<div class="metric-value">{metrics.get("Total Additions", 0)}</div></div>',
        unsafe_allow_html=True
    )
with col3:
    last_proc = metrics.get("Last Processed", "—")
    st.markdown(
        '<div class="card"><h4>Last Processed Date</h4>'
        f'<div class="metric-value">{last_proc}</div></div>',
        unsafe_allow_html=True
    )

st.caption(f"Data freshness: **{human_delta(last_proc)}**")
st.write("---")

# =========================================================
# 8) Load this firm's CSVs
# =========================================================
root = Path(config.BBG_EXTRACTION_ROOT) / selected_firm_id
confirmed_matches_file = root / "confirmed_matches" / f"{selected_firm_id}_matches.csv"
discrepancy_file       = root / "discrepancies" / f"{selected_firm_id}_discrepancies.csv"
additions_file         = root / "additions" / f"{selected_firm_id}_additions.csv"

df_confirmed = read_csv_safe(confirmed_matches_file)
df_d         = read_csv_safe(discrepancy_file)
df_a         = read_csv_safe(additions_file)

# =========================================================
# 9) Tabs
# =========================================================
tab1, tab2, tab3 = st.tabs(["Confirmed", "Discrepancies", "Additions"])

# ---------- Tab 1: Confirmed ----------
with tab1:
    st.subheader("Confirmed Master Records")

    if df_confirmed is None or df_confirmed.empty:
        st.info("No confirmed records found for this firm in the last run.")
    else:
        st.markdown(f"**Total Confirmed Records:** `{len(df_confirmed)}`")

        # --- Quick filters ---
        with st.expander("Quick Filters", expanded=False):
            q = st.text_input("Search Name / Title", placeholder="Type to filter…")
            c1, c2, c3 = st.columns(3)
            strategies = sorted(df_confirmed["Strategy"].dropna().unique()) if "Strategy" in df_confirmed else []
            functions  = sorted(df_confirmed["Function"].dropna().unique())  if "Function"  in df_confirmed else []
            locations  = sorted(df_confirmed["Location"].dropna().unique())  if "Location"  in df_confirmed else []
            with c1:
                sel_strat = st.multiselect("Strategy", strategies)
            with c2:
                sel_func  = st.multiselect("Function", functions)
            with c3:
                sel_loc   = st.multiselect("Location", locations)

        def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
            out = df.copy()
            if "Strategy" in out and sel_strat:
                out = out[out["Strategy"].isin(sel_strat)]
            if "Function" in out and sel_func:
                out = out[out["Function"].isin(sel_func)]
            if "Location" in out and sel_loc:
                out = out[out["Location"].isin(sel_loc)]
            if q:
                qlow = q.lower()
                cols = [c for c in ["Name", "Title", "Firm", "Function", "Strategy", "Location"] if c in out.columns]
                if cols:
                    out = out[out[cols].astype(str).apply(lambda s: s.str.lower().str.contains(qlow)).any(axis=1)]
            return out

        # Base filtered view
        df_view = apply_filters(df_confirmed)

        # --- Risk Taker toggle (from functions.json mapping) ---
        # Requires: from config import load_functions_map, attach_risk_flag
        func_map = load_functions_map()
        if func_map:
            df_view = attach_risk_flag(df_view, func_map)

            # Default sort by Function Order if available
            if "Function Order" in df_view.columns:
                df_view = df_view.sort_values(by="Function Order", ascending=True, na_position="last")

            # Risk taker filter
            risk_only = st.toggle("Risk takers only", value=False) if hasattr(st, "toggle") else st.checkbox("Risk takers only", value=False)
            if risk_only and "Risk Taker" in df_view.columns:
                df_view = df_view[df_view["Risk Taker"] == True]
        else:
            st.caption("_functions.json not loaded; risk-taker filter unavailable._")

        st.caption(f"Showing **{len(df_view)}** / {len(df_confirmed)} rows after filters.")

        # Optional nice pills (top strategies)
        if "Strategy" in df_confirmed.columns:
            top_counts = df_confirmed["Strategy"].fillna("Unknown").value_counts().head(10)
            pills_html = "".join(pill(f"{k}: {v}") for k, v in top_counts.items())
            st.markdown(pills_html, unsafe_allow_html=True)

        # Column config for readability
        col_cfg = {}
        if "ID" in df_view.columns:
            col_cfg["ID"] = st.column_config.TextColumn("ID", help="Primary identifier", width="small")
        if "Title" in df_view.columns:
            col_cfg["Title"] = st.column_config.TextColumn("Title", width="medium")
        if "Products" in df_view.columns:
            col_cfg["Products"] = st.column_config.TextColumn("Products", width="large")

        # Bigger viewport for easier reading
        st.dataframe(df_view, use_container_width=True, column_config=col_cfg, hide_index=True, height=900)

        # Downloads
        st.download_button(
            label="Download Confirmed Data (CSV)",
            data=df_confirmed.to_csv(index=False).encode("utf-8"),
            file_name=f"{selected_firm_id}_confirmed_records.csv",
            mime="text/csv",
            key="download_confirmed_data",
        )
        st.download_button(
            "Download All CSVs (ZIP)",
            data=zip_firm_files(selected_firm_id),
            file_name=f"{selected_firm_id}_bundle.zip",
            mime="application/zip",
            key="download_zip",
        )

        st.write("---")
        st.subheader("Team Composition Analysis")

        chart_col1, chart_col2, chart_col3 = st.columns(3)

        with chart_col1:
            st.markdown("#### Strategies")
            if "Strategy" in df_confirmed.columns:
                series = df_confirmed["Strategy"].fillna("Unknown").value_counts().sort_values(ascending=False)
                if not series.empty:
                    st.bar_chart(series.rename_axis(None), use_container_width=True)
                else:
                    st.caption("No strategy values present.")
            else:
                st.caption("No 'Strategy' column available.")

        with chart_col2:
            st.markdown("#### Functions")
            if "Function" in df_confirmed.columns:
                series = df_confirmed["Function"].fillna("Unknown").value_counts().sort_values(ascending=False)
                if not series.empty:
                    st.bar_chart(series.rename_axis(None), use_container_width=True)
                else:
                    st.caption("No function values present.")
            else:
                st.caption("No 'Function' column available.")

        with chart_col3:
            st.markdown("#### Location Distribution")
            if "Location" in df_confirmed.columns:
                series = df_confirmed["Location"].fillna("Unknown").value_counts().sort_values(ascending=False)
                if not series.empty:
                    st.bar_chart(series.rename_axis(None), use_container_width=True)
                else:
                    st.caption("No location values present.")
            else:
                st.caption("No 'Location' column available.")


# ---------- Tab 2: Discrepancies ----------
with tab2:
    st.subheader("Active Discrepancies")
    if df_d is None:
        st.info("No discrepancy file found.")
    else:
        status_col = next((c for c in df_d.columns if c.lower() == "status"), None)
        if not status_col:
            st.dataframe(df_d, use_container_width=True)
            st.caption(f"Showing **{len(df_d)}** discrepancy rows (no 'Status' column present).")
        else:
            df_active = df_d[df_d[status_col].astype(str).str.lower() == "active"]
            if df_active.empty:
                st.info("No active discrepancies found for this firm.")
            else:
                st.dataframe(df_active, use_container_width=True)
                st.caption(f"Showing **{len(df_active)}** actively tracked discrepancies.")

# ---------- Tab 3: Additions ----------
with tab3:
    st.subheader("New Additions")
    if df_a is None or df_a.empty:
        st.info("No new additions data to display from the last run.")
    else:
        st.dataframe(df_a, use_container_width=True)
        if "Location" in df_a.columns:
            st.markdown("#### Additions by Location")
            series = df_a["Location"].fillna("Unknown").value_counts().sort_values(ascending=False)
            if not series.empty:
                st.bar_chart(series.rename_axis(None), use_container_width=True)
            else:
                st.caption("No location values present in additions.")
