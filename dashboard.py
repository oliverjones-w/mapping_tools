import streamlit as st
import pandas as pd
import config  # Import your new config file

# --- Main Page Layout ---
st.set_page_config(layout="wide", page_title="Dashboard")
st.title("Data Reconciliation Dashboard")

# Load data using the shared functions
id_to_name_map = config.get_id_to_canonical_map(config.FIRM_ALIASES_FILE)
if not id_to_name_map:
    st.error("Failed to load firm aliases. Check file path and format.")
    st.stop()
    
# --- DELETE THIS BLOCK --- (This is causing your error)
# firm_count_map = config.get_confirmed_counts_by_firm(config.ALL_MATCHES_FILE)
# if firm_count_map is None:
#    st.error(f"Failed to load confirmed matches from {config.ALL_MATCHES_FILE}. Run your processing script.")
#    st.stop()
# --- END DELETE ---

firm_ids = config.get_all_firm_ids()
if not firm_ids:
    st.error(f"No firm folders found in {config.BBG_EXTRACTION_ROOT}. Run your processing script first.")
    st.stop()

# --- Get all metrics with one function call ---
# --- MODIFIED: This call no longer needs firm_count_map ---
all_metrics_list = config.get_all_firm_metrics(id_to_name_map)

# --- Global Metrics ---
st.subheader("Global Metrics")

# --- MODIFIED: Calculate total_confirmed from the new all_metrics_list ---
total_confirmed = 0
if all_metrics_list:
    total_confirmed = sum(item['Confirmed Headcount'] for item in all_metrics_list)

col1, col2 = st.columns(2)
with col1:
    st.metric("Total Firms Processed", len(firm_ids))
with col2:
    st.metric("Total Confirmed People (All Firms)", f"{total_confirmed:,}")

st.markdown("---")

st.subheader("At-a-Glance Firm Summary")

if all_metrics_list:
    df_summary = pd.DataFrame(all_metrics_list)
    
    column_order = [
        "Firm",
        "Total Headcount",
        "Confirmed Headcount",
        "Total Additions", 
        "Active Discrepancies",
        "Last Processed"
    ]
    
    final_columns = [col for col in column_order if col in df_summary.columns]
    df_summary = df_summary[final_columns].set_index("Firm")
    
    # Fixed warning: use_container_width=True -> width='stretch'
    st.dataframe(df_summary, width='stretch')
else:
    st.info("No metrics to display.")