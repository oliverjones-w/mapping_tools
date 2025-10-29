import streamlit as st
import pandas as pd
import os
import config  # Import your new config file

# --- Details Page Layout ---
st.set_page_config(layout="wide", page_title="Firm Details")
st.title("Firm Details")

# Load data using the shared functions
id_to_name_map = config.get_id_to_canonical_map(config.FIRM_ALIASES_FILE)
if not id_to_name_map:
    st.error("Failed to load firm aliases. Check file path and format.")
    st.stop()

firm_ids = config.get_all_firm_ids()
if not firm_ids:
    st.error(f"No firm folders found in {config.BBG_EXTRACTION_ROOT}. Run your processing script first.")
    st.stop()

# Get all metrics, then create a map for easy lookup
all_metrics_list = config.get_all_firm_metrics(id_to_name_map)
all_metrics_map = {item['Firm ID']: item for item in all_metrics_list}


# --- Sidebar for firm selection ---
selected_firm_id = st.sidebar.selectbox(
    label="Select Firm",
    options=firm_ids,
    index=0,
    # Use the map to show the "nice" name in the dropdown
    format_func=lambda firm_id: id_to_name_map.get(firm_id, firm_id.replace('_', ' ').title())
)

# Get the metrics for the selected firm
metrics = all_metrics_map.get(selected_firm_id)
if not metrics:
    st.error(f"Could not find metrics for {selected_firm_id}")
    st.stop()
    
canonical_firm_name = metrics.get("Firm", selected_firm_id)

st.header(f"Metrics for: {canonical_firm_name}")

# --- Metrics Display ---
col1, col2, col3 = st.columns(3)
with col1:
    st.metric(label="Active Discrepancies", value=metrics["Active Discrepancies"])
with col2:
    st.metric(label="New Additions (Pending)", value=metrics["Total Additions"])
with col3:
    st.metric(label="Last Processed On", value=metrics["Last Processed"])

# --- Confirmed Records Table ---
st.subheader(f"Confirmed Master Records: {canonical_firm_name}")

# Define the path to the new CSV
confirmed_matches_file = os.path.join(config.BBG_EXTRACTION_ROOT, selected_firm_id, "confirmed_matches", f"{selected_firm_id}_matches.csv")

if not os.path.exists(confirmed_matches_file):
    st.info("No confirmed records file found for this firm.")
else:
    try:
        df_confirmed = pd.read_csv(confirmed_matches_file)
        if df_confirmed.empty:
            st.info("No confirmed records found for this firm in the last run.")
        else:
            # Display the main table
            st.dataframe(df_confirmed, width='stretch')
            st.info(f"Showing {len(df_confirmed)} confirmed records from the last run.")
            
            # --- BEGIN NEW VISUALIZATION SECTION ---
            st.markdown("---")
            st.subheader("Confirmed Team Composition")
            
            viz_col1, viz_col2, viz_col3 = st.columns(3)

            # Chart 1: Strategies (from 'Strategy' column)
            with viz_col1:
                st.markdown("#### Strategies")
                if 'Strategy' in df_confirmed.columns:
                    strategy_counts = df_confirmed['Strategy'].fillna('Unknown').value_counts()
                    strategy_counts.name = "Count"
                    st.bar_chart(strategy_counts)
                else:
                    st.info("No 'Strategy' column found in matches file.")
            
            # Chart 2: Functions (from 'Function' column)
            with viz_col2:
                st.markdown("#### Functions")
                if 'Function' in df_confirmed.columns:
                    function_counts = df_confirmed['Function'].fillna('Unknown').value_counts()
                    function_counts.name = "Count"
                    st.bar_chart(function_counts)
                else:
                    st.info("No 'Function' column found in matches file.")
            
            # --- MODIFIED: Chart 3: Location Bar Chart ---
            with viz_col3:
                st.markdown("#### Location Distribution")
                if 'Location' in df_confirmed.columns:
                    location_counts = df_confirmed['Location'].fillna('Unknown').value_counts()
                    location_counts.name = "Count"
                    # --- REPLACED st.pie_chart with st.bar_chart ---
                    st.bar_chart(location_counts)
                else:
                    st.info("No 'Location' column found in matches file.")
            # --- END NEW VISUALIZATION SECTION ---
            
    except pd.errors.EmptyDataError:
        st.info("No confirmed records found for this firm in the last run.")
    except Exception as e:
        st.error(f"Error displaying confirmed records: {e}")

# --- Discrepancies Table ---
st.subheader("Active Discrepancies")
discrepancy_file = os.path.join(config.BBG_EXTRACTION_ROOT, selected_firm_id, "discrepancies", f"{selected_firm_id}_discrepancies.csv")
if os.path.exists(discrepancy_file):
    try:
        df_d = pd.read_csv(discrepancy_file)
        st.dataframe(df_d[df_d['Status'] == 'Active'], width='stretch')
    except pd.errors.EmptyDataError:
        st.info("No discrepancy data to display.")
    except Exception as e:
        st.warning(f"Could not read discrepancy file: {e}")
else:
    st.info("No discrepancy file found.")

# --- Additions Table & Chart ---
st.subheader("New Additions")
additions_file = os.path.join(config.BBG_EXTRACTION_ROOT, selected_firm_id, "additions", f"{selected_firm_id}_additions.csv")
if os.path.exists(additions_file):
    try:
        df_a = pd.read_csv(additions_file)
        st.dataframe(df_a, width='stretch')
        
        if 'Location' in df_a.columns and not df_a.empty:
            st.subheader("Additions by Location")
            location_counts = df_a['Location'].fillna('Unknown').value_counts()
            location_counts.name = "Count"
            st.bar_chart(location_counts)
    except pd.errors.EmptyDataError:
        st.info("No additions data to display.")
    except Exception as e:
        st.warning(f"Could not read additions file: {e}")
else:
    st.info("No additions file found.")