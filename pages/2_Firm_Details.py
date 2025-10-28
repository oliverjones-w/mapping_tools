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

all_confirmed_records = config.load_json_data(config.ALL_MATCHES_FILE)
if all_confirmed_records is None:
    st.error(f"Failed to load confirmed matches from {config.ALL_MATCHES_FILE}. Run your processing script.")
    st.stop()
    
firm_count_map = config.get_confirmed_counts_by_firm(config.ALL_MATCHES_FILE)

firm_ids = config.get_all_firm_ids()
if not firm_ids:
    st.error(f"No firm folders found in {config.BBG_EXTRACTION_ROOT}. Run your processing script first.")
    st.stop()

# Get all metrics, then create a map for easy lookup
all_metrics_list = config.get_all_firm_metrics(id_to_name_map, firm_count_map)
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
firm_specific_records = [
    record for record in all_confirmed_records
    if record.get('Firm') == canonical_firm_name
]

if not firm_specific_records:
    st.info("No confirmed records found for this firm in the last run.")
else:
    df_confirmed = pd.DataFrame(firm_specific_Drecords)
    st.dataframe(df_confirmed, use_container_width=True)
    st.info(f"Showing {len(firm_specific_records)} confirmed records.")

# --- Discrepancies Table ---
st.subheader("Active Discrepancies")
discrepancy_file = os.path.join(config.BBG_EXTRACTION_ROOT, selected_firm_id, "discrepancies", f"{selected_firm_id}_discrepancies.csv")
if os.path.exists(discrepancy_file):
    try:
        df_d = pd.read_csv(discrepancy_file)
        st.dataframe(df_d[df_d['Status'] == 'Active'], use_container_width=True)
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
        st.dataframe(df_a, use_container_width=True)
        
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