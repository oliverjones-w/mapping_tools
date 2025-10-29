import streamlit as st
import pandas as pd
import config  # Import your new config file
import altair as alt # Import Altair

# --- Main Page Layout ---
st.set_page_config(layout="wide", page_title="Dashboard")
st.title("Data Reconciliation Dashboard")

# --- Add custom CSS to increase dataframe font size ---
st.markdown("""
<style>
/* Target the data cells in the Streamlit dataframe */
.stDataFrame div[data-testid="stDataGridCell"] > div {
    font-size: 1.1rem;
}
/* Target the header cells in the Streamlit dataframe */
.stDataFrame div[data-testid="stHeaderCell"] > div {
    font-size: 1.1rem;
}
</style>
""", unsafe_allow_html=True)

# Load data using the shared functions
id_to_name_map = config.get_id_to_canonical_map(config.FIRM_ALIASES_FILE)
if not id_to_name_map:
    st.error("Failed to load firm aliases. Check file path and format.")
    st.stop()
    
firm_ids = config.get_all_firm_ids()
if not firm_ids:
    st.error(f"No firm folders found in {config.BBG_EXTRACTION_ROOT}. Run your processing script first.")
    st.stop()

# --- Get all metrics with one function call ---
all_metrics_list = config.get_all_firm_metrics(id_to_name_map)

# --- Calculate Tracking % ---
if all_metrics_list:
    for firm_metrics in all_metrics_list:
        c = firm_metrics.get('Confirmed Headcount', 0)
        a = firm_metrics.get('Total Additions', 0)
        d = firm_metrics.get('Active Discrepancies', 0)
        
        total_known = c + a + d
        
        if total_known == 0:
            firm_metrics['Tracking %'] = 0.0
        else:
            firm_metrics['Tracking %'] = (c / total_known) * 100

# --- Global Metrics ---
st.subheader("Global Metrics")

# --- Calculate all totals for the stacked bar ---
total_confirmed = 0
total_additions = 0
total_discrepancies = 0

if all_metrics_list:
    total_confirmed = sum(item['Confirmed Headcount'] for item in all_metrics_list)
    total_additions = sum(item['Total Additions'] for item in all_metrics_list)
    total_discrepancies = sum(item['Active Discrepancies'] for item in all_metrics_list)

col1, col2 = st.columns(2)
with col1:
    st.metric("Total Firms Processed", len(firm_ids))
with col2:
    st.metric("Total Confirmed People (All Firms)", f"{total_confirmed:,}")

# --- Overall Composition Stacked Bar Chart (using Altair) ---
st.markdown("#### Overall Composition (All Firms)")

total_all = total_confirmed + total_additions + total_discrepancies
if total_all > 0:
    # Create a DataFrame for the stacked bar
    composition_data = {
        "Confirmed": [total_confirmed],
        "Additions": [total_additions],
        "Discrepancies": [total_discrepancies]
    }
    df_composition = pd.DataFrame(composition_data)
    
    # --- THIS BLOCK REPLACES st.bar_chart() ---
    
    # Melt the dataframe from wide to long format for Altair
    df_melted = df_composition.melt(var_name='Category', value_name='Value')
    
    # Add a dummy column to stack on (for a single horizontal bar)
    df_melted['Composition'] = 'Overall Composition'
    
    # Create the Altair chart
    chart = alt.Chart(df_melted).mark_bar().encode(
        # X-axis is the count
        x=alt.X('Value:Q', title="Total Count"), 
        # Y-axis is our single dummy category (axis=None hides the label)
        y=alt.Y('Composition:N', axis=None), 
        # Color divides the bar
        color=alt.Color('Category:N', title="Category"),
        # Tooltip to show details on hover
        tooltip=['Category', 'Value']
    ) # --- REMOVED .properties(height=50) ---
    
    # Display the chart
    st.altair_chart(chart, use_container_width=True)
    
    # --- NEW: Add labels underneath for clarity ---
    st.markdown(f"**Confirmed:** `{total_confirmed:,}` | **Additions:** `{total_additions:,}` | **Discrepancies:** `{total_discrepancies:,}`")

else:
    st.info("No data to display for composition chart.")
# --- END NEW CHART ---

st.markdown("---")

st.subheader("At-a-Glance Firm Summary")

if all_metrics_list:
    df_summary = pd.DataFrame(all_metrics_list)
    
    column_order = [
        "Firm",
        "Total Headcount",
        "Tracking %",
        "Confirmed Headcount",
        "Total Additions",
        "Active Discrepancies",
        "Last Processed"
    ]
    
    final_columns = [col for col in column_order if col in df_summary.columns]
    df_summary = df_summary[final_columns].set_index("Firm")
    
    st.dataframe(
        df_summary,
        width='stretch',
        column_config={
            "Tracking %": st.column_config.ProgressColumn(
                "Tracking %",
                help="Confirmed / (Confirmed + Additions + Discrepancies)",
                format="%.1f%%",
                min_value=0,
                max_value=100,
            )
        }
    )
else:
    st.info("No metrics to display.")