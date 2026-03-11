import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import requests
import io
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure the Streamlit page
st.set_page_config(
    page_title="Rhocash AP Hiring Intelligence",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern premium styling
st.markdown("""
<style>
    /* Dark Premium Theme */
    .stApp {
        background-color: #0E1117;
        color: #FAFAFA;
    }
    .main .block-container {
        padding-top: 2rem;
    }
    
    /* Sleek Metric Cards */
    .metric-card {
        background: linear-gradient(145deg, #1A1C23, #15171C);
        padding: 24px;
        border-radius: 12px;
        border: 1px solid #2D3139;
        box-shadow: 0 4px 20px rgba(0,0,0,0.4);
        text-align: center;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 30px rgba(0,0,0,0.6);
        border-color: #3B82F6;
    }
    .metric-value {
        font-size: 2.8rem;
        font-weight: 800;
        background: -webkit-linear-gradient(#60A5FA, #3B82F6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 5px;
    }
    .metric-label {
        font-size: 0.95rem;
        color: #9CA3AF;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: 600;
    }
    
    /* Table Styling overrides */
    div[data-testid="stDataFrame"] {
        border-radius: 10px;
        border: 1px solid #2D3139;
        overflow: hidden;
    }
    
    /* Header Enhancements */
    h1 {
        font-weight: 800 !important;
        background: linear-gradient(90deg, #FFFFFF 0%, #A5B4FC 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)
def load_data():
    """Load dataset from local cache or fallback to Google Sheets."""
    data_path = os.path.join(".tmp", "jobs_with_intensity.csv")
    df = pd.DataFrame()
    
    # 1. Try Local Cache
    if os.path.exists(data_path):
        try:
            df = pd.read_csv(data_path)
            # intensity usually added locally
        except: pass
        
    # 2. Fallback to Google Sheets
    if df.empty:
        sheet_id = os.getenv("GOOGLE_SHEET_ID")
        service_account_info = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        
        if sheet_id and service_account_info:
            try:
                # Handle both file path and literal JSON string
                if service_account_info.strip().startswith('{'):
                    creds_info = json.loads(service_account_info)
                else:
                    with open(service_account_info, 'r') as f:
                        creds_info = json.load(f)
                
                scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
                creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
                client = gspread.authorize(creds)
                sheet = client.open_by_key(sheet_id).worksheet("Master Sheet")
                
                # Fetch all values and handle potential empty/duplicate headers
                all_values = sheet.get_all_values()
                if all_values:
                    headers = [str(h).strip().lower() for h in all_values[0]]
                    valid_indices = [i for i, h in enumerate(headers) if h != ""]
                    
                    if valid_indices:
                        clean_headers = [headers[i] for i in valid_indices]
                        clean_data = [[row[i] for i in valid_indices] for row in all_values[1:]]
                        df = pd.DataFrame(clean_data, columns=clean_headers)
                        
                        # CRITICAL: Filter out truly empty rows from Google Sheets
                        # Many sheets have 1000s of empty rows that appear as rows of empty strings
                        df.replace('', pd.NA, inplace=True)
                        # A row is valid if it has at least a job title or company
                        if 'company' in df.columns or 'job_title' in df.columns:
                            valid_cols = [c for c in ['company', 'job_title', 'job_url'] if c in df.columns]
                            df.dropna(subset=valid_cols, how='all', inplace=True)
            except Exception as e:
                st.sidebar.error(f"Cloud Sync Error: {e}")

    if not df.empty:
        # Standardize column naming
        df.columns = [c.lower() for c in df.columns]
        rename_map = {
            'company': 'Company',
            'job_title': 'Job Title',
            'location': 'Location',
            'scraped_date': 'first_seen_date',
            'job_url': 'Job url',
            'erp': 'ERP',
            'description': 'Job Description',
            'intensity': 'Intensity'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Ensure Intensity exists if missing from sheet (older runs)
        if 'Intensity' not in df.columns:
            df['Intensity'] = 'Low' 
        else:
            df['Intensity'] = df['Intensity'].fillna('Low')
            
        # Ensure first_seen_date exists
        if 'first_seen_date' not in df.columns:
            df['first_seen_date'] = datetime.today().strftime('%Y-%m-%d')
        else:
            df['first_seen_date'] = df['first_seen_date'].fillna(datetime.today().strftime('%Y-%m-%d'))
            
        return df
    return pd.DataFrame(columns=['Company', 'Job Title', 'Location', 'Intensity', 'ERP', 'first_seen_date', 'Job url'])

def run_app():
    # Header
    st.title("💼 Rhocash AP Hiring Intelligence Dashboard")
    st.markdown("Automated tracking of US companies actively hiring for Accounts Payable roles.")
    
    # Load dataset
    df = load_data()
    
    if df.empty:
        st.info("ℹ️ Dataset is currently empty. Data is automatically synced from Apify runs.")

    # ----------------------------------------------------------------------
    # Sidebar: Scrape Controls
    # ----------------------------------------------------------------------
    st.sidebar.markdown('### ⚡ Live Data Mining')
    
    # Load System Config
    config_path = ".tmp/system_config.json"
    system_sched = "Active (Dynamic)" # Generic Fallback
    last_sync = "Pending"
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                cfg = json.load(f)
                system_sched = cfg.get("schedule", system_sched)
                last_sync = cfg.get("last_sync", last_sync)
        except: pass

    st.sidebar.markdown('---')
    st.sidebar.markdown('### 🤖 Sync Automation')
    st.sidebar.success(f"🕒 Schedule: **{system_sched}**")
    st.sidebar.caption(f"Last successfully synced: {last_sync}")
                
    # ----------------------------------------------------------------------
    # Sidebar: Perspective Toggle
    # ----------------------------------------------------------------------
    st.sidebar.markdown('---')
    st.sidebar.markdown('### 🔍 View Perspective')
    view_mode = st.sidebar.radio(
        "Select Stats Perspective",
        ["Total (Lifetime)", "Today (Recent)"],
        index=0,
        help="Switch metrics between total historical data and today's new leads."
    )
    
    # 3. Dynamic Stats View
    st.sidebar.markdown("### 📊 Metrics Scope")
    
    # Calculate Today's date based on dataset latest
    latest_date_in_df = df['first_seen_date'].max() if not df.empty and 'first_seen_date' in df.columns else None
    today_str = pd.Timestamp.today().strftime('%Y-%m-%d')
    active_date = today_str if (not df.empty and today_str in df['first_seen_date'].values.tolist()) else latest_date_in_df
    
    current_leads_total = len(df) if not df.empty else 0
    today_leads_count = len(df[df['first_seen_date'] == active_date]) if not df.empty and 'first_seen_date' in df.columns else 0
    
    if view_mode == "Today (Recent)":
        st.sidebar.info(f"Showing leads for: **{active_date}**")
        st.sidebar.metric("Today's Leads (Database)", f"{today_leads_count}")
    else:
        st.sidebar.metric("Total Lead Database", f"{current_leads_total}")
    
    # Apify Quota Monitor (Client-side estimation / API Fetch)
    st.sidebar.markdown("### 🔌 Apify Quota Monitor")
    api_token = os.getenv("APIFY_API_TOKEN")
    if api_token:
        try:
            # Query Apify API for user limits and current usage
            resp = requests.get(f"https://api.apify.com/v2/users/me/limits?token={api_token}", timeout=5)
            if resp.status_code == 200:
                data = resp.json().get('data', {})
                limit_data = data.get('limits', {})
                limit = limit_data.get('maxMonthlyUsageUsd', limit_data.get('monthlyUsageUsd', 5.0))
                used = data.get('current', {}).get('monthlyUsageUsd', 0.0)
                
                st.sidebar.metric(
                    label="Monthly Credits Used", 
                    value=f"${used:.3f}", 
                    delta=f"${max(0, limit - used):.2f} remaining",
                    delta_color="normal"
                )
                
                if used >= limit:
                    st.sidebar.error("⚠️ Apify Credits Exhausted! Runs will fail.")
                else:
                    # Generic message if limit or used is zero/null
                    if limit > 0:
                        runs_left_est = int((limit - used) / 0.015) # Avg $0.015 per scrape
                        st.sidebar.success(f"Est. {runs_left_est} free scrapes remaining.")
                    else:
                        st.sidebar.success("Quota status: OK")
            else:
                st.sidebar.warning(f"Failed to fetch live API quota (Status: {resp.status_code}).")
        except Exception:
            st.sidebar.warning("Could not connect to Apify servers.")
    else:
         st.sidebar.info("Add API_TOKEN to .env to see live quota.")
    
    # ----------------------------------------------------------------------
    # Sidebar: Filters
    # ----------------------------------------------------------------------
    st.sidebar.header("Filter Data")
    
    # Safely get Location filter
    # Extract just the State from "City, ST" for the filter UI if possible
    def get_state(loc):
        loc_str = str(loc)
        return loc_str.split(',')[-1].strip() if ',' in loc_str else loc_str
        
    df['FilterState'] = df['Location'].apply(get_state) if 'Location' in df.columns else 'Unknown'
    
    all_states = sorted(df['FilterState'].unique().tolist())
    selected_states = st.sidebar.multiselect("Select State/Region", all_states, default=all_states)
    
    # Intensity Filter
    all_intensities = ["High", "Medium", "Low", "Unknown"]
    available_intensities = [i for i in all_intensities if i in df['Intensity'].unique()] if 'Intensity' in df.columns else []
    selected_intensities = st.sidebar.multiselect("Hiring Intensity", available_intensities, default=available_intensities)

    # ERP Filter
    if 'ERP' in df.columns:
        # Get all unique comma-separated ERPs, flatten, and dedup
        all_erps = set()
        for x in df['ERP'].fillna("Unknown"):
            val = str(x).strip()
            if val == '' or val == 'nan' or val == 'Unknown':
                all_erps.add("Unknown")
            else:
                for erp in val.split(','):
                    all_erps.add(erp.strip())
        
        all_erps = sorted(list(all_erps))
        if not all_erps:
            all_erps = ["Unknown"]
            
        selected_erps = st.sidebar.multiselect(
            "ERP System Used", 
            all_erps, 
            default=all_erps,
            help="Select one or more ERP systems to filter leads. 'Unknown' includes leads where no specific ERP was detected."
        )
        
        # Custom mask for comma-separated values
        def has_selected_erp(val):
            if not isinstance(val, str) or val == '' or val == 'nan' or val == 'Unknown':
                 return "Unknown" in selected_erps
            val_list = [v.strip() for v in val.split(',')]
            return any(e in val_list for e in selected_erps)
            
        erp_mask = df['ERP'].apply(has_selected_erp) if selected_erps else pd.Series(True, index=df.index)
    else:
        erp_mask = pd.Series(True, index=df.index)

    # Apply Filters
    mask = (
        df['FilterState'].isin(selected_states) &
        df.get('Intensity', pd.Series('Unknown', index=df.index)).isin(selected_intensities) &
        erp_mask
    )
    
    # Apply Perspective Filter to metrics/charts/table
    if view_mode == "Today (Recent)":
        mask = mask & (df['first_seen_date'] == active_date)
        
    filtered_df = df[mask].copy()

    # ----------------------------------------------------------------------
    # Top Level Metrics
    # ----------------------------------------------------------------------
    st.markdown("### 📊 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    # Use metrics based on mode
    metric_leads = len(filtered_df)
    metric_high = len(filtered_df[filtered_df['Intensity'] == 'High']) if 'Intensity' in filtered_df.columns else 0
    metric_unique = len(filtered_df['Company'].unique()) if 'Company' in filtered_df.columns else 0
    metric_last_sync = active_date if view_mode == "Today (Recent)" else (latest_date_in_df or "N/A")
    
    with col1:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{metric_leads:,}</div><div class="metric-label">{"Recent Leads" if "Today" in view_mode else "Total Leads"}</div></div>', unsafe_allow_html=True)
    with col2:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{metric_high:,}</div><div class="metric-label">High Intensity</div></div>', unsafe_allow_html=True)
    with col3:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{metric_unique:,}</div><div class="metric-label">Unique Companies</div></div>', unsafe_allow_html=True)
    with col4:
        # Use regions count if in Total mode, else show the active date
        if view_mode == "Today (Recent)":
            st.markdown(f'<div class="metric-card"><div class="metric-value">{active_date}</div><div class="metric-label">Data Date</div></div>', unsafe_allow_html=True)
        else:
            regions_count = filtered_df['FilterState'].nunique() if 'FilterState' in filtered_df.columns else 0
            st.markdown(f'<div class="metric-card"><div class="metric-value">{regions_count}</div><div class="metric-label">Regions Covered</div></div>', unsafe_allow_html=True)

    st.markdown("---")

    if not df.empty:
        # ----------------------------------------------------------------------
        # Charts & Visualizations
        # ----------------------------------------------------------------------
        st.markdown("### 📈 Visual Analytics")
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            # Jobs by State
            state_counts = filtered_df['FilterState'].value_counts().reset_index()
            state_counts.columns = ['Region', 'Job Count']
            if not state_counts.empty:
                fig_state = px.bar(
                    state_counts.head(15), 
                    x='Region', 
                    y='Job Count', 
                    title='Top 15 Regions by Job Volume',
                    color='Job Count',
                    color_continuous_scale=px.colors.sequential.Tealgrn,
                    template="plotly_dark"
                )
                fig_state.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_state, use_container_width=True)
            else:
                st.info("Not enough data for chart")
            
        with chart_col2:
            # Hiring Intensity Distribution
            if 'Intensity' in filtered_df.columns and not filtered_df.empty:
                intensity_counts = filtered_df['Intensity'].value_counts().reset_index()
                intensity_counts.columns = ['Intensity', 'Count']
                fig_intensity = px.pie(
                    intensity_counts, 
                    names='Intensity', 
                    values='Count', 
                    title='Hiring Intensity Breakdown',
                    hole=0.5,
                    color='Intensity',
                    color_discrete_map={'High': '#DC2626', 'Medium': '#F59E0B', 'Low': '#3B82F6', 'Unknown': '#6B7280'},
                    template="plotly_dark"
                )
                fig_intensity.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_intensity, use_container_width=True)
            else:
                st.info("Not enough data for chart")

    # Define display columns for the explorer
    display_cols = [
        'Company', 'Job Title', 'Job Description', 'ERP', 'Location', 
        'Employment type', 'Experience', 'Posted', 'first_seen_date', 'Job url', 
        'Source', 'Industry', 'Intensity', 'Timestamp'
    ]

    # ----------------------------------------------------------------------
    # Detailed Data Table
    # ----------------------------------------------------------------------
    st.markdown("### 📋 Lead Explorer")
    st.markdown("Search, sort, and explore individual job leads.")
    
    # Display dataframe using all requested columns in order
    display_df = filtered_df[[c for c in display_cols if c in filtered_df.columns]]
            
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=500
    )

    # ----------------------------------------------------------------------
    # Downloads Section
    # ----------------------------------------------------------------------
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📥 Export Leads")
    
    # Generate CSV strings
    @st.cache_data
    def convert_df(df_to_convert):
        return df_to_convert.to_csv(index=False).encode('utf-8')

    # Generate Excel strings
    @st.cache_data
    def convert_df_to_excel(df_to_convert):
        output = io.BytesIO()
        try:
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_to_convert.to_excel(writer, index=False, sheet_name='Leads')
        except Exception as e:
            st.error(f"Excel generation error: {e}")
            return None
        return output.getvalue()

    # Full Dataset Download
    st.sidebar.markdown("**Full Dataset**")
    col1, col2 = st.sidebar.columns(2)
    col1.download_button(
        label="CSV",
        data=convert_df(df),
        file_name='rhocash_ap_all_leads.csv',
        mime='text/csv',
        use_container_width=True
    )
    
    excel_data = convert_df_to_excel(df)
    if excel_data:
        col2.download_button(
            label="Excel",
            data=excel_data,
            file_name='rhocash_ap_all_leads.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            use_container_width=True
        )
    
    # High Intensity leads Download
    if 'Intensity' in df.columns:
        high_df = df[df['Intensity'] == 'High']
        if not high_df.empty:
            st.sidebar.markdown("**High-Intensity Leads**")
            col_h1, col_h2 = st.sidebar.columns(2)
            col_h1.download_button(
                label="CSV",
                data=convert_df(high_df),
                file_name='rhocash_ap_high_intensity.csv',
                mime='text/csv',
                use_container_width=True
            )
            high_excel = convert_df_to_excel(high_df)
            if high_excel:
                col_h2.download_button(
                    label="Excel",
                    data=high_excel,
                    file_name='rhocash_ap_high_intensity.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    use_container_width=True
                )
    
    # Historical Leads Download (filtered dynamically by date)
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Select Date for Export**")
    
    # Safely get max date with leads
    if not df.empty and 'first_seen_date' in df.columns:
        valid_dates = pd.to_datetime(df['first_seen_date']).dt.date.unique()
        max_date_with_data = max(valid_dates) if len(valid_dates) > 0 else datetime.today().date()
    else:
        max_date_with_data = datetime.today().date()
    
    selected_date = st.sidebar.date_input(
        "Select Date", 
        value=max_date_with_data,
        help="Download leads discovered on this specific date"
    )
    
    if selected_date:
        # Handle date parsing safely
        date_col = 'first_seen_date' if 'first_seen_date' in df.columns else 'Posted'
        if date_col in df.columns:
            mask = pd.to_datetime(df[date_col]).dt.date == selected_date
            range_df = df[mask]
            
            if not range_df.empty:
                st.sidebar.info(f"📍 {len(range_df)} leads found on {selected_date}.")
                col_r1, col_r2 = st.sidebar.columns(2)
                col_r1.download_button(
                    label="CSV",
                    data=convert_df(range_df),
                    file_name=f'rhocash_leads_{selected_date}.csv',
                    mime='text/csv',
                    use_container_width=True
                )
                range_excel = convert_df_to_excel(range_df)
                if range_excel:
                    col_r2.download_button(
                        label="Excel",
                        data=range_excel,
                        file_name=f'rhocash_leads_{selected_date}.xlsx',
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        use_container_width=True
                    )
            else:
                st.sidebar.warning(f"⚠️ No leads found for {selected_date}.")
                st.sidebar.caption("Try selecting a different date or check if the scraper ran successfully on that day.")

if __name__ == "__main__":
    run_app()
