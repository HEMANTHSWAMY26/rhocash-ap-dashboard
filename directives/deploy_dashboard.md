# Directive: Deploy Dashboard

**Goal:** Automate or document the flow for deploying the Streamlit application to Streamlit Cloud.

## Deployment Flow
- Platform: Streamlit Cloud
- Repository Source: GitHub

## Tools/Scripts
- Execution Tool: `execution/deploy_streamlit.py`
- Main Dashboard File: `dashboard.py`

## Dashboard Features Required
### Metrics
- Total jobs
- Unique companies
- High intensity companies
- States covered

### Charts
- Jobs by state (Bar/Map)
- Jobs by source (Pie/Bar)
- Hiring intensity distribution (Pie/Donut)

### Data Views
- Searchable, sortable job table

### Downloads
- Download full dataset (CSV from Master Sheet equivalent)
- Download high intensity leads
- Download today's leads

## Outputs
- A public dashboard URL (e.g., `https://rhocash-ap-dashboard.streamlit.app`)
- A functioning web interface.
