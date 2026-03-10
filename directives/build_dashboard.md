# Directive: Build Dashboard

**Goal:** Generate the required analytics dataset that will power the Streamlit dashboard metrics and visualizations.

## Inputs
- Input File: `.tmp/jobs_with_intensity.csv`

## Tasks
Prepare summarized data frames to support the following dashboard analytics:
1. Total jobs count
2. Unique companies count
3. Jobs distribution grouped by state
4. Distribution of hiring intensity (High vs Medium vs Low ratio)
5. Jobs grouped by source channel

## Tools/Scripts
- Execution Tool: `execution/generate_dashboard_data.py`

## Outputs
- File: `.tmp/dashboard_dataset.csv`
- Format: May be a single unified CSV or multiple aggregate tables prefixed as dashboard slices as needed by the frontend.

## Edge Cases
- Ensure any missing states or values count as "Unknown" rather than breaking the visualization distributions.
