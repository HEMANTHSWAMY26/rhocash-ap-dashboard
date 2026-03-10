# Accounts Payable Lead Generation Dashboard
An automated, end-to-end data mining pipeline and interactive dashboard for tracking high-value Accounts Payable jobs and leads.

## Overview
This system automatically scrubs the web for remote Accounts Payable job listings. It extracts critical metadata—such as hiring intensity, required software/ERP systems (NetSuite, Workday, SAP, etc.), job complexity, and company location—deduplicates the leads to ensure quality, and syncs the live results to an interactive Web UI and a Google Sheet.

### Features
* **Live Extraction**: Real-time integration with Apify headless cloud crawlers.
* **Smart Deduplication**: Automatic 3-day recency cutoff and exact-match job filtering.
* **Goal Tracking**: Daily velocity metrics measuring leads/day against a 10,000 lead goal.
* **Historical Downloading**: A UI Date-Picker allowing specific daily-batch CSV exports.
* **Background Scheduler**: A server-side daemon hardcoded to automatically launch the pipeline daily at 10:00 PM IST.

## Interacting with the Application
This application is designed to be completely hands-off. 
* **The Background Daemon**: Ensure the `▶️ Start Daily Daemon` button has been clicked. The dashboard will silently update itself every evening without manual intervention.
* **Manual Overrides**: If fresh data is needed instantly, the `▶️ Run Scraper` button connects to the Apify cloud, streams the extraction logs directly to the terminal UI, and injects the new leads into the interface upon completion.
* **Exporting Leads**: Use the `📥 Export Leads` sidebar module to selectively export data by `first_seen_date` or to dump the entire lifetime lead catalog into a single CSV.

## Stack Architecture
* **Frontend UI**: Built with Streamlit in Python.
* **Pipeline Backend**: A sequence of modular Python execution scripts located in `/execution`.
* **Database Sync**: Custom integration with Google Sheets API and local `.tmp` caching.
* **Data Mining**: Powered by the Apify Client API using headless Chromium.

*Built for robust remote lead generation.*
