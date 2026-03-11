import os
import logging
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

def update_google_sheet():
    """
    Append processed job leads to the Google Sheet with strict URL deduplication.
    Input: .tmp/processed_jobs.csv
    """
    input_path = os.path.join(".tmp", "processed_jobs.csv")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    service_account_info = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    logging.info("Starting Google Sheet update process")

    if not sheet_id or not service_account_info:
        logging.error("Missing Google Sheets configuration.")
        return

    if not os.path.exists(input_path):
        logging.error(f"Input file {input_path} does not exist.")
        return

    try:
        # Load credentials
        if service_account_info.strip().startswith('{'):
            creds_out = json.loads(service_account_info)
        else:
            with open(service_account_info, 'r') as f:
                creds_out = json.load(f)

        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_out, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)

        # Load processed data
        df_new = pd.read_csv(input_path)
        if df_new.empty:
            logging.info("No new data to sync.")
            return

        # Define Schema based on System Prompt
        # Fields: job_title, company, location, description, job_url, source, scraped_date, erp, run_id, intensity
        expected_cols = ['job_title', 'company', 'location', 'description', 'job_url', 'source', 'scraped_date', 'erp', 'run_id', 'intensity']
        
        # Ensure all columns exist
        for col in expected_cols:
            if col not in df_new.columns:
                df_new[col] = "N/A"

        # 1. Sync Master Sheet
        try:
            master_sheet = spreadsheet.worksheet("Master Sheet")
        except gspread.exceptions.WorksheetNotFound:
            logging.info("Creating Master Sheet...")
            master_sheet = spreadsheet.add_worksheet(title="Master Sheet", rows="1000", cols=str(len(expected_cols)))
            master_sheet.append_row(expected_cols)

        # Fetch existing URLs for deduplication
        # Optimization: Fetch only the URL column if possible, but gspread doesn't have a direct "get column" that is faster than get_all_records
        try:
            existing_records = master_sheet.get_all_records()
            existing_urls = {str(r.get('job_url', '')).strip() for r in existing_records if r.get('job_url')}
        except Exception:
            # If header is mismatched or other error, fallback to empty
            existing_urls = set()

        # 2. Deduplicate
        initial_count = len(df_new)
        df_new = df_new[~df_new['job_url'].astype(str).str.strip().isin(existing_urls)]
        logging.info(f"Deduplication: {initial_count} leads -> {len(df_new)} net-new records.")

        if df_new.empty:
            logging.info("No net-new records to append.")
            return

        # 3. Append to Master Sheet
        append_data = df_new[expected_cols].fillna("").values.tolist()
        master_sheet.append_rows(append_data, value_input_option='USER_ENTERED')
        logging.info(f"Successfully appended {len(append_data)} rows to Master Sheet.")

    except Exception as e:
        logging.error(f"Error updating Google Sheets: {e}")
        raise

if __name__ == "__main__":
    update_google_sheet()

if __name__ == "__main__":
    update_google_sheet()
