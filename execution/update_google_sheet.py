import os
import logging
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

def update_google_sheet():
    """
    Append processed job leads to the Google Sheet.
    Input: .tmp/jobs_merged.csv
    Requires: GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SHEET_ID
    Output: Google Sheet updated (Master + Daily)
    """
    input_path = os.path.join(".tmp", "jobs_merged.csv")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    service_account_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")

    logging.info("Starting Google Sheet update process")

    if not sheet_id:
        logging.error("GOOGLE_SHEET_ID is missing in the environment variables.")
        return

    if not os.path.exists(service_account_path):
        logging.error(f"Service account file {service_account_path} not found.")
        return

    if not os.path.exists(input_path):
        logging.error(f"Input file {input_path} does not exist.")
        raise FileNotFoundError(f"Missing {input_path}")

    try:
        try:
            df = pd.read_csv(input_path)
        except pd.errors.EmptyDataError:
            logging.warning(f"Input file {input_path} is completely empty (EmptyDataError). Nothing to append.")
            return

        if df.empty:
            logging.warning("Input dataset has headers but is empty. Nothing to append.")
            return

        # Add Timestamp column in requested format (DD-MM-YYYY at I:Mampm)
        df['Timestamp'] = datetime.now().strftime('%d-%m-%Y at %I:%M%p').lower()

        # Expected Columns Order per directive
        expected_cols = [
            'Company', 'Job Title', 'Job Description', 'Location', 
            'Employment type', 'Experience', 'Posted', 'first_seen_date', 'Job url', 
            'Source', 'Industry', 'Intensity', 'ERP', 'Timestamp'
        ]
        
        # We ensure missing optional merge cols don't break
        for col in expected_cols:
            if col not in df.columns:
                df[col] = "Unknown"

        append_data = df[expected_cols].fillna("").values.tolist()
        
        logging.info(f"Authenticating with Google Sheets for sheet ID: {sheet_id}")
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(service_account_path, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)

        # 1. Update Master Sheet with Deduplication
        try:
            master_sheet = spreadsheet.worksheet("Master Sheet")
            # Use get_all_values() instead of get_all_records() to avoid empty column duplicate header exceptions
            raw_data = master_sheet.get_all_values()
            
            existing_data = []
            if len(raw_data) > 1:
                headers = raw_data[0]
                for row in raw_data[1:]:
                    # map safely incase of length mismatch
                    row_dict = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
                    existing_data.append(row_dict)
            
            # Create a set of existing unique identifiers
            # Primary key: Job url. Secondary key: Company + Job Title + Location
            existing_keys = set()
            for row in existing_data:
                url = str(row.get('Job url', '')).strip()
                if url and url != 'Unknown' and url != 'None':
                    existing_keys.add(url)
                else:
                    # Fallback key
                    comp = str(row.get('Company', '')).strip().lower()
                    title = str(row.get('Job Title', '')).strip().lower()
                    loc = str(row.get('Location', '')).strip().lower()
                    existing_keys.add(f"{comp}|{title}|{loc}")
            
            # Filter new df against existing keys
            def is_new(row):
                url = str(row.get('Job url', '')).strip()
                if url and url != 'Unknown' and url != 'None':
                    return url not in existing_keys
                
                comp = str(row.get('Company', '')).strip().lower()
                title = str(row.get('Job Title', '')).strip().lower()
                loc = str(row.get('Location', '')).strip().lower()
                return f"{comp}|{title}|{loc}" not in existing_keys
                
            # Apply filter
            initial_count = len(df)
            df = df[df.apply(is_new, axis=1)]
            logging.info(f"Deduplication: {initial_count} total scraped -> {len(df)} net-new jobs to add.")
            
            if df.empty:
                logging.info("No net-new jobs found in this batch. All jobs already exist in Master Sheet. Skipping append.")
                return

        except gspread.exceptions.WorksheetNotFound:
            logging.info("Master Sheet not found, creating it recursively.")
            master_sheet = spreadsheet.add_worksheet(title="Master Sheet", rows="1000", cols="20")
            master_sheet.append_row(expected_cols)

        # Prepare append payload for strictly new jobs
        append_data = df[expected_cols].fillna("").values.tolist()

        logging.info(f"Appending {len(append_data)} net-new rows to Master Sheet")
        master_sheet.append_rows(append_data, value_input_option='USER_ENTERED')

        # 2. Update Daily Sheet
        daily_sheet_name = f"DailyJobs-{datetime.now().strftime('%Y-%m-%d')}"
        try:
            daily_sheet = spreadsheet.worksheet(daily_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            logging.info(f"Daily sheet '{daily_sheet_name}' not found, creating it.")
            daily_sheet = spreadsheet.add_worksheet(title=daily_sheet_name, rows="1000", cols="20")
            daily_sheet.append_row(expected_cols)

        logging.info(f"Appending {len(append_data)} rows to {daily_sheet_name}")
        daily_sheet.append_rows(append_data, value_input_option='USER_ENTERED')

        logging.info("Successfully updated Google Sheets.")

    except Exception as e:
        logging.error(f"Error updating Google Sheets: {e}")
        raise

if __name__ == "__main__":
    update_google_sheet()
