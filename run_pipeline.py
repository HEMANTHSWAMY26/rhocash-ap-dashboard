import subprocess
import logging
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline():
    """
    Executes the complete Accounts Payable Data Mining Pipeline end-to-end.
    """
    logging.info(f"[{datetime.now()}] Initiating Manual Pipeline Execution...")
    
    scripts = [
        ("execution/fetch_apify_data.py", "1/4: Fetching Apify Historical Runs"),
        ("execution/process_jobs.py", "2/4: Normalizing and Cleaning Leads"),
        ("execution/calculate_hiring_intensity.py", "3/4: Calculating Hiring Intensity"),
        ("execution/update_google_sheet.py", "4/4: Syncing with Google Sheets Master")
    ]
    
    for script_path, desc in scripts:
        logging.info(f"Starting {desc} ({script_path})...")
        try:
            # Using check=True to immediately halt pipeline on any critical sub-script failure
            subprocess.run(["python", script_path], check=True)
            logging.info(f"✔ Completed {desc}")
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ Pipeline halted. Critical error in {script_path}: {e}")
            sys.exit(1)
            
    logging.info(f"[{datetime.now()}] ✅ Pipeline completed successfully.")

if __name__ == "__main__":
    run_pipeline()
