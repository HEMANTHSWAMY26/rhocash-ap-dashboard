import time
import subprocess
import sys
from datetime import datetime, timedelta

def run_pipeline():
    print(f"[{datetime.now()}] Running scheduled pipeline...")
    try:
        subprocess.run(["python", "execution/run_apify_scraper.py"], check=True)
        subprocess.run(["python", "execution/process_jobs.py"], check=True)
        subprocess.run(["python", "execution/calculate_hiring_intensity.py"], check=True)
        subprocess.run(["python", "execution/generate_dashboard_data.py"], check=True)
        subprocess.run(["python", "execution/update_google_sheet.py"], check=True)
        print(f"[{datetime.now()}] Scheduled pipeline completed successfully.")
    except Exception as e:
        print(f"[{datetime.now()}] Pipeline Failed: {e}")

def main():
    print(f"Background scheduler started. Hardcoded to run daily at 10:00 PM IST.")
    
    while True:
        import pytz
        ist_tz = pytz.timezone('Asia/Kolkata')
        
        # Get current time in IST
        now_utc = datetime.now(pytz.utc)
        now_ist = now_utc.astimezone(ist_tz)
        
        # Target 10:00 PM (22:00:00) IST today
        target_ist = now_ist.replace(hour=22, minute=0, second=0, microsecond=0)
        
        # If 10 PM IST has already passed today, target 10 PM IST tomorrow
        if now_ist >= target_ist:
            target_ist += timedelta(days=1)
            
        seconds_to_target = (target_ist - now_ist).total_seconds()
        
        print(f"[{datetime.now()}] Sleeping for {int(seconds_to_target)} seconds until 10:00 PM IST (Target: {target_ist.strftime('%Y-%m-%d %H:%M:%S')} IST)...")
        time.sleep(seconds_to_target)
        run_pipeline()

if __name__ == "__main__":
    main()
