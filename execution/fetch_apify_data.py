import os
import pandas as pd
from apify_client import ApifyClient
from dotenv import load_dotenv
import logging
import json
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

def fetch_apify_data():
    """
    Fetch completed runs from TASK_US and TASK_CANADA.
    Tracks processed runs to avoid duplicates.
    Fetches dynamic schedule info from Apify.
    """
    api_token = os.getenv("APIFY_API_TOKEN")
    if not api_token:
        logging.error("APIFY_API_TOKEN is missing.")
        return

    client = ApifyClient(api_token)
    
    tasks = {
        "US": os.getenv("TASK_US"),
        "Canada": os.getenv("TASK_CANADA")
    }
    
    os.makedirs(".tmp", exist_ok=True)
    processed_runs_file = ".tmp/processed_runs.txt"
    config_file = ".tmp/system_config.json"
    
    # Load processed runs
    processed_runs = set()
    if os.path.exists(processed_runs_file):
        with open(processed_runs_file, "r") as f:
            processed_runs = set(line.strip() for line in f if line.strip())
            
    all_items = []
    new_runs_processed = []
    
    # 1. Fetch Dynamic Schedule Info
    try:
        task_schedule_info = "Not found"
        # Try finding by schedule list
        schedules = client.schedules().list().items
        for s in schedules:
            actions = s.get('actions', [])
            for a in actions:
                if a.get('taskId') in tasks.values():
                    task_schedule_info = s.get('cronExpression', 'Custom')
                    break
            if task_schedule_info != "Not found": break
            
        # Fallback: Check if the task itself has schedule metadata or is part of a known pattern
        if task_schedule_info == "Not found":
            # Check US task specifically
            t_us = client.task(tasks["US"]).get()
            # Some tasks have a 'stats' or similar that might hint at schedule, 
            # but usually it's in the schedule object. 
            # We'll stick to a cleaner "Enabled/Disabled" if nothing else.
            status = "Active" if not t_us.get('isArchived') else "Paused"
            task_schedule_info = f"{status} (Dynamic)"

        with open(config_file, "w") as f:
            json.dump({
                "schedule": task_schedule_info, 
                "last_sync": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }, f)
        logging.info(f"Dynamic schedule info saved: {task_schedule_info}")
    except Exception as e:
        logging.warning(f"Could not fetch schedule info: {e}")

    # 2. Fetch Runs
    for name, task_id in tasks.items():
        if not task_id:
            continue
            
        logging.info(f"Checking runs for {name} Task: {task_id}")
        try:
            runs_list = client.task(task_id).runs().list(limit=50, desc=True).items
            
            for run in runs_list:
                run_id = run['id']
                if run['status'] == 'SUCCEEDED' and run_id not in processed_runs:
                    started_at = run['startedAt']
                    
                    # Convert UTC to IST (+5:30)
                    from datetime import timedelta
                    if isinstance(started_at, datetime):
                        ist_time = started_at + timedelta(hours=5, minutes=30)
                        run_date = ist_time.strftime('%Y-%m-%d')
                    else:
                        # Fallback parsing
                        try:
                            dt_utc = datetime.strptime(str(started_at).split('.')[0], "%Y-%m-%dT%H:%M:%S")
                            ist_time = dt_utc + timedelta(hours=5, minutes=30)
                            run_date = ist_time.strftime('%Y-%m-%d')
                        except:
                            run_date = str(started_at).split('T')[0]
                        
                    logging.info(f"New Run Detected: {run_id} (IST Date: {run_date})")
                    
                    dataset_items = client.dataset(run['defaultDatasetId']).list_items().items
                    
                    for item in dataset_items:
                        item['run_id'] = run_id
                        item['scraped_date'] = run_date
                        all_items.append(item)
                    
                    new_runs_processed.append(run_id)
            
        except Exception as e:
            logging.error(f"Error fetching data for {name}: {e}")

    # 3. Save Data and Update Tracker
    if all_items:
        df_new = pd.DataFrame(all_items)
        output_path = os.path.join(".tmp", "jobs_merged.csv")
        
        # Append if file exists, else create
        if os.path.exists(output_path):
            df_old = pd.read_csv(output_path)
            df_final = pd.concat([df_old, df_new], ignore_index=True)
            df_final.to_csv(output_path, index=False)
        else:
            df_new.to_csv(output_path, index=False)
            
        # Update tracker
        with open(processed_runs_file, "a") as f:
            for rid in new_runs_processed:
                f.write(f"{rid}\n")
        
        logging.info(f"Processed {len(new_runs_processed)} new runs. Total raw items in cache: {len(all_items) if not os.path.exists(output_path) else 'Updated'}")
    else:
        logging.info("No new runs to process.")

if __name__ == "__main__":
    fetch_apify_data()
