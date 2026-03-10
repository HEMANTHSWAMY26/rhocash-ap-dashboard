import os
import json
import logging
from apify_client import ApifyClient
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

def main():
    """
    Run the Apify scraper to collect Accounts Payable jobs in the US.
    Output: .tmp/raw_jobs.json
    """
    logging.info("Starting Apify Job Scraper for Accounts Payable roles")

    # Initialize the ApifyClient with your API token
    api_token = os.getenv("APIFY_API_TOKEN")
    if not api_token:
        logging.error("APIFY_API_TOKEN is missing or empty in the environment variables.")
        return

    client = ApifyClient(api_token)

    # Find all TASK_* variables from the environment
    task_env_vars = {k: v for k, v in os.environ.items() if k.startswith("TASK_") and v}
    
    if not task_env_vars:
        logging.error("No TASK_ environment variables found. Please configure them in .env")
        return

    logging.info(f"Found {len(task_env_vars)} Apify tasks to run: {', '.join(task_env_vars.keys())}")

    all_items = []
    seen_urls = set()
    
    # Ensure .tmp directory exists before we start
    os.makedirs(".tmp", exist_ok=True)
    output_path = os.path.join(".tmp", "jobs_merged.csv")

    try:
        for task_name, task_id in task_env_vars.items():
            logging.info(f"--- Starting {task_name} (Task ID: {task_id}) ---")
            
            try:
                import time
                # Start the Task asynchronously
                run_info = client.task(task_id).start()
                run_id = run_info["id"]
                logging.info(f"Apify Cloud instance booted. Run ID: {run_id}")
                
                # Poll for completion to keep logs flowing to the UI
                while True:
                    run = client.run(run_id).get()
                    status = run["status"]
                    if status in ["SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"]:
                        break
                    logging.info(f"Apify is navigating job boards and extracting data... (Status: {status})")
                    time.sleep(10)
                    
                if run["status"] != "SUCCEEDED":
                    raise Exception(f"Apify Task failed with status: {run['status']}")
                # Fetch Task results from the run's dataset
                dataset_id = run["defaultDatasetId"]
                logging.info(f"{task_name} finished successfully. Fetching dataset {dataset_id}...")
                
                items = client.dataset(dataset_id).list_items().items
                logging.info(f"Retrieved {len(items)} job listings from {task_name}.")
                
                # Combine items and remove duplicates based on 'job_url' (if present in payload)
                added_count = 0
                for item in items:
                    url = item.get("job_url", item.get("url")) # Adjust based on exact Actor output structure
                    
                    # If no URL is present, we still add it but warn. 
                    if not url:
                        all_items.append(item)
                        added_count += 1
                    elif url not in seen_urls:
                        seen_urls.add(url)
                        all_items.append(item)
                        added_count += 1
                        
                logging.info(f"Added {added_count} new unique jobs from {task_name}")
            except Exception as task_error:
                logging.error(f"Error executing {task_name} ({task_id}): {task_error}")
                logging.warning(f"Skipping {task_name} due to failure, continuing to next task...")

        logging.info(f"--- Scraping Complete ---")
        logging.info(f"Total unique jobs collected across all tasks: {len(all_items)}")
        
        # Save merged results
        import pandas as pd
        if all_items:
            df = pd.DataFrame(all_items)
            df.to_csv(output_path, index=False)
            logging.info(f"Merged jobs successfully saved to {output_path}")
        else:
            logging.warning("No jobs were found across any task.")
            pd.DataFrame().to_csv(output_path, index=False)
        
    except Exception as e:
        logging.error(f"Critical failure in Apify scraper pipeline: {e}")
        # Re-raise so orchestration knows it failed completely
        raise

if __name__ == "__main__":
    main()
