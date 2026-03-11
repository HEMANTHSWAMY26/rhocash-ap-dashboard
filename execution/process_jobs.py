import os
import pandas as pd
import logging
import re
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def normalize_company(name):
    """Clean company names: strip Inc, LLC, etc., and title case."""
    if not isinstance(name, str): return "Unknown"
    name = name.strip()
    name = re.sub(r'(?i)\b(inc\.?|llc\.?|corp\.?|corporation|ltd\.?|limited|pvt\.?)\b', '', name).strip()
    # Remove trailing punctuation
    name = re.sub(r'[,.]$', '', name).strip()
    return name.title()

def normalize_title(title):
    """Normalize job titles: remove common noise."""
    if not isinstance(title, str): return "Unknown"
    title = title.strip().title()
    # Remove things like "(Remote)", "- Full Time", etc.
    title = re.sub(r'\(.*?\)', '', title).strip()
    title = re.sub(r'\s-\s.*$', '', title).strip()
    return title

def extract_source_from_url(url):
    """Determine source from domain if missing."""
    if not isinstance(url, str): return "Unknown"
    url_low = url.lower()
    if 'linkedin.com' in url_low: return 'LinkedIn'
    if 'indeed.com' in url_low: return 'Indeed'
    if 'ziprecruiter.com' in url_low: return 'ZipRecruiter'
    if 'glassdoor.com' in url_low: return 'Glassdoor'
    if 'recruit.net' in url_low: return 'Recruit.net'
    if 'monster.com' in url_low: return 'Monster'
    if 'google.' in url_low and 'jobs' in url_low: return 'Google Jobs'
    return "Unknown"

def extract_erp(text):
    """Detect ERP systems mentioned in the description."""
    text = str(text).lower()
    found_erps = []
    erps = {
        'NetSuite': 'netsuite', 'Oracle': 'oracle', 'SAP': 'sap', 
        'Workday': 'workday', 'MS Dynamics': 'dynamics', 'Sage': 'sage',
        'QuickBooks': 'quickbooks', 'Xero': 'xero', 'Coupa': 'coupa',
        'Concur': 'concur', 'Bill.com': 'bill.com', 'BlackLine': 'blackline',
        'Yardi': 'yardi', 'AppFolio': 'appfolio'
    }
    for name, keyword in erps.items():
        if keyword in text: found_erps.append(name)
    return ", ".join(found_erps) if found_erps else "Unknown"

def process_jobs():
    """
    Process and clean raw job data into structured leads.
    Input: .tmp/jobs_merged.csv
    Output: .tmp/processed_jobs.csv
    """
    input_path = os.path.join(".tmp", "jobs_merged.csv")
    output_path = os.path.join(".tmp", "processed_jobs.csv")

    logging.info(f"Starting processing of {input_path}")
    
    if not os.path.exists(input_path):
        logging.error(f"Input file {input_path} does not exist.")
        return

    try:
        df = pd.read_csv(input_path)
        if df.empty:
            logging.warning("No data found to process.")
            return

        logging.info(f"Loaded {len(df)} initial records.")
        
        # 1. Map Columns
        # Prioritize known good columns, fallback to raw Apify actor columns
        col_map = {
            'job_title': ['Job Title', 'title', 'jobTitle'],
            'company': ['Company', 'companyName', 'company'],
            'location': ['Location', 'location', 'city'],
            'description': ['Job Description', 'description', 'jobDescription'],
            'job_url': ['Job url', 'job_url', 'url', 'applyLink'],
            'source': ['Source', 'source'],
            'scraped_date': ['scraped_date', 'first_seen_date'],
            'run_id': ['run_id']
        }

        processed_data = []

        for _, row in df.iterrows():
            lead = {}
            for target_col, src_options in col_map.items():
                val = None
                for opt in src_options:
                    if opt in row and pd.notna(row[opt]):
                        val = row[opt]
                        break
                lead[target_col] = val

            # Mandatory cleaning and processing
            lead['company'] = normalize_company(lead.get('company'))
            lead['job_title'] = normalize_title(lead.get('job_title'))
            
            # Determine Source
            if not lead.get('source') or str(lead['source']) == 'Unknown':
                lead['source'] = extract_source_from_url(lead.get('job_url'))

            # Determine ERP
            lead['erp'] = extract_erp(lead.get('description', ''))

            # Ensure scraped_date exists (fallback only, should be fetched from run metadata)
            if not lead.get('scraped_date'):
                lead['scraped_date'] = datetime.today().strftime('%Y-%m-%d')
            
            if not lead.get('run_id'):
                lead['run_id'] = 'Unknown'

            processed_data.append(lead)

        df_processed = pd.DataFrame(processed_data)

        # 2. Strict Deduplication by job_url
        initial_len = len(df_processed)
        df_processed = df_processed[df_processed['job_url'].notna() & (df_processed['job_url'] != "Unknown")]
        df_processed = df_processed.drop_duplicates(subset=['job_url'], keep='first')
        
        # Also secondary deduplication by company + title + location just in case
        df_processed = df_processed.drop_duplicates(subset=['company', 'job_title', 'location'], keep='first')
        
        logging.info(f"Deduplication: {initial_len} -> {len(df_processed)}")

        # 3. Save
        df_processed.to_csv(output_path, index=False)
        logging.info(f"Successfully processed {len(df_processed)} jobs to {output_path}")

    except Exception as e:
        logging.error(f"Error processing jobs: {e}")
        raise

if __name__ == "__main__":
    process_jobs()

if __name__ == "__main__":
    process_jobs()
