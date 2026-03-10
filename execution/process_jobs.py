import os
import json
import logging
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        logging.error(f"Input file {input_path} does not exist. Run the scraper first.")
        raise FileNotFoundError(f"Missing {input_path}")

    try:
        try:
            df = pd.read_csv(input_path)
        except pd.errors.EmptyDataError:
            logging.warning("No data found in jobs_merged.csv (EmptyDataError). Creating empty processed dataset.")
            pd.DataFrame(columns=[
                "Company Name", "Job Title", "Job Description", "City", 
                "State", "Country", "Posted Date", "Job URL", "Source"
            ]).to_csv(output_path, index=False)
            return

        if df.empty:
            logging.warning("jobs_merged.csv has headers but no rows. Creating empty processed dataset.")
            pd.DataFrame(columns=[
                "Company Name", "Job Title", "Job Description", "City", 
                "State", "Country", "Posted Date", "Job URL", "Source"
            ]).to_csv(output_path, index=False)
            return
        logging.info(f"Loaded {len(df)} initial records.")
        
        # Map Apify raw columns to expected columns if present
        rename_map = {
            'companyName': 'Company Name',
            'title': 'Job Title',
            'description': 'Job Description',
            'date': 'Posted Date',
            'source': 'Source'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Parse City and State from 'location'
        if 'location' in df.columns:
            df['City'] = df['location'].astype(str).str.split(',').str[0].str.strip()
            df['State'] = df['location'].astype(str).str.split(',').str[1].str.strip().str[:2]
            
        # Merge URL columns safely
        if 'applyLink' in df.columns:
            df['Job URL'] = df['applyLink']
        elif 'job_url' in df.columns:
            df['Job URL'] = df['job_url']

        # 1. Ensure all required columns exist (fill with 'Unknown' or None if missing)
        required_cols = {
            "Company": "Unknown",
            "Job Title": "Unknown",
            "Job Description": "",
            "Location": "Unknown",
            "Employment type": "Full Time",
            "Experience": "Unknown",
            "Posted": datetime.today().strftime('%Y-%m-%d'),
            "Job url": "Unknown",
            "Source": "Unknown",
            "Industry": "Unknown",
            "ERP": "Unknown",
            "first_seen_date": datetime.today().strftime('%Y-%m-%d')
        }

        for col, default_val in required_cols.items():
            if col not in df.columns:
                df[col] = default_val

        # 2. Normalize and Extract Advanced Fields
        
        # Match standard schema explicitly
        df['Company'] = df.get('Company Name', pd.Series(dtype='object')).str.strip().str.title()
        df['Company'] = df['Company'].replace(r'(?i)\b(inc\.?|llc\.?|corp\.?)\b', '', regex=True).str.strip()
        
        df['Location'] = df['location'] if 'location' in df.columns else df['City'] + ", " + df['State']
        df['Posted'] = df.get('Posted Date', pd.Series(dtype='object'))
        df['Job url'] = df.get('Job URL', pd.Series(dtype='object'))
        
        # Heuristics for missing fields based on title and description
        def extract_experience(text):
            text = str(text).lower()
            if any(w in text for w in ['senior', 'manager', 'lead', 'director', '5+ years', '5 years']): return '5+ yrs'
            if any(w in text for w in ['junior', 'entry', '0-1', '0-2', 'trainee', 'new grad']): return 'Entry Level'
            if any(w in text for w in ['3+ years', '3 years', '2-4']): return '2-4 yrs'
            return '1-3 yrs' # Default assumption for mid-level AP roles
            
        def extract_employment_type(text):
            text = str(text).lower()
            if any(w in text for w in ['part time', 'part-time', 'pt']): return 'Part Time'
            if any(w in text for w in ['contract', 'temporary', 'freelance']): return 'Contract'
            return 'Full Time'
            
        def summarize_description(text):
            text = str(text).replace('\n', ' ').strip()
            # Truncate to first 120 chars for a simple one-liner
            return text[:120] + "..." if len(text) > 120 else text

        def extract_erp(text):
            text = str(text).lower()
            found_erps = []
            if 'netsuite' in text: found_erps.append('NetSuite')
            if 'oracle' in text: found_erps.append('Oracle')
            if 'sap' in text: found_erps.append('SAP')
            if 'workday' in text: found_erps.append('Workday')
            if 'dynamics' in text: found_erps.append('MS Dynamics')
            if 'sage' in text: found_erps.append('Sage')
            if 'quickbooks' in text: found_erps.append('QuickBooks')
            if 'xero' in text: found_erps.append('Xero')
            if 'coupa' in text: found_erps.append('Coupa')
            if 'concur' in text: found_erps.append('Concur')
            if 'bill.com' in text: found_erps.append('Bill.com')
            if 'blackline' in text: found_erps.append('BlackLine')
            if 'yardi' in text: found_erps.append('Yardi')
            if 'appfolio' in text: found_erps.append('AppFolio')
            
            if not found_erps:
                return "Unknown"
            return ", ".join(found_erps)

        # Apply ERP extraction BEFORE summarization truncates it!
        df['ERP'] = df['Job Description'].apply(extract_erp)
        
        def extract_source(url):
            url_str = str(url).lower()
            if 'indeed.' in url_str: return 'Indeed'
            if 'linkedin.' in url_str: return 'LinkedIn'
            if 'ziprecruiter.' in url_str: return 'ZipRecruiter'
            if 'glassdoor.' in url_str: return 'Glassdoor'
            if 'monster.' in url_str: return 'Monster'
            if 'careerbuilder.' in url_str: return 'CareerBuilder'
            if 'dice.' in url_str: return 'Dice'
            if 'builtin' in url_str: return 'Built In'
            if 'simplyhired' in url_str: return 'SimplyHired'
            if 'flexjobs' in url_str: return 'FlexJobs'
            if 'google' in url_str and 'jobs' in url_str: return 'Google Jobs'
            return 'Unknown'


        
        df['Experience'] = (df['Job Title'].astype(str) + " " + df['Job Description'].astype(str)).apply(extract_experience)
        df['Employment type'] = df['Job Description'].apply(extract_employment_type)
        df['Industry'] = "Finance / Accounting" # Default industry for Accounts Payable dashboard
        df['Job Description'] = df['Job Description'].apply(summarize_description)
        df['Source'] = df['Job url'].apply(extract_source)

        # 3. Normalize Dates to YYYY-MM-DD
        df['Posted'] = pd.to_datetime(df['Posted'], errors='coerce')
        df['Posted'] = df['Posted'].fillna(pd.Timestamp.today())
        
        # 4. Apply strict 3-Day Cutoff Filter
        cutoff_date = pd.Timestamp.today() - pd.Timedelta(days=3)
        initial_len = len(df)
        df = df[df['Posted'] >= cutoff_date].copy()
        
        # Format string dates back to standard
        df['Posted'] = df['Posted'].dt.strftime('%Y-%m-%d')
        logging.info(f"Dropped {initial_len - len(df)} jobs older than 3 days. Remaining: {len(df)}")
        
        # 5. Geographic Filter (US & Canada Only)
        def is_north_america(loc):
            loc_str = str(loc).upper()
            # Common patterns for US/Canada locations
            geos = [
                'US', 'USA', 'UNITED STATES', 'CA', 'CANADA', ', AL', ', AK', ', AZ', ', AR', ', CA', 
                ', CO', ', CT', ', DE', ', FL', ', GA', ', HI', ', ID', ', IL', ', IN', ', IA', ', KS', 
                ', KY', ', LA', ', ME', ', MD', ', MA', ', MI', ', MN', ', MS', ', MO', ', MT', ', NE', 
                ', NV', ', NH', ', NJ', ', NM', ', NY', ', NC', ', ND', ', OH', ', OK', ', OR', ', PA', 
                ', RI', ', SC', ', SD', ', TN', ', TX', ', UT', ', VT', ', VA', ', WA', ', WV', ', WI', ', WY',
                'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'ON', 'PE', 'QC', 'SK'
            ]
            return any(geo in loc_str for geo in geos)

        initial_len = len(df)
        df = df[df['Location'].apply(is_north_america)].copy()
        logging.info(f"Geographic Filter: Dropped {initial_len - len(df)} non-NA jobs.")

        # 6. Local URL Cache & Deduplication
        initial_len = len(df)
        cache_file = os.path.join(".tmp", "seen_jobs.csv")
        seen_urls = set()
        
        # Load local cache if it exists
        if os.path.exists(cache_file):
            try:
                cache_df = pd.read_csv(cache_file)
                if 'url' in cache_df.columns:
                    seen_urls = set(cache_df['url'].dropna().tolist())
            except Exception as e:
                logging.warning(f"Failed to read local cache: {e}")
        
        # First dedup internally within this exact batch
        if 'Job url' in df.columns:
            df = df[~df.duplicated(subset=['Job url'], keep='first') | df['Job url'].isna() | (df['Job url'] == "Unknown")]
            
        df.drop_duplicates(subset=['Company', 'Job Title', 'Location'], inplace=True)
        
        # Then dedup against local cache
        if 'Job url' in df.columns:
            df = df[~df['Job url'].isin(seen_urls)]
            
        logging.info(f"Removed {initial_len - len(df)} duplicates (internal + cache). Remaining: {len(df)}")
        
        # Update local cache with newly seen URLs
        if not df.empty and 'Job url' in df.columns:
            new_urls = df[df['Job url'].notna() & (df['Job url'] != "Unknown")]['Job url'].unique()
            if len(new_urls) > 0:
                url_df = pd.DataFrame({'url': new_urls})
                url_df.to_csv(cache_file, mode='a', header=not os.path.exists(cache_file), index=False)
                logging.info(f"Added {len(new_urls)} new URLs to local cache '{cache_file}'")

        # Limit to only the required columns and order them
        final_df = df[[col for col in required_cols.keys() if col in df.columns]].copy()
        
        # Enforce all keys exist fully
        for col, default_val in required_cols.items():
            if col not in final_df.columns:
                final_df[col] = default_val

        # 5. Save output
        final_df.to_csv(output_path, index=False)
        logging.info(f"Successfully processed {len(final_df)} jobs to {output_path}")

    except Exception as e:
        logging.error(f"Error processing jobs: {e}")
        raise

if __name__ == "__main__":
    process_jobs()
