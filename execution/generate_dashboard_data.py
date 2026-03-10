import os
import logging
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_dashboard_data():
    """
    Generate aggregate analytics dataset for the Streamlit dashboard.
    Input: .tmp/jobs_with_intensity.csv
    Output: .tmp/dashboard_dataset.csv
    """
    input_path = os.path.join(".tmp", "jobs_with_intensity.csv")
    output_path = os.path.join(".tmp", "dashboard_dataset.csv")

    logging.info(f"Starting dashboard dataset generation from {input_path}")

    if not os.path.exists(input_path):
        logging.error(f"Input file {input_path} does not exist.")
        raise FileNotFoundError(f"Missing {input_path}")

    try:
        try:
            df = pd.read_csv(input_path)
        except pd.errors.EmptyDataError:
            logging.warning(f"Input file {input_path} is completely empty (EmptyDataError). Outputting empty dashboard dataset.")
            pd.DataFrame().to_csv(output_path, index=False)
            return
            
        # Even if empty, we save the empty frame so dashboard doesn't crash on file read
        if df.empty:
            logging.warning("Input dataset is empty. Outputting empty dashboard dataset.")
            df.to_csv(output_path, index=False)
            return

        # Basic cleans for dashboard aesthetics
        if 'Location' in df.columns: df['Location'] = df['Location'].fillna("Unknown")
        if 'Source' in df.columns: df['Source'] = df['Source'].fillna("Unknown")
        if 'Intensity' in df.columns: df['Intensity'] = df['Intensity'].fillna("Unknown")

        # In this implementation, the dashboard uses the primary `.tmp/dashboard_dataset.csv` 
        # as its central data source and then does on-the-fly aggregations via Pandas in Streamlit.
        # So we ensure the dataset is fully clean and ready.
        
        df.to_csv(output_path, index=False)
        logging.info(f"Successfully generated dashboard dataset with {len(df)} records at {output_path}")

    except Exception as e:
        logging.error(f"Error generating dashboard dataset: {e}")
        raise

if __name__ == "__main__":
    generate_dashboard_data()
