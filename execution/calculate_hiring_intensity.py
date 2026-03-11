import os
import logging
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def calculate_intensity():
    """
    Calculate hiring intensity per company.
    Input: .tmp/processed_jobs.csv
    Output: .tmp/jobs_with_intensity.csv
    """
    input_path = os.path.join(".tmp", "processed_jobs.csv")
    output_path = os.path.join(".tmp", "jobs_with_intensity.csv")

    logging.info(f"Starting hiring intensity calculation on {input_path}")

    if not os.path.exists(input_path):
        logging.error(f"Input file {input_path} does not exist.")
        raise FileNotFoundError(f"Missing {input_path}")

    try:
        try:
            df = pd.read_csv(input_path)
        except pd.errors.EmptyDataError:
            logging.warning(f"Input file {input_path} is completely empty (EmptyDataError). Outputting identical empty dataset.")
            pd.DataFrame(columns=['company', 'Intensity']).to_csv(output_path, index=False)
            return
            
        if df.empty:
            logging.warning("Input dataset is empty. Outputting identical empty dataset.")
            df['Intensity'] = []
            df.to_csv(output_path, index=False)
            return

        # Group by Company and count the number of jobs
        job_counts = df.groupby('company').size().reset_index(name='Job Count')
        
        # Define logic: 1 job -> Low, 2-4 -> Medium, 5+ -> High
        def get_intensity(count):
            if count >= 5:
                return 'High'
            elif count >= 2:
                return 'Medium'
            else:
                return 'Low'

        job_counts['Intensity'] = job_counts['Job Count'].apply(get_intensity)
        
        logging.info(f"Calculated intensity for {len(job_counts)} unique companies.")

        # Merge the intensity metric back into the main jobs dataset
        merged_df = pd.merge(df, job_counts[['company', 'Intensity']], on='company', how='left')

        # Save output
        merged_df.to_csv(output_path, index=False)
        logging.info(f"Successfully saved {len(merged_df)} jobs with intensity tags to {output_path}")

    except Exception as e:
        logging.error(f"Error calculating intensity: {e}")
        raise

if __name__ == "__main__":
    calculate_intensity()
