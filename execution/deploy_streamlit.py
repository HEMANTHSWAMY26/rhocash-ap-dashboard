import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def deploy_streamlit():
    """
    Provides deployment instructions/automation for the Streamlit dashboard.
    """
    logging.info("Starting deployment helper for Streamlit Dashboard")

    print("\n--- Streamlit Dashboard Deployment ---\n")
    print("The Rhocash AP dashboard relies on Streamlit Cloud for deployment.")
    print("To deploy the application:")
    print("1. Ensure this entire project is pushed to a GitHub repository.")
    print("2. Commit the `requirements.txt` and `dashboard.py` files.")
    print("3. Go to https://share.streamlit.io/")
    print("4. Click 'New app'")
    print("5. Select your GitHub repository, branch, and specify 'dashboard.py' as the Main file path.")
    print("6. Under 'Advanced Settings', copy-paste your `.env` variables (e.g., APIFY_API_TOKEN, etc.) into the Secrets box.")
    print("7. Click 'Deploy!")
    print("\nFor local testing before deployment, run:")
    print("    streamlit run dashboard.py\n")

    logging.info("Deployment instructions generated successfully.")

if __name__ == "__main__":
    deploy_streamlit()
