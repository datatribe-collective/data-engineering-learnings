import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
load_dotenv()

def get_bigquery_client():
    # Define credential paths for both local and container environments
    cred_path_local = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH", "creds/gcp_service_account.json")
    cred_path_container = "/opt/airflow/creds/gcp_service_account.json"

    # Determine whether running inside a Docker container
    is_container = os.path.exists("/.dockerenv") or os.getenv("DOCKER_CONTAINER") == "true"

    # Choose appropriate credential path
    cred_path = cred_path_container if is_container else cred_path_local
    print(f"Using credential path: {cred_path}")

    # Raise error if the credential file does not exist
    if not os.path.exists(cred_path):
        raise FileNotFoundError(f"Credential file not found: {cred_path}")

    # Load credentials and initialize BigQuery client
    credentials = service_account.Credentials.from_service_account_file(cred_path)
    return bigquery.Client(credentials=credentials, location="US")