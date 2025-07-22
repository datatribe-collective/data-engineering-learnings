import os
import json
from utils import config
from google.cloud import bigquery
from google.oauth2 import service_account

def get_bigquery_client():
    cred_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if cred_json:
        info = json.loads(cred_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=info.get('project_id'))
    
    # Fallback to Local ADC or file
    cred_file = config.SERVICE_ACCOUNT_PATH
    if os.path.isfile(cred_file):
        creds = service_account.Credentials.from_service_account_file(cred_file)
        return bigquery.Client(credentials=creds)
    return bigquery.Client()