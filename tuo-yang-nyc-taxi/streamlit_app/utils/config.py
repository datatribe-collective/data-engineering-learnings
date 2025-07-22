# scripts/config.py

import os
import yaml
from dotenv import load_dotenv

# Load environment variables from .env (if present)
load_dotenv()

# -----------------------------------
# Base directory of the project in container
# -----------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__) + '/..'))
PROJECT_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# -----------------------------------
# Load settings.yaml
# -----------------------------------
SETTINGS_PATH = os.path.join(BASE_DIR, "config.yaml")
with open(SETTINGS_PATH, "r") as f:
    SETTINGS = yaml.safe_load(f)

# -----------------------------------
# BigQuery Configuration
# -----------------------------------
raw_project_id = SETTINGS.get('bigquery_config', {}).get('project_id', '')
PROJECT_ID = os.path.expandvars(raw_project_id)

raw_dataset_id = SETTINGS.get('bigquery_config', {}).get('dataset', '')
DATASET_ID = os.path.expandvars(raw_dataset_id)

raw_table_name = SETTINGS.get('bigquery_config', {}).get('table', '')
TABLE_NAME = os.path.expandvars(raw_table_name)

raw_summary_table_name = SETTINGS.get('bigquery_config', {}).get('summary_table', '')
SUMMARY_TABLE_NAME = os.path.expandvars(raw_summary_table_name)

# -----------------------------------
# GeoJSON and Map Config
# -----------------------------------
GEOJSON_URL = os.path.join(BASE_DIR, SETTINGS.get('geojson_url', ''))
DEFAULT_GRANULARITIES = SETTINGS.get('default_granularities', ["Hourly", "Daily", "Weekly"])
PLOTLY_TEMPLATE = SETTINGS.get('plotly_template', "plotly_white")
MAP_CENTER = SETTINGS.get('map_center', {"lat": 40.7128, "lon": -74.0060})

# -----------------------------------
# Credentials
# -----------------------------------
raws_creds_path = SETTINGS.get('google_application_credentials', '')
SERVICE_ACCOUNT_PATH = os.path.join(PROJECT_BASE_DIR, os.path.expandvars(raws_creds_path))