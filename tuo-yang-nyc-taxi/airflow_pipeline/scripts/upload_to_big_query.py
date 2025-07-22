# scripts/upload_to_big_query.py

"""
Step 3 of NYC Yellow Taxi Data Engineering Project:
Upload cleaned CSV data to Google BigQuery using service account cerdentials
Project ID is read dynamically from the service account JSON
"""

import os
import sys
import traceback
# Auto-Locate project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts import config
import pandas as pd
from google.cloud import bigquery
from scripts.get_bigquery_client import get_bigquery_client
from scripts.sql_utils import render_create_summary_sql, render_create_zone_summary_sql

def build_csv_filename_from_config() -> str:
    """Construct csv filename based on year and months in settings."""
    year = config.SETTINGS["data_config"]["year"]
    months = config.SETTINGS["data_config"]["months"]
    return f"yellow_tripdata_{year}_m{min(months)}to{max(months)}_cleaned.csv"

def upload_csv_to_bq():
    """Upload a local csv file to BigQuery."""
    assert config.TABLE_NAME != config.SUMMARY_TABLE_NAME, \
           "TABLE_NAME and SUMMARY_TABLE_NAME must be different!"

    client = get_bigquery_client()

    # Construct file path
    csv_file = build_csv_filename_from_config()
    local_path = os.path.join(config.LOCAL_CSV_PATH, csv_file)

    df = pd.read_csv(local_path)
    table_ref = f"{config.PROJECT_ID}.{config.DATASET_ID}.{config.TABLE_NAME}"

    job_config = bigquery.LoadJobConfig(
        # Overwrite the table if it already exists (TRUNCATE means delete and replace)
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Automatically detect table schema (column names, types) from source file
        autodetect = True,
        # Specify the input file format (in this case, CSV)
        source_format = bigquery.SourceFormat.CSV
    )

    print(f"Uploading {config.LOCAL_CSV_PATH} to {table_ref} ...")
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()
    print(f"Upload complete: {load_job.output_rows} rows")

def execute_dynamic_sql(granularity="HOUR"):
    """Generate and execute dynamic SQL from config + granularity."""
    client = get_bigquery_client()

    query = render_create_summary_sql(granularity=granularity)
    print(f"Executing dynamic SQL for summary ({granularity}) ...")

    query_job = client.query(query)
    query_job.result()
    print("SQL execution complete.")

def execute_zone_summary_sql(granularity="DAY", location_type="pickup"):
    """Create zone-level summary table by pickup or dropoff zones."""
    client = get_bigquery_client()
    
    query = render_create_zone_summary_sql(granularity=granularity, location_type=location_type)
    print(f"Creating zone summary for {location_type} ({granularity}) ...")

    query_job = client.query(query)
    query_job.result()
    print(f"Zone summary table for {location_type} created.")

def run_upload_to_bq():
    upload_csv_to_bq()
    execute_dynamic_sql(granularity="HOUR")
    execute_zone_summary_sql("DAY", "pickup")
    execute_zone_summary_sql("DAY", "dropoff")

if __name__ == "__main__":
    run_upload_to_bq()
