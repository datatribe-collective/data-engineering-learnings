# scripts/main.py
import argparse
import os
import sys
import pandas as pd

# Append project root to sys.path for relative imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.upload_to_big_query import upload_csv_to_bq, execute_dynamic_sql

def step_1_and_2_transform_with_spark():
    print(f"Step 1-2: Loading raw data & tranforming with Spark...")
    from scripts import config
    from scripts.transform_trip_data_spark import main as spark_transform_main
    year = config.SETTINGS["data_config"]["year"]
    months = config.SETTINGS["data_config"]["months"]
    spark_transform_main(year, months)

def step_3_upload_to_bigquery():
    print("Step 3: Uploading to BigQuery...")
    upload_csv_to_bq()

def step_4_create_summary(granularity="HOUR"):
    print(f"Step 4: Creating summary table ({granularity})...")
    execute_dynamic_sql(granularity=granularity)

def run_pipeline(run_all=True, step=None):
    # Run the full pipeline if no specific step is provided
    if step is None:
        step_1_and_2_transform_with_spark()
        step_3_upload_to_bigquery()
        step_4_create_summary()
    elif step == "spark":
        step_1_and_2_transform_with_spark()
    elif step == "upload":
        step_3_upload_to_bigquery()
    elif step == "summary":
        step_4_create_summary()
    else:
        print(f"Invalid step: {step}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Yellow Taxi Data Pipeline")
    parser.add_argument("--step", choices=["spark", "upload", "summary"],
                        help="Run a specific step only. If not provided, run all steps.")
    args = parser.parse_args()
    run_pipeline(step=args.step)