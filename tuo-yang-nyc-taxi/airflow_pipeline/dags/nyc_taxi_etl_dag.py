# nyc_taxi_etl_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import traceback

# Ensure the 'scripts' module is on the path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Import existing pipeline functions
from scripts import main_debug
from scripts import upload_to_big_query

# Wrap functions with error logging
def wrap_with_log(func):
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"\nError in {func.__name__}: {e}")
            traceback.print_exc()
            raise
    return wrapped

# Wrapped callables
@wrap_with_log
def wrapped_step_3_upload(**kwargs):
    main_debug.step_3_upload_to_bigquery()

@wrap_with_log
def wrapped_step_4_summary(**kwargs):
    main_debug.step_4_create_summary(granularity='HOUR')

@wrap_with_log
def wrapped_zone_summary_pickup(**kwargs):
    upload_to_big_query.execute_zone_summary_sql(granularity='DAY', location_type='pickup')

@wrap_with_log
def wrapped_zone_summary_dropoff(**kwargs):
    upload_to_big_query.execute_zone_summary_sql(granularity='DAY', location_type='dropoff')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': [],
    'email_on_failure': False,
    'eamil_on_retry': False,
}

with DAG(
    dag_id="nyc_taxi_data_pipeline_bash",
    default_args=default_args,
    description="NYC Yellow Taxi Data Pipeline using BashOperator for PySpark",
    schedule_interval=None,     # manual trigger
    catchup=False,
    tags=['nyc_taxi', 'data_pipeline'],
    ) as dag:

    # Step 1-2 PySpark cleaning via BashOperator
    transform_with_spark = BashOperator(
        task_id="transform_with_spark",
        bash_command="python /opt/airflow/scripts/transform_trip_data_spark.py"
    )

    # Step 3: Upload cleaned CSV to BigQuery
    upload_data_to_big_query = PythonOperator(
        task_id="upload_to_big_query",
        python_callable=wrapped_step_3_upload
    )

    # Step 4a: Create trip_summary_hourly table in BigQuery
    create_trip_summary = PythonOperator(
        task_id="create_trip_summary",
        python_callable=wrapped_step_4_summary
    )

    # Step 4b: Create zone-level summary table for pickup zones
    create_zone_summary_pickup = PythonOperator(
        task_id="create_zone_summary_pickup",
        python_callable=wrapped_zone_summary_pickup
    )

    # Step 4c: Create zone-level summary table for dropoff zones
    create_zone_summary_dropoff = PythonOperator(
        task_id="create_zone_summary_dropoff",
        python_callable=wrapped_zone_summary_dropoff
    )

    # Define task dependencies
    transform_with_spark >> upload_data_to_big_query >> create_trip_summary
    create_trip_summary >> [create_zone_summary_pickup, create_zone_summary_dropoff]



