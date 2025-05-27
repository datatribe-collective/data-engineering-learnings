from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'jay',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='zepto_pipeline_dag',
    default_args=default_args,
    description='Automated Zepto data pipeline',
    schedule=None,
    catchup=False,
) as dag:

    run_cleaning_script = BashOperator(
        task_id='clean_zepto_data',
        bash_command='python3 /Users/jaypatel/Downloads/clean_data.py'
    )

    upload_to_bigquery = BashOperator(
        task_id='upload_to_bigquery',
        bash_command='python3 /Users/jaypatel/Downloads/upload_to_bigquery.py'
    )

    run_cleaning_script >> upload_to_bigquery

