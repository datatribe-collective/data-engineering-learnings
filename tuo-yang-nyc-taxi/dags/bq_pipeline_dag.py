from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("bq_pipeline", start_date=datetime(2024, 1, 1), schedule_interval="@once", catchup=False) as dag:
    run_pipeline = BashOperator(
        task_id="run_bq_load",
        bash_command="poetry run python scripts/process_and_load.py"
    )

    run_pipeline
