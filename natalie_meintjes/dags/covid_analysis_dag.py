
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'you',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='covid_data_analysis_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Runs COVID-19 notebook analysis',
) as dag:

    run_analysis = BashOperator(
        task_id='run_analysis_notebook',
        bash_command='papermill notebooks/covid_analysis.ipynb output/output.ipynb',
    )
