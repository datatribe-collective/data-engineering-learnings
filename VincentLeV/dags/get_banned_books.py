from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from utils.transform_banned_books import merge_data
from utils.constants import BASE_DATA_URL, POSTGRES_CONN_ID
from tasks.banned_books_tasks import create_data_dir, get_pen_data, insert_ban_status, insert_banned_books

with DAG(
  dag_id="get_banned_books",
  start_date=datetime.now() - timedelta(days=1),
  end_date=datetime.now() + timedelta(days=1),
  schedule="@daily",
  catchup=False,
  is_paused_upon_creation=False,
) as dag:
  
  task_create_data_dir = PythonOperator(
    task_id="create_data_dir",
    python_callable=create_data_dir,
  )

  task_get_pen_data = PythonOperator(
    task_id="get_pen_data",
    python_callable=get_pen_data,
  )

  task_merge_data = PythonOperator(
    task_id="merge_data",
    python_callable=merge_data,
  )

  task_copy_files_to_local = BashOperator(
    task_id="copy_files_to_local",
    bash_command=(
        f"mkdir -p ./app/data/banned_books && "
        f"cp -r {BASE_DATA_URL}/* ./app/data/banned_books/ || echo 'No files to copy.'"
    ),
  )

  task_drop_banned_books_table = SQLExecuteQueryOperator(
    task_id="drop_banned_books_table",
    conn_id=POSTGRES_CONN_ID,
    sql="""
    DROP TABLE IF EXISTS banned_books;
    """,
  )

  task_drop_ban_status_table = SQLExecuteQueryOperator(
    task_id="drop_ban_status_table",
    conn_id=POSTGRES_CONN_ID,
    sql="""
    DROP TABLE IF EXISTS ban_status;
    """,
  )

  task_create_ban_status_table = SQLExecuteQueryOperator(
    task_id="create_ban_status_table",
    conn_id=POSTGRES_CONN_ID,
    sql="""
    CREATE TABLE IF NOT EXISTS ban_status (
        status VARCHAR(50) UNIQUE PRIMARY KEY,
        description TEXT
    );
    """,
  )

  task_create_banned_books_table = SQLExecuteQueryOperator(
    task_id="create_banned_books_table",
    conn_id=POSTGRES_CONN_ID,
    sql="""
    CREATE TABLE IF NOT EXISTS banned_books (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      title VARCHAR(255) NOT NULL,
      author VARCHAR(255),
      secondary_author VARCHAR(500),
      illustrator VARCHAR(255),
      translator VARCHAR(255),
      series_name VARCHAR(255),
      state VARCHAR(50),
      district VARCHAR(100),
      date_of_challenge VARCHAR(50),
      year VARCHAR(4),
      ban_status VARCHAR(50),
      origin_of_challenge VARCHAR(255),
      CONSTRAINT fk_ban_status FOREIGN KEY (ban_status) REFERENCES ban_status (status)
    );
    """,
  )

  task_insert_ban_status_task = PythonOperator(
    task_id="insert_ban_status",
    python_callable=insert_ban_status,
  )


  task_insert_banned_books_task = PythonOperator(
    task_id="insert_banned_books",
    python_callable=insert_banned_books,
  )

  task_create_data_dir >> task_get_pen_data >> task_merge_data >> task_copy_files_to_local
  task_copy_files_to_local >> task_drop_banned_books_table
  task_drop_banned_books_table >> task_drop_ban_status_table >> task_create_ban_status_table >> task_insert_ban_status_task
  task_insert_ban_status_task >> task_create_banned_books_table >> task_insert_banned_books_task