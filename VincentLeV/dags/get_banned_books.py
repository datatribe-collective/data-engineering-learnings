from datetime import datetime, timedelta
import os
from airflow import DAG
import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from scripts.transform_banned_books import transform_data, merge_data
from scripts.constants import BASE_DATA_URL

POSTGRES_CONN_ID = "banned_books"

def create_data_dir():
  os.makedirs(BASE_DATA_URL, exist_ok=True)
  print(f"Directory {BASE_DATA_URL} created or already exists.")

def get_pen_data():
  slugs = [
    "banned-book-list-2021-2022", 
    "2023-banned-book-list", 
    "pen-america-index-of-school-book-bans-2023-2024"
  ]
  
  for slug in slugs:
    url = f"https://pen.org/book-bans/{slug}"

    response = requests.get(url)
    html = BeautifulSoup(response.text, "html.parser")

    download_link = html.find("a", string="Download the index")
    if download_link and download_link["href"]:
      file_url = download_link["href"]

      if "docs.google.com" in file_url:
        # Convert the Google Docs URL to a direct download link
        if "export" not in file_url:
          base_url = file_url.split("/edit")[0]
          file_url = f"{base_url}/export?format=csv"

      # Extract the year from the slug using regex
      match = re.search(r"\d{4}(?:-\d{4})?", slug)
      year = match.group(0) if match else "unknown"

      file_name = f"pen-{year}.csv"
      file_path = os.path.join(BASE_DATA_URL, file_name)

      if os.path.exists(file_path):
        print(f"File {file_name} already exists. Skipping download.")
      else:
        print(f"Downloading {file_name}...")
        file_response = requests.get(file_url)
        with open(file_path, "wb") as file:
            file.write(file_response.content)

      transform_data(file_path)



def insert_ban_status():
  merged_file_path = os.path.join(BASE_DATA_URL, "banned_books.csv")
  dataset = pd.read_csv(merged_file_path)

  if "Ban Status" in dataset.columns:
    dataset["Ban Status"] = dataset["Ban Status"].str.lower()
    unique_ban_statuses = dataset["Ban Status"].dropna().unique()
  else:
    print("Column 'Ban Status' not found in the dataset.")
    return
  
  status_descriptions = {
    "banned": "Books that have been completely prohibited",
    "banned pending investigation": "Books that are pending a review to determine what restrictions, if any, to implement on them",
    "banned by restriction": "Grade-level or school-level restrictions or books that require parental permissions",
    "banned from libraries and classrooms": "Books that are banned from libraries or classrooms",
  }

  data_to_insert = [
    (status, status_descriptions.get(status, f"Description for {status}"))
    for status in unique_ban_statuses
  ]

  insert_query = """
  INSERT INTO ban_status (status, description)
  VALUES (%s, %s)
  ON CONFLICT (status) DO NOTHING;
  """

  from airflow.providers.postgres.hooks.postgres import PostgresHook
  postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
  for record in data_to_insert:
    postgres_hook.run(insert_query, parameters=record)

def insert_banned_books(batch_size=500):
  merged_file_path = os.path.join(BASE_DATA_URL, "banned_books.csv")
  dataset = pd.read_csv(merged_file_path)

  column_mapping = {
    "Author": "author",
    "Title": "title",
    "Secondary Author(s)": "secondary_author",
    "Translator(s)": "translator",
    "Illustrator(s)": "illustrator",
    "State": "state",
    "District": "district",
    "Date of Challenge/Removal": "date_of_challenge",
    "Year": "year",
    "Origin of Challenge": "origin_of_challenge",
    "Ban Status": "ban_status",
    "Series Name": "series_name",
  }

  # Rename dataset columns to match database column names
  dataset.rename(columns=column_mapping, inplace=True)
  
  # Replace NaN values with None
  dataset = dataset.where(pd.notnull(dataset), None)
  
  if "year" in dataset.columns:
    dataset["year"] = dataset["year"].astype(str)
  
  records = dataset.to_dict(orient="records")

  target_fields = [
    "title", "author", "secondary_author", "illustrator", "translator", 
    "series_name", "state", "district", "date_of_challenge", "year", 
    "ban_status", "origin_of_challenge"
  ]

  rows = [
    tuple(record.get(key) for key in target_fields)
    for record in records
  ]

  from airflow.providers.postgres.hooks.postgres import PostgresHook
  postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

  try:
    # Insert records in batches using insert_rows
    for i in range(0, len(rows), batch_size):
      batch_rows = rows[i:i + batch_size]
      postgres_hook.insert_rows(
        table="banned_books",
        rows=batch_rows,
        target_fields=target_fields,
        commit_every=batch_size
      )
      print(f"Inserted batch {i // batch_size + 1} with {len(batch_rows)} records.")
  except Exception as e:
    print(f"Error inserting records into banned_books table: {e}")
    raise

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