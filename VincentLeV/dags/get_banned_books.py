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

POSTGRES_CONN_ID = "banned_books"

BASE_DATA_URL = os.path.join(os.getcwd(), "data", "banned_books")

def create_data_dir():
  os.makedirs(BASE_DATA_URL, exist_ok=True)
  print(f"Directory {BASE_DATA_URL} created or already exists.")

def transform_data(file_path, header_row=None):
  possible_headers = ["Author,Title", "Title,Author"]

  with open(file_path, "r", encoding="utf-8") as file:
    lines = file.readlines()

  header_index = None
  for header_row in possible_headers:
    header_index = next((i for i, line in enumerate(lines) if header_row in line), None)
    if header_index is not None:
      break

  if header_index is None:
    print(f"Header row '{header_row}' not found in {file_path}")
    return

  # Keep only rows starting from the header and remove trailing unrelated rows
  cleaned_lines = lines[header_index:]
  cleaned_lines = [line for line in cleaned_lines if line.strip()]  # Remove empty lines

  # Write the cleaned data back to the file
  with open(file_path, "w", encoding="utf-8") as file:
      file.writelines(cleaned_lines)

  try:
    dataset = pd.read_csv(file_path)

    # Remove rows containing "PEN America" or "PEN AMERICA"
    dataset = dataset[~dataset.apply(lambda row: row.astype(str).str.contains("PEN America|PEN AMERICA", case=False).any(), axis=1)]

    # Remove rows without both Title and Author
    if "Title" in dataset.columns and "Author" in dataset.columns:
      print(f"Removing rows without both Title and Author in {file_path}")
      dataset = dataset.dropna(subset=["Title", "Author"], how="all")

    # Reformat the "Author" column to "Firstname Lastname" format
    if "Author" in dataset.columns:
      print(f"Reformatting Author column in {file_path}")
      dataset["Author"] = dataset["Author"].apply(
        lambda x: " ".join(x.split(", ")[::-1]) if isinstance(x, str) and ", " in x else x
      )
    if "Type of Ban" in dataset.columns:
      print(f"Renaming Type of Ban to Ban Status in {file_path}")
      dataset.rename(columns={"Type of Ban": "Ban Status"}, inplace=True)
    if "Initiating Action" in dataset.columns:
      print(f"Renaming Initiating Action to Origin of Challenge in {file_path}")
      dataset.rename(columns={"Initiating Action": "Origin of Challenge"}, inplace=True)
    if "Origin of Challenge" in dataset.columns:
      print(f"Replacing values in Origin of Challenge column in {file_path}")
      dataset["Origin of Challenge"] = dataset["Origin of Challenge"].replace({
        "Administrator": "Administration",
        "Formal Challenge, Administration": "Administration/Formal Challenge",
        "Informal Challenge, Administration": "Administration/Informal Challenge"
      })
    if "Date of Challenge/Removal" in dataset.columns:
      print(f"Extracting year from Date of Challenge/Removal column in {file_path}")
      dataset["Year"] = dataset["Date of Challenge/Removal"].apply(
          lambda x: str(x.split()[-1]) if pd.notnull(x) and isinstance(x, str) else None
      )
  
    dataset.to_csv(file_path, index=False)
  except Exception as e:
    print(f"Error processing column in {file_path}: {e}")
    raise

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

def transform_merged_data(dataset, exclude_columns=None):
  if exclude_columns is None:
    exclude_columns = []

  # Identify base columns by removing suffixes (_x, _y, etc.)
  base_columns = set(col.split("_")[0] for col in dataset.columns if "_" in col or col in dataset.columns)

  for base_col in base_columns:
    if base_col not in exclude_columns:
      # Find all variations of the column (e.g., column, column_x, column_y)
      variations = [col for col in dataset.columns if col == base_col or col.startswith(f"{base_col}_")]

      if len(variations) > 1:
        # Merge all variations into the base column
        merged_column = dataset[variations].bfill(axis=1).iloc[:, 0]
        dataset[base_col] = merged_column.infer_objects(copy=False)

        # Drop all variations except the base column
        dataset.drop(columns=[col for col in variations if col != base_col], inplace=True)

  if "Ban Status" in dataset.columns:
    print("Normalizing Ban Status column")
    dataset["Ban Status"] = dataset["Ban Status"].fillna("").str.lower().str.strip()
    dataset["Ban Status"] = dataset["Ban Status"].apply(
      lambda x: "banned from libraries and classrooms"
      if "classrooms" in x and "libraries" in x else
      "banned from libraries and classrooms"
      if "classrooms" in x or "libraries" in x else x
    )
  if "Year" in dataset.columns:
    print("Normalizing Year column")
    dataset["Year"] = dataset["Year"].apply(lambda x: 
      str(int(x.split("-")[1])) if isinstance(x, str) and "-" in x else
      str(int(x)) if isinstance(x, (int, float)) else
      x.replace(".0", "") if isinstance(x, str) and ".0" in x else
      str(x) if pd.notnull(x) else None
    )

  return dataset

def merge_data():
  try:
    merged_file_path = os.path.join(BASE_DATA_URL, "banned_books.csv")

    dataset_2021_2022_path = os.path.join(BASE_DATA_URL, "pen-2021-2022.csv")
    dataset_2023_path = os.path.join(BASE_DATA_URL, "pen-2023.csv")
    dataset_2023_2024_path = os.path.join(BASE_DATA_URL, "pen-2023-2024.csv")

    dataset_2021_2022 = pd.read_csv(dataset_2021_2022_path)
    dataset_2023 = pd.read_csv(dataset_2023_path)
    dataset_2023_2024 = pd.read_csv(dataset_2023_2024_path)

    # Merge datasets
    merged_dataset = dataset_2021_2022.merge(
      dataset_2023, on=["Title", "Author"], how="outer"
    ).merge(
      dataset_2023_2024, on=["Title", "Author"], how="outer"
    )

    merged_dataset = transform_merged_data(merged_dataset, exclude_columns=["Title", "Author"])

    merged_dataset.to_csv(merged_file_path, index=False)
    print(f"Merged dataset saved to {merged_file_path}")
  except Exception as e:
    print(f"Error in merge_data: {e}")
    raise

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