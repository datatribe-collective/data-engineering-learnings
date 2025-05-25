from google.cloud import bigquery
import pandas as pd

def upload_data():
    client = bigquery.Client()

    # Load cleaned data
    df = pd.read_csv('/Users/jaypatel/Downloads/zepto_data_cleaned.csv')

    table_id = "stable-argon-460618-b6.Zepto_dataset_1.Product"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", 
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  
    print(" Data uploaded to BigQuery!")
