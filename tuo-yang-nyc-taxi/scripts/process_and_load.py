import pandas as pd
from pandas_gbq import to_gbq
import os

def clean_data(df):
    df = df.dropna()
    df['total'] = df['quantity'] * df['price']
    return df

def upload_to_bigquery(df, table_id, project_id):
    to_gbq(df, table_id, project_id=project_id, if_exists='replace')
    print(f"Data uploaded to {project_id}:{table_id}")

if __name__ == "__main__":
    df = pd.read_csv("data/sales_data.csv")
    df = clean_data(df)
    upload_to_bigquery(df, table_id="capstone_dataset.sales", project_id="your-gcp-project-id")
