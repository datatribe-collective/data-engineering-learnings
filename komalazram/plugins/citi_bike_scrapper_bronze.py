import requests
import json
from datetime import datetime
from google.cloud import storage


def fetch_and_upload(**kwargs):

    API_URL = "https://api.citybik.es/v2/networks/citi-bike-nyc"

    try:
        # Get bucket name from Airflow params
        params = kwargs.get("params", {})
        bucket_name = params.get("bronze_bucket", "bronze")

        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()

        # Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"raw/station_status_{timestamp}.json"

        blob = bucket.blob(filename)
        blob.upload_from_string(json.dumps(data))

        print(f"Successfully uploaded {filename} to {bucket_name}")
        return True

    except Exception as e:
        print(f"Error: {str(e)}")
        raise
