# load_nyc_yellow_taxi_data.py

"""
Step 1 of NYC Yellow Taxi Data Engineering Project:
Download and load monthly trip data from official cloudfront URLs.
This script dynamically constructs URLs based on year/month and loads data into pandas DataFrames
"""

import os
import sys
import requests
from typing import List

# Auto-Locate project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts import config

def generate_url(year: int, month: int) -> str:
    """Generate the download URL for the given year and month"""
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    return base_url + file_name

def download_to_local(year: int, months: List[int]) -> List[str]:
    local_paths = []
    os.makedirs(config.TMP_PARQUET_PATH, exist_ok=True)

    for month in months:
        url = generate_url(year, month)
        file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        local_path = os.path.join(config.TMP_PARQUET_PATH, file_name)

        if not os.path.exists(local_path):  # To prevent repetitive downloading
            print(f"📥 Downloading {url}")
            try:
                response = requests.get(url)
                response.raise_for_status()
                with open(local_path, "wb") as f:
                    f.write(response.content)
            except Exception as e:
                print(f"❌ Failed to download {url}: {e}")
                continue

        local_paths.append(local_path)
    return local_paths
    
if __name__ == "__main__":
    # Example: Load data from January, February, March 2023
    year = 2023
    months = [1, 2, 3]
    local_paths = download_to_local(year, months)
    print(local_paths)