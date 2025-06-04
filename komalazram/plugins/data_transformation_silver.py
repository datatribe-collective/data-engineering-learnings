from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timezone
import pandas as pd
import json
import io


def raw_transformation(**kwargs):
    # Read parameters from DAG
    bronze_bucket = kwargs["dag"].params["bronze_bucket"]
    silver_bucket = kwargs["dag"].params["silver_bucket"]

    gcs_hook = GCSHook()

    # Get the latest file from bronze bucket
    list_of_files = gcs_hook.list(bronze_bucket, prefix="raw/")
    if not list_of_files:
        raise ValueError("No files found in bronze")

    latest_file = sorted(list_of_files)[-1]
    json_data = json.loads(
        gcs_hook.download(bronze_bucket, latest_file).decode("utf-8")
    )

    def flatten_json(data):
        flattened = []
        network = data["network"]
        for station in network["stations"]:
            record = {
                "network_id": network["id"],
                "network_name": network["name"],
                "station_id": station.get("id"),
                "latitude": station.get("latitude"),
                "longitude": station.get("longitude"),
                "timestamp": station.get("timestamp"),
                "free_bikes": station.get("free_bikes"),
                "empty_slots": station.get("empty_slots"),
                "extra_uid": station.get("extra", {}).get("uid"),
                "renting": station.get("extra", {}).get("renting"),
                "returning": station.get("extra", {}).get("returning"),
                "has_ebikes": station.get("extra", {}).get("has_ebikes"),
                "ebikes": station.get("extra", {}).get("ebikes"),
            }
            flattened.append(record)
        return pd.DataFrame(flattened)

    df = flatten_json(json_data)

    # add snapshot time
    snapshot_time = datetime.now(timezone.utc)
    df["snapshot_time"] = snapshot_time

    try:
        if df["timestamp"].dtype.kind in "iuf":
            df["timestamp"] = pd.to_datetime(
                df["timestamp"].astype("int64") / 1000,
                unit="ns",
                utc=True,
            )
        else:
            df["timestamp"] = (
                df["timestamp"]
                .astype(str)
                .str.strip()
                .str.replace("Z", "", regex=False)
                .replace({"": None, "None": None, "nan": None})
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

        df = df[df["timestamp"].between("2000-01-01", "2100-01-01")]
        df["timestamp"] = df["timestamp"].astype("int64") // 1000

    except Exception as e:
        raise ValueError(f"Timestamp conversion failed: {str(e)}")

    numeric_cols = ["free_bikes", "empty_slots", "ebikes", "latitude", "longitude"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    bool_cols = ["has_ebikes", "renting", "returning"]
    df[bool_cols] = df[bool_cols].astype(bool)

    df.fillna(
        {
            "free_bikes": 0,
            "empty_slots": 0,
            "ebikes": 0,
            "latitude": 0.0,
            "longitude": 0.0,
        },
        inplace=True,
    )

    df["is_station_empty"] = df["free_bikes"] == 0
    df["is_station_full"] = df["empty_slots"] == 0

    parquet_buffer = io.BytesIO()
    df.to_parquet(
        parquet_buffer,
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )

    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Upload to silver bucket
    gcs_hook.upload(
        bucket_name=silver_bucket,
        object_name="citi-bike/latest_data.parquet",
        data=parquet_buffer.getvalue(),
    )

    gcs_hook.upload(
        bucket_name=silver_bucket,
        object_name=f"citi-bike/history/data_{timestamp_str}.parquet",
        data=parquet_buffer.getvalue(),
    )
