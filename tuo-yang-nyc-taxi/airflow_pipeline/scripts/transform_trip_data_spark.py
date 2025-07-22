# transform_trip_data_spark.py

"""
Step 2 of NYC Yellow Taxi Data Engineering Project:
Clean and standardize trip data using PySpark before loading to data warehouse.

This script:
- Automatically reads multiple monthly parquet files
- Normalizes column names and types (especially for integer vs. double mismatches)
- Drops empty or duplicated columns (like duplicated Airport_fee)
- Converts datetime columns
- Filters out invalid rows (e.g., fare_amount <= 0)
- Returns a cleaned Spark DataFrame (or optionally save to local parquet)
"""

import os
import sys
# Auto-Locate project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, regexp_replace, trim
from pyspark.sql.types import *
from scripts.load_nyc_yellow_taxi_data import download_to_local
from typing import List
from pathlib import Path
from scripts import config

# Fields that must be cast to specific types for consistency
LONG_COLUMNS = ['vendorid', 'ratecodeid', 'pulocationid', 'dolocationid', 'payment_type']
TIMPSTAMP_COLUMNS = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

# ---------- Transformation Functions ----------

def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Standardize column names: lowercase, strip spaces, replace with underscores
    """
    for col_name in df.columns:
        new_name = col_name.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(col_name, new_name)
    return df

def cast_column_types(df: DataFrame) -> DataFrame:
    for col_name in LONG_COLUMNS:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(LongType()))
    for col_name in TIMPSTAMP_COLUMNS:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(TimestampType()))
    return df

def drop_useless_columns(df: DataFrame) -> DataFrame:
    """
    Drop empty columns and fix duplicated columns like 'Airport_fee'
    """
    # Drop fully null columns
    # Find out columns whose values are the same one, and this value is empty string or None
    null_cols = [c for c in df.columns if df.select(col(c)).distinct().count() == 1 and df.select(col(c)).first()[0] in ('', None)]

    # Drop 'Airport_fee' if both versions exist
    if 'airport_fee' in df.columns and 'Airport_fee' in df.columns:
        null_cols.append('Airport_fee')

    return df.drop(*null_cols)

def filter_invalid_rows(df: DataFrame) -> DataFrame:
    """
    Filter out clearly invalid records (e.g. negative fare or distance)
    """
    if 'fare_amount' in df.columns:
        df = df.filter(col('fare_amount') > 0)
    if 'trip_distance' in df.columns:
        df = df.filter(col('trip_distance') > 0)
    if 'passenger_count' in df.columns:
        df = df.filter(col('passenger_count') > 0)
    if 'total_amount' in df.columns:
        df = df.filter(col('total_amount') > 0)
    return df

def transform_trip_data(df: DataFrame) -> DataFrame:
    """
    Full transformation pipeline
    """
    df = clean_column_names(df)
    df = cast_column_types(df)
    df = drop_useless_columns(df)
    df = filter_invalid_rows(df)
    return df

# ---------- Main Function ----------
def main(year: int, months: List[int]) -> DataFrame:
    """
    Clean NYC Yellow Taxi trip data using Spark, returned cleaned pandas DataFrame

    Parameters:
    - year (int): year to process
    - months (List[int]): list of month numbers
    """
    # Spark use the vectorized reader by default (having strict restrictions for column types), so disable it
    spark = SparkSession.builder.appName("YellowTaxiCleaning").getOrCreate()

    # Read raw parquet and fix schema errors using explicit cast
    input_paths = [f"file:///{p.replace(os.sep, '/')}" for p in download_to_local(year, months)]

    # Read and transform each file independently
    dfs = []
    for path in input_paths:
        print(f"Reading: {path}")
        df = spark.read.parquet(path)
        df = transform_trip_data(df)
        dfs.append(df)

    # Union all cleaned monthly data (use the 1st element as the initial value, then getting other tables merged)
    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df)

    # Save output to processed directory with short name
    filename = f"yellow_tripdata_{year}_m{min(months)}to{max(months)}_cleaned.csv"

    # Save a random 5% sample to CSV to reduce memory usage
    df_all_sampled = df_all.sample(fraction=0.05, seed=42)

    # Save the sampled DataFrame to a CSV file using Snappy compression
    pdf_all_sampled = df_all_sampled.toPandas()

    # Save as a CSV file using standard Python filesystem (no winutils dependency)
    output_csv_path = os.path.join(config.LOCAL_CSV_PATH, filename)
    pdf_all_sampled.to_csv(output_csv_path, index=False)
    print(f"Saved cleaned data to: {output_csv_path}")

    return df_all

def run_spark_transform_job(year, months):
    main(year, months)

# ---------------------- Run Main ----------------------
if __name__ == "__main__":
    run_spark_transform_job(2023, [1, 2, 3])
    
