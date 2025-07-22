# scripts/sql_utils.py

from utils import config

def render_create_summary_sql(granularity: str = "Hour") -> str:
    """
    Generate BigQuery SQL to create a summary table with specified granularity
    Currently supports 'HOUR', 'DAY', or 'WEEK' (default is 'HOUR')
    """
    assert granularity in ["HOUR", "DAY", "WEEK"], "Invalid granularity"

    trunc_func = f"DATETIME_TRUNC(CAST(tpep_pickup_datetime AS TIMESTAMP), {granularity})"
    group_col = "pickup_" + granularity.lower()

    return f"""
    -- Drop table if exists
    DROP TABLE IF EXISTS `{config.PROJECT_ID}.{config.DATASET_ID}.{config.SUMMARY_TABLE_NAME}`;

    -- Create summary table
    CREATE OR REPLACE TABLE `{config.PROJECT_ID}.{config.DATASET_ID}.{config.SUMMARY_TABLE_NAME}` AS
    SELECT
        {trunc_func} AS {group_col},
        COUNT(*) AS trip_count,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(AVG(tip_amount), 2) AS avg_tip,
        SUM(passenger_count) AS total_passengers,
        ROUND(AVG(trip_distance), 2) AS avg_distance
    FROM
        `{config.PROJECT_ID}.{config.DATASET_ID}.{config.TABLE_NAME}`
    WHERE
        tpep_pickup_datetime BETWEEN '2023-01-01' AND '2023-03-31'
    GROUP BY
        {group_col}
    ORDER BY
        {group_col};
    """

def render_create_zone_summary_sql(granularity: str = "DAY", location_type: str = "pickup") -> str:
    """
    Generate BigQuery SQL to create a zone-level trip summary table.
    Supports location_type = 'pickup' or 'dropoff' and granularity = 'DAY' or 'WEEK'
    """
    assert granularity in ["DAY", "WEEK"], "Only DAY or WEEK granularity supported"
    assert location_type in ["pickup", "dropoff"], "location_type must be 'pickup' or 'dropoff'"

    trunc_func = f"DATE_TRUNC(DATE(tpep_{location_type}_datetime), {granularity})"
    group_col = f"{location_type}_" + granularity.lower()
    zone_col = f"{'PU' if location_type == 'pickup' else 'DO'}LocationID"
    output_table_name = f"zone_summary_{location_type}_{granularity.lower()}"

    return f"""
    -- Drop if exists
    DROP TABLE IF EXISTS `{config.PROJECT_ID}.{config.DATASET_ID}.{output_table_name}`;

    -- Create {location_type} zone-level summary table
    CREATE OR REPLACE TABLE `{config.PROJECT_ID}.{config.DATASET_ID}.{output_table_name}` AS
    SELECT
        {trunc_func} AS {group_col},
        {zone_col} AS zone_id,
        COUNT(*) AS trip_count,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(AVG(tip_amount), 2) AS avg_tip,
        SUM(passenger_count) AS total_passengers,
        ROUND(AVG(trip_distance), 2) AS avg_distance
    FROM
        `{config.PROJECT_ID}.{config.DATASET_ID}.{config.TABLE_NAME}`
    WHERE
        tpep_pickup_datetime BETWEEN '2023-01-01' AND '2023-03-31'
    GROUP BY
        {group_col}, zone_id
    ORDER BY
        {group_col}, zone_id;
    """

def get_trip_summary_query():
    """
    Return SQL to query the trip summary created by render_create_summary_sql().
    """
    return f"""
    SELECT
        pickup_hour,
        trip_count,
        avg_fare,
        avg_tip,
        total_passengers,
        avg_distance
    FROM `{config.PROJECT_ID}.{config.DATASET_ID}.{config.SUMMARY_TABLE_NAME}`
    ORDER BY pickup_hour
    """

if __name__ == "__main__":
    print(render_create_summary_sql(granularity="HOUR"))
    print(render_create_zone_summary_sql(granularity="DAY", location_type="pickup"))
    print(render_create_zone_summary_sql(granularity="DAY", location_type="dropoff"))
    print(get_trip_summary_query())