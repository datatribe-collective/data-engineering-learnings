CREATE OR REPLACE TABLE lake_gold.gold_station_utilization_weekly AS 
WITH weekly_data AS (
  SELECT
    station_id,
    network_id,
    DATE_TRUNC(DATE(timestamp), WEEK(MONDAY)) AS week_start,
    COUNTIF(is_station_full) / COUNT(*) > 0.8 AS is_frequently_full,
    COUNTIF(is_station_empty) / COUNT(*) > 0.8 AS is_frequently_empty,
    AVG(free_bikes) AS avg_free_bikes,
    AVG(empty_slots) AS avg_empty_slots,
    ARRAY_AGG(TIMESTAMP_TRUNC(timestamp, HOUR) ORDER BY (free_bikes + empty_slots) DESC LIMIT 1)[OFFSET(0)] AS peak_hour
  FROM lake_silver.master_bike_station_status
  WHERE timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY station_id, network_id, week_start
)
SELECT * FROM weekly_data;
