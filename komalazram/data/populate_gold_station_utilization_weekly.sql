CREATE OR REPLACE TABLE lake_gold.gold_station_utilization_weekly AS 
WITH weekly_data AS (
  SELECT
    station_id,
    network_id,
    DATE_TRUNC(DATE(timestamp), WEEK(MONDAY)) AS week_start,
    COUNTIF(empty_slots = 0) / COUNT(*) > 0.8 AS is_frequently_full,
    COUNTIF(free_bikes = 0) / COUNT(*)  > 0.8 AS is_frequently_empty,
    AVG(free_bikes) AS avg_free_bikes,
    AVG(empty_slots) AS avg_empty_slots
  FROM lake_silver.master_bike_station_status
  WHERE timestamp >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
  GROUP BY station_id, network_id, week_start
)
SELECT * FROM weekly_data;
