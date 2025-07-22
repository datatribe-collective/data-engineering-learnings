-- Create hourly summary table
CREATE OR REPLACE TABLE `precise-antenna-461516-u3.nyc_taxi.trip_summary_hourly` AS
SELECT
    DATETIME_TRUNC(CAST(tpep_pickup_datetime AS TIMESTAMP), HOUR) AS pickup_hour,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    SUM(passenger_count) AS total_passengers,
    ROUND(AVG(trip_distance), 2) AS avg_distance
FROM
    `precise-antenna-461516-u3.nyc_taxi.yellow_tripdata_cleaned`
WHERE
    tpep_pickup_datetime BETWEEN '2023-01-01' AND '2023-03-31'
GROUP BY
    pickup_hour
ORDER BY
    pickup_hour;