SELECT
    DATE_TRUNC('hour', tpep_pickup_datetime) AS pickup_hour,
    COUNT(*) AS trip_count,
    AVG(trip_distance) AS avg_distance,
    AVG(total_amount) AS avg_total_amount,
    AVG(EXTRACT(EPOCH FROM tpep_dropoff_datetime - tpep_pickup_datetime)/60.0) AS avg_duration_min
FROM yellow_taxi
WHERE trip_distance > 0 AND total_amount > 0
    AND tpep_pickup_datetime BETWEEN '2023-01-01' AND '2023-02-01'
GROUP BY pickup_hour
ORDER BY pickup_hour;