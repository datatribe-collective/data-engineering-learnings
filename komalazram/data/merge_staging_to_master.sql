MERGE `citi-bike-459310.lake_silver.master_bike_station_status` T
USING (
  SELECT * EXCEPT(snapshot_time),
  ROW_NUMBER() OVER (PARTITION BY station_id, timestamp ORDER BY snapshot_time DESC) AS row_num
  FROM `citi-bike-459310.lake_silver._staging_master_bike_station_status`
) S
ON T.station_id = S.station_id AND T.timestamp = S.timestamp
WHEN MATCHED AND S.row_num = 1 THEN
  UPDATE SET
    free_bikes = S.free_bikes,
    empty_slots = S.empty_slots,
    ebikes = S.ebikes,
    renting = S.renting,
    returning = S.returning,
    has_ebikes = S.has_ebikes
WHEN NOT MATCHED AND S.row_num = 1 THEN
  INSERT (
    network_id, network_name, station_id, latitude, longitude, timestamp,
    free_bikes, empty_slots, extra_uid, renting, returning, has_ebikes,
    ebikes
  )
  VALUES (
    network_id, network_name, station_id, latitude, longitude, timestamp,
    free_bikes, empty_slots, extra_uid, renting, returning, has_ebikes,
    ebikes
  );
