CREATE TABLE IF NOT EXISTS lake_gold.gold_station_utilization_daily (
  station_id STRING,
  network_id STRING,
  date DATE,
  is_frequently_full BOOL,
  is_frequently_empty BOOL,
  avg_free_bikes FLOAT64,
  avg_empty_slots FLOAT64,
  total_check_outs INT64,
  total_check_ins INT64,
  peak_hour INT64
);
