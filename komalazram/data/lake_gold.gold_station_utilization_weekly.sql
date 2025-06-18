CREATE TABLE IF NOT EXISTS lake_gold.gold_station_utilization_weekly (
  station_id STRING,
  network_id STRING,
  week_start DATE,
  is_frequently_full BOOL,
  is_frequently_empty BOOL,
  avg_free_bikes FLOAT64,
  avg_empty_slots FLOAT64
);
