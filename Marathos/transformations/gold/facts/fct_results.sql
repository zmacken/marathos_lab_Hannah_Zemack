CREATE OR REFRESH STREAMING TABLE marathos.gold.fct_results
  COMMENT "Fact table - gold layer" AS
SELECT
  sha2(concat_ws('|', event_id, athlete_id, year_of_event), 256) AS result_id,
  event_id,
  athlete_id,
  year_of_event,
  event_start_date,
  event_number_of_finishers,
  performance_hours,
  performance_km,
  athlete_avg_speed_kmh
FROM
  STREAM marathos.silver.marathos_obt;