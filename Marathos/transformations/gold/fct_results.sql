CREATE OR REFRESH STREAMING TABLE marathos.gold.fct_results
  COMMENT "Fact table - gold layer" AS
SELECT
  event_id,
  athlete_id,
  year_of_event,
  event_dates,
  event_number_of_finishers,
  performance_hours,
  performance_km,
  athlete_average_speed
FROM
  STREAM marathos.silver.marathos_obt;