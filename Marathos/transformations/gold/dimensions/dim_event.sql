CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
  COMMENT "Dim table - gold layer" AS
SELECT
  event_id,
  MAX(event_name_clean)       AS event_name_clean,
  MAX(event_country)          AS event_country,
  MAX(event_distance_length)  AS event_distance_length,
  MAX(event_unit)             AS event_unit,
  MAX(event_type)             AS event_type
FROM
  marathos.silver.marathos_obt
GROUP BY
  event_id;