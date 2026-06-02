CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_date
    COMMENT "Dim table - gold layer" AS
SELECT DISTINCT
  event_start_date                    AS date_id,
  event_start_date                    AS full_date,
  year(event_start_date)              AS year,
  month(event_start_date)             AS month,
  dayofmonth(event_start_date)        AS day,
  dayofweek(event_start_date)         AS day_of_week,
  quarter(event_start_date)           AS quarter,
  date_format(event_start_date, 'MMMM') AS month_name
FROM marathos.silver.marathos_obt
WHERE event_start_date IS NOT NULL;