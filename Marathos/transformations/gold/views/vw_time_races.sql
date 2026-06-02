CREATE OR REPLACE VIEW marathos.gold.vw_time_races
    COMMENT "View table - gold layer" AS
SELECT
  r.event_id, r.athlete_id, r.year_of_event,
  r.event_start_date, r.performance_km,
  r.athlete_avg_speed_kmh, r.event_number_of_finishers,
  e.event_name_clean, e.event_distance_length, e.event_unit,
  e.event_country,
  a.athlete_gender, a.athlete_age_category,
  a.athlete_country_name, a.athlete_year_of_birth
FROM marathos.gold.fct_results r
JOIN marathos.gold.dim_event e ON r.event_id = e.event_id
JOIN marathos.gold.dim_athlete a ON r.athlete_id = a.athlete_id
WHERE e.event_unit = 'h';