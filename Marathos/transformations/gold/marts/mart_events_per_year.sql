CREATE OR REPLACE VIEW marathos.gold.mart_events_per_year
    COMMENT "Mart table - gold layer" AS
SELECT
  year_of_event,
  e.event_country,
  count(DISTINCT r.event_id) AS antal_unika_event,
  count(*) AS antal_starter
FROM marathos.gold.fct_results r
JOIN marathos.gold.dim_event e ON r.event_id = e.event_id
GROUP BY year_of_event, e.event_country;