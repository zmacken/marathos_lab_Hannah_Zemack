CREATE OR REPLACE VIEW marathos.gold.mart_event_distance AS
SELECT
  e.event_distance_length,
  e.event_unit,
  e.event_type,
  count(*) AS antal_deltagare,
  count(DISTINCT r.event_id) AS antal_unika_event
FROM marathos.gold.fct_results r
JOIN marathos.gold.dim_event e ON r.event_id = e.event_id
GROUP BY e.event_distance_length, e.event_unit, e.event_type;