CREATE OR REPLACE VIEW marathos.gold.mart_gender_distribution
    COMMENT "Mart table - gold layer" AS
SELECT
  year_of_event,
  athlete_gender,
  count(*) AS antal_deltagare
FROM marathos.gold.fct_results r
JOIN marathos.gold.dim_athlete a ON r.athlete_id = a.athlete_id
GROUP BY year_of_event, athlete_gender;