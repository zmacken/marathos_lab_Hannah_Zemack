CREATE OR REPLACE VIEW marathos.gold.mart_country_representation
    COMMENT "Mart table - gold layer" AS
SELECT
  a.athlete_country_name,
  a.athlete_country,
  count(DISTINCT r.athlete_id)  AS antal_unika_lopare,
  count(*)                      AS antal_starter,
  round(
  count(DISTINCT r.athlete_id) * 100.0 / sum(count(DISTINCT r.athlete_id)) OVER (), 2
  ) AS andel_procent --kanske ta bort
FROM marathos.gold.fct_results r

JOIN marathos.gold.dim_athlete a ON r.athlete_id = a.athlete_id
GROUP BY a.athlete_country_name, a.athlete_country
ORDER BY antal_unika_lopare DESC;