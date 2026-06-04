CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
  COMMENT "Dim table - gold layer" AS
SELECT
  athlete_id,
  MAX_BY(athlete_country, year_of_event)      AS athlete_country,
  MAX_BY(athlete_country_name, year_of_event) AS athlete_country_name,
  MAX_BY(athlete_club, year_of_event)         AS athlete_club,
  MAX_BY(athlete_year_of_birth, year_of_event) AS athlete_year_of_birth,
  MAX_BY(athlete_gender, year_of_event)       AS athlete_gender,
  MAX_BY(athlete_gender_label, year_of_event) AS athlete_gender_label,
  MAX_BY(athlete_age_category, year_of_event) AS athlete_age_category
FROM
  marathos.silver.marathos_obt
GROUP BY
  athlete_id;