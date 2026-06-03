CREATE OR REPLACE VIEW marathos.gold.mart_events_per_year
    COMMENT "Mart table - gold layer" AS
SELECT
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    e.event_country,
    e.event_type,
    COUNT(DISTINCT r.event_id)  AS antal_unika_event,
    COUNT(*)                    AS antal_starter
FROM marathos.gold.fct_results r
JOIN marathos.gold.dim_event e  ON r.event_id = e.event_id
JOIN marathos.gold.dim_date d   ON r.event_start_date = d.date_id
GROUP BY d.year, d.quarter, d.month, d.month_name, e.event_country, e.event_type
ORDER BY d.year, d.month;