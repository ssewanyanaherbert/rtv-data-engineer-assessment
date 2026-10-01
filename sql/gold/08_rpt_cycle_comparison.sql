-- Cross-cycle comparison per district and overall, including change vs Baseline in percentage points.
-- Completion rate = completed / households attempted; consent rate = consented / households found.

DROP TABLE IF EXISTS gold.rpt_cycle_comparison CASCADE;

CREATE TABLE gold.rpt_cycle_comparison AS
WITH by_level AS (
    SELECT
        CASE WHEN grouping(g.district_name) = 1 THEN 'overall' ELSE 'district' END AS level,
        coalesce(g.district_name, 'All districts')                 AS district_name,
        f.cycle_key,
        count(*)                                                   AS households,
        count(*) FILTER (WHERE f.is_found)                         AS households_found,
        count(*) FILTER (WHERE f.is_consented)                     AS households_consented,
        count(*) FILTER (WHERE f.is_completed)                     AS households_completed,
        count(*) FILTER (WHERE NOT f.has_valid_location)           AS missing_location
    FROM gold.fct_survey_submissions f
    JOIN gold.dim_geography g USING (geography_key)
    GROUP BY GROUPING SETS ((g.district_name, f.cycle_key), (f.cycle_key))
),
rates AS (
    SELECT
        b.*,
        round(100.0 * households_completed / nullif(households, 0), 2)       AS completion_rate_pct,
        round(100.0 * households_consented / nullif(households_found, 0), 2) AS consent_rate_pct,
        round(100.0 * missing_location / nullif(households, 0), 2)          AS missing_location_rate_pct
    FROM by_level b
)
SELECT
    r.level,
    r.district_name,
    r.cycle_key,
    c.cycle_name,
    r.households,
    r.households_found,
    r.households_consented,
    r.households_completed,
    r.missing_location,
    r.completion_rate_pct,
    r.consent_rate_pct,
    r.missing_location_rate_pct,
    r.completion_rate_pct - base.completion_rate_pct AS completion_rate_change_vs_baseline_pp,
    r.consent_rate_pct - base.consent_rate_pct       AS consent_rate_change_vs_baseline_pp
FROM rates r
JOIN gold.dim_survey_cycle c USING (cycle_key)
LEFT JOIN rates base
    ON base.level = r.level AND base.district_name = r.district_name
   AND base.cycle_key = (SELECT cycle_key FROM gold.dim_survey_cycle WHERE cycle_code = 'baseline');
