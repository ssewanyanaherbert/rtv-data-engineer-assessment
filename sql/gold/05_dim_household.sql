-- One row per household across all cycles: the backbone for longitudinal metrics.

DROP TABLE IF EXISTS gold.dim_household CASCADE;

CREATE TABLE gold.dim_household AS
WITH per_household AS (
    SELECT
        f.household_key,
        bool_and(f.is_linkable)                                          AS is_linkable,
        min(f.cycle_key)                                                 AS first_cycle_key,
        max(f.cycle_key)                                                 AS last_cycle_key,
        count(*)                                                         AS cycles_observed,
        count(*) FILTER (WHERE f.is_completed)                           AS cycles_completed,
        bool_or(c.cycle_code = 'baseline')                               AS in_baseline,
        bool_or(c.cycle_code = 'year1')                                  AS in_year1,
        bool_or(c.cycle_code = 'year2')                                  AS in_year2,
        bool_or(c.cycle_code = 'baseline' AND f.is_completed)            AS completed_baseline,
        bool_or(c.cycle_code = 'year1' AND f.is_completed)               AS completed_year1,
        bool_or(c.cycle_code = 'year2' AND f.is_completed)               AS completed_year2,
        (array_agg(f.geography_key ORDER BY f.cycle_key))[1]             AS home_geography_key
    FROM gold.fct_survey_submissions f
    JOIN gold.dim_survey_cycle c USING (cycle_key)
    GROUP BY f.household_key
)
SELECT
    p.*,
    concat_ws(' + ',
        CASE WHEN in_baseline THEN 'Baseline' END,
        CASE WHEN in_year1 THEN 'Year 1' END,
        CASE WHEN in_year2 THEN 'Year 2' END)                            AS cycle_pattern
FROM per_household p;

ALTER TABLE gold.dim_household ADD PRIMARY KEY (household_key);
