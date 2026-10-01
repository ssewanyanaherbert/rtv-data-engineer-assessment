-- severity: error
-- layer: gold
-- description: Every fact row joins to dim_survey_cycle, dim_geography, dim_time and dim_household
SELECT f.submission_sk
FROM gold.fct_survey_submissions f
LEFT JOIN gold.dim_survey_cycle c ON c.cycle_key = f.cycle_key
LEFT JOIN gold.dim_geography g ON g.geography_key = f.geography_key
LEFT JOIN gold.dim_time t ON t.date_key = f.submission_date_key
LEFT JOIN gold.dim_household h ON h.household_key = f.household_key
WHERE c.cycle_key IS NULL
   OR g.geography_key IS NULL
   OR t.date_key IS NULL
   OR h.household_key IS NULL
