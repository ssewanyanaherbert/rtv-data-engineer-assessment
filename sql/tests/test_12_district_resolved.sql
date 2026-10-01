-- severity: warn
-- layer: gold
-- description: Every household is placed in a known district
SELECT submission_sk, household_key
FROM gold.fct_survey_submissions
WHERE geography_key = -1
