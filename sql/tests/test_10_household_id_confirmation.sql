-- severity: warn
-- layer: silver
-- description: The re-typed household ID matches the first entry
SELECT survey_cycle, submission_key, household_id
FROM silver.survey_submissions
WHERE household_id_confirmed IS FALSE
