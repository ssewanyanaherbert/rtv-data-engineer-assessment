-- severity: warn
-- layer: silver
-- description: Household head age is between 13 and 120 (the Year 2 form constraint)
SELECT survey_cycle, submission_key, hhh_age
FROM silver.survey_submissions
WHERE hhh_age NOT BETWEEN 13 AND 120
