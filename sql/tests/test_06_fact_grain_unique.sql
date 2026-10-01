-- severity: error
-- layer: gold
-- description: fct_survey_submissions has one row per household per cycle
SELECT household_key, cycle_key, count(*)
FROM gold.fct_survey_submissions
GROUP BY household_key, cycle_key
HAVING count(*) > 1
