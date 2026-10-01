-- severity: warn
-- layer: gold
-- description: No more than 2% of completed interviews per cycle have a missing or implausible duration
SELECT cycle_key,
       count(*) FILTER (WHERE NOT is_duration_valid) AS implausible,
       count(*) AS completed
FROM gold.fct_survey_submissions
WHERE is_completed
GROUP BY cycle_key
HAVING count(*) FILTER (WHERE NOT is_duration_valid) > 0.02 * count(*)
