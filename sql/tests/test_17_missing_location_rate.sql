-- severity: warn
-- layer: gold
-- description: Missing or out-of-country GPS fixes stay below 10% of households per cycle
SELECT cycle_key,
       count(*) FILTER (WHERE NOT has_valid_location) AS missing_location,
       count(*) AS households
FROM gold.fct_survey_submissions
GROUP BY cycle_key
HAVING count(*) FILTER (WHERE NOT has_valid_location) > 0.10 * count(*)
