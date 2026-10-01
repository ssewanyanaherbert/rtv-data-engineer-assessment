-- severity: warn
-- layer: silver
-- description: At least 95% of household IDs per cycle follow the tracking-sheet pattern (DST-XXX-XXX-X0000)
SELECT survey_cycle,
       count(*) FILTER (WHERE household_key !~ {{ household_id_pattern }} OR household_key IS NULL) AS non_conforming,
       count(*) AS total
FROM silver.survey_submissions
GROUP BY survey_cycle
HAVING count(*) FILTER (WHERE household_key !~ {{ household_id_pattern }} OR household_key IS NULL) > 0.05 * count(*)
