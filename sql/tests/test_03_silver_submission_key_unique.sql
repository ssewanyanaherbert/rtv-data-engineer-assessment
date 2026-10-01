-- severity: error
-- layer: silver
-- description: SurveyCTO KEY is unique within each cycle
SELECT survey_cycle, submission_key, count(*)
FROM silver.survey_submissions
GROUP BY survey_cycle, submission_key
HAVING count(*) > 1
