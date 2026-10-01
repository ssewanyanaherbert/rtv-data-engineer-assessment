-- severity: error
-- layer: silver
-- description: Submission key, submission timestamp and interview status are populated
SELECT survey_cycle, submission_key
FROM silver.survey_submissions
WHERE submission_key IS NULL
   OR submission_ts IS NULL
   OR interview_status_code IS NULL
