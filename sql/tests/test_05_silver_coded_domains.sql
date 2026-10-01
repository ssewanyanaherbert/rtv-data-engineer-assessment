-- severity: error
-- layer: silver
-- description: Interview status is a known code (1-5) and consent answers are 0/1
SELECT survey_cycle, submission_key, interview_status_code, consent_participate, consent_followup
FROM silver.survey_submissions
WHERE interview_status_code NOT BETWEEN 1 AND 5
   OR consent_participate NOT IN (0, 1)
   OR consent_followup NOT IN (0, 1)
