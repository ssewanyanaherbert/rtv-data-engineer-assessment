-- severity: warn
-- layer: silver
-- description: Consent is only recorded when the listed household was found (status = 1)
SELECT survey_cycle, submission_key
FROM silver.survey_submissions
WHERE consent_participate IS NOT NULL
  AND interview_status_code <> 1
