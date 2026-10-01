-- severity: warn
-- layer: silver
-- description: Interview starts before it ends, is uploaded after it starts, and falls in a plausible window
SELECT survey_cycle, submission_key, start_ts, end_ts, submission_ts
FROM silver.survey_submissions
WHERE start_ts > end_ts
   OR submission_ts < start_ts
   OR submission_ts NOT BETWEEN timestamp '2015-01-01' AND now()
