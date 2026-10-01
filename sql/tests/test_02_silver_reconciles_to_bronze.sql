-- severity: error
-- layer: silver
-- description: Silver holds exactly one row per Bronze record for each cycle
WITH latest AS (
    SELECT DISTINCT ON (survey_cycle) survey_cycle, ingest_id, bronze_rows
    FROM meta.ingest_log
    ORDER BY survey_cycle, ingested_at DESC
),
silver AS (
    SELECT survey_cycle, bronze_ingest_id, count(*) AS silver_rows
    FROM silver.survey_submissions
    GROUP BY survey_cycle, bronze_ingest_id
)
SELECT l.survey_cycle, l.bronze_rows, s.silver_rows
FROM latest l
FULL JOIN silver s ON s.survey_cycle = l.survey_cycle AND s.bronze_ingest_id = l.ingest_id
WHERE s.silver_rows IS DISTINCT FROM l.bronze_rows
