-- severity: error
-- layer: bronze
-- description: Every record in the source CSV landed in Bronze (latest ingest per cycle)
SELECT survey_cycle, source_rows, bronze_rows
FROM (
    SELECT DISTINCT ON (survey_cycle) survey_cycle, source_rows, bronze_rows
    FROM meta.ingest_log
    ORDER BY survey_cycle, ingested_at DESC
) latest
WHERE source_rows <> bronze_rows
