-- One row per survey cycle, with the ingest that Gold was built from.

DROP TABLE IF EXISTS gold.dim_survey_cycle CASCADE;

CREATE TABLE gold.dim_survey_cycle AS
WITH latest_ingest AS (
    SELECT DISTINCT ON (survey_cycle) survey_cycle, ingest_id, source_rows, source_columns, ingested_at
    FROM meta.ingest_log
    ORDER BY survey_cycle, ingested_at DESC
)
SELECT
    c.cycle_order::smallint AS cycle_key,
    c.cycle_code,
    c.cycle_name,
    c.cycle_order,
    CASE c.cycle_code WHEN 'baseline' THEN 'BL' WHEN 'year1' THEN 'Y1' WHEN 'year2' THEN 'Y2' END AS cycle_short_name,
    c.source_file,
    c.form_file,
    i.ingest_id,
    i.source_rows,
    i.source_columns,
    i.ingested_at,
    (i.ingest_id IS NOT NULL) AS is_loaded
FROM meta.ref_survey_cycle c
LEFT JOIN latest_ingest i ON i.survey_cycle = c.cycle_code;

ALTER TABLE gold.dim_survey_cycle ADD PRIMARY KEY (cycle_key);
