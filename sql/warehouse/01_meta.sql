-- Schemas and operational metadata. Idempotent: safe to run on every pipeline start.

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS meta.pipeline_run_log (
    log_id        bigserial PRIMARY KEY,
    run_id        text        NOT NULL,
    step          text        NOT NULL,
    survey_cycle  text,
    status        text        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at    timestamptz NOT NULL,
    finished_at   timestamptz,
    duration_sec  numeric(10, 2),
    rows_in       bigint,
    rows_out      bigint,
    details       jsonb,
    error         text
);
CREATE INDEX IF NOT EXISTS ix_run_log_run ON meta.pipeline_run_log (run_id);

CREATE TABLE IF NOT EXISTS meta.ingest_log (
    survey_cycle     text        NOT NULL,
    ingest_id        text        NOT NULL,
    source_file      text        NOT NULL,
    file_sha256      text        NOT NULL,
    file_bytes       bigint      NOT NULL,
    source_rows      integer     NOT NULL,
    bronze_rows      integer     NOT NULL,
    source_columns   integer     NOT NULL,
    bronze_path      text        NOT NULL,
    ingested_at      timestamptz NOT NULL,
    run_id           text        NOT NULL,
    last_seen_run_id text,
    PRIMARY KEY (survey_cycle, ingest_id)
);

CREATE TABLE IF NOT EXISTS meta.bronze_columns (
    survey_cycle   text    NOT NULL,
    ingest_id      text    NOT NULL,
    ordinal        integer NOT NULL,
    source_name    text    NOT NULL,
    bronze_name    text    NOT NULL,
    non_blank_rows integer NOT NULL,
    fill_rate      numeric(6, 4) NOT NULL,
    PRIMARY KEY (survey_cycle, ingest_id, ordinal)
);

CREATE TABLE IF NOT EXISTS meta.form_fields (
    survey_cycle    text    NOT NULL,
    field_name      text    NOT NULL,
    occurrence      integer NOT NULL,
    label           text,
    is_required     boolean,
    constraint_expr text,
    relevance_expr  text,
    hint            text,
    choice_count    integer,
    PRIMARY KEY (survey_cycle, field_name, occurrence)
);

CREATE TABLE IF NOT EXISTS meta.form_choices (
    survey_cycle text    NOT NULL,
    field_name   text    NOT NULL,
    sort_order   integer NOT NULL,
    choice_value text,
    choice_label text,
    PRIMARY KEY (survey_cycle, field_name, sort_order)
);

CREATE TABLE IF NOT EXISTS meta.variable_catalog (
    field_name     text    NOT NULL,
    survey_cycle   text    NOT NULL,
    source_column  text,
    bronze_column  text,
    is_present     boolean NOT NULL,
    data_type      text    NOT NULL,
    decode_field   text,
    choice_count   integer,
    form_label     text,
    description    text,
    non_blank_rows integer,
    fill_rate      numeric(6, 4),
    PRIMARY KEY (field_name, survey_cycle)
);

CREATE TABLE IF NOT EXISTS meta.ref_geography (
    district_name  text PRIMARY KEY,
    district_code  text NOT NULL,
    subregion_name text NOT NULL,
    region_name    text NOT NULL,
    region_code    text NOT NULL
);

CREATE TABLE IF NOT EXISTS meta.dq_results (
    run_id        text        NOT NULL,
    test_name     text        NOT NULL,
    layer         text        NOT NULL,
    severity      text        NOT NULL,
    status        text        NOT NULL,
    failing_rows  integer,
    description   text,
    checked_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, test_name)
);

-- Columns present in each cycle (latest ingest): basis of the schema-drift report.
CREATE OR REPLACE VIEW meta.schema_drift AS
WITH latest AS (
    SELECT DISTINCT ON (survey_cycle) survey_cycle, ingest_id
    FROM meta.ingest_log
    ORDER BY survey_cycle, ingested_at DESC
),
cols AS (
    SELECT b.survey_cycle, lower(b.source_name) AS column_name
    FROM meta.bronze_columns b
    JOIN latest l USING (survey_cycle, ingest_id)
)
SELECT
    column_name,
    bool_or(survey_cycle = 'baseline') AS in_baseline,
    bool_or(survey_cycle = 'year1')    AS in_year1,
    bool_or(survey_cycle = 'year2')    AS in_year2,
    count(DISTINCT survey_cycle)       AS cycles_present
FROM cols
GROUP BY column_name;

CREATE TABLE IF NOT EXISTS meta.ref_survey_cycle (
    cycle_code  text PRIMARY KEY,
    cycle_name  text     NOT NULL,
    cycle_order smallint NOT NULL UNIQUE,
    source_file text     NOT NULL,
    form_file   text     NOT NULL
);

-- Read-only access for the dashboard (role created by docker/postgres/init-databases.sh):
-- the Gold layer plus the run-health tables in meta.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bi_reader') THEN
        GRANT USAGE ON SCHEMA gold, meta TO bi_reader;
        GRANT SELECT ON ALL TABLES IN SCHEMA gold TO bi_reader;
        ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO bi_reader;
        GRANT SELECT ON meta.ingest_log, meta.pipeline_run_log, meta.dq_results TO bi_reader;
    END IF;
END
$$;
