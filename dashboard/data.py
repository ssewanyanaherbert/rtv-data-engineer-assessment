"""Read-only access to the Gold layer and run-health tables."""

from __future__ import annotations

import os

import pandas as pd
import psycopg

QUERIES = {
    "cycles": "SELECT cycle_key, cycle_code, cycle_name, cycle_order, source_rows, source_columns "
              "FROM gold.dim_survey_cycle ORDER BY cycle_order",
    "fact": """
        SELECT f.household_key, f.is_linkable, f.cycle_key, c.cycle_name, c.cycle_order,
               g.district_name, g.subregion_name, g.region_name,
               t.calendar_date, t.month_start, t.year_month,
               f.interview_status, f.is_found, f.is_consented, f.is_followup_consented, f.is_completed,
               f.hhh_sex, f.hhh_age, f.hhh_age_band, f.hhh_age_band_order, f.hh_size,
               f.has_valid_location, f.duration_min, f.is_duration_valid, f.submissions_received
        FROM gold.fct_survey_submissions f
        JOIN gold.dim_survey_cycle c USING (cycle_key)
        JOIN gold.dim_geography g USING (geography_key)
        LEFT JOIN gold.dim_time t ON t.date_key = f.submission_date_key
    """,
    "field_ops": """
        SELECT c.cycle_name, c.cycle_order, g.district_name, t.calendar_date, t.week_start, r.interview_status,
               r.raw_submissions, r.dedup_submissions, r.duplicate_submissions_dropped,
               r.households_with_duplicates, r.raw_missing_location, r.dedup_missing_location,
               r.completed_interviews, r.timed_interviews, r.interview_minutes
        FROM gold.rpt_field_operations r
        JOIN gold.dim_survey_cycle c USING (cycle_key)
        JOIN gold.dim_geography g USING (geography_key)
        LEFT JOIN gold.dim_time t ON t.date_key = r.submission_date_key
    """,
    "coverage": """
        SELECT g.district_name, r.cycle_pattern, r.cycles_observed, r.households,
               r.completed_baseline, r.completed_baseline_and_year1, r.completed_baseline_and_year2
        FROM gold.rpt_household_coverage r
        JOIN gold.dim_geography g USING (geography_key)
    """,
    "ingest": """
        SELECT DISTINCT ON (survey_cycle) survey_cycle, source_file, source_rows, bronze_rows, source_columns,
               ingested_at
        FROM meta.ingest_log ORDER BY survey_cycle, ingested_at DESC
    """,
    "runs": """
        SELECT step, coalesce(survey_cycle, '') AS cycle, status, started_at, duration_sec, rows_in, rows_out, error
        FROM meta.pipeline_run_log
        WHERE run_id = (SELECT run_id FROM meta.pipeline_run_log ORDER BY started_at DESC LIMIT 1)
        ORDER BY started_at
    """,
    "tests": """
        SELECT test_name, layer, severity, status, failing_rows, description
        FROM meta.dq_results
        WHERE run_id = (SELECT run_id FROM meta.dq_results ORDER BY checked_at DESC LIMIT 1)
        ORDER BY test_name
    """,
}


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "rtv_warehouse"),
        user=os.environ.get("DB_USER", "bi_reader"),
        password=os.environ.get("DB_PASSWORD", ""),
        connect_timeout=10,
    )


def fetch(conn: psycopg.Connection, sql: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])


def load_all() -> dict[str, pd.DataFrame]:
    with connect() as conn:
        frames = {name: fetch(conn, sql) for name, sql in QUERIES.items()}
    for name in ("fact", "field_ops"):
        frames[name]["calendar_date"] = pd.to_datetime(frames[name]["calendar_date"])
    frames["field_ops"]["week_start"] = pd.to_datetime(frames["field_ops"]["week_start"])
    return frames
