"""Run report: row counts, operational and program metrics, data tests and schema drift.

Writes reports/pipeline_report.md plus CSV extracts that can be attached to a review.
"""

from __future__ import annotations

import csv
from datetime import datetime

from rtv_pipeline import warehouse
from rtv_pipeline.settings import get_settings

QUERIES = {
    "row_counts": """
        WITH silver AS (
            SELECT survey_cycle,
                   count(*) AS silver_rows,
                   count(*) FILTER (WHERE is_latest) AS dedup_rows,
                   count(*) FILTER (WHERE NOT is_latest) AS duplicates_dropped,
                   count(DISTINCT household_key) FILTER (WHERE household_submission_count > 1)
                       AS households_with_duplicates
            FROM silver.survey_submissions GROUP BY survey_cycle
        )
        SELECT c.cycle_name AS cycle, c.source_file, c.source_columns AS columns, c.source_rows AS source_rows,
               i.bronze_rows, s.silver_rows, s.dedup_rows AS gold_fact_rows, s.duplicates_dropped,
               s.households_with_duplicates,
               round(100.0 * s.duplicates_dropped / nullif(s.silver_rows, 0), 2) AS duplicate_rate_pct
        FROM gold.dim_survey_cycle c
        LEFT JOIN meta.ingest_log i ON i.survey_cycle = c.cycle_code AND i.ingest_id = c.ingest_id
        LEFT JOIN silver s ON s.survey_cycle = c.cycle_code
        ORDER BY c.cycle_order
    """,
    "field_operations": """
        SELECT c.cycle_name AS cycle,
               min(t.calendar_date) AS first_submission, max(t.calendar_date) AS last_submission,
               sum(raw_submissions) AS raw_submissions, sum(dedup_submissions) AS dedup_submissions,
               round(100.0 * sum(raw_missing_location) / nullif(sum(raw_submissions), 0), 2)
                   AS raw_missing_location_pct,
               round(100.0 * sum(dedup_missing_location) / nullif(sum(dedup_submissions), 0), 2)
                   AS dedup_missing_location_pct,
               round(sum(interview_minutes) / nullif(sum(timed_interviews), 0), 1) AS avg_interview_min
        FROM gold.rpt_field_operations r
        JOIN gold.dim_survey_cycle c USING (cycle_key)
        JOIN gold.dim_time t ON t.date_key = r.submission_date_key
        GROUP BY c.cycle_order, c.cycle_name ORDER BY c.cycle_order
    """,
    "status_mix": """
        SELECT c.cycle_name AS cycle, f.interview_status, count(*) AS households,
               round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY c.cycle_name), 2) AS share_pct
        FROM gold.fct_survey_submissions f JOIN gold.dim_survey_cycle c USING (cycle_key)
        GROUP BY c.cycle_order, c.cycle_name, f.interview_status ORDER BY c.cycle_order, households DESC
    """,
    "cycle_comparison": """
        SELECT district_name AS district, cycle_name AS cycle, households, households_found AS found,
               households_consented AS consented, households_completed AS completed,
               completion_rate_pct, consent_rate_pct, completion_rate_change_vs_baseline_pp,
               consent_rate_change_vs_baseline_pp
        FROM gold.rpt_cycle_comparison
        ORDER BY level DESC, district_name, cycle_key
    """,
    "household_coverage": """
        SELECT cycle_pattern, cycles_observed, sum(households) AS households
        FROM gold.rpt_household_coverage
        GROUP BY cycle_pattern, cycles_observed ORDER BY cycles_observed DESC, households DESC
    """,
    "data_tests": """
        SELECT test_name, layer, severity, status, failing_rows, description
        FROM meta.dq_results
        WHERE run_id = (SELECT run_id FROM meta.dq_results ORDER BY checked_at DESC LIMIT 1)
        ORDER BY test_name
    """,
    "schema_drift": """
        SELECT
            count(*) FILTER (WHERE cycles_present = 3) AS columns_in_all_cycles,
            count(*) FILTER (WHERE in_baseline AND cycles_present = 1) AS baseline_only,
            count(*) FILTER (WHERE in_year1 AND cycles_present = 1) AS year1_only,
            count(*) FILTER (WHERE in_year2 AND cycles_present = 1) AS year2_only,
            count(*) AS distinct_columns
        FROM meta.schema_drift
    """,
    "variable_catalog": """
        SELECT field_name, survey_cycle, source_column, is_present, data_type, decode_field,
               choice_count, fill_rate, form_label
        FROM meta.variable_catalog ORDER BY field_name, survey_cycle
    """,
    "run_log": """
        SELECT step, coalesce(survey_cycle, '') AS cycle, status, duration_sec, rows_in, rows_out
        FROM meta.pipeline_run_log
        WHERE run_id = %(run_id)s AND step <> 'report'
        ORDER BY started_at
    """,
}

SECTIONS = [
    ("Row counts by layer", "row_counts"),
    ("Field operations", "field_operations"),
    ("Interview status mix (deduplicated)", "status_mix"),
    ("Completion and consent by cycle", "cycle_comparison"),
    ("Households observed across cycles", "household_coverage"),
    ("Data tests", "data_tests"),
    ("Schema drift across exports", "schema_drift"),
    ("Pipeline steps (this run)", "run_log"),
]

CSV_EXPORTS = ["data_tests", "variable_catalog", "cycle_comparison", "row_counts"]


def _fmt(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value).replace("|", "\\|")


def markdown_table(columns: list[str], rows: list[tuple]) -> str:
    if not rows:
        return "_No rows._\n"
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join(_fmt(v) for v in row) + " |" for row in rows]
    return "\n".join(lines) + "\n"


def fetch(conn, name: str):
    cur = conn.execute(QUERIES[name], {"run_id": warehouse.run_id()})
    return [d.name for d in cur.description], cur.fetchall()


def run() -> None:
    out_dir = get_settings().reports_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    with warehouse.track_step("report") as stats, warehouse.connect() as conn:
        parts = [
            "# RTV Household Survey Pipeline - Run Report\n",
            f"Run `{warehouse.run_id()}` - generated {datetime.now():%Y-%m-%d %H:%M}\n",
        ]
        for title, name in SECTIONS:
            cols, rows = fetch(conn, name)
            parts.append(f"\n## {title}\n\n{markdown_table(cols, rows)}")
        (out_dir / "pipeline_report.md").write_text("".join(parts), encoding="utf-8")

        for name in CSV_EXPORTS:
            cols, rows = fetch(conn, name)
            with open(out_dir / f"{name}.csv", "w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                writer.writerow(cols)
                writer.writerows(rows)
        stats["files"] = ["pipeline_report.md"] + [f"{n}.csv" for n in CSV_EXPORTS]
