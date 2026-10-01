"""Postgres helpers: connections, SQL file execution, bulk loads and the run log."""

from __future__ import annotations

import json
import logging
import math
import os
import time
import uuid
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Sequence

import psycopg

from rtv_pipeline.settings import SQL_DIR, warehouse_dsn

log = logging.getLogger(__name__)


def connect() -> psycopg.Connection:
    return psycopg.connect(warehouse_dsn())


def run_id() -> str:
    return os.environ.get("PIPELINE_RUN_ID") or f"manual__{datetime.now():%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"


def execute_sql_file(conn: psycopg.Connection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    log.info("Running %s", path.relative_to(SQL_DIR))
    with conn.cursor() as cur:
        cur.execute(sql)


def execute_sql_dir(conn: psycopg.Connection, folder: str) -> list[Path]:
    files = sorted((SQL_DIR / folder).glob("*.sql"))
    for f in files:
        execute_sql_file(conn, f)
    return files


def ensure_schema() -> None:
    with connect() as conn:
        execute_sql_dir(conn, "warehouse")


def _clean(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if hasattr(value, "to_pydatetime"):
        return None if value != value else value.to_pydatetime()
    return value


def copy_rows(
    conn: psycopg.Connection,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence],
) -> int:
    count = 0
    col_list = ", ".join(columns)
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} ({col_list}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row([_clean(v) for v in row])
                count += 1
    return count


def replace_rows(
    conn: psycopg.Connection,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence],
    where: str | None = None,
    params: Sequence | None = None,
) -> int:
    """Delete (all or a slice) then bulk insert, inside the caller's transaction."""
    with conn.cursor() as cur:
        if where:
            cur.execute(f"DELETE FROM {table} WHERE {where}", params)
        else:
            cur.execute(f"TRUNCATE {table}")
    return copy_rows(conn, table, columns, rows)


def copy_dataframe(conn: psycopg.Connection, table: str, df, where=None, params=None) -> int:
    columns = list(df.columns)
    rows = df.astype(object).where(df.notna(), None).itertuples(index=False, name=None)
    return replace_rows(conn, table, columns, rows, where=where, params=params)


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


@contextmanager
def track_step(step: str, cycle: str | None = None):
    """Record start/end/status of a pipeline step in meta.pipeline_run_log.

    Usage::

        with track_step("bronze", "baseline") as stats:
            stats["rows_out"] = 123
    """
    rid = run_id()
    stats: dict = {}
    started = time.time()
    with connect() as conn:
        log_id = conn.execute(
            "INSERT INTO meta.pipeline_run_log (run_id, step, survey_cycle, status, started_at) "
            "VALUES (%s, %s, %s, 'running', now()) RETURNING log_id",
            (rid, step, cycle),
        ).fetchone()[0]

    status, error = "success", None
    try:
        yield stats
    except Exception as exc:
        status, error = "failed", f"{type(exc).__name__}: {exc}".replace("\x00", "")[:4000]
        raise
    finally:
        with connect() as conn:
            conn.execute(
                "UPDATE meta.pipeline_run_log SET status = %s, finished_at = now(), duration_sec = %s, "
                "rows_in = %s, rows_out = %s, details = %s::jsonb, error = %s WHERE log_id = %s",
                (
                    status,
                    round(time.time() - started, 2),
                    stats.pop("rows_in", None),
                    stats.pop("rows_out", None),
                    json.dumps(stats, default=_json_default),
                    error,
                    log_id,
                ),
            )
        log.info("%s%s finished: %s", step, f" [{cycle}]" if cycle else "", status)
