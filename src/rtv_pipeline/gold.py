"""Gold layer: dimensional model built in Postgres from Silver (the "T" of ELT)."""

from __future__ import annotations

import logging

from rtv_pipeline import sqlrunner, warehouse
from rtv_pipeline.settings import SQL_DIR

log = logging.getLogger(__name__)


def run() -> None:
    models = sorted((SQL_DIR / "gold").glob("*.sql"))
    with warehouse.track_step("gold") as stats, warehouse.connect() as conn:
        # Single transaction: BI readers see either the previous build or the new one, never a half-built model.
        with conn.transaction(), conn.cursor() as cur:
            for model in models:
                log.info("Building %s", model.stem)
                cur.execute(sqlrunner.read(model))

        counts = {}
        for model in models:
            table = model.stem.split("_", 1)[1]
            counts[table] = conn.execute(f"SELECT count(*) FROM gold.{table}").fetchone()[0]
            conn.execute(f"ANALYZE gold.{table}")
        stats.update(rows_out=counts["fct_survey_submissions"], table_rows=counts)
