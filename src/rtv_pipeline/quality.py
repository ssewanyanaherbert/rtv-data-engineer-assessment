"""Data tests. Each file in sql/tests returns the rows that violate the rule (zero rows = pass).

Header block in each file::

    -- severity: error | warn
    -- layer: bronze | silver | gold
    -- description: what is being asserted

All tests are recorded in meta.dq_results; the step fails if any ``error`` test fails.
"""

from __future__ import annotations

import logging

from rtv_pipeline import sqlrunner, warehouse
from rtv_pipeline.settings import SQL_DIR

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


def run_tests() -> list[dict]:
    rid = warehouse.run_id()
    results = []
    with warehouse.connect() as conn:
        for path in sorted((SQL_DIR / "tests").glob("*.sql")):
            meta = sqlrunner.header(path)
            name = path.stem
            try:
                failing = conn.execute(f"SELECT count(*) FROM ({sqlrunner.read(path)}) AS violations").fetchone()[0]
                status = "pass" if failing == 0 else "fail"
            except Exception as exc:  # a broken test must not hide the other results
                conn.rollback()
                log.error("Test %s errored: %s", name, exc)
                failing, status = None, "error"
            results.append(
                {
                    "test_name": name,
                    "layer": meta.get("layer", "unknown"),
                    "severity": meta.get("severity", "error"),
                    "status": status,
                    "failing_rows": failing,
                    "description": meta.get("description"),
                }
            )
            log.info("%-45s %-5s %s", name, status.upper(), "" if failing in (0, None) else f"({failing} rows)")

        conn.execute("DELETE FROM meta.dq_results WHERE run_id = %s", (rid,))
        warehouse.copy_rows(
            conn,
            "meta.dq_results",
            ["run_id", "test_name", "layer", "severity", "status", "failing_rows", "description"],
            ((rid, r["test_name"], r["layer"], r["severity"], r["status"], r["failing_rows"], r["description"])
             for r in results),
        )
    return results


def run() -> None:
    with warehouse.track_step("data_tests") as stats:
        results = run_tests()
        blocking = [r["test_name"] for r in results if r["severity"] == "error" and r["status"] != "pass"]
        warnings = [r["test_name"] for r in results if r["severity"] == "warn" and r["status"] != "pass"]
        stats.update(tests=len(results), failed_errors=blocking, failed_warnings=warnings)
        if blocking:
            raise DataQualityError(f"Blocking data tests failed: {', '.join(blocking)}")
