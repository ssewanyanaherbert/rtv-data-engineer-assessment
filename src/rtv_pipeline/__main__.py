"""Command line entrypoint used by the Airflow tasks.

    python -m rtv_pipeline init
    python -m rtv_pipeline catalog
    python -m rtv_pipeline bronze --cycle baseline
    python -m rtv_pipeline silver
    python -m rtv_pipeline gold
    python -m rtv_pipeline test
    python -m rtv_pipeline report
    python -m rtv_pipeline all          # everything above, in order, without Airflow
"""

from __future__ import annotations

import argparse
import logging
import sys

from rtv_pipeline import catalog, gold, quality, report, warehouse
from rtv_pipeline.settings import get_settings


def _spark():
    from rtv_pipeline.spark import get_spark

    return get_spark()


def cmd_bronze(cycles: list[str]) -> None:
    from rtv_pipeline import bronze

    spark = _spark()
    for code in cycles:
        bronze.run(spark, code)


def cmd_silver() -> None:
    from rtv_pipeline import silver

    silver.run(_spark())


def cmd_all() -> None:
    warehouse.ensure_schema()
    catalog.run()
    cmd_bronze([c.code for c in get_settings().cycles])
    cmd_silver()
    gold.run()
    try:
        quality.run()
    finally:
        report.run()


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(prog="rtv_pipeline")
    parser.add_argument("command", choices=["init", "catalog", "bronze", "silver", "gold", "test", "report", "all"])
    parser.add_argument("--cycle", action="append", help="survey cycle code (bronze only); repeatable")
    args = parser.parse_args(argv)

    if args.command == "init":
        warehouse.ensure_schema()
    elif args.command == "catalog":
        catalog.run()
    elif args.command == "bronze":
        cmd_bronze(args.cycle or [c.code for c in get_settings().cycles])
    elif args.command == "silver":
        cmd_silver()
    elif args.command == "gold":
        gold.run()
    elif args.command == "test":
        quality.run()
    elif args.command == "report":
        report.run()
    else:
        cmd_all()
    return 0


if __name__ == "__main__":
    sys.exit(main())
