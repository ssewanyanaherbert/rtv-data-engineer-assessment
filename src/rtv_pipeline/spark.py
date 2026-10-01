from __future__ import annotations

from pyspark.sql import SparkSession

from rtv_pipeline.settings import get_settings


def get_spark(app_name: str = "rtv-survey-pipeline") -> SparkSession:
    cfg = get_settings().spark
    spark = (
        SparkSession.builder.appName(app_name)
        .master(cfg["master"])
        .config("spark.driver.memory", cfg["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(cfg["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", "Africa/Kampala")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # Wide exports (7k+ columns) produce very large plans.
        .config("spark.sql.debug.maxToStringFields", "10000")
        .config("spark.sql.codegen.wholeStage", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
