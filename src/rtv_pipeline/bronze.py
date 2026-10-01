"""Bronze layer: land each SurveyCTO export in the lake untouched, plus ingest metadata.

Layout::

    lake/bronze/survey_cycle=<code>/ingest_id=<sha256[:16]>/part-*.parquet
                                                            _manifest.json

A partition is written once and never modified. Re-running with the same file is a no-op;
a changed export produces a new ingest_id next to the old one, and Silver reads the latest.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from rtv_pipeline import warehouse
from rtv_pipeline.settings import Cycle, get_settings

log = logging.getLogger(__name__)

META_COLUMNS = ["_survey_cycle", "_source_file", "_ingest_id", "_ingested_at", "_record_seq", "_row_hash"]

csv.field_size_limit(sys.maxsize)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_header_and_count(path: Path) -> tuple[list[str], int]:
    """Header and number of data records (quoted newlines are handled by the csv module)."""
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = sum(1 for _ in reader)
    return header, rows


def bronze_column_names(header: list[str]) -> list[str]:
    """Parquet/Spark safe, case-insensitively unique names. The original names are kept in the manifest."""
    names, used = [], set()
    for i, raw in enumerate(header):
        base = re.sub(r"[^0-9a-zA-Z]+", "_", raw.strip()).strip("_").lower() or f"unnamed_{i}"
        if base[0].isdigit():
            base = f"c_{base}"
        name, n = base, 1
        while name in used:
            n += 1
            name = f"{base}_{n}"
        used.add(name)
        names.append(name)
    return names


def with_ingest_metadata(df: DataFrame, cycle: Cycle, ingest_id: str, ingested_at: datetime) -> DataFrame:
    source_cols = df.columns
    row_hash = F.sha2(F.concat_ws("\x1f", *[F.coalesce(F.col(c), F.lit("\x00")) for c in source_cols]), 256)
    return df.select(
        *source_cols,
        F.lit(cycle.code).alias("_survey_cycle"),
        F.lit(cycle.source_file).alias("_source_file"),
        F.lit(ingest_id).alias("_ingest_id"),
        F.lit(ingested_at.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp_ntz").alias("_ingested_at"),  # UTC
        F.monotonically_increasing_id().alias("_record_seq"),
        row_hash.alias("_row_hash"),
    )


def profile_columns(parquet_dir: Path, columns: list[str]) -> dict[str, int]:
    """Populated values per column, read from Parquet footer statistics (no data scan).

    The CSV reader maps empty fields to null, so populated = rows - nulls.
    """
    counts = dict.fromkeys(columns, 0)
    for part in parquet_dir.glob("*.parquet"):
        meta = pq.ParquetFile(part).metadata
        for rg in range(meta.num_row_groups):
            group = meta.row_group(rg)
            for i in range(group.num_columns):
                chunk = group.column(i)
                name = chunk.path_in_schema
                if name in counts and chunk.statistics is not None and chunk.statistics.has_null_count:
                    counts[name] += group.num_rows - chunk.statistics.null_count
    return counts


def partition_path(cycle: Cycle, ingest_id: str) -> Path:
    return get_settings().bronze_dir / f"survey_cycle={cycle.code}" / f"ingest_id={ingest_id}"


def read_manifest(path: Path) -> dict:
    return json.loads((path / "_manifest.json").read_text(encoding="utf-8"))


def latest_partition(cycle: Cycle) -> Path:
    root = get_settings().bronze_dir / f"survey_cycle={cycle.code}"
    candidates = [p for p in root.glob("ingest_id=*") if (p / "_manifest.json").exists()]
    if not candidates:
        raise FileNotFoundError(f"No bronze data for cycle '{cycle.code}'. Run the bronze step first.")
    return max(candidates, key=lambda p: read_manifest(p)["ingested_at"])


def ingest_cycle(spark: SparkSession, cycle: Cycle) -> dict:
    settings = get_settings()
    source = settings.data_dir / cycle.source_file
    if not source.exists():
        raise FileNotFoundError(f"Source export not found: {source}")

    sha = file_sha256(source)
    ingest_id = sha[:16]
    target = partition_path(cycle, ingest_id)

    if (target / "_manifest.json").exists():
        log.info("%s already ingested as %s, skipping write", cycle.source_file, ingest_id)
        manifest = read_manifest(target)
        manifest["skipped"] = True
        return manifest

    header, source_rows = read_header_and_count(source)
    names = bronze_column_names(header)
    schema = StructType([StructField(n, StringType()) for n in names])

    raw = (
        spark.read.schema(schema)
        .option("header", True)
        .option("multiLine", True)
        # Quoted values contain CRLF; without an explicit separator the parser auto-detects
        # CRLF from the header and then runs records together.
        .option("lineSep", "\n")
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .option("maxColumns", 50000)
        .option("mode", "FAILFAST")
        .csv(str(source))
    )
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0)
    df = with_ingest_metadata(raw, cycle, ingest_id, ingested_at)

    staging = target.with_name(target.name + "._staging")
    shutil.rmtree(staging, ignore_errors=True)
    df.write.mode("error").option("compression", "snappy").parquet(str(staging))

    written = spark.read.parquet(str(staging))
    bronze_rows = written.count()
    if bronze_rows != source_rows:
        shutil.rmtree(staging, ignore_errors=True)
        raise ValueError(f"{cycle.code}: {source_rows} records in source but {bronze_rows} parsed")

    non_blank = profile_columns(staging, names)
    manifest = {
        "survey_cycle": cycle.code,
        "ingest_id": ingest_id,
        "source_file": cycle.source_file,
        "file_sha256": sha,
        "file_bytes": source.stat().st_size,
        "source_rows": source_rows,
        "bronze_rows": bronze_rows,
        "source_columns": len(header),
        "ingested_at": ingested_at.isoformat(),
        "columns": [
            {"ordinal": i + 1, "source_name": src, "bronze_name": name, "non_blank_rows": non_blank[name]}
            for i, (src, name) in enumerate(zip(header, names))
        ],
    }
    (staging / "_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging.rename(target)
    manifest["skipped"] = False
    return manifest


def register_ingest(manifest: dict) -> None:
    """Mirror the manifest into the warehouse catalog (idempotent)."""
    cycle, ingest_id = manifest["survey_cycle"], manifest["ingest_id"]
    with warehouse.connect() as conn:
        conn.execute(
            """
            INSERT INTO meta.ingest_log (survey_cycle, ingest_id, source_file, file_sha256, file_bytes,
                source_rows, bronze_rows, source_columns, bronze_path, ingested_at, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (survey_cycle, ingest_id) DO UPDATE SET last_seen_run_id = EXCLUDED.run_id
            """,
            (
                cycle,
                ingest_id,
                manifest["source_file"],
                manifest["file_sha256"],
                manifest["file_bytes"],
                manifest["source_rows"],
                manifest["bronze_rows"],
                manifest["source_columns"],
                str(partition_path(get_settings().cycle(cycle), ingest_id)),
                manifest["ingested_at"],
                warehouse.run_id(),
            ),
        )
        rows = manifest["bronze_rows"] or 1
        warehouse.replace_rows(
            conn,
            "meta.bronze_columns",
            ["survey_cycle", "ingest_id", "ordinal", "source_name", "bronze_name", "non_blank_rows", "fill_rate"],
            (
                (cycle, ingest_id, c["ordinal"], c["source_name"], c["bronze_name"], c["non_blank_rows"],
                 round(c["non_blank_rows"] / rows, 4))
                for c in manifest["columns"]
            ),
            where="survey_cycle = %s AND ingest_id = %s",
            params=(cycle, ingest_id),
        )


def run(spark: SparkSession, cycle_code: str) -> None:
    cycle = get_settings().cycle(cycle_code)
    with warehouse.track_step("bronze", cycle.code) as stats:
        manifest = ingest_cycle(spark, cycle)
        register_ingest(manifest)
        stats.update(
            rows_in=manifest["source_rows"],
            rows_out=manifest["bronze_rows"],
            ingest_id=manifest["ingest_id"],
            columns=manifest["source_columns"],
            skipped=manifest["skipped"],
        )
