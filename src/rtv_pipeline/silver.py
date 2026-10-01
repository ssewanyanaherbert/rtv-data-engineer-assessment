"""Silver layer: conform the three wide exports to one narrow, typed schema.

* Only the columns listed in config/variable_map.yml are read from Bronze (column pruning).
* Coded answers are labelled with the choice list from that cycle's own form.
* Every submission is kept; dedup is expressed as ``dedup_rank`` / ``is_latest`` so the
  impact of deduplication stays measurable.
"""

from __future__ import annotations

import csv
import logging
import re
from functools import reduce

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from rtv_pipeline import bronze, forms, warehouse
from rtv_pipeline.settings import CONFIG_DIR, Cycle, get_settings, get_variable_map

log = logging.getLogger(__name__)

TIMESTAMP_FORMATS = [
    "M/d/yyyy, h:mm:ss a",
    "MMM d, yyyy h:mm:ss a",
    "MMM d, yyyy, h:mm:ss a",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    "yyyy-MM-dd'T'HH:mm:ss",
    "d/M/yyyy H:mm:ss",
]

SPARK_TYPES = {"string": "string", "int": "int", "double": "double", "timestamp": "timestamp_ntz"}

SILVER_COLUMNS = [
    "survey_cycle", "cycle_order", "submission_key", "instance_id", "household_id", "household_key",
    "household_id_confirmed", "submission_ts", "submission_date", "start_ts", "end_ts", "form_version",
    "duration_sec", "duration_min", "surveyor", "district_code", "district_code_label", "district_preload",
    "district_name", "district_source", "district_form_conflict", "subcounty", "parish", "village", "cluster",
    "interview_status_code", "interview_status_code_label", "track_status", "consent_participate",
    "consent_followup", "refusal_reason_code", "refusal_reason_code_label", "respondent_type_code",
    "respondent_type_code_label", "respondent_sex_code", "respondent_sex_code_label", "hhh_sex_code",
    "hhh_sex_code_label", "hhh_age", "hhh_marital_status_code", "hhh_marital_status_code_label",
    "hhh_education_code", "hhh_education_code_label", "hh_size", "gps_latitude", "gps_longitude",
    "gps_accuracy", "has_valid_gps", "income_consumption_usd_day_aeq", "household_submission_count",
    "dedup_rank", "is_latest", "bronze_ingest_id", "bronze_row_hash", "source_file", "ingested_at",
]


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def resolve_source(candidates: list[str], columns: list[dict]) -> dict | None:
    """Pick the export column for a field: exact name, then case-insensitive, then punctuation-insensitive."""
    for matcher in (
        lambda cand, col: col["source_name"] == cand,
        lambda cand, col: col["source_name"].lower() == cand.lower(),
        lambda cand, col: normalize_name(col["source_name"]) == normalize_name(cand),
    ):
        for cand in candidates:
            for col in columns:
                if matcher(cand, col):
                    return col
    return None


def candidates_for(spec: dict, cycle_code: str) -> list[str]:
    sources = spec["sources"]
    if isinstance(sources, dict):
        sources = sources.get(cycle_code, [])
    return list(sources)


def parse_timestamp(col: Column) -> Column:
    cleaned = F.trim(col)
    # Survey times are local wall-clock values (Uganda); keep them timezone-free end to end.
    return F.coalesce(*[F.to_timestamp_ntz(cleaned, F.lit(fmt)) for fmt in TIMESTAMP_FORMATS])


def cast_typed(col: Column, type_name: str) -> Column:
    if type_name == "timestamp":
        return parse_timestamp(col)
    if type_name == "int":
        return F.round(F.trim(col).cast("double")).cast("int")
    if type_name == "double":
        return F.trim(col).cast("double")
    return F.when(F.trim(col) != "", F.trim(col))


def decode(col: Column, choices: dict[str, str]) -> Column:
    if not choices:
        return F.lit(None).cast("string")
    mapping = F.create_map(*[F.lit(x) for kv in choices.items() for x in kv])
    return mapping[col.cast("string")]


def load_geography() -> list[dict]:
    with open(CONFIG_DIR / "geography.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def conform_cycle(spark: SparkSession, cycle: Cycle) -> tuple[DataFrame, list[dict]]:
    """Return the conformed DataFrame for one cycle and its variable-catalog rows."""
    partition = bronze.latest_partition(cycle)
    manifest = bronze.read_manifest(partition)
    columns = manifest["columns"]
    form_fields = forms.load_form_catalog(cycle)
    choices = forms.choice_lists(form_fields)
    form_labels = {}
    for f in form_fields:
        form_labels.setdefault(f["field_name"].lower(), f["label"])

    bronze_df = spark.read.parquet(str(partition))
    selected, catalog = [], []
    for field, spec in get_variable_map().items():
        source = resolve_source(candidates_for(spec, cycle.code), columns)
        type_name = SPARK_TYPES[spec["type"]]
        if source is None:
            if spec.get("required"):
                raise KeyError(f"{cycle.code}: required field '{field}' not found (tried {spec['sources']})")
            log.warning("%s: optional field %s not present in export", cycle.code, field)
            selected.append(F.lit(None).cast(type_name).alias(field))
        else:
            selected.append(cast_typed(F.col(source["bronze_name"]), spec["type"]).alias(field))

        decode_field = spec.get("decode")
        if decode_field:
            selected.append(decode(F.col(source["bronze_name"]) if source else F.lit(None),
                                   choices.get(decode_field, {})).alias(f"{field}_label"))

        source_name = source["source_name"] if source else None
        form_key = (source_name or "").split("-")[0].lower()
        catalog.append(
            {
                "field_name": field,
                "survey_cycle": cycle.code,
                "source_column": source_name,
                "bronze_column": source["bronze_name"] if source else None,
                "is_present": source is not None,
                "data_type": spec["type"],
                "decode_field": decode_field,
                "choice_count": len(choices.get(decode_field, {})) if decode_field else None,
                "form_label": form_labels.get(form_key),
                "description": spec.get("description"),
                "non_blank_rows": source["non_blank_rows"] if source else 0,
                "fill_rate": round(source["non_blank_rows"] / max(manifest["bronze_rows"], 1), 4) if source else 0.0,
            }
        )

    lineage = [
        F.col("_ingest_id").alias("bronze_ingest_id"),
        F.col("_row_hash").alias("bronze_row_hash"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("ingested_at"),
    ]
    df = bronze_df.select(
        F.lit(cycle.code).alias("survey_cycle"), F.lit(cycle.order).alias("cycle_order"), *selected, *lineage
    )
    return df, catalog


def add_derived(df: DataFrame) -> DataFrame:
    rules = get_settings().rules
    bounds = rules["gps_bounds"]
    geo = load_geography()
    by_name = {g["district_name"].lower(): g["district_name"] for g in geo}
    by_code = {g["district_code"]: g["district_name"] for g in geo}
    name_map = F.create_map(*[F.lit(x) for kv in by_name.items() for x in kv])
    code_map = F.create_map(*[F.lit(x) for kv in by_code.items() for x in kv])

    hh_key = F.upper(F.regexp_replace(F.col("household_id"), r"\s+", ""))
    hh_confirm = F.upper(F.regexp_replace(F.col("household_id_confirm"), r"\s+", ""))
    from_preload = name_map[F.lower(F.trim(F.col("district_preload")))]
    from_hhid = code_map[F.substring_index(hh_key, "-", 1)]
    from_form = name_map[F.lower(F.col("district_code_label"))]

    lat, lon = F.col("gps_latitude"), F.col("gps_longitude")
    valid_gps = (
        lat.isNotNull() & lon.isNotNull() & ~((lat == 0) & (lon == 0))
        & lat.between(bounds["lat_min"], bounds["lat_max"]) & lon.between(bounds["lon_min"], bounds["lon_max"])
    )

    return (
        df.withColumn("household_key", F.when(hh_key != "", hh_key))
        .withColumn(
            "household_id_confirmed",
            F.when(F.col("household_id_confirm").isNull(), F.lit(None).cast("boolean")).otherwise(hh_key == hh_confirm),
        )
        .withColumn("district_name", F.coalesce(from_preload, from_hhid, from_form))
        .withColumn(
            "district_source",
            F.when(from_preload.isNotNull(), "preload")
            .when(from_hhid.isNotNull(), "household_id_prefix")
            .when(from_form.isNotNull(), "form_choice"),
        )
        .withColumn(
            "district_form_conflict",
            F.coalesce(F.col("district_code_label") != F.col("district_name"), F.lit(False)),
        )
        .withColumn("has_valid_gps", F.coalesce(valid_gps, F.lit(False)))
        .withColumn("submission_date", F.to_date("submission_ts"))
        .withColumn("duration_min", F.round(F.col("duration_sec") / 60.0, 2))
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Rank submissions within (cycle, household).

    Tie-breakers, in order: latest SubmissionDate, latest endtime, then the highest
    submission KEY so the result is deterministic. Submissions without a household ID
    cannot be linked and are treated as their own household.
    """
    group_key = F.coalesce(F.col("household_key"), F.concat(F.lit("NOHHID:"), F.col("submission_key")))
    w_order = Window.partitionBy("survey_cycle", group_key).orderBy(
        F.col("submission_ts").desc_nulls_last(),
        F.col("end_ts").desc_nulls_last(),
        F.col("submission_key").desc(),
    )
    w_all = Window.partitionBy("survey_cycle", group_key)
    return (
        df.withColumn("dedup_rank", F.row_number().over(w_order))
        .withColumn("household_submission_count", F.count(F.lit(1)).over(w_all))
        .withColumn("is_latest", F.col("dedup_rank") == 1)
    )


def available_cycles() -> list[Cycle]:
    root = get_settings().bronze_dir
    return [
        c for c in get_settings().cycles
        if any((root / f"survey_cycle={c.code}").glob("ingest_id=*/_manifest.json"))
    ]


def build(spark: SparkSession) -> tuple[DataFrame, list[dict]]:
    frames, catalog = [], []
    for cycle in available_cycles():
        df, rows = conform_cycle(spark, cycle)
        frames.append(df)
        catalog.extend(rows)
    if not frames:
        raise FileNotFoundError("No bronze partitions found for any survey cycle")
    unioned = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
    return deduplicate(add_derived(unioned)).select(*SILVER_COLUMNS), catalog


def run(spark: SparkSession) -> None:
    settings = get_settings()
    with warehouse.track_step("silver") as stats:
        df, catalog = build(spark)
        out = settings.silver_dir / "survey_submissions"
        df.write.mode("overwrite").partitionBy("survey_cycle").parquet(str(out))

        # Silver is narrow and small, so it is loaded straight from the lake files with pyarrow.
        pdf = pd.read_parquet(out)
        pdf["survey_cycle"] = pdf["survey_cycle"].astype(str)
        pdf = pdf[SILVER_COLUMNS]
        int_cols = [c for c in pdf.columns if c.endswith(("_code", "_count", "_rank", "_order"))
                    or c in ("hhh_age", "hh_size", "consent_participate", "consent_followup")]
        for c in int_cols:
            pdf[c] = pdf[c].astype("Int64")

        with warehouse.connect() as conn:
            loaded = warehouse.copy_dataframe(conn, "silver.survey_submissions", pdf)
            warehouse.replace_rows(
                conn,
                "meta.variable_catalog",
                list(catalog[0].keys()),
                (tuple(r.values()) for r in catalog),
            )

        per_cycle = pdf.groupby("survey_cycle").size().to_dict()
        stats.update(rows_in=len(pdf), rows_out=loaded, rows_per_cycle=per_cycle,
                     missing_optional_fields=[f"{r['survey_cycle']}.{r['field_name']}" for r in catalog
                                              if not r["is_present"]])
