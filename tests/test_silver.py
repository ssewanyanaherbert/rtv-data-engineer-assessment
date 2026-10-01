from datetime import datetime

from pyspark.sql import functions as F

from rtv_pipeline import silver

COLUMNS = [
    {"source_name": "status", "bronze_name": "status"},
    {"source_name": "Status", "bronze_name": "status_2"},
    {"source_name": "GPS-Latitude", "bronze_name": "gps_latitude"},
]


def test_resolve_prefers_exact_then_case_then_punctuation():
    assert silver.resolve_source(["status"], COLUMNS)["bronze_name"] == "status"
    assert silver.resolve_source(["Status"], COLUMNS)["bronze_name"] == "status_2"
    assert silver.resolve_source(["gps_latitude"], COLUMNS)["bronze_name"] == "gps_latitude"
    assert silver.resolve_source(["missing"], COLUMNS) is None


def test_candidates_can_differ_per_cycle():
    spec = {"sources": {"baseline": ["a"], "year2": ["b"]}}
    assert silver.candidates_for(spec, "year2") == ["b"]
    assert silver.candidates_for(spec, "year1") == []


def test_parses_surveycto_timestamp_formats(spark):
    df = spark.createDataFrame(
        [("6/30/2020, 6:49:56 PM",), ("Jun 30, 2021 6:49:56 PM",), ("2022-06-30 18:49:56",), ("garbage",)], ["raw"]
    )
    parsed = [r[0] for r in df.select(silver.parse_timestamp(F.col("raw"))).collect()]
    assert parsed[:3] == [datetime(2020, 6, 30, 18, 49, 56), datetime(2021, 6, 30, 18, 49, 56),
                          datetime(2022, 6, 30, 18, 49, 56)]
    assert parsed[3] is None


def test_int_cast_tolerates_decimal_text_and_blanks(spark):
    df = spark.createDataFrame([("1",), ("2.0",), ("",), (None,)], ["raw"])
    assert [r[0] for r in df.select(silver.cast_typed(F.col("raw"), "int")).collect()] == [1, 2, None, None]


def test_dedup_keeps_latest_with_deterministic_tie_breaks(spark):
    ts = datetime(2021, 7, 1, 10, 0, 0)
    later = datetime(2021, 7, 2, 10, 0, 0)
    rows = [
        ("year1", "HH1", "k1", ts, ts),
        ("year1", "HH1", "k2", later, ts),       # latest submission wins
        ("year1", "HH2", "k3", ts, ts),
        ("year1", "HH2", "k4", ts, ts),          # full tie -> highest KEY wins
        ("year1", None, "k5", ts, ts),           # no household id -> kept on its own
        ("year1", None, "k6", ts, ts),
        ("year2", "HH1", "k7", ts, ts),          # other cycle is ranked separately
    ]
    df = spark.createDataFrame(rows, "survey_cycle string, household_key string, submission_key string, "
                                     "submission_ts timestamp_ntz, end_ts timestamp_ntz")
    out = {r["submission_key"]: r for r in silver.deduplicate(df).collect()}
    latest = sorted(k for k, r in out.items() if r["is_latest"])
    assert latest == ["k2", "k4", "k5", "k6", "k7"]
    assert out["k1"]["household_submission_count"] == 2
    assert out["k5"]["household_submission_count"] == 1


def test_district_precedence_and_gps_validation(spark, workspace):
    rows = [
        # preload wins over a conflicting form code
        ("KAN-AAA-BBB-K1", "KAN-AAA-BBB-K1", "Kanungu", "Kagadi", -0.9, 29.7),
        # no preload -> household id prefix
        ("MIT-AAA-BBB-M1", "MIT-AAA-BBB-M2", None, None, 0.0, 0.0),
        # nothing but the form choice
        (" xyz ", None, None, "Kisoro", 10.0, 29.7),
    ]
    df = spark.createDataFrame(
        rows, "household_id string, household_id_confirm string, district_preload string, "
              "district_code_label string, gps_latitude double, gps_longitude double"
    ).withColumn("submission_ts", F.lit(None).cast("timestamp_ntz")).withColumn("duration_sec", F.lit(600.0))
    out = silver.add_derived(df).collect()

    assert [r["district_name"] for r in out] == ["Kanungu", "Mitooma", "Kisoro"]
    assert [r["district_source"] for r in out] == ["preload", "household_id_prefix", "form_choice"]
    assert [r["district_form_conflict"] for r in out] == [True, False, False]
    assert [r["household_id_confirmed"] for r in out] == [True, False, None]
    assert [r["has_valid_gps"] for r in out] == [True, False, False]
    assert out[2]["household_key"] == "XYZ"
