import pytest

from rtv_pipeline import bronze
from rtv_pipeline.settings import get_settings

CSV = (
    ',SubmissionDate,KEY,status,Status,"$1.25 / Day\r\n2005 PPP",GPS-Latitude\n'
    '0,"6/30/2020, 6:49:56 PM",uuid:a,1,RTV,0.5,-0.94\n'
    '1,"7/1/2020, 9:00:00 AM",uuid:b,1,RTV,"multi\r\nline",\n'
)


def test_column_names_are_safe_and_unique_ignoring_case():
    names = bronze.bronze_column_names(["", "status", "Status", "GPS-Latitude", "$1.25 / Day", "1st value"])
    assert names == ["unnamed_0", "status", "status_2", "gps_latitude", "c_1_25_day", "c_1st_value"]


def test_header_and_count_handle_quoted_newlines(tmp_path):
    path = tmp_path / "x.csv"
    path.write_text(CSV, encoding="utf-8", newline="")
    header, rows = bronze.read_header_and_count(path)
    assert rows == 2
    assert header[5] == "$1.25 / Day\r\n2005 PPP"


@pytest.fixture()
def baseline_csv(workspace):
    (workspace / "data" / "01_baseline.csv").write_text(CSV, encoding="utf-8", newline="")
    return get_settings().cycle("baseline")


def test_ingest_preserves_rows_columns_and_adds_metadata(spark, baseline_csv):
    manifest = bronze.ingest_cycle(spark, baseline_csv)
    assert manifest["source_rows"] == manifest["bronze_rows"] == 2
    assert [c["source_name"] for c in manifest["columns"]][1:4] == ["SubmissionDate", "KEY", "status"]

    df = spark.read.parquet(str(bronze.latest_partition(baseline_csv)))
    assert set(bronze.META_COLUMNS) <= set(df.columns)
    row = df.orderBy("_record_seq").collect()[1]
    assert row["c_1_25_day_2005_ppp"] == "multi\r\nline"
    assert row["_survey_cycle"] == "baseline"
    assert len(row["_row_hash"]) == 64


def test_reingesting_same_file_is_a_no_op(spark, baseline_csv):
    first = bronze.ingest_cycle(spark, baseline_csv)
    second = bronze.ingest_cycle(spark, baseline_csv)
    assert second["skipped"] is True
    assert second["ingest_id"] == first["ingest_id"]
    partitions = list((get_settings().bronze_dir / "survey_cycle=baseline").glob("ingest_id=*"))
    assert len(partitions) == 1


def test_missing_export_fails_clearly(spark, workspace):
    with pytest.raises(FileNotFoundError, match="01_baseline.csv"):
        bronze.ingest_cycle(spark, get_settings().cycle("baseline"))
