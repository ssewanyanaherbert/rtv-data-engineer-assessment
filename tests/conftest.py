import os
import shutil
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    from rtv_pipeline.spark import get_spark

    os.environ.setdefault("SPARK_MASTER", "local[1]")
    session = get_spark("rtv-pipeline-tests")
    yield session
    session.stop()


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    """Isolated data/lake/reports folders with the settings cache reset."""
    from rtv_pipeline import settings

    for name in ("data", "lake", "reports"):
        (tmp_path / name).mkdir()
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("LAKE_DIR", str(tmp_path / "lake"))
    monkeypatch.setenv("REPORTS_DIR", str(tmp_path / "reports"))
    settings.get_settings.cache_clear()
    shutil.copy(FIXTURES / "mini_form.html", tmp_path / "data" / "ahs_2021_baseline.html")
    yield tmp_path
    settings.get_settings.cache_clear()
