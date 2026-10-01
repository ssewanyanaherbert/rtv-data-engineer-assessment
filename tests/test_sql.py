import pytest

from rtv_pipeline import sqlrunner
from rtv_pipeline.settings import SQL_DIR


def test_render_substitutes_rules_and_quotes_strings():
    sql = sqlrunner.render("x <= {{ max_plausible_duration_min }} AND id ~ {{ household_id_pattern }}")
    assert sql.startswith("x <= 240 AND id ~ '^[A-Z]{3}")


def test_render_rejects_unknown_variables():
    with pytest.raises(KeyError):
        sqlrunner.render("{{ nope }}")


@pytest.mark.parametrize("path", sorted((SQL_DIR / "tests").glob("*.sql")), ids=lambda p: p.stem)
def test_every_data_test_declares_severity_layer_and_description(path):
    meta = sqlrunner.header(path)
    assert meta.get("severity") in {"error", "warn"}
    assert meta.get("layer") in {"bronze", "silver", "gold"}
    assert meta.get("description")
    sqlrunner.read(path)
