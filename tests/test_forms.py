from rtv_pipeline.forms import choice_lists, parse_form

from conftest import FIXTURES


def _fields():
    return parse_form((FIXTURES / "mini_form.html").read_text(encoding="utf-8"))


def test_parses_names_labels_and_required_flag():
    fields = _fields()
    assert [f["field_name"] for f in fields] == ["district", "hhh_age", "member_age", "member_age"]
    district = fields[0]
    assert district["label"] == "Select which district you are in"
    assert district["required"] is True
    assert fields[2]["required"] is False


def test_constraint_and_relevance_are_split_from_label():
    age = _fields()[1]
    assert age["label"] == "Household head Age"
    assert age["constraint"] == "Response constrained to: .>=13 and .<=120"
    assert _fields()[2]["relevance"].startswith("Question relevant when")


def test_choice_lists_and_repeat_occurrences():
    fields = _fields()
    assert choice_lists(fields) == {"district": {"1": "Kisoro", "2": "Kanungu"}}
    assert [f["occurrence"] for f in fields if f["field_name"] == "member_age"] == [1, 2]
