"""Parse SurveyCTO printable form exports (HTML) into a field dictionary.

Each question is an ``tr.entryRow`` with the field name in ``td.fieldCell``, the label in
``td.questionCell`` (constraint/relevance/hint as nested ``<p>``) and, for select questions,
a nested table of ``value | label`` choices.
"""

from __future__ import annotations

import json
from pathlib import Path

from bs4 import BeautifulSoup

from rtv_pipeline.settings import Cycle, get_settings


def _text(node) -> str | None:
    if node is None:
        return None
    text = " ".join(node.get_text(" ", strip=True).split())
    return text or None


def parse_form(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    fields: list[dict] = []
    seen: dict[str, int] = {}

    for row in soup.select("tr.entryRow"):
        name_cell = row.select_one("td.fieldCell")
        question = row.select_one("td.questionCell")
        if name_cell is None or question is None:
            continue

        required = name_cell.select_one(".required") is not None
        for tag in name_cell.select(".required"):
            tag.decompose()
        name = _text(name_cell)
        if not name:
            continue

        extras = {}
        for cls in ("constraint", "relevance", "hint"):
            node = question.select_one(f"p.{cls}")
            extras[cls] = _text(node)
            if node is not None:
                node.decompose()

        choices = []
        answer_cell = question.find_next_sibling("td")
        if answer_cell is not None:
            for choice_row in answer_cell.select("tr"):
                cells = choice_row.find_all("td", recursive=False)
                if len(cells) == 3:
                    choices.append({"value": _text(cells[1]), "label": _text(cells[2])})

        seen[name] = seen.get(name, 0) + 1
        fields.append(
            {
                "field_name": name,
                "occurrence": seen[name],
                "label": _text(question),
                "required": required,
                "constraint": extras["constraint"],
                "relevance": extras["relevance"],
                "hint": extras["hint"],
                "choices": choices,
            }
        )
    return fields


def choice_lists(fields: list[dict]) -> dict[str, dict[str, str]]:
    """Field name -> {code: label}, taken from the first occurrence that has choices."""
    lists: dict[str, dict[str, str]] = {}
    for f in fields:
        if f["choices"] and f["field_name"] not in lists:
            lists[f["field_name"]] = {c["value"]: c["label"] for c in f["choices"] if c["value"] is not None}
    return lists


def catalog_path(cycle: Cycle) -> Path:
    return get_settings().catalog_dir / f"form_{cycle.code}.json"


def build_form_catalog(cycle: Cycle) -> list[dict]:
    """Parse the cycle's form and persist it next to the lake so Spark jobs can read it."""
    settings = get_settings()
    source = settings.data_dir / cycle.form_file
    fields = parse_form(source.read_text(encoding="utf-8"))
    out = catalog_path(cycle)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(fields, ensure_ascii=False), encoding="utf-8")
    return fields


def load_form_catalog(cycle: Cycle) -> list[dict]:
    path = catalog_path(cycle)
    if not path.exists():
        return build_form_catalog(cycle)
    return json.loads(path.read_text(encoding="utf-8"))
