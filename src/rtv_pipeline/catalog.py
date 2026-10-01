"""Reference data: form dictionaries for every cycle and the geography seed."""

from __future__ import annotations

from rtv_pipeline import forms, warehouse
from rtv_pipeline.settings import get_settings
from rtv_pipeline.silver import load_geography


def run() -> None:
    settings = get_settings()
    with warehouse.track_step("catalog") as stats, warehouse.connect() as conn:
        field_rows, choice_rows = [], []
        for cycle in settings.cycles:
            fields = forms.build_form_catalog(cycle)
            for f in fields:
                field_rows.append((
                    cycle.code, f["field_name"], f["occurrence"], f["label"], f["required"],
                    f["constraint"], f["relevance"], f["hint"], len(f["choices"]) or None,
                ))
                if f["occurrence"] == 1:
                    choice_rows.extend(
                        (cycle.code, f["field_name"], i + 1, c["value"], c["label"]) for i, c in enumerate(f["choices"])
                    )
            stats[f"{cycle.code}_form_fields"] = len(fields)

        warehouse.replace_rows(
            conn, "meta.form_fields",
            ["survey_cycle", "field_name", "occurrence", "label", "is_required", "constraint_expr",
             "relevance_expr", "hint", "choice_count"],
            field_rows,
        )
        warehouse.replace_rows(
            conn, "meta.form_choices", ["survey_cycle", "field_name", "sort_order", "choice_value", "choice_label"],
            choice_rows,
        )
        warehouse.replace_rows(
            conn, "meta.ref_survey_cycle", ["cycle_code", "cycle_name", "cycle_order", "source_file", "form_file"],
            ((c.code, c.name, c.order, c.source_file, c.form_file) for c in settings.cycles),
        )
        geo = load_geography()
        warehouse.replace_rows(
            conn, "meta.ref_geography", list(geo[0].keys()), (tuple(g.values()) for g in geo)
        )
        stats["rows_out"] = len(field_rows)
