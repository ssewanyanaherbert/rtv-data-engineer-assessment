import math

import pandas as pd

import metrics


def _fact():
    rows = [
        # cycle, district, found, consented, followup, completed
        ("Baseline", "Kanungu", True, True, True, True),
        ("Baseline", "Kanungu", True, True, False, True),
        ("Year 2", "Kanungu", True, True, True, True),
        ("Year 2", "Kanungu", True, False, False, False),
        ("Year 2", "Kanungu", False, False, False, False),
        ("Year 2", "Kisoro", True, True, True, True),
    ]
    return pd.DataFrame(rows, columns=["cycle_name", "district_name", "is_found", "is_consented",
                                       "is_followup_consented", "is_completed"])


def test_program_rates_use_the_documented_denominators():
    out = metrics.program(_fact(), ["cycle_name"]).set_index("cycle_name")
    year2 = out.loc["Year 2"]
    assert year2["households"] == 4
    assert year2["completion_rate"] == 2 / 4      # completed / attempted
    assert year2["consent_rate"] == 2 / 3         # consented / found
    assert year2["followup_consent_rate"] == 1.0  # follow-up / consented


def test_change_vs_baseline_in_percentage_points_and_missing_baseline():
    rates = metrics.program(_fact(), ["cycle_name", "district_name"])
    out = metrics.change_vs_baseline(rates, keys=["district_name"]).set_index(["cycle_name", "district_name"])
    assert out.loc[("Year 2", "Kanungu"), "completion_change_pp"] == (1 / 3 - 1) * 100
    assert math.isnan(out.loc[("Year 2", "Kisoro"), "completion_change_pp"])


def test_field_operations_rates_are_ratios_of_sums():
    fo = pd.DataFrame({
        "cycle_name": ["A", "A"], "raw_submissions": [10, 30], "dedup_submissions": [9, 28],
        "duplicate_submissions_dropped": [1, 2], "households_with_duplicates": [1, 2],
        "raw_missing_location": [1, 3], "dedup_missing_location": [1, 2], "completed_interviews": [8, 20],
        "timed_interviews": [8, 20], "interview_minutes": [400.0, 800.0],
    })
    out = metrics.field_operations(fo).iloc[0]
    assert out["duplicate_rate"] == 3 / 40
    assert out["missing_location_rate"] == 3 / 37
    assert out["avg_duration_min"] == 1200 / 28


def test_coverage_retention():
    cov = pd.DataFrame({"cycles_observed": [3, 2, 1], "households": [6, 3, 1], "completed_baseline": [6, 3, 1],
                        "completed_baseline_and_year1": [6, 3, 0], "completed_baseline_and_year2": [6, 0, 0]})
    out = metrics.coverage(cov)
    assert out["share_all_cycles"] == 0.6
    assert out["retention_year1"] == 0.9
    assert out["retention_year2"] == 0.6
