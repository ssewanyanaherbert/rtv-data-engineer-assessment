"""Metric calculations for the dashboard.

Every rate is a ratio of sums (numerator / denominator) at the level being shown, never an average of
pre-computed rates. Denominators follow README section 7.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

FIELD_OPS_SUMS = [
    "raw_submissions", "dedup_submissions", "duplicate_submissions_dropped", "households_with_duplicates",
    "raw_missing_location", "dedup_missing_location", "completed_interviews", "timed_interviews",
    "interview_minutes",
]


def ratio(numerator, denominator):
    numerator = np.asarray(numerator, dtype=float)
    denominator = np.asarray(denominator, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(denominator > 0, numerator / denominator, np.nan)


def field_operations(fo: pd.DataFrame, by: list[str] | None = None) -> pd.DataFrame:
    """Raw vs deduplicated volumes, duplicate rate, missing location and duration."""
    if by:
        out = fo.groupby(by, observed=True)[FIELD_OPS_SUMS].sum().reset_index()
    else:
        out = fo[FIELD_OPS_SUMS].sum().to_frame().T
    out["duplicate_rate"] = ratio(out["duplicate_submissions_dropped"], out["raw_submissions"])
    out["missing_location_rate_raw"] = ratio(out["raw_missing_location"], out["raw_submissions"])
    out["missing_location_rate"] = ratio(out["dedup_missing_location"], out["dedup_submissions"])
    out["avg_duration_min"] = ratio(out["interview_minutes"], out["timed_interviews"])
    return out


def program(fact: pd.DataFrame, by: list[str] | None = None) -> pd.DataFrame:
    """Households attempted, found, consented and completed, with completion and consent rates."""
    flags = fact.assign(households=1)[
        ["households", "is_found", "is_consented", "is_followup_consented", "is_completed"]
        + (by or [])
    ]
    if by:
        out = flags.groupby(by, observed=True).sum(numeric_only=True).reset_index()
    else:
        out = flags.sum(numeric_only=True).to_frame().T
    out = out.rename(columns={
        "is_found": "found", "is_consented": "consented",
        "is_followup_consented": "followup_consented", "is_completed": "completed",
    })
    out["completion_rate"] = ratio(out["completed"], out["households"])
    out["consent_rate"] = ratio(out["consented"], out["found"])
    out["followup_consent_rate"] = ratio(out["followup_consented"], out["consented"])
    return out


def change_vs_baseline(rates: pd.DataFrame, keys: list[str], cycle_col: str = "cycle_name",
                       baseline: str = "Baseline") -> pd.DataFrame:
    """Percentage-point change of completion and consent rate against the Baseline row with the same keys."""
    base = rates.loc[rates[cycle_col] == baseline, keys + ["completion_rate", "consent_rate"]]
    base = base.rename(columns={"completion_rate": "base_completion", "consent_rate": "base_consent"})
    out = rates.merge(base, on=keys, how="left") if keys else rates.assign(
        base_completion=base["base_completion"].iloc[0] if len(base) else np.nan,
        base_consent=base["base_consent"].iloc[0] if len(base) else np.nan,
    )
    out["completion_change_pp"] = (out["completion_rate"] - out["base_completion"]) * 100
    out["consent_change_pp"] = (out["consent_rate"] - out["base_consent"]) * 100
    return out.drop(columns=["base_completion", "base_consent"])


def coverage(cov: pd.DataFrame) -> dict[str, float]:
    """Longitudinal headline numbers from rpt_household_coverage."""
    observed = cov["households"].sum()
    all_cycles = cov.loc[cov["cycles_observed"] == 3, "households"].sum()
    completed_baseline = cov["completed_baseline"].sum()
    return {
        "households_observed": observed,
        "share_all_cycles": float(ratio(all_cycles, observed)),
        "retention_year1": float(ratio(cov["completed_baseline_and_year1"].sum(), completed_baseline)),
        "retention_year2": float(ratio(cov["completed_baseline_and_year2"].sum(), completed_baseline)),
    }
