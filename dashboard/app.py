"""RTV household survey dashboard (Streamlit). Reads the Gold layer only."""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

import data
import metrics

st.set_page_config(page_title="RTV Household Survey", layout="wide")

PALETTE = ["#1F6F8B", "#E07A5F", "#81B29A", "#F2CC8F", "#3D405B", "#9C6644", "#6D597A"]
SEX_COLORS = {"Male": "#1F6F8B", "Female": "#E07A5F", "Unknown": "#B0B0B0"}


@st.cache_data(ttl=300, show_spinner="Loading warehouse data...")
def load() -> dict[str, pd.DataFrame]:
    return data.load_all()


def pct(value) -> str:
    return "n/a" if value is None or (isinstance(value, float) and math.isnan(value)) else f"{value:.1%}"


def num(value, digits: int = 0) -> str:
    return "n/a" if value is None or (isinstance(value, float) and math.isnan(value)) else f"{value:,.{digits}f}"


def chart(fig, height: int = 360) -> None:
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=30, b=10), legend_title_text="")
    st.plotly_chart(fig, width="stretch")


def heading(title: str, subtitle: str) -> None:
    st.markdown(f"**{title}**")
    st.caption(subtitle)


try:
    frames = load()
except Exception as exc:  # no warehouse yet, wrong credentials, pipeline not run
    st.error(f"Could not read the warehouse: {exc}")
    st.info("Start the stack and run the pipeline first (see README, Quick start).")
    st.stop()

cycles = frames["cycles"]
cycle_names = cycles["cycle_name"].tolist()
cycle_colors = dict(zip(cycle_names, PALETTE))
baseline_name = cycles.loc[cycles["cycle_order"].idxmin(), "cycle_name"]
last_name = cycles.loc[cycles["cycle_order"].idxmax(), "cycle_name"]
order = {"cycle_name": cycle_names}

# ------------------------------------------------------------------ filters
st.sidebar.header("Filters")
chosen_cycles = st.sidebar.multiselect("Survey cycle", cycle_names, default=cycle_names)
districts = sorted(d for d in frames["fact"]["district_name"].unique() if d != "Unknown")
if (frames["fact"]["district_name"] == "Unknown").any():
    districts.append("Unknown")
chosen_districts = st.sidebar.multiselect("District", districts, default=districts)
st.sidebar.caption("Longitudinal metrics compare cycles, so they follow the district filter only.")
if st.sidebar.button("Reload data"):
    load.clear()
    st.rerun()

fact_all = frames["fact"][frames["fact"]["district_name"].isin(chosen_districts)]
fact = fact_all[fact_all["cycle_name"].isin(chosen_cycles)]
fo = frames["field_ops"][
    frames["field_ops"]["cycle_name"].isin(chosen_cycles) & frames["field_ops"]["district_name"].isin(chosen_districts)
]
cov = frames["coverage"][frames["coverage"]["district_name"].isin(chosen_districts)]

st.title("RTV Household Survey")
st.caption("Baseline, Year 1 and Year 2 cohort - field operations, program reach and longitudinal follow-up")

if fact.empty:
    st.info("No households match the current filters.")
    st.stop()

tab_ops, tab_program, tab_long, tab_health, tab_defs = st.tabs(
    ["Field Operations", "Program Summary", "Longitudinal", "Pipeline Health", "Metric Definitions"]
)

# ------------------------------------------------------------------ field operations
with tab_ops:
    total = metrics.field_operations(fo).iloc[0]
    k = st.columns(5)
    k[0].metric("Raw submissions", num(total["raw_submissions"]), help="Every submission received")
    k[1].metric("Deduplicated submissions", num(total["dedup_submissions"]),
                help="Latest submission per household per cycle")
    k[2].metric("Duplicate rate", pct(total["duplicate_rate"]), help="Duplicates dropped / raw submissions")
    k[3].metric("Missing location rate", pct(total["missing_location_rate"]),
                help="Deduplicated households without a valid GPS fix")
    k[4].metric("Avg interview duration", f"{num(total['avg_duration_min'], 1)} min",
                help="Completed interviews, excluding durations of 0 or above 240 minutes")

    left, right = st.columns([3, 2])
    with left:
        heading("Submission volume over time", "Raw submissions received per week, one panel per survey cycle")
        weekly = fo.groupby(["cycle_name", "week_start"], as_index=False)["raw_submissions"].sum()
        fig = px.bar(weekly, x="week_start", y="raw_submissions", color="cycle_name", facet_col="cycle_name",
                     category_orders=order, color_discrete_map=cycle_colors,
                     labels={"week_start": "Week", "raw_submissions": "Submissions"})
        fig.update_xaxes(matches=None, title_text="")
        fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
        fig.update_layout(showlegend=False)
        chart(fig)
    with right:
        heading("Interview status mix", "Share of deduplicated households by field outcome")
        status = fo.groupby(["cycle_name", "interview_status"], as_index=False)["dedup_submissions"].sum()
        totals = status.groupby("cycle_name")["dedup_submissions"].transform("sum")
        status["share"] = status["dedup_submissions"] / totals
        fig = px.bar(status, y="cycle_name", x="share", color="interview_status", orientation="h",
                     category_orders=order, color_discrete_sequence=PALETTE,
                     labels={"cycle_name": "", "share": "Share of households", "interview_status": "Status"})
        fig.update_xaxes(tickformat=".0%")
        chart(fig)

    by_cycle = metrics.field_operations(fo, ["cycle_order", "cycle_name"]).sort_values("cycle_order")
    left, right = st.columns(2)
    with left:
        heading("Deduplication impact", "Raw vs deduplicated submissions per cycle")
        long = by_cycle.melt(id_vars="cycle_name", value_vars=["raw_submissions", "dedup_submissions"])
        long["variable"] = long["variable"].map({"raw_submissions": "Raw", "dedup_submissions": "Deduplicated"})
        fig = px.bar(long, x="cycle_name", y="value", color="variable", barmode="group", text_auto=",",
                     category_orders=order, color_discrete_sequence=["#B0B0B0", "#1F6F8B"],
                     labels={"cycle_name": "", "value": "Submissions"})
        chart(fig, 300)
        st.dataframe(
            by_cycle[["cycle_name", "raw_submissions", "dedup_submissions", "duplicate_submissions_dropped",
                      "households_with_duplicates", "duplicate_rate"]],
            hide_index=True, width="stretch",
            column_config={
                "cycle_name": "Cycle", "raw_submissions": "Raw", "dedup_submissions": "Deduplicated",
                "duplicate_submissions_dropped": "Dropped", "households_with_duplicates": "Households with duplicates",
                "duplicate_rate": st.column_config.NumberColumn("Duplicate rate", format="percent"),
            },
        )
    with right:
        heading("Missing location rate", "Submissions without a valid GPS fix in Uganda, before and after dedup")
        long = by_cycle.melt(id_vars="cycle_name", value_vars=["missing_location_rate_raw", "missing_location_rate"])
        long["variable"] = long["variable"].map(
            {"missing_location_rate_raw": "Raw", "missing_location_rate": "Deduplicated"})
        fig = px.bar(long, x="cycle_name", y="value", color="variable", barmode="group", text_auto=".1%",
                     category_orders=order, color_discrete_sequence=["#B0B0B0", "#E07A5F"],
                     labels={"cycle_name": "", "value": "Missing location rate"})
        fig.update_yaxes(tickformat=".0%")
        chart(fig, 300)
        heading("Average interview duration", "Minutes per completed interview")
        fig = px.bar(by_cycle, x="cycle_name", y="avg_duration_min", color="cycle_name", text_auto=".1f",
                     category_orders=order, color_discrete_map=cycle_colors,
                     labels={"cycle_name": "", "avg_duration_min": "Minutes"})
        fig.update_layout(showlegend=False)
        chart(fig, 240)

# ------------------------------------------------------------------ program summary
with tab_program:
    total = metrics.program(fact).iloc[0]
    completed = fact[fact["is_completed"]]
    k = st.columns(5)
    k[0].metric("Households attempted", num(total["households"]), help="Deduplicated households")
    k[1].metric("Completion rate", pct(total["completion_rate"]), help="Completed / households attempted")
    k[2].metric("Consent rate", pct(total["consent_rate"]), help="Consented / households found")
    k[3].metric("Follow-up consent rate", pct(total["followup_consent_rate"]),
                help="Agreed to be re-contacted / consented")
    k[4].metric("Avg household size", num(completed["hh_size"].mean(), 1), help="Completed interviews")

    by_district = metrics.program(fact, ["cycle_order", "cycle_name", "district_name"]).sort_values("cycle_order")
    left, right = st.columns(2)
    with left:
        heading("Completion rate by district", "Completed / households attempted, per cycle")
        fig = px.bar(by_district, x="district_name", y="completion_rate", color="cycle_name", barmode="group",
                     category_orders=order, color_discrete_map=cycle_colors,
                     labels={"district_name": "", "completion_rate": "Completion rate"})
        fig.update_yaxes(tickformat=".0%", range=[0, 1.05])
        chart(fig)
    with right:
        heading("Consent rate by district", "Consented / households found, per cycle")
        fig = px.bar(by_district, x="district_name", y="consent_rate", color="cycle_name", barmode="group",
                     category_orders=order, color_discrete_map=cycle_colors,
                     labels={"district_name": "", "consent_rate": "Consent rate"})
        fig.update_yaxes(tickformat=".0%", range=[0, 1.05])
        chart(fig)

    heading("Completions by district and month", "Completed interviews per district for each month of fieldwork")
    monthly = (completed.groupby(["district_name", "year_month"]).size().unstack(fill_value=0)
               .reindex(columns=sorted(completed["year_month"].dropna().unique())))
    if not monthly.empty:
        fig = px.imshow(monthly, text_auto=True, aspect="auto", color_continuous_scale="Blues",
                        labels={"x": "Month", "y": "", "color": "Completed"})
        fig.update_xaxes(type="category")
        chart(fig, 80 + 45 * len(monthly))

    left, right = st.columns([3, 2])
    with left:
        heading("Who was interviewed", "Households by head-of-household age band and sex")
        group = st.radio("Households", ["Completed", "Consented"], horizontal=True, label_visibility="collapsed")
        subset = fact[fact["is_completed"] if group == "Completed" else fact["is_consented"]]
        demo = (subset.groupby(["cycle_name", "hhh_age_band", "hhh_age_band_order", "hhh_sex"], as_index=False)
                .size().sort_values("hhh_age_band_order"))
        fig = px.bar(demo, x="hhh_age_band", y="size", color="hhh_sex", facet_col="cycle_name", barmode="group",
                     category_orders=order, color_discrete_map=SEX_COLORS,
                     labels={"hhh_age_band": "Head age band", "size": "Households", "hhh_sex": "Head sex"})
        fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
        chart(fig)
    with right:
        heading("Demographic profile", f"{group} households, per cycle")
        profile = subset.groupby(["cycle_order", "cycle_name"]).agg(
            households=("household_key", "size"),
            female_headed=("hhh_sex", lambda s: (s == "Female").mean()),
            avg_head_age=("hhh_age", "mean"),
            avg_household_size=("hh_size", "mean"),
        ).reset_index().sort_values("cycle_order").drop(columns="cycle_order")
        st.dataframe(profile, hide_index=True, width="stretch", column_config={
            "cycle_name": "Cycle", "households": "Households",
            "female_headed": st.column_config.NumberColumn("Female-headed", format="percent"),
            "avg_head_age": st.column_config.NumberColumn("Avg head age", format="%.1f"),
            "avg_household_size": st.column_config.NumberColumn("Avg household size", format="%.1f"),
        })

    heading("Cycle comparison", "Rates per cycle and change against Baseline in percentage points")
    compare = metrics.change_vs_baseline(
        metrics.program(fact_all, ["cycle_order", "cycle_name"]), keys=[], baseline=baseline_name
    ).sort_values("cycle_order")
    compare = compare[compare["cycle_name"].isin(chosen_cycles)]
    st.dataframe(
        compare[["cycle_name", "households", "found", "consented", "completed", "completion_rate",
                 "completion_change_pp", "consent_rate", "consent_change_pp", "followup_consent_rate"]],
        hide_index=True, width="stretch",
        column_config={
            "cycle_name": "Cycle", "households": "Households", "found": "Found", "consented": "Consented",
            "completed": "Completed",
            "completion_rate": st.column_config.NumberColumn("Completion rate", format="percent"),
            "completion_change_pp": st.column_config.NumberColumn("Completion vs Baseline (pp)", format="%+.1f"),
            "consent_rate": st.column_config.NumberColumn("Consent rate", format="percent"),
            "consent_change_pp": st.column_config.NumberColumn("Consent vs Baseline (pp)", format="%+.1f"),
            "followup_consent_rate": st.column_config.NumberColumn("Follow-up consent", format="percent"),
        },
    )
    st.caption("The Baseline export only contains found and consented households, so Baseline rates are 100% "
               "by construction. Year 1 vs Year 2 is the like-for-like comparison.")

# ------------------------------------------------------------------ longitudinal
with tab_long:
    head = metrics.coverage(cov)
    k = st.columns(4)
    k[0].metric("Households observed", num(head["households_observed"]), help="Linked households, any cycle")
    k[1].metric("Seen in all three cycles", pct(head["share_all_cycles"]))
    k[2].metric("Baseline to Year 1 retention", pct(head["retention_year1"]),
                help="Completed at Baseline and completed again in Year 1")
    k[3].metric("Baseline to Year 2 retention", pct(head["retention_year2"]),
                help="Completed at Baseline and completed again in Year 2")

    left, right = st.columns(2)
    with left:
        heading("Households by cycle pattern", "Which combination of cycles each household appears in")
        patterns = (cov.groupby(["cycle_pattern", "cycles_observed"], as_index=False)["households"].sum()
                    .sort_values(["cycles_observed", "households"]))
        fig = px.bar(patterns, y="cycle_pattern", x="households", orientation="h", text_auto=",",
                     color="cycles_observed", color_continuous_scale=["#F2CC8F", "#1F6F8B"],
                     labels={"cycle_pattern": "", "households": "Households", "cycles_observed": "Cycles"})
        fig.update_coloraxes(showscale=False)
        chart(fig)
    with right:
        heading(f"Change from {baseline_name} to {last_name}",
                "Percentage-point change in completion and consent rate by district")
        change = metrics.change_vs_baseline(
            metrics.program(fact_all, ["cycle_name", "district_name"]), keys=["district_name"],
            baseline=baseline_name,
        )
        change = change[change["cycle_name"] == last_name].melt(
            id_vars="district_name", value_vars=["completion_change_pp", "consent_change_pp"])
        change["variable"] = change["variable"].map(
            {"completion_change_pp": "Completion rate", "consent_change_pp": "Consent rate"})
        fig = px.bar(change.dropna(), x="district_name", y="value", color="variable", barmode="group",
                     text_auto=".1f", color_discrete_sequence=["#1F6F8B", "#E07A5F"],
                     labels={"district_name": "", "value": "Change (pp)"})
        chart(fig)
        st.caption("Districts without a Baseline survey have no comparison and are not shown.")

    heading("Rates across survey cycles", "Completion, consent and follow-up consent rate per cycle")
    trend = metrics.program(fact_all, ["cycle_order", "cycle_name"]).sort_values("cycle_order").melt(
        id_vars="cycle_name", value_vars=["completion_rate", "consent_rate", "followup_consent_rate"])
    trend["variable"] = trend["variable"].map({"completion_rate": "Completion", "consent_rate": "Consent",
                                               "followup_consent_rate": "Follow-up consent"})
    fig = px.line(trend, x="cycle_name", y="value", color="variable", markers=True, category_orders=order,
                  color_discrete_sequence=PALETTE, labels={"cycle_name": "", "value": "Rate"})
    fig.update_yaxes(tickformat=".0%")
    chart(fig, 320)

# ------------------------------------------------------------------ pipeline health
with tab_health:
    heading("Row counts by layer", "Source file -> Bronze -> deduplicated Gold fact, per cycle")
    raw = frames["field_ops"].groupby("cycle_name", as_index=False)["raw_submissions"].sum()
    gold = frames["fact"].groupby("cycle_name", as_index=False).size().rename(columns={"size": "gold_households"})
    counts = (cycles[["cycle_order", "cycle_name"]]
              .merge(frames["ingest"].rename(columns={"survey_cycle": "cycle_code"})
                     .merge(cycles[["cycle_code", "cycle_name"]], on="cycle_code"), on="cycle_name", how="left")
              .merge(raw, on="cycle_name", how="left").merge(gold, on="cycle_name", how="left")
              .sort_values("cycle_order"))
    st.dataframe(
        counts[["cycle_name", "source_file", "source_columns", "source_rows", "bronze_rows", "raw_submissions",
                "gold_households", "ingested_at"]],
        hide_index=True, width="stretch",
        column_config={"cycle_name": "Cycle", "source_file": "File", "source_columns": "Columns",
                       "source_rows": "Source rows", "bronze_rows": "Bronze rows",
                       "raw_submissions": "Silver rows", "gold_households": "Gold households",
                       "ingested_at": "Ingested (UTC)"},
    )

    tests = frames["tests"]
    left, right = st.columns([3, 2])
    with left:
        passed = (tests["status"] == "pass").sum()
        blocking = ((tests["severity"] == "error") & (tests["status"] != "pass")).sum()
        heading("Data tests (latest run)", f"{passed} of {len(tests)} passed, {blocking} blocking failures")
        st.dataframe(tests, hide_index=True, width="stretch", column_config={
            "test_name": "Test", "layer": "Layer", "severity": "Severity", "status": "Status",
            "failing_rows": "Failing rows", "description": "What it checks"})
    with right:
        heading("Pipeline steps (latest run)", "Status, duration and rows per step")
        st.dataframe(frames["runs"], hide_index=True, width="stretch", column_config={
            "step": "Step", "cycle": "Cycle", "status": "Status", "started_at": None,
            "duration_sec": st.column_config.NumberColumn("Seconds", format="%.1f"),
            "rows_in": "Rows in", "rows_out": "Rows out", "error": "Error"})

# ------------------------------------------------------------------ definitions
with tab_defs:
    st.markdown((Path(__file__).parent / "metric_definitions.md").read_text(encoding="utf-8"))
