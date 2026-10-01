-- Program reach and demographics. Grain: cycle x district x month x head-of-household sex x age band.
-- All measures are counts/sums so rates can be recomputed at any level (numerator / denominator).

DROP TABLE IF EXISTS gold.rpt_program_summary CASCADE;

CREATE TABLE gold.rpt_program_summary AS
SELECT
    f.cycle_key,
    f.geography_key,
    t.month_start,
    t.year_month,
    f.hhh_sex,
    f.hhh_age_band,
    f.hhh_age_band_order,
    count(*)                                                       AS households,
    count(*) FILTER (WHERE f.is_found)                             AS households_found,
    count(*) FILTER (WHERE f.is_consented)                         AS households_consented,
    count(*) FILTER (WHERE f.is_followup_consented)                AS households_followup_consented,
    count(*) FILTER (WHERE f.is_completed)                         AS households_completed,
    coalesce(sum(f.hh_size) FILTER (WHERE f.is_completed), 0)      AS completed_members_total,
    count(f.hh_size) FILTER (WHERE f.is_completed)                 AS completed_with_size,
    coalesce(sum(f.hhh_age) FILTER (WHERE f.is_completed), 0)      AS completed_head_age_total,
    count(f.hhh_age) FILTER (WHERE f.is_completed)                 AS completed_with_age
FROM gold.fct_survey_submissions f
LEFT JOIN gold.dim_time t ON t.date_key = f.submission_date_key
GROUP BY f.cycle_key, f.geography_key, t.month_start, t.year_month, f.hhh_sex, f.hhh_age_band, f.hhh_age_band_order;

CREATE INDEX ix_rpt_program_cycle ON gold.rpt_program_summary (cycle_key, geography_key);
