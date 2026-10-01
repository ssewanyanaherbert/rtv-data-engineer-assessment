-- Grain: one row per household per survey cycle, after keeping the latest submission
-- (see silver dedup_rank). Submissions without a household ID stay as unlinked rows.

DROP TABLE IF EXISTS gold.fct_survey_submissions CASCADE;

CREATE TABLE gold.fct_survey_submissions AS
WITH latest AS (
    SELECT s.*
    FROM silver.survey_submissions s
    WHERE s.is_latest
),
shaped AS (
    SELECT
        l.*,
        coalesce(l.household_key, 'UNLINKED:' || l.submission_key)               AS fact_household_key,
        (l.interview_status_code = 1)                                            AS is_found,
        (l.interview_status_code = 1 AND l.consent_participate = 1)              AS is_consented,
        (l.interview_status_code = 1 AND l.consent_participate = 1
            AND l.end_ts IS NOT NULL)                                            AS is_completed
    FROM latest l
)
SELECT
    row_number() OVER (ORDER BY s.cycle_order, s.fact_household_key)::int   AS submission_sk,
    s.fact_household_key                                                    AS household_key,
    (s.household_key IS NOT NULL)                                           AS is_linkable,
    s.cycle_order::smallint                                                 AS cycle_key,
    coalesce(g.geography_key, -1)                                           AS geography_key,
    to_char(s.submission_date, 'YYYYMMDD')::int                             AS submission_date_key,
    s.submission_key,
    s.submission_ts,
    s.start_ts,
    s.end_ts,
    s.form_version,
    s.surveyor,
    s.subcounty,
    s.parish,
    s.village,
    s.district_source,
    s.district_form_conflict,
    s.interview_status_code,
    CASE s.interview_status_code
        WHEN 1 THEN 'Found'
        WHEN 2 THEN 'Moved away'
        WHEN 3 THEN 'Not found'
        WHEN 4 THEN 'Unavailable'
        WHEN 5 THEN 'Disability'
        ELSE 'Unknown'
    END                                                                      AS interview_status,
    s.track_status,
    s.consent_participate,
    s.consent_followup,
    s.is_found,
    s.is_consented,
    (s.is_consented AND s.consent_followup = 1)                             AS is_followup_consented,
    s.is_completed,
    s.refusal_reason_code_label                                             AS refusal_reason,
    s.respondent_type_code_label                                            AS respondent_type,
    s.respondent_sex_code_label                                             AS respondent_sex,
    coalesce(s.hhh_sex_code_label, 'Unknown')                               AS hhh_sex,
    s.hhh_age,
    CASE
        WHEN s.hhh_age IS NULL THEN 'Unknown'
        WHEN s.hhh_age < 25 THEN 'Under 25'
        WHEN s.hhh_age < 35 THEN '25-34'
        WHEN s.hhh_age < 45 THEN '35-44'
        WHEN s.hhh_age < 55 THEN '45-54'
        WHEN s.hhh_age < 65 THEN '55-64'
        ELSE '65+'
    END                                                                      AS hhh_age_band,
    CASE
        WHEN s.hhh_age IS NULL THEN 99
        WHEN s.hhh_age < 25 THEN 1
        WHEN s.hhh_age < 35 THEN 2
        WHEN s.hhh_age < 45 THEN 3
        WHEN s.hhh_age < 55 THEN 4
        WHEN s.hhh_age < 65 THEN 5
        ELSE 6
    END::smallint                                                            AS hhh_age_band_order,
    s.hhh_marital_status_code_label                                         AS hhh_marital_status,
    s.hhh_education_code_label                                              AS hhh_education,
    s.hh_size,
    s.has_valid_gps                                                         AS has_valid_location,
    s.gps_latitude,
    s.gps_longitude,
    s.duration_min,
    (s.is_completed AND s.duration_min > 0
        AND s.duration_min <= {{ max_plausible_duration_min }})                AS is_duration_valid,
    s.income_consumption_usd_day_aeq,
    s.household_submission_count::smallint                                  AS submissions_received,
    (s.household_submission_count - 1)::smallint                            AS duplicate_submissions_dropped
FROM shaped s
LEFT JOIN gold.dim_geography g ON g.district_name = s.district_name;

ALTER TABLE gold.fct_survey_submissions ADD PRIMARY KEY (submission_sk);
CREATE UNIQUE INDEX ux_fct_household_cycle ON gold.fct_survey_submissions (household_key, cycle_key);
CREATE INDEX ix_fct_cycle ON gold.fct_survey_submissions (cycle_key);
CREATE INDEX ix_fct_geography ON gold.fct_survey_submissions (geography_key);
CREATE INDEX ix_fct_date ON gold.fct_survey_submissions (submission_date_key);
