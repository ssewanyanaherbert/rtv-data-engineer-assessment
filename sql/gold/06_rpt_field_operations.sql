-- Field operations, additive so any slice can be re-aggregated in the BI layer.
-- Grain: cycle x submission date x district x interview status.
-- Raw counts use every submission in Silver; dedup counts use only the latest per household.

DROP TABLE IF EXISTS gold.rpt_field_operations CASCADE;

CREATE TABLE gold.rpt_field_operations AS
WITH subs AS (
    SELECT
        s.cycle_order::smallint                                   AS cycle_key,
        to_char(s.submission_date, 'YYYYMMDD')::int               AS submission_date_key,
        coalesce(g.geography_key, -1)                             AS geography_key,
        CASE s.interview_status_code
            WHEN 1 THEN 'Found' WHEN 2 THEN 'Moved away' WHEN 3 THEN 'Not found'
            WHEN 4 THEN 'Unavailable' WHEN 5 THEN 'Disability' ELSE 'Unknown'
        END                                                       AS interview_status,
        s.is_latest,
        s.has_valid_gps,
        s.household_submission_count,
        (s.interview_status_code = 1 AND s.consent_participate = 1 AND s.end_ts IS NOT NULL) AS is_completed,
        s.duration_min
    FROM silver.survey_submissions s
    LEFT JOIN gold.dim_geography g ON g.district_name = s.district_name
)
SELECT
    cycle_key,
    submission_date_key,
    geography_key,
    interview_status,
    count(*)                                                                    AS raw_submissions,
    count(*) FILTER (WHERE is_latest)                                           AS dedup_submissions,
    count(*) FILTER (WHERE NOT is_latest)                                       AS duplicate_submissions_dropped,
    count(*) FILTER (WHERE is_latest AND household_submission_count > 1)        AS households_with_duplicates,
    count(*) FILTER (WHERE NOT has_valid_gps)                                   AS raw_missing_location,
    count(*) FILTER (WHERE is_latest AND NOT has_valid_gps)                     AS dedup_missing_location,
    count(*) FILTER (WHERE is_latest AND is_completed)                          AS completed_interviews,
    count(*) FILTER (WHERE is_latest AND is_completed
                     AND duration_min > 0 AND duration_min <= {{ max_plausible_duration_min }}) AS timed_interviews,
    coalesce(sum(duration_min) FILTER (WHERE is_latest AND is_completed
                     AND duration_min > 0 AND duration_min <= {{ max_plausible_duration_min }}), 0) AS interview_minutes
FROM subs
GROUP BY cycle_key, submission_date_key, geography_key, interview_status;

CREATE INDEX ix_rpt_fieldops_cycle_date ON gold.rpt_field_operations (cycle_key, submission_date_key);
