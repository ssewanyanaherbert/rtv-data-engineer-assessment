-- Calendar for every day between the first and last submission (whole months, so MoM visuals are clean).

DROP TABLE IF EXISTS gold.dim_time CASCADE;

CREATE TABLE gold.dim_time AS
WITH bounds AS (
    SELECT
        date_trunc('month', min(submission_date))::date AS first_day,
        (date_trunc('month', max(submission_date)) + interval '1 month - 1 day')::date AS last_day
    FROM silver.survey_submissions
)
SELECT
    to_char(d, 'YYYYMMDD')::int          AS date_key,
    d::date                              AS calendar_date,
    extract(year FROM d)::smallint       AS year,
    extract(quarter FROM d)::smallint    AS quarter,
    'Q' || extract(quarter FROM d)       AS quarter_name,
    extract(month FROM d)::smallint      AS month,
    to_char(d, 'Mon')                    AS month_short_name,
    to_char(d, 'FMMonth')                AS month_name,
    date_trunc('month', d)::date         AS month_start,
    to_char(d, 'YYYY-MM')                AS year_month,
    extract(isoyear FROM d)::smallint    AS iso_year,
    extract(week FROM d)::smallint       AS iso_week,
    date_trunc('week', d)::date          AS week_start,
    extract(day FROM d)::smallint        AS day_of_month,
    extract(isodow FROM d)::smallint     AS day_of_week,
    to_char(d, 'FMDay')                  AS day_name,
    extract(isodow FROM d) IN (6, 7)     AS is_weekend
FROM bounds, generate_series(bounds.first_day, bounds.last_day, interval '1 day') AS g(d);

ALTER TABLE gold.dim_time ADD PRIMARY KEY (date_key);
