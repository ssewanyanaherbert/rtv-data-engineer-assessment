-- Longitudinal coverage: how many households were observed in 1, 2 or 3 cycles, and in which combination.

DROP TABLE IF EXISTS gold.rpt_household_coverage CASCADE;

CREATE TABLE gold.rpt_household_coverage AS
SELECT
    h.home_geography_key                                        AS geography_key,
    h.cycle_pattern,
    h.cycles_observed,
    count(*)                                                    AS households,
    count(*) FILTER (WHERE h.cycles_completed = h.cycles_observed) AS households_completed_every_visit,
    count(*) FILTER (WHERE h.completed_baseline)                AS completed_baseline,
    count(*) FILTER (WHERE h.completed_baseline AND h.completed_year1) AS completed_baseline_and_year1,
    count(*) FILTER (WHERE h.completed_baseline AND h.completed_year2) AS completed_baseline_and_year2
FROM gold.dim_household h
WHERE h.is_linkable
GROUP BY h.home_geography_key, h.cycle_pattern, h.cycles_observed;
