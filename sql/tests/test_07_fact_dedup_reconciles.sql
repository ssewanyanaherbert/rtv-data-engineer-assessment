-- severity: error
-- layer: gold
-- description: Fact rows per cycle equal distinct households in Silver (dedup lost or added nothing)
WITH expected AS (
    SELECT cycle_order AS cycle_key,
           count(DISTINCT coalesce(household_key, 'UNLINKED:' || submission_key)) AS households
    FROM silver.survey_submissions
    GROUP BY cycle_order
),
actual AS (
    SELECT cycle_key, count(*) AS households
    FROM gold.fct_survey_submissions
    GROUP BY cycle_key
)
SELECT e.cycle_key, e.households AS expected, a.households AS actual
FROM expected e
FULL JOIN actual a USING (cycle_key)
WHERE e.households IS DISTINCT FROM a.households
