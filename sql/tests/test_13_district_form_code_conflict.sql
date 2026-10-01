-- severity: warn
-- layer: silver
-- description: The district chosen on the form agrees with the tracking-sheet district
SELECT survey_cycle, district_code, district_code_label, district_name, count(*) AS submissions
FROM silver.survey_submissions
WHERE district_form_conflict
GROUP BY survey_cycle, district_code, district_code_label, district_name
