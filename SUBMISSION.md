# Submission - RTV Data Engineer Technical Assessment

**Candidate:** Herbert Ssewanyana
**Time spent:** about 5.5 hours

Setup and run instructions, the architecture diagram and design details are in [README.md](README.md).
This note covers how I approached the task, what I assumed, and what I would do next.

---

## How I approached it

**1. Understanding the data before writing code (~45 min).** I profiled the Baseline export and read the
three forms side by side. A few things shaped the design early:

* The exports are very wide (Baseline has 6,717 columns) and differ between cycles, so I decided early that
  the wide data would stay in a columnar lake and only a conformed, narrow model would go into the
  warehouse. Postgres caps tables at 1,600 columns anyway.
* Choice codes are not stable across forms: `district = 1` means a different district in each cycle. Any
  decoding had to use the matching form, so I parse the forms into dictionaries rather than hard-coding
  labels.
* The Baseline district codes do not even match the attached Baseline form (codes 1/2/4 are Rukungiri,
  Kanungu and Mitooma according to the preloaded `pre_district` and the household-ID prefix). That made me
  resolve district from the tracking-sheet preload first and treat the form code as a last resort, with a
  data test that reports the conflicts.
* Every Baseline row is "found and consented", so the export looks pre-filtered. This affects how
  completion and consent can be compared across cycles, and I documented that rather than presenting a
  misleading trend.

**2. Bronze and the lake (~1 h).** PySpark reads each CSV as strings, adds the ingest metadata and writes
Parquet partitioned by cycle and a content-hash `ingest_id`, through a staging folder that is only promoted
after the row count reconciles. Re-running is a no-op; a corrected file becomes a new partition. The main
problem I hit was the CSV itself: quoted values contain CRLF while records end in LF, and Spark's
line-separator auto-detection picked up CRLF and read the whole file as one record. Pinning `lineSep` fixed
it. I then compared Bronze against the CSV column by column to confirm nothing was altered.

**3. Silver (~1 h).** A YAML mapping drives the conformed schema: about 35 fields, candidate source columns,
types, which form field decodes them, and whether they are required. Silver reads only those columns,
decodes coded answers with each cycle's own choice list, derives the household key, district and GPS
validity, and ranks duplicates instead of deleting them so dedup impact stays visible. Each run writes a
variable catalog showing which source column fed each field in each cycle and how well it was filled.

**4. Gold, tests and reporting (~1 h 15 min).** The dimensional model is plain SQL in Postgres, rebuilt in one
transaction. I made the report tables additive (counts and sums only) so every rate in the dashboard is a
ratio of sums and stays correct at any filter level. The 17 data tests are SQL files that return violating
rows, split into blocking errors and warnings. The run report is generated after every run, including
failed ones.

**5. Orchestration and packaging (~30 min).** Airflow 3 with one task per step and one Bronze task per
cycle, each calling the same CLI I used during development. Docker Compose brings up Postgres and Airflow;
an init script creates the Airflow metadata database and a read-only `bi_reader` login for the dashboard.

**6. Dashboard (~30 min).** Streamlit, so the dashboard lives in the same folder and starts with the same
`docker compose up` as the pipeline, with no desktop tool needed to review it. It reads only the Gold layer
through the read-only login, keeps all metric logic in one tested module, and adds a Pipeline Health tab
with row counts per layer and the latest data-test results.

---

## Assumptions

* **Household identity:** `hhid_2` (tracking-sheet ID) identifies a household within and across cycles. I
  did not attempt fuzzy matching on names or phone numbers; with more time I would use it only to audit
  ID re-use, not to link.
* **Latest submission wins** within a cycle (by `SubmissionDate`, then `endtime`, then `KEY` for a
  deterministic tie-break). I assumed a re-upload is a correction of the earlier one.
* **Completed interview** = household found (`status = 1`), consented (`consent_1 = 1`) and the form was
  finalised (`endtime` present). SurveyCTO only uploads finalised forms, so in practice this is found +
  consented.
* **Consent rate denominator** is households found, because consent is only asked once the listed
  household or spouse is found.
* **Status codes 1-5** mean the same in all forms (checked in the three form definitions); only the label
  wording differs, so Gold uses short standard labels.
* **Timestamps** are local Uganda time as exported and are stored without timezone conversion.
  `_ingested_at` is UTC.
* **Durations** over 240 minutes or not positive are treated as forms left open and excluded from average
  duration, but the rows are kept.
* **Valid GPS** = latitude and longitude present, not (0,0) and inside Uganda's bounding box.
* **Geography:** all districts in the forms are in Western Region (`UG-W`); sub-regions come from a small
  reference file (`config/geography.csv`) because the forms carry district only.
* **Age bands** for the household head: Under 25, 25-34, 35-44, 45-54, 55-64, 65+.
* **Confidentiality:** the source files never leave `data/` and are git-ignored, as are the generated lake
  and reports. Names, phone numbers and photo paths are not carried beyond Bronze.

---

## Data quality summary (Baseline export)

| Check | Result |
|---|---|
| Source rows -> Bronze -> Silver | 1,419 -> 1,419 -> 1,419 (all 6,717 columns preserved) |
| Duplicate submissions | 5 households submitted twice; 1,414 households after dedup (0.35% duplicate rate) |
| District code vs tracking sheet | 964 submissions conflict with the attached form's codes, 455 have a code (4) the form does not define; all resolved from the preload |
| Missing / invalid GPS | 0 |
| Interview duration | median 42 min, mean 45 min (excluding one 760-minute outlier) |
| Timestamp anomaly | 1 submission uploaded before its recorded start time |
| Follow-up consent | 14 consented households declined future follow-up |

The full results for all three cycles are produced by each run in `reports/pipeline_report.md` and
`reports/data_tests.csv`.

---

## What I would do next for production

1. **Ingest directly from SurveyCTO** (API, incremental by `SubmissionDate`) instead of manual CSV drops,
   with the DAG triggered by new data rather than run by hand, and alerting on failure (Slack/e-mail).
2. **Object storage and Delta** for the lake (S3/ADLS or Databricks volumes), with Delta `MERGE` for
   incremental Silver loads and time travel for audits; this also maps directly onto RTV's Databricks and
   Unity Catalog setup (README section 12).
3. **PII handling:** keep names, phone numbers and photo URLs in a restricted Bronze zone only, hash the
   household ID for analytics users, and apply column-level permissions.
4. **Data contracts with the M&E team:** stable choice codes across form versions (or a maintained code
   crosswalk), a required household ID constraint on the form, and a published variable mapping per new
   survey cycle so adding Year 3 is a config change.
5. **Richer testing:** row-level quarantine table for failing records, freshness checks, great_expectations
   or dbt tests if the team standardises on one, and a CI pipeline running the unit tests and a small
   synthetic end-to-end run on every pull request.
6. **Outcome metrics:** the next valuable layer for RTV's mission would be conformed poverty and livelihood
   indicators (income, consumption, assets) across cycles, once their definitions are agreed with the
   program team.
7. **Dashboard:** authentication in front of Streamlit (or move the same Gold model into Power BI for the
   wider organisation), row-level filtering by district for field managers, and maps of household GPS
   points for field supervision.