# How it works

This page follows the data from a CSV file to the early-warning mart, and explains the
important code on the way. File references point to `src/oulad_lakehouse/` and `sql/gold/`.

## 0. The data: seven OULAD tables

`tables.py` lists the seven tables and their exact column names. The important relations:

- `courses`, `assessments` and `vle` describe a **module presentation**, e.g. module `AAA`
  run in `2013J` (J = starts in October, B = February).
- `studentInfo` has one row per **enrolment** (student × module presentation), with
  demographics and the final result: Pass, Distinction, Fail or Withdrawn.
- `studentRegistration`, `studentAssessment` and `studentVle` are events. All dates are days
  relative to the module start; negative means before the course opened.
- `studentAssessment` has no presentation column: it only has `id_assessment`. The
  presentation comes from `assessments`.

`synthetic.py` writes these files for three presentations (2013J, 2014B, 2014J). Each
enrolment has a hidden engagement level `z`; it drives daily activity, submissions, scores and
the final result. The generator also plants OULAD's quirks and a few broken rows, and records
all of it in `data/landing/_generator.json`.

## 1. Bronze: keep what arrived, prove where it came from

`bronze.py → ingest` walks `data/landing/` recursively:

1. The table is taken from the file name: `studentVle.csv` and `studentVle__late_1.csv` both
   feed `studentVle` (`tables.table_for_file`).
2. `file_hash` computes the SHA-256 of the file. If the manifest (`bronze/_manifest.jsonl`)
   already has that hash, the file is skipped. This is the idempotency rule: same bytes, same
   file, whatever its name.
3. `read_raw_csv` checks the header against `TABLE_COLUMNS` and raises
   `SchemaMismatchError` before writing anything if it differs. Every column is read as a
   **string**, and empty cells stay `""`.
4. `_with_lineage` adds `_ingested_at`, `_source_file`, `_file_hash` and `_row_number`.
5. The Parquet file is written to `bronze/<table>/<hash prefix>.parquet`, then the manifest
   entry is added. If the process dies between the two steps, the retry writes the same path.

## 2. Planning: which presentations changed?

`incremental.py → bronze_fingerprints` runs one DuckDB query over the bronze files and
returns, per presentation, the sorted list of `table:file_hash` that contribute rows to it.
Submissions are mapped to a presentation through `assessments`; submissions to an unknown
assessment land in the `__unknown__` partition.

`digest` turns each list into a short hash. `changed_partitions` compares those digests with
the ones silver stored last time in `silver/_state.json`. Same files → same digest → nothing to
do. A new file for 2014B changes only 2014B's digest.

## 3. Silver: contracts, references, quarantine, de-duplication (PySpark)

`silver.py → run_silver` starts a local Spark session only when something changed, and
`build_silver` reads only the bronze files that feed those partitions (`files_for`). Tables are
processed parents first (`TABLE_ORDER`), because child tables check their foreign keys against
the parents built in the same run.

For each table, `build_table` does:

1. **Partition key** (`with_partition`): the trimmed `code_presentation`, or for submissions
   the assessment's presentation.
2. **Contract** (`apply_contract`, rules in `contracts.py`), in three projections:
   - `_value`: trim, turn `?` and `""` into NULL, and normalise `10-20` to `10-20%`;
   - `_cast`: cast to `bigint` or `double` only when a regex matches. Without the regex Spark
     would turn `"12.5"` into `12` without complaint;
   - `_checks`: one `(condition, reason)` pair per rule: missing, not an integer, bad format,
     unexpected value, out of range. The reasons are collected into an array `_reasons`.
   Each step selects named columns, so later checks refer to a column instead of repeating
   the whole cleaning expression; the plan stays small and readable in `explain()`.
3. **References** (`check_references`): left join to the parent's keys; if the key is present
   but has no match, add a reason such as `id_site: unknown VLE site`. A row whose key is
   already NULL (because it failed the type check) does not get a second reason.
4. **Quarantine**: rows with any reason are written to `silver/_quarantine/` with the table
   name, the joined reasons, and the raw values as JSON.
5. **De-duplication** (`deduplicate`): a window over the key, ordered by delivery
   (`_ingested_at`, `_source_file`, `_row_number`).
   - `latest`: newest delivery per primary key wins, so a correction file replaces a row;
   - `exact` (studentVle): identical rows collapse to the first delivery; two different click
     counts for the same student, site and day are both kept, as in the real logs.
6. **DQ metrics** (`_dq_metrics`): grouping by `(_partition, _reasons)` gives rows in,
   quarantined rows and reason counts in one job; a second job counts rows out and NULLs per
   column. Duplicates removed = rows in − quarantined − rows out, so the three always add up.

`_replace_partitions` deletes the folders of the partitions being rebuilt and appends the new
rows. Other partitions are never opened. At the end, one JSON DQ record per partition is written
to `silver/_dq/` and the new digests go to `silver/_state.json`.

## 4. Gold: SQL models in DuckDB

`gold.py → run_gold` compares silver's digests with `data/gold/_state.json` to find the
partitions gold has not built yet. `register_silver` creates views such as
`silver_student_vle` over the Parquet folders. `build` then runs `sql/gold/*.sql` in name
order inside one transaction:

| Model | Grain | Notes |
| --- | --- | --- |
| `dim_module_presentation` | module presentation | length in days, complete weeks, counts |
| `dim_student` | enrolment | demographics, registration dates, final result, outcome group |
| `dim_date` | course day | relative day, `week = floor(day / 7)`, before-start flag |
| `fact_weekly_engagement` | enrolment × week with activity | clicks by activity type, active days, `n_vle_rows` |
| `fact_assessment` | submission | deadline (exam without date = end of course), days late, score |
| `mart_engagement_vs_outcome` | presentation × result × complete week | zero-filled, so every student counts every week |
| `mart_early_warning_features` | enrolment × as-of week | features known at the start of week N, plus `label_*` columns |

The first line of each file says `-- materialized: full` or `incremental`. For incremental
models the runner deletes the rebuilt presentations and inserts the model's rows for them;
`load_models` refuses an incremental model that does not filter on `$partitions`.

A detail that matters: in DuckDB `-1 // 7` is `0` (integer division truncates), so the week is
`floor(day / 7.0)`. Day −1 belongs to week −1, not to the first week of the course.

After the build, `reconcile` compares silver and gold (rows, clicks, students × weeks). If any
pair differs, `run_gold` raises `ReconciliationError` and the Airflow task fails.

## 5. The early-warning mart and leakage

`07_mart_early_warning_features.sql` builds one row per enrolment and as-of week N
(`config.AS_OF_WEEKS = (2, 4, 8, 12)`), with `cutoff_day = 7 * N`. Every event is filtered with
`date < cutoff_day`: clicks, submissions and scores. Deadlines are used to count missed
assessments, because the schedule is published in advance. `is_enrolled_at_cutoff` only says
whether the student had already unregistered, never when they will.

`tests/test_gold.py → test_early_warning_features_do_not_see_the_future` copies silver,
changes everything on or after the cutoff (multiplies later clicks, adds rows exactly on the
cutoff day, flips later scores, moves unregistrations, flips every final result), rebuilds gold
and checks that every non-label column is identical. `test_leakage_check_is_not_vacuous` adds
one click on the day before the cutoff and checks that the features do change.

## 6. Orchestration and report

`cli.py` exposes every step. `run_all` times ingest, silver and gold and appends a record to
`data/runs.jsonl`. `dags/oulad_lakehouse_dag.py` runs the same four commands nightly with
retries and `max_active_runs=1`, because partitions are replaced in place.

`report.py → write_report` reads the run log, the DQ records, the generator summary and the
gold database, and writes `docs/results.md` and the two charts. The chart titles are computed
from the data, so they stay true when the report is rerun on the real OULAD.
