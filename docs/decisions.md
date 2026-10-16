# Design decisions

Short records of the main choices: what was decided, why, and what it costs. Format inspired by
Architecture Decision Records (ADRs).

## 1. Bronze stores every value as a string

- **Decision:** bronze is the CSV as Parquet, all columns `string`, plus lineage columns.
- **Why:** typing at the edge loses information. `"?"`, `"12.5"` in an integer column or an
  empty cell would become NULL or an error before anyone can see what arrived. With raw strings,
  silver can say exactly why it rejected a row, and a fixed contract can be replayed from bronze
  without asking the source again.
- **Cost:** bronze is bigger than a typed copy, and every consumer must go through silver.

## 2. A file's identity is its content hash

- **Decision:** SHA-256 of the file decides whether it is new; the manifest stores hashes.
- **Why:** names and timestamps lie. The same export re-sent under another name is skipped, and
  a retry after a crash writes to the same content-addressed path.
- **Cost:** a file with one changed byte is a new file, and its overlap with older files is
  handled by silver's de-duplication, not by bronze.

## 3. Partition by presentation, rebuild by fingerprint

- **Decision:** silver and gold are partitioned by `code_presentation`. A partition is rebuilt
  when the set of bronze files feeding it changes.
- **Why:** OULAD data arrives per course run, and a correction or late log for one run should
  not rewrite the others. Comparing input fingerprints instead of "last modified" times makes a
  rerun with the same inputs a no-op by construction.
- **Cost:** the unit of work is a whole presentation; a 50-row late file still rebuilds all of
  2014B. Fine for this data size; for streams of small updates a merge (upsert) would be better.
- **Alternatives:** Delta Lake / Iceberg `MERGE` or `replaceWhere` (atomic, but more
  infrastructure for a portfolio repo).

## 4. Replace a partition by delete-then-append

- **Decision:** silver deletes the partition folder, then appends the new files; gold deletes
  the partition's rows, then inserts, inside one DuckDB transaction.
- **Why:** Spark's "dynamic partition overwrite" keeps an old partition when the new data for
  it is empty (for example, every row quarantined). Deleting first is explicit and correct.
- **Cost:** silver is not atomic: a reader during the rebuild can see a missing partition, and
  a crash between delete and write leaves it empty until the next run (the state file is only
  updated at the end, so the next run rebuilds it). Gold is atomic. A table format with
  transactions (decision 3 alternatives) removes this gap.

## 5. Contracts are data, reasons are stable strings

- **Decision:** `contracts.py` declares type, nullability, allowed values, format, range,
  de-duplication and foreign keys per table. Each failed rule produces a fixed reason string.
- **Why:** one file to review, and the same strings appear in the quarantine, the DQ report and
  the tests. A light, hand-written checker keeps the dependency list short and runs inside
  Spark; pandera or Great Expectations would add a lot for these rules.
- **Cost:** no automatic documentation site or profiling; rules beyond one column or one join
  need code.

## 6. Quarantine instead of dropping, NULL instead of rejecting

- **Decision:** rows that break a rule go to `silver/_quarantine` with the reasons and raw
  values. Values that are *unknown* but allowed to be (`"?"` IMD band or score, blank
  unregistration date) become NULL and stay.
- **Why:** a `?` score is a real submission with a missing mark; dropping it would undercount
  submissions. A negative click count is not data, but someone must be able to see it.
- **Cost:** a quarantined parent row (e.g. a student) also rejects its children
  (`id_student: not enrolled`); the reasons make that cascade visible.

## 7. Spark for bronze → silver, DuckDB for gold

- **Decision:** PySpark (local mode) for the row-level cleaning; DuckDB SQL files for the star
  schema and marts.
- **Why:** the row-level work is where volume lives (OULAD has about 10 million click rows) and
  where Spark's distribution pays off on a cluster. Gold is aggregates and joins over cleaned
  data: SQL files are easy to review, DuckDB rebuilds them in about a second, and the models
  would move to dbt or a warehouse with little change.
- **Cost:** two engines to know. Spark has a large fixed cost per job: on this 2-CPU sandbox a
  silver run takes one to two minutes even for tiny data, so the tests share one Spark run.

## 8. Reconcile gold against silver on every run

- **Decision:** after each gold build, eight checks compare counts and sums across layers
  (every silver click row and click is in the weekly fact, every enrolment in `dim_student`,
  students × weeks in the mart, ...). Any mismatch fails the run.
- **Why:** a wrong join (inner instead of left, a missing key) silently loses rows. Counting is
  cheap and catches that class of bug at once; a test proves it catches a deleted row.
- **Cost:** the checks know the models; a new model needs a new check.

## 9. Early-warning features as of a cutoff, tested by perturbation

- **Decision:** features for week N use only events with `day < 7·N`. Labels are prefixed
  `label_`. A test changes everything on or after the cutoff and requires identical features.
- **Why:** leakage is the classic way an early-warning model looks great offline and fails in
  use. A perturbation test checks behaviour, not code, so a new leaky feature fails it.
- **Cost / known limitation:** a score is treated as known on its submission day. In reality
  marking takes days or weeks; a stricter version would add a marking lag to `score` features.

## 10. Synthetic data with planted defects and a planted effect

- **Decision:** a seeded generator writes the exact OULAD files with OULAD's quirks, a few
  broken rows with known reasons, duplicates, and a hidden engagement level that drives results.
- **Why:** CI and readers can run everything offline, the tests can check "caught exactly what
  was planted", and the charts show a real, explainable effect.
- **Cost:** the effect is stronger and cleaner than in real data, so the numbers are labelled
  synthetic and say nothing about real students.

## 11. Airflow calls the CLI and is not a dependency

- **Decision:** the DAG has four tasks that call `oulad_lakehouse.cli.main`. Airflow is not in
  `pyproject.toml`; the DAG is excluded from mypy and coverage.
- **Why:** the same code runs locally, in tests and in Airflow; Airflow's large dependency tree
  stays out of the package and CI. A test parses the DAG and checks that it only calls existing
  commands.
- **Cost:** the DAG itself is not executed in CI.

## 12. Local disk by default, S3-compatible as an option

- **Decision:** bronze and silver paths go through fsspec, so `OULAD_LAKE_URI=s3://...` works
  with the `s3` extra; Spark gets `s3a` settings and DuckDB reads through the same fsspec
  filesystem. The DuckDB gold file stays on local disk.
- **Why:** the default must run anywhere without services, while the code should not assume a
  local disk.
- **Cost:** the S3 path is only tested with an in-memory fsspec filesystem; the MinIO compose
  file was validated but not run in the build environment.
