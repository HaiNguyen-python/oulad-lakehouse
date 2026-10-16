# Learning guide (for me, Hai)

Goal: be able to explain every layer of this lakehouse in an interview, change it confidently,
and defend every number in the README. This repo is my main "batch data engineering" project:
ingestion, contracts, data quality, incremental loads, dimensional modelling, orchestration.

Start with [docs/how-it-works.md](docs/how-it-works.md) (the code walkthrough) and
[docs/decisions.md](docs/decisions.md) (the reasons). This file is the practice plan.

## 1. Run it and watch each layer (1-2 hours)

```bash
make install && make test
oulad generate
oulad ingest                 # look at data/lake/bronze/_manifest.jsonl
oulad ingest                 # again: every file is skipped. Why?
oulad silver                 # look at data/lake/silver/_dq/2013J.json and _state.json
oulad gold
oulad generate-late --presentation 2014B
oulad run-all --label "late file"
oulad report
```

Then:

- Open the quarantine with DuckDB:
  `SELECT table_name, reasons, record FROM read_parquet('data/lake/silver/_quarantine/*/*/*.parquet', hive_partitioning=true)`.
  Find each planted defect in `data/landing/_generator.json`.
- Compare file names and modification times under `data/lake/silver/studentVle/` before and
  after the late file. Which folder changed?
- Break something on purpose: edit a SQL model to use `JOIN` where a `LEFT JOIN` is needed
  (e.g. in `06_mart_engagement_vs_outcome.sql`), run `oulad gold --full-refresh`, and read the
  reconciliation error.

## 2. Read the code in this order

| File | What to understand | Question to answer myself |
| --- | --- | --- |
| `tables.py` | The 7 OULAD tables and how they relate | Why does `studentAssessment` need `assessments` to find its presentation? |
| `synthetic.py` | Hidden engagement `z`, quirks, planted defects | Which numbers in the report are "planted", and how would real data differ? |
| `bronze.py` | All-string Parquet, lineage, SHA-256 manifest | What happens if the process crashes between the Parquet write and the manifest? |
| `incremental.py` | Fingerprint per partition, `changed_partitions` | Why compare fingerprints instead of file modification times? |
| `contracts.py` | Declarative rules and reason strings | Where would I add "weight must sum to 100 per presentation"? |
| `silver.py` | Contract projections, references, quarantine, window dedup, DQ | Why the regex before the cast? What is the difference between `latest` and `exact`? |
| `sql/gold/*.sql` | Grain of each table, zero-fill, `floor(day / 7.0)` | What is the grain of `fact_weekly_engagement`? Why is the mart zero-filled? |
| `gold.py` | Full vs incremental models, transaction, reconciliation | Why is gold atomic and silver not? |
| `07_mart_early_warning_features.sql` | Cutoff, `label_` columns | Which feature would leak if I removed `date < cutoff_day`? |
| `tests/test_gold.py` | Perturbation test and positive control | Why do I need the positive control? |
| `dags/oulad_lakehouse_dag.py` | Thin DAG calling the CLI | Why `max_active_runs=1`? |

## 3. Concepts in my own words (fill these in)

- **Medallion architecture (bronze / silver / gold):** ...
- **Idempotency and content addressing:** ...
- **Schema contract vs data-quality check:** ...
- **Quarantine (dead-letter) table:** ...
- **Partitioning and partition pruning:** ...
- **Incremental load strategies (append, delete + insert, merge/upsert):** ...
- **Star schema, grain, dimension vs fact:** ...
- **Reconciliation between layers:** ...
- **Data leakage and point-in-time ("as of") features:** ...
- **Spark lazy evaluation, jobs, shuffles and caching:** ...  (why `cache()` before three aggregations?)

## 4. Interview questions this repo answers

1. *How do you make a batch pipeline safe to rerun?* Content hashes in bronze, fingerprints per
   partition, delete-then-write per partition, a state file updated only at the end. Show the
   "rerun, nothing new" row in the results.
2. *What do you do with bad data?* Contracts with reasons, quarantine with the raw record, NULL
   for "unknown but allowed", DQ record per partition; show "caught 24 of 24 planted".
3. *How do you handle late-arriving data?* The late file changes one fingerprint; only 2014B is
   rebuilt in silver and gold; the test checks 2013J's files are untouched.
4. *How do you know no rows were lost?* rows in = rows out + quarantined + duplicates in silver,
   and eight silver-to-gold checks; one test deletes a row and expects the check to fail.
5. *How do you model this for analysts?* Walk through the grain of each dimension and fact.
6. *How do you build features without leakage?* As-of cutoff, label prefix, perturbation test.
7. *Why Spark and DuckDB?* Volume and cluster-readiness for row-level work; readable, fast SQL
   for the gold models. Be honest about Spark's overhead on small data.
8. *How would you run this in the cloud?* Lake on S3 (the `s3` option), Spark on EMR/Databricks
   or Glue, gold in a warehouse or dbt, Airflow (MWAA) calling the same CLI, Iceberg/Delta for
   atomic partition replacement.

## 5. Make it mine (do at least three)

- [ ] Download the real OULAD, run `oulad run-all` and `oulad report --data-label "OULAD"`, and
      update the README with real numbers. What new quarantine reasons appear?
- [ ] Add a contract rule across rows: assessment weights of TMAs sum to 100 per presentation.
- [ ] Add a marking lag (e.g. 14 days) to score features and rerun the leakage test.
- [ ] Replace delete-then-append in silver with Delta Lake `replaceWhere` and compare.
- [ ] Run `make up` (MinIO) and the pipeline with `OULAD_LAKE_URI=s3://oulad/lake`; fix
      whatever breaks and add it to CI as a service container.
- [ ] Move `sql/gold` into a small dbt project with `dbt-duckdb` and keep the reconciliation as
      dbt tests.

## 6. Quality checks I should be able to explain

- `make lint` (ruff), `mypy src`, `pytest` (30 tests, coverage report), `pre-commit install`.
- Why the Spark tests share one session-scoped run (`tests/conftest.py → pipeline`).
- Why the DAG is excluded from mypy and coverage, and how its test still catches drift.

## 7. Honesty checklist before pushing

- [ ] I can explain every file without looking at it.
- [ ] README and results numbers are from runs I did myself (`make demo`).
- [ ] I say clearly that the data is synthetic and the effects are planted.
- [ ] Anything I haven't built or run is under "What I'd do next" or marked as untested.

Note: the MinIO / S3 path (`make up`, `OULAD_LAKE_URI=s3://...`) was not run in the environment
this repo was first built in (no Docker there). The local pipeline, Spark included, is tested.
