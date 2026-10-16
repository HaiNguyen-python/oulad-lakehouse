"""Nightly Airflow DAG: ingest -> silver -> gold -> report.

Each task calls the same CLI used locally, so the DAG holds no business logic and every step
is safe to retry (bronze skips files it has seen, silver and gold rebuild only changed
partitions). Airflow is not a dependency of the package: this file parses only where Airflow
is installed, and it is excluded from mypy and coverage.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=10)}


@dag(
    dag_id="oulad_lakehouse",
    schedule="15 2 * * *",  # after the nightly VLE export
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # partitions are replaced in place; never run two builds at once
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "learning-analytics"],
)
def oulad_lakehouse():
    @task
    def ingest():
        from oulad_lakehouse.cli import main

        main(["ingest"])

    @task
    def silver():
        from oulad_lakehouse.cli import main

        main(["silver"])

    @task
    def gold():
        from oulad_lakehouse.cli import main

        # Fails the task if any silver -> gold reconciliation check fails.
        main(["gold"])

    @task
    def report():
        from oulad_lakehouse.cli import main

        main(["report", "--out", "/opt/airflow/reports"])

    ingest() >> silver() >> gold() >> report()


oulad_lakehouse()
