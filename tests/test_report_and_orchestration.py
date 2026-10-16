"""Report output, the Airflow DAG wiring and the SQL model loader."""

from __future__ import annotations

import argparse
import ast
from pathlib import Path

import pytest
from conftest import PipelineRun, requires_spark

from oulad_lakehouse.cli import _parser
from oulad_lakehouse.gold import load_models
from oulad_lakehouse.report import layer_totals, write_report
from oulad_lakehouse.silver import read_dq

DAG_FILE = Path(__file__).parents[1] / "dags" / "oulad_lakehouse_dag.py"


@requires_spark
@pytest.mark.spark
def test_report_numbers_come_from_the_dq_records(pipeline: PipelineRun, tmp_path: Path) -> None:
    path = write_report(pipeline.layout, tmp_path / "docs")
    text = path.read_text()
    totals = layer_totals(read_dq(pipeline.layout.silver))
    vle = totals["studentVle"]
    assert f"| studentVle | {vle['rows_in']:,} | {vle['rows_out']:,} |" in text
    for heading in (
        "## Findings",
        "## Row counts per layer",
        "## Data quality issues caught",
        "## Reconciliation, silver to gold",
        "## Runs",
    ):
        assert heading in text
    assert "**NO**" not in text  # every reconciliation check passed
    assert "late files for 2014B" in text and "| `2014B` |" in text
    for png in ("weekly_clicks_by_outcome.png", "at_risk_by_engagement.png"):
        size = (tmp_path / "docs" / "img" / png).stat().st_size
        assert 10_000 < size < 300_000


def _cli_commands() -> set[str]:
    sub = next(a for a in _parser()._actions if isinstance(a, argparse._SubParsersAction))
    return set(sub.choices)


def test_airflow_dag_only_calls_existing_cli_commands() -> None:
    """The DAG is not importable without Airflow, so check its CLI calls statically."""
    tree = ast.parse(DAG_FILE.read_text())
    calls = [
        node.args[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "main" and node.args
    ]
    commands = [c.elts[0].value for c in calls if isinstance(c, ast.List)]  # type: ignore[attr-defined]
    assert commands == ["ingest", "silver", "gold", "report"]
    assert set(commands) <= _cli_commands()


def test_incremental_models_must_filter_on_partitions(tmp_path: Path) -> None:
    (tmp_path / "01_bad.sql").write_text("-- materialized: incremental\nSELECT 1 AS x\n")
    with pytest.raises(ValueError, match=r"must filter on \$partitions"):
        load_models(tmp_path)
    (tmp_path / "01_bad.sql").write_text("SELECT 1 AS x\n")
    with pytest.raises(ValueError, match="first line"):
        load_models(tmp_path)


def test_repo_models_are_ordered_and_declared() -> None:
    models = load_models()
    assert [m.name for m in models][:3] == ["dim_module_presentation", "dim_student", "dim_date"]
    assert [m.name for m in models if not m.incremental] == ["dim_date"]
    assert next(m for m in models if m.name == "mart_early_warning_features").params == [
        "as_of_weeks",
        "partitions",
    ]
