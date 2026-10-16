"""Shared fixtures.

`pipeline` runs the real CLI end to end once per test session on a tiny synthetic dataset
(60 students per presentation): initial load, a no-op rerun, then a late file plus a
correction for 2014B. Spark start-up dominates the cost, so every Spark-dependent test reads
from this one run instead of starting its own.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from oulad_lakehouse.cli import main, run_all
from oulad_lakehouse.config import Layout
from oulad_lakehouse.silver import read_dq
from oulad_lakehouse.synthetic import generate_late

TINY_STUDENTS = 60


def spark_available() -> bool:
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    return shutil.which("java") is not None


requires_spark = pytest.mark.skipif(not spark_available(), reason="needs pyspark and Java 17+")


def file_listing(folder: Path) -> dict[str, float]:
    return {str(p.relative_to(folder)): p.stat().st_mtime_ns for p in folder.rglob("*.parquet")}


def gold_rows(layout: Layout, table: str, presentation: str) -> pd.DataFrame:
    with duckdb.connect(str(layout.gold_db), read_only=True) as con:
        df = con.execute(f"SELECT * FROM {table} WHERE code_presentation = ?", [presentation]).df()
    return df.sort_values(list(df.columns)).reset_index(drop=True)


@dataclass
class PipelineRun:
    layout: Layout
    runs: list[dict] = field(default_factory=list)
    silver_files_before_late: dict[str, float] = field(default_factory=dict)
    silver_files_after_late: dict[str, float] = field(default_factory=dict)
    gold_2013j_before_late: pd.DataFrame | None = None
    gold_2013j_after_late: pd.DataFrame | None = None
    dq_2014b_before_late: dict = field(default_factory=dict)
    late_file: dict = field(default_factory=dict)
    corrected_student: int = 0


@pytest.fixture(scope="session")
def pipeline(tmp_path_factory: pytest.TempPathFactory) -> PipelineRun:
    if not spark_available():
        pytest.skip("needs pyspark and Java 17+")
    data_dir = tmp_path_factory.mktemp("oulad")
    layout = Layout.from_env(data_dir)
    main(["--data-dir", str(data_dir), "generate", "--students", str(TINY_STUDENTS)])
    run = PipelineRun(layout)
    run.runs.append(run_all(layout, "initial load"))
    run.runs.append(run_all(layout, "rerun, nothing new"))
    run.silver_files_before_late = file_listing(Path(layout.silver))
    run.gold_2013j_before_late = gold_rows(layout, "fact_weekly_engagement", "2013J")
    run.dq_2014b_before_late = read_dq(layout.silver)["2014B"]

    run.late_file = generate_late(layout.landing, "2014B")["late_files"][-1]
    # A corrected studentInfo row for one 2014B student: the newest delivery must win.
    info = pd.read_csv(layout.landing / "2014B" / "studentInfo.csv", dtype=str)
    fix = info.iloc[[5]].assign(studied_credits="240")
    run.corrected_student = int(fix["id_student"].iloc[0])
    fix.to_csv(layout.landing / "2014B" / "studentInfo__fix_1.csv", index=False)
    run.runs.append(run_all(layout, "late files for 2014B"))
    run.silver_files_after_late = file_listing(Path(layout.silver))
    run.gold_2013j_after_late = gold_rows(layout, "fact_weekly_engagement", "2013J")
    return run
