"""Gold: a star schema and analytics marts in DuckDB, built from the SQL files in sql/gold/.

Each SQL file is one model: a single SELECT whose first line declares how it is materialised.

- `-- materialized: full` rebuilds the whole table every run (small dimensions).
- `-- materialized: incremental` must filter on `$partitions`. The runner deletes those
  presentations from the table and inserts the fresh rows ("delete + insert", as dbt calls it),
  so a late file for 2014B rewrites 2014B rows only.

Models run in file-name order inside one transaction: gold is never left half-updated. After
the build, reconciliation checks prove that every silver row is accounted for.
"""

from __future__ import annotations

import os
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from . import storage
from .config import AS_OF_WEEKS, Layout
from .incremental import changed_partitions
from .silver import STATE_FILE

SILVER_VIEWS = {
    "silver_courses": "courses",
    "silver_assessments": "assessments",
    "silver_vle": "vle",
    "silver_student_info": "studentInfo",
    "silver_student_registration": "studentRegistration",
    "silver_student_assessment": "studentAssessment",
    "silver_student_vle": "studentVle",
}


def sql_dir() -> Path:
    """SQL models live in the repo (sql/gold) so they read like any dbt-style project."""
    default = Path(__file__).resolve().parents[2] / "sql" / "gold"
    return Path(os.environ.get("OULAD_SQL_DIR", default))


@dataclass(frozen=True)
class Model:
    name: str
    sql: str
    incremental: bool

    @property
    def params(self) -> list[str]:
        return sorted(set(re.findall(r"\$([a-z_]+)", self.sql)))


def load_models(folder: Path | None = None) -> list[Model]:
    models = []
    for path in sorted((folder or sql_dir()).glob("*.sql")):
        text = path.read_text()
        match = re.match(r"--\s*materialized:\s*(full|incremental)", text)
        if not match:
            raise ValueError(f"{path.name}: first line must be '-- materialized: full|incremental'")
        incremental = match.group(1) == "incremental"
        if incremental and "$partitions" not in text:
            raise ValueError(f"{path.name}: an incremental model must filter on $partitions")
        models.append(Model(path.stem.split("_", 1)[1], text, incremental))
    return models


def register_silver(con: duckdb.DuckDBPyConnection, silver: str) -> None:
    storage.attach_lake(con, silver)
    for view, table in SILVER_VIEWS.items():
        glob = storage.join(silver, table, "*", "*.parquet")
        con.execute(
            f"CREATE OR REPLACE TEMP VIEW {view} AS SELECT * FROM "
            f"read_parquet('{glob}', hive_partitioning = true, union_by_name = true)"
        )


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    query = "SELECT count(*) FROM information_schema.tables WHERE table_name = ?"
    row = con.execute(query, [name]).fetchone()
    return bool(row and row[0])


def build(
    con: duckdb.DuckDBPyConnection,
    partitions: Sequence[str],
    full_refresh: bool,
    as_of_weeks: Sequence[int] = AS_OF_WEEKS,
    models: list[Model] | None = None,
) -> None:
    values = {"partitions": list(partitions), "as_of_weeks": list(as_of_weeks)}
    con.execute("BEGIN TRANSACTION")
    try:
        for model in models or load_models():
            select = model.sql.strip().rstrip(";")
            params = {k: values[k] for k in model.params}
            if full_refresh or not model.incremental or not _table_exists(con, model.name):
                con.execute(f"CREATE OR REPLACE TABLE {model.name} AS {select}", params)
            else:
                con.execute(
                    f"DELETE FROM {model.name} WHERE list_contains($partitions, code_presentation)",
                    {"partitions": values["partitions"]},
                )
                con.execute(f"INSERT INTO {model.name} BY NAME {select}", params)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


@dataclass(frozen=True)
class Check:
    name: str
    expected: float
    actual: float

    @property
    def ok(self) -> bool:
        return self.expected == self.actual


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> float:
    row = con.execute(sql).fetchone()
    return float(row[0] or 0) if row else 0.0


def reconcile(
    con: duckdb.DuckDBPyConnection, as_of_weeks: Sequence[int] = AS_OF_WEEKS
) -> list[Check]:
    """Silver -> gold accounting. Each pair must match exactly."""
    pairs = [
        (
            "dim_student rows = silver studentInfo rows",
            "SELECT count(*) FROM silver_student_info",
            "SELECT count(*) FROM dim_student",
        ),
        (
            "dim_module_presentation rows = silver courses rows",
            "SELECT count(*) FROM silver_courses",
            "SELECT count(*) FROM dim_module_presentation",
        ),
        (
            "fact_weekly_engagement covers every silver studentVle row",
            "SELECT count(*) FROM silver_student_vle",
            "SELECT sum(n_vle_rows) FROM fact_weekly_engagement",
        ),
        (
            "fact_weekly_engagement clicks = silver studentVle clicks",
            "SELECT sum(sum_click) FROM silver_student_vle",
            "SELECT sum(total_clicks) FROM fact_weekly_engagement",
        ),
        (
            "activity-type columns add up to total clicks",
            "SELECT sum(total_clicks) FROM fact_weekly_engagement",
            "SELECT sum(clicks_homepage + clicks_oucontent + clicks_forumng + clicks_quiz"
            " + clicks_resource + clicks_other) FROM fact_weekly_engagement",
        ),
        (
            "fact_assessment rows = silver studentAssessment rows",
            "SELECT count(*) FROM silver_student_assessment",
            "SELECT count(*) FROM fact_assessment",
        ),
        (
            "early-warning rows = students x as-of weeks",
            f"SELECT count(*) * {len(as_of_weeks)} FROM dim_student",
            "SELECT count(*) FROM mart_early_warning_features",
        ),
        (
            "engagement mart students = students x complete weeks",
            "SELECT sum(d.full_weeks * d.n_students) FROM dim_module_presentation d",
            "SELECT sum(n_students) FROM mart_engagement_vs_outcome",
        ),
    ]
    return [Check(name, _scalar(con, exp), _scalar(con, act)) for name, exp, act in pairs]


class ReconciliationError(RuntimeError):
    pass


@dataclass
class GoldResult:
    partitions: list[str]
    checks: list[Check] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)


def table_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    names = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        ).fetchall()
    ]
    return {n: int(_scalar(con, f"SELECT count(*) FROM {n}")) for n in names}


def run_gold(layout: Layout, full_refresh: bool = False) -> GoldResult:
    """Rebuild the presentations whose silver inputs changed since gold last saw them."""
    silver_state = storage.read_json(storage.join(layout.silver, STATE_FILE), {})
    if not silver_state:
        raise RuntimeError("silver is empty; run `oulad silver` first")
    first_build = not layout.gold_db.exists()
    if full_refresh or first_build:
        built = {}
    else:
        built = storage.read_json(str(layout.gold_state), {})
    todo = changed_partitions(silver_state, built)
    layout.gold_db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(layout.gold_db))
    try:
        register_silver(con, layout.silver)
        if todo:
            build(con, todo, full_refresh=full_refresh or first_build)
        checks = reconcile(con)
        failed = [c for c in checks if not c.ok]
        if failed:
            raise ReconciliationError(
                "; ".join(f"{c.name}: {c.expected} != {c.actual}" for c in failed)
            )
        counts = table_counts(con)
    finally:
        con.close()
    storage.write_json(str(layout.gold_state), {**built, **{p: silver_state[p] for p in todo}})
    return GoldResult(todo, checks, counts)
