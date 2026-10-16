"""Gold star schema, reconciliation and the no-leakage guarantee of the early-warning mart."""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from conftest import PipelineRun

from oulad_lakehouse.config import AS_OF_WEEKS
from oulad_lakehouse.gold import build, load_models, reconcile, register_silver

pytestmark = pytest.mark.spark

SILVER_TABLES = (
    "courses",
    "assessments",
    "vle",
    "studentInfo",
    "studentRegistration",
    "studentAssessment",
    "studentVle",
)


def gold(run: PipelineRun) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(run.layout.gold_db), read_only=True)
    register_silver(con, run.layout.silver)
    return con


def test_every_silver_row_is_accounted_for_in_gold(pipeline: PipelineRun) -> None:
    with gold(pipeline) as con:
        checks = reconcile(con)
    assert len(checks) == 8
    assert all(c.ok for c in checks), [c for c in checks if not c.ok]


def test_reconciliation_catches_a_lost_row(pipeline: PipelineRun, tmp_path: Path) -> None:
    db = tmp_path / "broken.duckdb"
    shutil.copy(pipeline.layout.gold_db, db)
    with duckdb.connect(str(db)) as con:
        con.execute(
            "DELETE FROM fact_assessment WHERE rowid = (SELECT min(rowid) FROM fact_assessment)"
        )
        register_silver(con, pipeline.layout.silver)
        failed = [c.name for c in reconcile(con) if not c.ok]
    assert failed == ["fact_assessment rows = silver studentAssessment rows"]


def test_relative_course_weeks_handle_negative_days(pipeline: PipelineRun) -> None:
    with gold(pipeline) as con:
        weeks = dict(
            con.execute(
                "SELECT day, week FROM dim_date WHERE day IN (-8, -7, -1, 0, 6, 7)"
            ).fetchall()
        )
        pre_start = con.execute(
            "SELECT count(*) FROM fact_weekly_engagement WHERE week < 0"
        ).fetchone()
    assert weeks == {-8: -2, -7: -1, -1: -1, 0: 0, 6: 0, 7: 1}
    assert pre_start and pre_start[0] > 0  # clicks before the module opens are kept


def test_engagement_mart_counts_every_student_every_week(pipeline: PipelineRun) -> None:
    with gold(pipeline) as con:
        spread = con.execute(
            """
            SELECT m.code_presentation, m.final_result, min(m.n_students), max(m.n_students),
                   any_value(d.n) AS enrolled
            FROM mart_engagement_vs_outcome m
            JOIN (SELECT code_presentation, final_result, count(*) AS n FROM dim_student
                  GROUP BY ALL) d USING (code_presentation, final_result)
            GROUP BY ALL
            """
        ).fetchall()
    # Zero-filled grid: the denominator never shrinks when students stop clicking.
    assert spread and all(lo == hi == n for _, _, lo, hi, n in spread)


def _features(con: duckdb.DuckDBPyConnection, max_week: int) -> pd.DataFrame:
    df = con.execute(
        "SELECT * FROM mart_early_warning_features WHERE as_of_week <= ?", [max_week]
    ).df()
    features = df[[c for c in df.columns if not c.startswith("label_")]]
    return features.sort_values(["student_key", "as_of_week"]).reset_index(drop=True)


def _rebuild(silver: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    register_silver(con, str(silver))
    build(con, ["2013J", "2014B", "2014J"], full_refresh=True, models=load_models())
    return con


def _rewrite(silver: Path, table: str, select: str) -> None:
    """Replace one silver table with `select` over its old contents (DuckDB, same layout)."""
    old = silver / f"{table}__old"
    (silver / table).rename(old)
    with duckdb.connect() as con:
        con.execute(
            "CREATE VIEW t AS SELECT * FROM "
            f"read_parquet('{old}/*/*.parquet', hive_partitioning = true)"
        )
        con.execute(
            f"COPY ({select}) TO '{silver / table}' "
            "(FORMAT parquet, PARTITION_BY (code_presentation))"
        )
    shutil.rmtree(old)


def _copy_silver(run: PipelineRun, dest: Path) -> Path:
    for table in SILVER_TABLES:
        shutil.copytree(Path(run.layout.silver) / table, dest / table)
    return dest


@pytest.mark.parametrize("week", AS_OF_WEEKS)
def test_early_warning_features_do_not_see_the_future(
    pipeline: PipelineRun, tmp_path: Path, week: int
) -> None:
    """Change everything that happens on or after the cutoff; features must not move."""
    cutoff = 7 * week
    with _rebuild(Path(pipeline.layout.silver)) as con:
        original = _features(con, week)
    silver = _copy_silver(pipeline, tmp_path / "silver")
    _rewrite(
        silver,
        "studentVle",
        f"""
        SELECT * REPLACE (CASE WHEN date >= {cutoff} THEN sum_click * 10 + 3
                               ELSE sum_click END AS sum_click) FROM t
        UNION ALL BY NAME
        SELECT * REPLACE ({cutoff} AS date, 50 AS sum_click) FROM t WHERE date < {cutoff}""",
    )
    _rewrite(
        silver,
        "studentAssessment",
        f"""
        SELECT * REPLACE (
            CASE WHEN date_submitted >= {cutoff} THEN 100 - coalesce(score, 0) ELSE score END
                AS score,
            CASE WHEN date_submitted >= {cutoff} THEN {cutoff} ELSE date_submitted END
                AS date_submitted) FROM t""",
    )
    _rewrite(
        silver,
        "studentRegistration",
        f"""
        SELECT * REPLACE (CASE WHEN date_unregistration IS NULL THEN {cutoff} + 5
                               WHEN date_unregistration >= {cutoff} THEN {cutoff}
                               ELSE date_unregistration END AS date_unregistration) FROM t""",
    )
    _rewrite(
        silver,
        "studentInfo",
        """
        SELECT * REPLACE (CASE final_result WHEN 'Pass' THEN 'Fail' ELSE 'Pass' END
                          AS final_result) FROM t""",
    )
    with _rebuild(silver) as con:
        perturbed = _features(con, week)
        labels_changed = con.execute(
            "SELECT count(DISTINCT label_final_result) FROM mart_early_warning_features"
        ).fetchone()
    pd.testing.assert_frame_equal(original, perturbed)
    assert labels_changed and labels_changed[0] == 2  # the perturbation really happened


def test_leakage_check_is_not_vacuous(pipeline: PipelineRun, tmp_path: Path) -> None:
    """Positive control: changing clicks before the cutoff must change the features."""
    week = AS_OF_WEEKS[0]
    with _rebuild(Path(pipeline.layout.silver)) as con:
        original = _features(con, week)
    silver = _copy_silver(pipeline, tmp_path / "silver")
    _rewrite(
        silver,
        "studentVle",
        f"""
        SELECT * REPLACE (CASE WHEN date = {7 * week - 1} THEN sum_click + 1
                               ELSE sum_click END AS sum_click) FROM t""",
    )
    with _rebuild(silver) as con:
        changed = _features(con, week)
    assert (changed["clicks_to_date"] - original["clicks_to_date"]).sum() > 0


def test_click_features_match_the_weekly_fact(pipeline: PipelineRun) -> None:
    with gold(pipeline) as con:
        mismatches = con.execute(
            """
            SELECT count(*) FROM mart_early_warning_features m
            LEFT JOIN (
                SELECT f.student_key, w.as_of_week, sum(f.total_clicks) AS clicks
                FROM fact_weekly_engagement f
                JOIN (SELECT DISTINCT as_of_week FROM mart_early_warning_features) w
                    ON f.week < w.as_of_week
                GROUP BY ALL
            ) x USING (student_key, as_of_week)
            WHERE m.clicks_to_date <> coalesce(x.clicks, 0)
            """
        ).fetchone()
    assert mismatches == (0,)
