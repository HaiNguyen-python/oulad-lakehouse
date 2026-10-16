"""Silver contracts, quarantine, de-duplication and partition-only rebuilds (local Spark)."""

from __future__ import annotations

import json

import duckdb
import pytest
from conftest import PipelineRun

from oulad_lakehouse.silver import QUARANTINE, read_dq
from oulad_lakehouse.synthetic import load_summary

pytestmark = pytest.mark.spark


def _silver(run: PipelineRun, sql: str) -> list[tuple]:
    silver = run.layout.silver
    with duckdb.connect() as con:
        for name in ("studentInfo", "studentAssessment", "studentVle", QUARANTINE):
            depth = "*/*/*.parquet" if name == QUARANTINE else "*/*.parquet"
            con.execute(
                f"CREATE VIEW {name} AS SELECT * FROM "
                f"read_parquet('{silver}/{name}/{depth}', hive_partitioning = true)"
            )
        return con.execute(sql).fetchall()


def test_contract_violations_land_in_quarantine_with_reasons(pipeline: PipelineRun) -> None:
    planted = load_summary(pipeline.layout.landing)["quarantine"]
    rows = _silver(
        pipeline,
        f"SELECT table_name, reasons, count(*) FROM {QUARANTINE} GROUP BY ALL ORDER BY ALL",
    )
    caught = {(t, r): n for t, r, n in rows}
    expected = {(t, r): n for t, reasons in planted.items() for r, n in reasons.items()}
    assert caught == expected  # every planted defect, each with exactly one reason
    record = _silver(
        pipeline, f"SELECT record FROM {QUARANTINE} WHERE reasons = 'sum_click: below 1' LIMIT 1"
    )[0][0]
    assert json.loads(record)["sum_click"] == "-4"  # raw value kept for debugging


def test_question_marks_become_nulls_and_imd_band_is_normalised(pipeline: PipelineRun) -> None:
    missing = load_summary(pipeline.layout.landing)["missing_tokens"]
    bands = {r[0] for r in _silver(pipeline, "SELECT DISTINCT imd_band FROM studentInfo")}
    assert "10-20" not in bands and "10-20%" in bands and "?" not in bands
    null_imd = _silver(pipeline, "SELECT count(*) FROM studentInfo WHERE imd_band IS NULL")
    assert null_imd[0][0] == missing["studentInfo.imd_band"]
    # A "?" score is a real submission with an unknown mark: kept, not quarantined.
    null_scores = _silver(pipeline, "SELECT count(*) FROM studentAssessment WHERE score IS NULL")
    assert null_scores[0][0] == missing["studentAssessment.score"]


def test_duplicates_removed_and_latest_correction_wins(pipeline: PipelineRun) -> None:
    summary = load_summary(pipeline.layout.landing)
    dq = read_dq(pipeline.layout.silver)
    removed = sum(r["tables"]["studentVle"]["duplicates_removed"] for r in dq.values())
    assert removed == summary["duplicates"]["studentVle"]  # incl. rows re-sent in the late file
    exact = _silver(
        pipeline,
        "SELECT count(*) FROM (SELECT code_presentation, id_student, id_site, date, sum_click "
        "FROM studentVle GROUP BY ALL HAVING count(*) > 1)",
    )
    assert exact[0][0] == 0
    credits = _silver(
        pipeline,
        "SELECT studied_credits, _source_file FROM studentInfo "
        f"WHERE id_student = {pipeline.corrected_student} AND code_presentation = '2014B'",
    )
    assert credits == [(240, "2014B/studentInfo__fix_1.csv")]
    assert dq["2014B"]["tables"]["studentInfo"]["duplicates_removed"] == 1


def test_dq_report_accounts_for_every_bronze_row(pipeline: PipelineRun) -> None:
    dq = read_dq(pipeline.layout.silver)
    assert set(dq) == {"2013J", "2014B", "2014J", "__unknown__"}
    for record in dq.values():
        for m in record["tables"].values():
            assert m["rows_in"] == m["rows_out"] + m["quarantined"] + m["duplicates_removed"]
            assert all(0 <= n <= m["rows_out"] for n in m["null_counts"].values())
    vle = dq["2013J"]["tables"]["vle"]
    assert vle["null_counts"]["week_from"] > 0  # "?" week ranges, as in the real vle.csv


def test_rerun_with_nothing_new_rebuilds_nothing(pipeline: PipelineRun) -> None:
    rerun = {s["step"]: s for s in pipeline.runs[1]["steps"]}
    assert rerun["ingest"]["files_ingested"] == 0
    assert rerun["silver"]["partitions_rebuilt"] == []
    assert rerun["gold"]["partitions_rebuilt"] == []


def test_late_file_rebuilds_only_its_partition(pipeline: PipelineRun) -> None:
    late = {s["step"]: s for s in pipeline.runs[2]["steps"]}
    assert late["ingest"]["files_ingested"] == 2
    assert late["silver"]["partitions_rebuilt"] == ["2014B"]
    assert late["gold"]["partitions_rebuilt"] == ["2014B"]
    before, after = pipeline.silver_files_before_late, pipeline.silver_files_after_late
    untouched = {k: v for k, v in before.items() if "2014B" not in k}
    assert all(after.get(k) == v for k, v in untouched.items())  # same files, same mtimes
    rebuilt = [k for k in before if "code_presentation=2014B" in k]
    assert rebuilt and not any(k in after for k in rebuilt)
    # Gold rows for 2013J are identical; 2014B gained exactly the new rows of the late file.
    assert pipeline.gold_2013j_before_late is not None
    assert pipeline.gold_2013j_before_late.equals(pipeline.gold_2013j_after_late)
    before_vle = pipeline.dq_2014b_before_late["tables"]["studentVle"]
    after_vle = read_dq(pipeline.layout.silver)["2014B"]["tables"]["studentVle"]
    assert after_vle["rows_out"] == before_vle["rows_out"] + pipeline.late_file["new_rows"]
    assert after_vle["duplicates_removed"] == (
        before_vle["duplicates_removed"] + pipeline.late_file["resent_rows"]
    )
