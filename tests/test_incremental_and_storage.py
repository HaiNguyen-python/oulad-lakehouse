"""Partition planning and the storage layer, including a non-local (in-memory) lake."""

from __future__ import annotations

from pathlib import Path

import fsspec
import pandas as pd

from oulad_lakehouse.bronze import ingest
from oulad_lakehouse.incremental import bronze_fingerprints, changed_partitions, digest
from oulad_lakehouse.storage import spark_path, spark_s3_conf
from oulad_lakehouse.synthetic import generate
from oulad_lakehouse.tables import UNKNOWN_PARTITION


def test_fingerprints_map_submissions_through_assessments(tmp_path: Path) -> None:
    summary = generate(tmp_path / "landing", students_per_presentation=15, seed=2)
    bronze = str(tmp_path / "bronze")
    ingest(tmp_path / "landing", bronze)
    prints = bronze_fingerprints(bronze)
    assert set(prints) == {*summary["presentations"], UNKNOWN_PARTITION}
    # Each presentation is fed by its own 7 files and nothing else.
    for p in summary["presentations"]:
        assert len(prints[p]) == 7
        assert all(len(item.split(":")) == 2 for item in prints[p])
    # Planted submissions to a non-existent assessment have no presentation to go to.
    assert [i.split(":")[0] for i in prints[UNKNOWN_PARTITION]] == ["studentAssessment"] * 3


def test_late_file_changes_only_its_partition(tmp_path: Path) -> None:
    landing, bronze = tmp_path / "landing", str(tmp_path / "bronze")
    generate(landing, students_per_presentation=15, seed=2)
    ingest(landing, bronze)
    before = {p: digest(i) for p, i in bronze_fingerprints(bronze).items()}
    assert changed_partitions(before, before) == []
    rows = pd.read_csv(landing / "2014B" / "studentVle.csv").head(5)
    rows.to_csv(landing / "2014B" / "studentVle__late_1.csv", index=False)
    ingest(landing, bronze)
    after = {p: digest(i) for p, i in bronze_fingerprints(bronze).items()}
    assert changed_partitions(after, before) == ["2014B"]
    assert changed_partitions(after, {}) == sorted(after)


def test_bronze_and_planner_work_on_a_non_local_filesystem(tmp_path: Path) -> None:
    """memory:// stands in for s3:// here: same fsspec code path, no network."""
    landing = tmp_path / "landing"
    generate(landing, students_per_presentation=10, seed=5)
    lake = "memory://oulad-test/bronze"
    result = ingest(landing, lake)
    assert len(result.ingested) == 21
    assert fsspec.filesystem("memory").exists("/oulad-test/bronze/_manifest.jsonl")
    assert "2014J" in bronze_fingerprints(lake)


def test_spark_s3_settings_for_minio() -> None:
    assert spark_path("s3://lake/silver") == "s3a://lake/silver"
    conf = spark_s3_conf(
        {
            "AWS_ACCESS_KEY_ID": "k",
            "AWS_SECRET_ACCESS_KEY": "s",
            "AWS_ENDPOINT_URL": "http://localhost:9000",
        }
    )
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://localhost:9000"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert "spark.hadoop.fs.s3a.access.key" not in spark_s3_conf({})
