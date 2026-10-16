"""Synthetic data and bronze ingestion (no Spark needed)."""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from oulad_lakehouse.bronze import SchemaMismatchError, file_hash, ingest, read_manifest
from oulad_lakehouse.synthetic import generate
from oulad_lakehouse.tables import METADATA_COLUMNS, TABLE_COLUMNS


@pytest.fixture
def landing(tmp_path: Path) -> Path:
    generate(tmp_path / "landing", students_per_presentation=20, seed=1)
    return tmp_path / "landing"


def test_generator_writes_exact_oulad_headers(landing: Path) -> None:
    for folder in ("2013J", "2014B", "2014J"):
        for table, columns in TABLE_COLUMNS.items():
            header = (landing / folder / f"{table}.csv").read_text().splitlines()[0]
            assert tuple(header.split(",")) == columns


def test_generator_is_deterministic(tmp_path: Path) -> None:
    def hashes(seed: int, name: str) -> dict[str, str]:
        out = tmp_path / name
        generate(out, students_per_presentation=20, seed=seed)
        return {p.name: file_hash(p) for p in sorted(out.rglob("*.csv"))}

    assert hashes(3, "a") == hashes(3, "b")
    assert hashes(3, "a") != hashes(4, "c")


def test_bronze_keeps_raw_strings_and_lineage(landing: Path, tmp_path: Path) -> None:
    bronze = str(tmp_path / "bronze")
    result = ingest(landing, bronze)
    assert len(result.ingested) == 21  # 7 tables x 3 presentations
    entry = next(e for e in result.ingested if e["source_file"] == "2013J/studentInfo.csv")
    table = pq.read_table(entry["bronze_path"])
    assert set(METADATA_COLUMNS) <= set(table.column_names)
    imd = table.column("imd_band").to_pylist()
    # "?" and the inconsistent "10-20" band survive untouched; silver fixes them, not bronze.
    assert all(isinstance(v, str) for v in imd)
    assert table.column("_file_hash").to_pylist()[0] == entry["file_hash"]
    assert table.column("_row_number").to_pylist()[:3] == [1, 2, 3]


def test_reingest_is_idempotent_even_under_a_new_name(landing: Path, tmp_path: Path) -> None:
    bronze = str(tmp_path / "bronze")
    ingest(landing, bronze)
    files_before = sorted(Path(bronze).rglob("*.parquet"))
    shutil.copy(landing / "2013J" / "studentVle.csv", landing / "2013J" / "studentVle__copy.csv")
    again = ingest(landing, bronze)
    assert again.ingested == []
    assert len(again.skipped) == 22
    assert sorted(Path(bronze).rglob("*.parquet")) == files_before
    assert len(read_manifest(bronze)) == 21


def test_wrong_header_is_rejected_before_anything_is_written(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "studentVle.csv").write_text("code_module,id_student,clicks\nAAA,1,3\n")
    bronze = str(tmp_path / "bronze")
    with pytest.raises(SchemaMismatchError, match="missing columns"):
        ingest(landing, bronze)
    assert read_manifest(bronze) == []
    assert not list(tmp_path.rglob("*.parquet"))


def test_unknown_files_are_ignored(landing: Path, tmp_path: Path) -> None:
    (landing / "notes.csv").write_text("a,b\n1,2\n")
    result = ingest(landing, str(tmp_path / "bronze"))
    assert result.ignored == ["notes.csv"]
