"""Where every layer lives, in one place.

Two roots, because they have different needs:
- the *local data dir* holds landing CSVs, the DuckDB gold file and the run log. A DuckDB
  database file must be on a local disk to be written.
- the *lake URI* holds bronze and silver Parquet. It is a local folder by default and can be an
  `s3://bucket/prefix` URL (MinIO or AWS) with the `s3` extra installed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .storage import join

# Early-warning snapshots: features as known at the start of these course weeks.
AS_OF_WEEKS: tuple[int, ...] = (2, 4, 8, 12)


@dataclass(frozen=True)
class Layout:
    data_dir: Path
    lake: str

    @classmethod
    def from_env(cls, data_dir: str | Path | None = None, lake: str | None = None) -> Layout:
        base = Path(data_dir or os.environ.get("OULAD_DATA_DIR", "data")).resolve()
        lake_uri = lake or os.environ.get("OULAD_LAKE_URI") or str(base / "lake")
        return cls(base, lake_uri)

    @property
    def landing(self) -> Path:
        return self.data_dir / "landing"

    @property
    def bronze(self) -> str:
        return join(self.lake, "bronze")

    @property
    def silver(self) -> str:
        return join(self.lake, "silver")

    @property
    def gold_db(self) -> Path:
        return self.data_dir / "gold" / "oulad.duckdb"

    @property
    def gold_state(self) -> Path:
        return self.data_dir / "gold" / "_state.json"

    @property
    def run_log(self) -> Path:
        return self.data_dir / "runs.jsonl"
