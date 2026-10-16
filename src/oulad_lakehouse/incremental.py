"""Incremental planning: which presentations need rebuilding?

Silver and gold are partitioned by `code_presentation` (2013J, 2014B, ...). A partition's
*fingerprint* is the sorted list of bronze files that contribute rows to it. If a late file for
2014B lands, only 2014B's fingerprint changes, so only 2014B is rebuilt. Comparing fingerprints
(not timestamps) makes reruns idempotent: same inputs, same fingerprints, nothing to do.

`studentAssessment` has no presentation column, so its rows are mapped through `assessments`
(id_assessment -> presentation). Rows that map nowhere go to the `__unknown__` partition, where
silver quarantines them.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping

import duckdb

from .storage import attach_lake, filesystem, join
from .tables import TABLE_ORDER, UNKNOWN_PARTITION

Fingerprints = dict[str, list[str]]


def _has_files(bronze: str, table: str) -> bool:
    folder = join(bronze, table)
    fs = filesystem(folder)
    return bool(fs.exists(folder) and fs.glob(join(folder, "*.parquet")))


def partition_key_sql(column: str) -> str:
    """Trimmed presentation code, or the unknown partition. Silver uses the same rule."""
    return f"coalesce(nullif(trim({column}), ''), '{UNKNOWN_PARTITION}')"


def bronze_fingerprints(bronze: str) -> Fingerprints:
    """partition -> sorted ['table:file_hash', ...] for every bronze file feeding it."""
    con = duckdb.connect()
    attach_lake(con, bronze)
    parts: list[str] = []
    for table in TABLE_ORDER:
        if not _has_files(bronze, table):
            continue
        src = f"read_parquet('{join(bronze, table, '*.parquet')}')"
        if table == "studentAssessment":
            if not _has_files(bronze, "assessments"):
                key = f"'{UNKNOWN_PARTITION}'"
                parts.append(
                    f"SELECT DISTINCT {key} AS p, '{table}:' || _file_hash AS f FROM {src}"
                )
                continue
            lookup = (
                "SELECT trim(id_assessment) AS id_assessment, min(code_presentation) AS cp "
                f"FROM read_parquet('{join(bronze, 'assessments', '*.parquet')}') GROUP BY 1"
            )
            parts.append(
                f"SELECT DISTINCT {partition_key_sql('a.cp')} AS p, "
                f"'{table}:' || s._file_hash AS f FROM {src} s "
                f"LEFT JOIN ({lookup}) a ON trim(s.id_assessment) = a.id_assessment"
            )
        else:
            parts.append(
                f"SELECT DISTINCT {partition_key_sql('code_presentation')} AS p, "
                f"'{table}:' || _file_hash AS f FROM {src}"
            )
    if not parts:
        return {}
    rows = con.execute(" UNION ALL ".join(parts)).fetchall()
    out: Fingerprints = {}
    for partition, item in rows:
        out.setdefault(partition, []).append(item)
    return {p: sorted(items) for p, items in sorted(out.items())}


def digest(items: list[str]) -> str:
    return hashlib.sha256("\n".join(items).encode()).hexdigest()[:16]


def changed_partitions(current: Mapping[str, str], built: Mapping[str, str]) -> list[str]:
    """Partitions whose input digest differs from the digest they were last built from."""
    return sorted(p for p, d in current.items() if built.get(p) != d)
