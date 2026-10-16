"""Bronze: land raw CSVs as Parquet, unchanged, with lineage columns.

Every value is kept as a string. Bronze is the "what we received" record: "?" stays "?", a
broken number stays broken, so silver can explain why it rejected a row and a fixed contract can
be replayed from bronze without asking the source again.

Idempotency is by content: a file's SHA-256 is its identity. A file already in the manifest is
skipped, even under a new name, so retries and repeated drops never duplicate data.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import fsspec
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from .storage import filesystem, join
from .tables import TABLE_COLUMNS, table_for_file

MANIFEST = "_manifest.jsonl"


class SchemaMismatchError(ValueError):
    """The file header is not the OULAD header for its table; nothing is written."""


@dataclass
class IngestResult:
    ingested: list[dict] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    ignored: list[str] = field(default_factory=list)


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_manifest(bronze: str) -> list[dict]:
    uri = join(bronze, MANIFEST)
    if not filesystem(uri).exists(uri):
        return []
    with fsspec.open(uri, "r") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _append_manifest(bronze: str, entry: dict) -> None:
    entries = [*read_manifest(bronze), entry]
    uri = join(bronze, MANIFEST)
    filesystem(uri).makedirs(bronze, exist_ok=True)
    with fsspec.open(uri, "w") as fh:
        fh.writelines(json.dumps(e, sort_keys=True) + "\n" for e in entries)


def read_raw_csv(path: Path, table: str) -> pa.Table:
    """Read every column as a string and keep empty cells as "" (not NULL)."""
    expected = TABLE_COLUMNS[table]
    with path.open() as fh:
        header = [h.strip().strip('"') for h in fh.readline().rstrip("\r\n").split(",")]
    if sorted(header) != sorted(expected):
        missing = sorted(set(expected) - set(header))
        extra = sorted(set(header) - set(expected))
        raise SchemaMismatchError(f"{path.name}: missing columns {missing}, unexpected {extra}")
    return pacsv.read_csv(
        path,
        convert_options=pacsv.ConvertOptions(
            column_types={c: pa.string() for c in header}, strings_can_be_null=False
        ),
    ).select(list(expected))


def _with_lineage(raw: pa.Table, source: str, digest: str, now: datetime) -> pa.Table:
    n = raw.num_rows
    return (
        raw.append_column("_ingested_at", pa.array([now] * n, pa.timestamp("us", tz="UTC")))
        .append_column("_source_file", pa.array([source] * n, pa.string()))
        .append_column("_file_hash", pa.array([digest] * n, pa.string()))
        .append_column("_row_number", pa.array(range(1, n + 1), pa.int64()))
    )


def ingest(landing: Path, bronze: str) -> IngestResult:
    """Ingest every CSV under `landing` (recursively) that bronze has not seen yet."""
    result = IngestResult()
    seen = {e["file_hash"] for e in read_manifest(bronze)}
    fs = filesystem(bronze)
    for path in sorted(landing.rglob("*.csv")):
        source = path.relative_to(landing).as_posix()
        table = table_for_file(path.stem)
        if table is None:
            result.ignored.append(source)
            continue
        digest = file_hash(path)
        if digest in seen:
            result.skipped.append(source)
            continue
        raw = read_raw_csv(path, table)
        now = datetime.now(UTC)
        target = join(bronze, table, f"{digest[:16]}.parquet")
        fs.makedirs(join(bronze, table), exist_ok=True)
        # Parquet first, manifest second: a crash in between leaves a file that the retry
        # overwrites at the same content-addressed path.
        pq.write_table(_with_lineage(raw, source, digest, now), target, filesystem=fs)
        entry = {
            "file_hash": digest,
            "table": table,
            "source_file": source,
            "rows": raw.num_rows,
            "ingested_at": now.isoformat(),
            "bronze_path": target,
        }
        _append_manifest(bronze, entry)
        seen.add(digest)
        result.ingested.append(entry)
    return result
