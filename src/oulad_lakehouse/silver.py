"""Silver: typed, validated, de-duplicated tables, built with PySpark in local mode.

For each presentation partition that changed (see incremental.py), every table goes through:

1. contract checks (contracts.py): "?" and "" become NULL, values are typed, and each failed
   rule adds a reason string to the row;
2. reference checks: foreign keys must resolve in the parent table of the same partition;
3. quarantine: rows with at least one reason go to `silver/_quarantine` with their raw values;
4. de-duplication: latest delivery per primary key, or exact duplicates dropped (studentVle);
5. a DQ record per table and partition: rows in, rows out, quarantined, duplicates removed,
   reasons and null counts. rows_in = rows_out + quarantined + duplicates_removed, always.

Partitions are replaced by deleting their folder and appending the new rows, so a rebuild of
2014B never touches 2013J. The same functions run on a cluster; only the session changes.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import reduce
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import storage
from .config import Layout
from .contracts import CONTRACTS, MISSING_TOKENS, Contract, Field
from .contracts import reason_format as r_format
from .contracts import reason_missing as r_missing
from .contracts import reason_range as r_range
from .contracts import reason_type as r_type
from .contracts import reason_unexpected as r_unexpected
from .incremental import bronze_fingerprints, changed_partitions, digest
from .tables import METADATA_COLUMNS, TABLE_ORDER, UNKNOWN_PARTITION

log = logging.getLogger(__name__)

STATE_FILE = "_state.json"
QUARANTINE = "_quarantine"
DQ_DIR = "_dq"
LINEAGE = ("_source_file", "_file_hash", "_ingested_at")


@dataclass
class SilverResult:
    partitions: list[str]
    dq: dict[str, dict[str, Any]] = field(default_factory=dict)


def spark_session(lake: str = "") -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("oulad-silver")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    )
    if lake.startswith("s3://"):
        for key, value in storage.spark_s3_conf().items():
            builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def partition_key(col: Column) -> Column:
    """Same rule as incremental.partition_key_sql, so planner and builder agree."""
    trimmed = F.trim(col)
    return F.when(trimmed.isNull() | (trimmed == ""), F.lit(UNKNOWN_PARTITION)).otherwise(trimmed)


def _value(f: Field) -> Column:
    """Raw string -> trimmed, missing tokens as NULL, normalised. Still a string."""
    raw = F.trim(F.col(f"_raw_{f.name}"))
    value = F.when(~(raw.isNull() | raw.isin(*MISSING_TOKENS)), raw)
    if f.normalize == "percent_band":
        value = F.when(value.rlike(r"^\d+-\d+$"), F.concat(value, F.lit("%"))).otherwise(value)
    return value


def _cast(f: Field) -> Column:
    value = F.col(f"_v_{f.name}")
    if f.dtype == "str":
        return value
    spark_type, regex = (
        ("bigint", r"^-?\d+$") if f.dtype == "int" else ("double", r"^-?\d+(\.\d+)?$")
    )
    # The regex stops Spark from quietly truncating "12.5" to 12; the cast returning NULL
    # catches integers too large for the type.
    return F.when(value.rlike(regex), value.cast(spark_type))


def _checks(f: Field) -> list[tuple[Column, str]]:
    """(failed condition, reason) pairs, written against already computed columns."""
    value, typed = F.col(f"_v_{f.name}"), F.col(f.name)
    present = typed.isNotNull()
    checks: list[tuple[Column, str]] = []
    if not f.nullable:
        checks.append((value.isNull(), r_missing(f.name)))
    if f.dtype != "str":
        checks.append((value.isNotNull() & typed.isNull(), r_type(f.name, f.dtype)))
    if f.pattern:
        checks.append((present & ~typed.rlike(f.pattern), r_format(f.name)))
    if f.allowed:
        checks.append((present & ~typed.isin(*f.allowed), r_unexpected(f.name)))
    bounds = []
    if f.min is not None:
        bounds.append(typed < F.lit(f.min))
    if f.max is not None:
        bounds.append(typed > F.lit(f.max))
    if bounds:
        checks.append((present & reduce(lambda a, b: a | b, bounds), r_range(f.name, f.min, f.max)))
    return checks


def apply_contract(df: DataFrame, contract: Contract) -> DataFrame:
    """Raw strings move to `_raw_<col>`; `<col>` becomes typed; `_reasons` lists failures.

    Built in three projections (raw -> cleaned string -> typed -> reasons) so each expression
    is computed once; one nested expression would make Spark's plan and generated code explode.
    """
    keep = [*METADATA_COLUMNS, "_partition"]
    raw_cols = [f"_raw_{c}" for c in contract.columns]
    df = df.select(*[F.col(c).alias(f"_raw_{c}") for c in contract.columns], *keep)
    df = df.select(*raw_cols, *keep, *[_value(f).alias(f"_v_{f.name}") for f in contract.fields])
    df = df.select("*", *[_cast(f).alias(f.name) for f in contract.fields])
    conditions = [
        F.when(cond, F.lit(reason)) for f in contract.fields for cond, reason in _checks(f)
    ]
    reasons = F.filter(F.array(*conditions), lambda x: x.isNotNull())
    return df.select(*raw_cols, *keep, *contract.columns, reasons.alias("_reasons"))


def check_references(df: DataFrame, contract: Contract, parents: dict[str, DataFrame]) -> DataFrame:
    """Add a reason when a foreign key is present but has no row in the parent table."""
    for i, ref in enumerate(contract.references):
        flag = f"_ref_{i}"
        parent = parents[ref.parent].select(*ref.columns).distinct().withColumn(flag, F.lit(True))
        df = df.join(parent, on=list(ref.columns), how="left")
        keys_present = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in ref.columns])
        unresolved = keys_present & F.col(flag).isNull()
        df = df.withColumn(
            "_reasons",
            F.when(unresolved, F.concat("_reasons", F.array(F.lit(ref.reason)))).otherwise(
                F.col("_reasons")
            ),
        ).drop(flag)
    return df


def deduplicate(valid: DataFrame, contract: Contract) -> DataFrame:
    """'latest': newest delivery per key wins. 'exact': first delivery of identical rows wins."""
    order = [F.col("_ingested_at"), F.col("_source_file"), F.col("_row_number")]
    if contract.dedup == "latest":
        order = [c.desc() for c in order]
    window = Window.partitionBy(*contract.primary_key).orderBy(*order)
    return valid.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def read_bronze(
    spark: SparkSession, bronze: str, table: str, hashes: list[str] | None = None
) -> DataFrame:
    """Read a bronze table, or only the files with these content hashes (file pruning)."""
    if not hashes:
        return spark.read.parquet(storage.spark_path(storage.join(bronze, table)))
    paths = [storage.spark_path(storage.join(bronze, table, f"{h[:16]}.parquet")) for h in hashes]
    return spark.read.parquet(*paths)


def files_for(fingerprints: dict[str, list[str]], partitions: list[str]) -> dict[str, list[str]]:
    """table -> bronze file hashes that feed at least one of `partitions`."""
    out: dict[str, set[str]] = {}
    for p in partitions:
        for item in fingerprints.get(p, []):
            table, file_hash = item.split(":", 1)
            out.setdefault(table, set()).add(file_hash)
    return {t: sorted(h) for t, h in out.items()}


def with_partition(df: DataFrame, table: str, raw_assessments: DataFrame) -> DataFrame:
    if table != "studentAssessment":
        return df.withColumn("_partition", partition_key(F.col("code_presentation")))
    lookup = raw_assessments.groupBy(F.trim("id_assessment").alias("_aid")).agg(
        F.min("code_presentation").alias("_cp")
    )
    joined = df.join(lookup, F.trim(df["id_assessment"]) == lookup["_aid"], "left")
    return joined.withColumn("_partition", partition_key(F.col("_cp"))).drop("_aid", "_cp")


@dataclass
class TableBuild:
    clean: DataFrame
    quarantine: DataFrame
    dq: dict[str, dict[str, Any]]
    cached: list[DataFrame]


def _dq_metrics(checked: DataFrame, clean: DataFrame, columns: list[str]) -> dict[str, dict]:
    """Per partition: counts, reasons and null counts, in two small Spark jobs.

    Grouping by the whole reasons array gives one group for all valid rows (empty array) and
    a few for the rejected ones, so counts and reasons come out of the same job.
    """
    out: dict[str, dict[str, Any]] = {}
    for row in checked.groupBy("_partition", "_reasons").count().collect():
        entry = out.setdefault(row["_partition"], _empty_dq(columns))
        entry["rows_in"] += row["count"]
        if row["_reasons"]:
            entry["quarantined"] += row["count"]
            for reason in row["_reasons"]:
                entry["reasons"][reason] = entry["reasons"].get(reason, 0) + row["count"]
    nulls = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]
    for row in clean.groupBy("_partition").agg(F.count(F.lit(1)).alias("_n"), *nulls).collect():
        entry = out[row["_partition"]]
        entry["rows_out"] = row["_n"]
        entry["null_counts"] = {c: row[c] for c in columns}
    for entry in out.values():
        entry["duplicates_removed"] = entry["rows_in"] - entry["quarantined"] - entry["rows_out"]
    return out


def build_table(
    table: str,
    bronze_df: DataFrame,
    raw_assessments: DataFrame,
    partitions: list[str],
    parents: dict[str, DataFrame],
) -> TableBuild:
    contract = CONTRACTS[table]
    df = with_partition(bronze_df, table, raw_assessments).filter(
        F.col("_partition").isin(partitions)
    )
    checked = apply_contract(df, contract)
    columns = list(contract.columns)
    if table == "studentAssessment":
        # OULAD keeps the presentation on the assessment, not the submission; bring it over.
        checked = checked.join(
            parents["assessments"].select("id_assessment", "code_module", "code_presentation"),
            "id_assessment",
            "left",
        )
        columns += ["code_module", "code_presentation"]
    checked = check_references(checked, contract, parents).cache()
    valid = checked.filter(F.size("_reasons") == 0)
    clean = deduplicate(valid, contract).cache()
    dq = _dq_metrics(checked, clean, columns)
    raw = F.struct(*[F.col(f"_raw_{c}").alias(c) for c in contract.columns])
    quarantine = checked.filter(F.size("_reasons") > 0).select(
        F.lit(table).alias("table_name"),
        F.col("_partition").alias("code_presentation"),
        F.concat_ws("; ", "_reasons").alias("reasons"),
        F.to_json(raw).alias("record"),
        *METADATA_COLUMNS,
    )
    out_cols = [c for c in columns if c != "code_presentation"]
    clean_out = clean.select(
        *out_cols, *LINEAGE, "_partition", F.col("_partition").alias("code_presentation")
    )
    return TableBuild(clean_out, quarantine, dq, [checked, clean])


def _empty_dq(columns: list[str]) -> dict[str, Any]:
    return {
        "rows_in": 0,
        "rows_out": 0,
        "quarantined": 0,
        "duplicates_removed": 0,
        "reasons": {},
        "null_counts": dict.fromkeys(columns, 0),
    }


def _replace_partitions(df: DataFrame, uri: str, folders: list[str], by: list[str]) -> None:
    for folder in folders:
        storage.remove(storage.join(uri, folder))
    (df.repartition(*by).write.mode("append").partitionBy(*by).parquet(storage.spark_path(uri)))


def build_silver(
    spark: SparkSession,
    bronze: str,
    silver: str,
    partitions: list[str],
    files: dict[str, list[str]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Rebuild `partitions` of every silver table and the quarantine. Returns DQ by partition.

    With `files`, Spark reads only the bronze files that feed these partitions; the filter on
    the partition key still runs, because one file can hold several presentations.
    """
    files = files or {}
    raw = {t: read_bronze(spark, bronze, t, files.get(t)) for t in TABLE_ORDER}
    # The id_assessment -> presentation lookup always needs every assessments file.
    raw_assessments = read_bronze(spark, bronze, "assessments")
    parents: dict[str, DataFrame] = {}
    quarantines: list[DataFrame] = []
    cached: list[DataFrame] = []
    dq: dict[str, dict[str, Any]] = {p: {} for p in partitions}
    for table in TABLE_ORDER:
        start = time.perf_counter()
        built = build_table(table, raw[table], raw_assessments, partitions, parents)
        parents[table] = built.clean
        quarantines.append(built.quarantine)
        cached += built.cached
        for p in partitions:
            dq[p][table] = built.dq.get(p, _empty_dq(list(CONTRACTS[table].columns)))
        _replace_partitions(
            built.clean.drop("_partition"),
            storage.join(silver, table),
            [f"code_presentation={p}" for p in partitions],
            ["code_presentation"],
        )
        rows = sum(built.dq.get(p, {}).get("rows_out", 0) for p in partitions)
        log.info("silver %s: %d rows in %.1fs", table, rows, time.perf_counter() - start)
    _replace_partitions(
        reduce(DataFrame.unionByName, quarantines),
        storage.join(silver, QUARANTINE),
        [f"table_name={t}/code_presentation={p}" for t in TABLE_ORDER for p in partitions],
        ["table_name", "code_presentation"],
    )
    for frame in cached:
        frame.unpersist()
    return dq


def run_silver(layout: Layout, full_refresh: bool = False) -> SilverResult:
    """Plan from bronze fingerprints, rebuild what changed, then record DQ and state."""
    fingerprints = bronze_fingerprints(layout.bronze)
    seen = {item.split(":", 1)[0] for items in fingerprints.values() for item in items}
    missing = [t for t in TABLE_ORDER if t not in seen]
    if missing:
        raise RuntimeError(f"bronze has no files for {missing}; run `oulad ingest` first")
    current = {p: digest(items) for p, items in fingerprints.items()}
    state_uri = storage.join(layout.silver, STATE_FILE)
    built = {} if full_refresh else storage.read_json(state_uri, {})
    todo = changed_partitions(current, built)
    if not todo:
        return SilverResult([])
    spark = spark_session(layout.lake)
    dq = build_silver(spark, layout.bronze, layout.silver, todo, files_for(fingerprints, todo))
    now = datetime.now(UTC).isoformat()
    for p in todo:
        storage.write_json(
            storage.join(layout.silver, DQ_DIR, f"{p}.json"),
            {"partition": p, "built_at": now, "input_digest": current[p], "tables": dq[p]},
        )
    storage.write_json(state_uri, {**built, **{p: current[p] for p in todo}})
    return SilverResult(todo, dq)


def read_dq(silver: str) -> dict[str, dict[str, Any]]:
    """The latest DQ record of every partition (each is replaced when its partition rebuilds)."""
    folder = storage.join(silver, DQ_DIR)
    fs = storage.filesystem(folder)
    if not fs.exists(folder):
        return {}
    records = []
    for path in sorted(fs.glob(storage.join(folder, "*.json"))):
        with fs.open(path, "r") as fh:
            records.append(json.load(fh))
    return {r["partition"]: r for r in records}
