"""Thin storage layer so the same code writes to a local folder or an S3-compatible bucket.

Python-side I/O (bronze Parquet, manifests, state, DQ JSON) goes through fsspec. Spark and
DuckDB need their own settings for S3; the helpers here build them from environment variables.
The local path is what CI tests; the S3 path is documented in the README and was not run
against a live bucket in this repo.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import duckdb
import fsspec
from fsspec import AbstractFileSystem


def is_local(uri: str) -> bool:
    return "://" not in uri or uri.startswith("file://")


def join(uri: str, *parts: str) -> str:
    """Join path parts with '/', keeping a URL scheme such as s3:// intact."""
    return "/".join([uri.rstrip("/"), *(p.strip("/") for p in parts)])


def filesystem(uri: str) -> AbstractFileSystem:
    fs, _ = fsspec.core.url_to_fs(uri)
    return fs


def exists(uri: str) -> bool:
    return bool(filesystem(uri).exists(uri))


def remove(uri: str) -> None:
    fs = filesystem(uri)
    if fs.exists(uri):
        fs.rm(uri, recursive=True)


def read_json(uri: str, default: Any) -> Any:
    if not exists(uri):
        return default
    with fsspec.open(uri, "r") as fh:
        return json.load(fh)


def write_json(uri: str, payload: Any) -> None:
    fs = filesystem(uri)
    parent = uri.rsplit("/", 1)[0]
    fs.makedirs(parent, exist_ok=True)
    with fsspec.open(uri, "w") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)


def spark_path(uri: str) -> str:
    """Spark's Hadoop S3 connector uses the s3a:// scheme; local paths must be absolute."""
    if uri.startswith("s3://"):
        return "s3a://" + uri.removeprefix("s3://")
    return uri if not is_local(uri) else str(Path(uri.removeprefix("file://")).resolve())


def spark_s3_conf(env: Mapping[str, str] = os.environ) -> dict[str, str]:
    """Spark settings for an S3-compatible endpoint (MinIO needs path-style access)."""
    conf = {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
    }
    if "AWS_ACCESS_KEY_ID" in env:
        conf["spark.hadoop.fs.s3a.access.key"] = env["AWS_ACCESS_KEY_ID"]
        conf["spark.hadoop.fs.s3a.secret.key"] = env.get("AWS_SECRET_ACCESS_KEY", "")
    if "AWS_ENDPOINT_URL" in env:
        endpoint = env["AWS_ENDPOINT_URL"]
        conf["spark.hadoop.fs.s3a.endpoint"] = endpoint
        conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(
            endpoint.startswith("https")
        ).lower()
    return conf


def attach_lake(con: duckdb.DuckDBPyConnection, uri: str) -> None:
    """Let DuckDB read a non-local lake through the same fsspec filesystem Python uses."""
    if not is_local(uri):
        con.register_filesystem(filesystem(uri))
