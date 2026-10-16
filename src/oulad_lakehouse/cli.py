"""Command line: `oulad generate | generate-late | ingest | silver | gold | run-all | report`.

Every step is a plain function with a JSON-friendly summary, so the Airflow DAG, the Makefile
and the tests all call exactly the same code.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import Layout

log = logging.getLogger("oulad")


def step_ingest(layout: Layout) -> dict[str, Any]:
    from .bronze import ingest

    result = ingest(layout.landing, layout.bronze)
    return {
        "files_ingested": len(result.ingested),
        "files_skipped": len(result.skipped),
        "rows_ingested": sum(e["rows"] for e in result.ingested),
    }


def step_silver(layout: Layout, full_refresh: bool = False) -> dict[str, Any]:
    from .silver import run_silver

    result = run_silver(layout, full_refresh)
    quarantined = sum(t["quarantined"] for d in result.dq.values() for t in d.values())
    return {"partitions_rebuilt": result.partitions, "rows_quarantined": quarantined}


def step_gold(layout: Layout, full_refresh: bool = False) -> dict[str, Any]:
    from .gold import run_gold

    result = run_gold(layout, full_refresh)
    return {
        "partitions_rebuilt": result.partitions,
        "reconciliation_checks_passed": sum(c.ok for c in result.checks),
        "row_counts": result.row_counts,
    }


def timed(name: str, fn: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    start = time.perf_counter()
    summary = fn()
    summary = {"step": name, "seconds": round(time.perf_counter() - start, 2), **summary}
    log.info("%s", json.dumps(summary))
    return summary


def run_all(layout: Layout, label: str, full_refresh: bool = False) -> dict[str, Any]:
    """ingest -> silver -> gold, timed, appended to the run log that `oulad report` reads."""
    started = datetime.now(UTC).isoformat()
    steps = [
        timed("ingest", lambda: step_ingest(layout)),
        timed("silver", lambda: step_silver(layout, full_refresh)),
        timed("gold", lambda: step_gold(layout, full_refresh)),
    ]
    record = {"label": label, "started_at": started, "steps": steps}
    layout.run_log.parent.mkdir(parents=True, exist_ok=True)
    with layout.run_log.open("a") as fh:
        fh.write(json.dumps(record) + "\n")
    return record


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="oulad", description=__doc__)
    p.add_argument("--data-dir", help="local data folder (default: $OULAD_DATA_DIR or ./data)")
    p.add_argument("--lake", help="bronze/silver root, local or s3:// (default: <data-dir>/lake)")
    sub = p.add_subparsers(dest="command", required=True)
    g = sub.add_parser("generate", help="write a seeded synthetic OULAD to <data-dir>/landing")
    g.add_argument("--students", type=int, default=1000, help="students per presentation")
    g.add_argument("--seed", type=int, default=42)
    late = sub.add_parser("generate-late", help="add a late studentVle file for one presentation")
    late.add_argument("--presentation", required=True)
    late.add_argument("--seed", type=int, default=7)
    sub.add_parser("ingest", help="landing CSV -> bronze Parquet (skips files already seen)")
    for name in ("silver", "gold", "run-all"):
        s = sub.add_parser(name)
        s.add_argument("--full-refresh", action="store_true", help="rebuild every partition")
        if name == "run-all":
            s.add_argument("--label", default="manual run", help="name shown in the report")
    r = sub.add_parser("report", help="write docs/results.md and docs/img/*.png")
    r.add_argument("--out", default="docs", type=Path)
    r.add_argument("--data-label", help="defaults to a description of the synthetic data")
    return p


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parser().parse_args(argv)
    layout = Layout.from_env(args.data_dir, args.lake)
    if args.command == "generate":
        from .synthetic import generate

        summary = generate(layout.landing, args.students, args.seed)
        log.info("synthetic OULAD written to %s (seed %s)", layout.landing, summary["seed"])
    elif args.command == "generate-late":
        from .synthetic import generate_late

        late = generate_late(layout.landing, args.presentation, args.seed)["late_files"][-1]
        log.info("late file for %s: %s", args.presentation, json.dumps(late))
    elif args.command == "ingest":
        timed("ingest", lambda: step_ingest(layout))
    elif args.command == "silver":
        timed("silver", lambda: step_silver(layout, args.full_refresh))
    elif args.command == "gold":
        timed("gold", lambda: step_gold(layout, args.full_refresh))
    elif args.command == "run-all":
        run_all(layout, args.label, args.full_refresh)
    elif args.command == "report":
        from .report import write_report

        path = write_report(layout, args.out, args.data_label)
        log.info("report written to %s", path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
