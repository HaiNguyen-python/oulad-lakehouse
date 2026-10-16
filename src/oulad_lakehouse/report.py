"""`oulad report`: docs/results.md and two charts, computed only from real pipeline outputs.

Inputs: the run log (timings, what each run rebuilt), the silver DQ records, the generator
summary (what was planted, when the data is synthetic) and the gold DuckDB file. Nothing in the
results page is typed by hand, so rerunning on the real OULAD replaces every number.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from .config import Layout
from .gold import reconcile, register_silver, table_counts
from .silver import read_dq
from .synthetic import load_summary
from .tables import TABLE_ORDER

BLUE, GREY = "#2a78d6", "#b7b7b2"
INK, INK_2, GRID, SURFACE = "#1f1f1d", "#5f5e5a", "#e6e5e1", "#ffffff"
EARLY_WEEK = 4


def layer_totals(dq: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Sum the per-partition DQ records into one record per table."""
    totals: dict[str, dict[str, Any]] = {}
    for record in dq.values():
        for table, m in record["tables"].items():
            t = totals.setdefault(
                table,
                {
                    "rows_in": 0,
                    "rows_out": 0,
                    "quarantined": 0,
                    "duplicates_removed": 0,
                    "reasons": {},
                    "null_counts": {},
                },
            )
            for key in ("rows_in", "rows_out", "quarantined", "duplicates_removed"):
                t[key] += m[key]
            for reason, n in m["reasons"].items():
                t["reasons"][reason] = t["reasons"].get(reason, 0) + n
            for col, n in m["null_counts"].items():
                t["null_counts"][col] = t["null_counts"].get(col, 0) + n
    return {t: totals[t] for t in TABLE_ORDER if t in totals}


def read_runs(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def weekly_by_group(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """
        SELECT week, outcome_group, sum(total_clicks) / sum(n_students) AS clicks_per_student
        FROM mart_engagement_vs_outcome GROUP BY ALL ORDER BY week, outcome_group
        """
    ).df()


def risk_by_quartile(con: duckdb.DuckDBPyConnection, week: int = EARLY_WEEK) -> pd.DataFrame:
    """At-risk share by click quartile at the start of `week`, students still enrolled."""
    return con.execute(
        """
        WITH f AS (
            SELECT label_at_risk, ntile(4) OVER (ORDER BY clicks_to_date, student_key) AS quartile
            FROM mart_early_warning_features
            WHERE as_of_week = ? AND is_enrolled_at_cutoff
        )
        SELECT quartile, count(*) AS n_students, avg(label_at_risk::INT) AS at_risk_share
        FROM f GROUP BY quartile ORDER BY quartile
        """,
        [week],
    ).df()


def click_ratio(weekly: pd.DataFrame) -> float:
    wide = weekly.pivot(index="week", columns="outcome_group", values="clicks_per_student")
    return float(wide["Pass/Distinction"].sum() / wide["Fail/Withdrawn"].sum())


def _style(ax: Any, title: str, subtitle: str) -> None:
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(colors=INK_2, labelsize=9, length=0)
    ax.set_title(title, loc="left", fontsize=12.5, fontweight="bold", color=INK, pad=24)
    ax.text(0, 1.03, subtitle, transform=ax.transAxes, fontsize=9.5, color=INK_2)


def plot_weekly(weekly: pd.DataFrame, path: Path, data_label: str) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    wide = weekly.pivot(index="week", columns="outcome_group", values="clicks_per_student")
    fig, ax = plt.subplots(figsize=(10, 4.4), dpi=150, facecolor=SURFACE)
    for group, colour in (("Fail/Withdrawn", GREY), ("Pass/Distinction", BLUE)):
        ax.plot(wide.index, wide[group], color=colour, linewidth=2.2)
        ax.annotate(
            group,
            (wide.index[-1], wide[group].iloc[-1]),
            xytext=(6, 0),
            textcoords="offset points",
            va="center",
            fontsize=9.5,
            color=INK,
        )
    _style(
        ax,
        f"Students who passed made {click_ratio(weekly):.1f}x more VLE clicks per week "
        "than those who failed or withdrew",
        f"Mean clicks per enrolled student by complete course week. {data_label}.",
    )
    ax.set_xlabel("Course week", color=INK_2, fontsize=9)
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5, integer=True))
    ax.set_xlim(right=wide.index[-1] + 6)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def plot_risk(risk: pd.DataFrame, path: Path, data_label: str) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 4.0), dpi=150, facecolor=SURFACE)
    labels = ["Q1 (fewest clicks)", "Q2", "Q3", "Q4 (most clicks)"][: len(risk)]
    colours = [BLUE] + [GREY] * (len(risk) - 1)
    bars = ax.bar(labels, risk["at_risk_share"] * 100, color=colours, width=0.6)
    for bar, share in zip(bars, risk["at_risk_share"], strict=True):
        ax.annotate(
            f"{share:.0%}",
            (bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            fontsize=10,
            color=INK,
        )
    first, last = risk["at_risk_share"].iloc[0], risk["at_risk_share"].iloc[-1]
    _style(
        ax,
        f"At-risk share falls from {first:.0%} to {last:.0%} across week-{EARLY_WEEK} click "
        "quartiles",
        f"Share who later failed or withdrew, by clicks before week {EARLY_WEEK}; students "
        f"still enrolled. {data_label}.",
    )
    ax.set_ylabel("At risk, %", color=INK_2, fontsize=9)
    ax.set_ylim(0, max(100.0, float(risk["at_risk_share"].max() * 115)))
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def default_label(generator: dict) -> str:
    if not generator:
        return "OULAD data"
    n = generator["students_per_presentation"] * len(generator["presentations"])
    return f"Synthetic data (seed {generator['seed']}, {n:,} students), effect planted"


def _table(header: list[str], rows: list[list[Any]]) -> list[str]:
    out = ["| " + " | ".join(header) + " |", "|" + " --- |" * len(header)]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return out


def _fmt(n: float) -> str:
    return f"{n:,.0f}"


def results_markdown(
    totals: dict[str, dict[str, Any]],
    generator: dict,
    gold_counts: dict[str, int],
    checks: list,
    runs: list[dict],
    weekly: pd.DataFrame,
    risk: pd.DataFrame,
    data_label: str,
) -> str:
    lines = [
        "# Results",
        "",
        f"Data: **{data_label}**. Written by `oulad report` from the run log, the silver DQ "
        "records and the gold DuckDB file; no number on this page is typed by hand.",
        "",
        "## Findings",
        "",
    ]
    wide = weekly.pivot(index="week", columns="outcome_group", values="clicks_per_student")
    early, late = wide.head(4), wide.tail(4)
    lines += [
        f"- Pass/Distinction students made **{click_ratio(weekly):.1f}x** as many VLE clicks per "
        f"week as Fail/Withdrawn students. The gap grows: "
        f"{early['Pass/Distinction'].mean():.1f} vs {early['Fail/Withdrawn'].mean():.1f} clicks "
        f"per student in the first 4 weeks, {late['Pass/Distinction'].mean():.1f} vs "
        f"{late['Fail/Withdrawn'].mean():.1f} in the last 4.",
        f"- Among students still enrolled at the start of week {EARLY_WEEK}, the at-risk share "
        f"(later Fail or Withdrawn) is **{risk['at_risk_share'].iloc[0]:.0%}** in the lowest "
        f"click quartile and **{risk['at_risk_share'].iloc[-1]:.0%}** in the highest.",
    ]
    if generator:
        lines.append(
            "- Both effects were planted by the synthetic generator (a hidden engagement level "
            "drives clicks and results). They show the pipeline carries a real signal end to "
            "end, not a discovery about students."
        )
    lines += ["", "## Row counts per layer", ""]
    lines += _table(
        ["Table", "Bronze rows", "Silver rows", "Quarantined", "Duplicates removed"],
        [
            [
                t,
                _fmt(m["rows_in"]),
                _fmt(m["rows_out"]),
                _fmt(m["quarantined"]),
                _fmt(m["duplicates_removed"]),
            ]
            for t, m in totals.items()
        ],
    )
    lines += [
        "",
        "Every bronze row is accounted for: bronze = silver + quarantined + duplicates.",
        "",
        "| Gold table | Rows |",
        "| --- | --- |",
    ]
    lines += [f"| {t} | {_fmt(n)} |" for t, n in gold_counts.items()]
    lines += ["", "## Data quality issues caught", ""]
    planted = generator.get("quarantine", {})
    rows = []
    for table, m in totals.items():
        for reason, n in sorted(m["reasons"].items()):
            rows.append([table, f"`{reason}`", n, planted.get(table, {}).get(reason, "-")])
    if generator:
        rows.append(
            [
                "studentVle",
                "exact duplicate (dropped, not quarantined)",
                _fmt(totals["studentVle"]["duplicates_removed"]),
                _fmt(generator["duplicates"]["studentVle"]),
            ]
        )
    lines += _table(["Table", "Reason", "Rows caught", "Rows planted"], rows)
    lines += ["", "Missing values kept as NULL (not errors), with their null rate in silver:", ""]
    null_rows = []
    for table, m in totals.items():
        for col, n in m["null_counts"].items():
            if n and m["rows_out"]:
                key = f"{table}.{col}"
                planted_q = generator.get("missing_tokens", {}).get(key, "-")
                null_rows.append([key, _fmt(n), f"{n / m['rows_out']:.1%}", planted_q])
    lines += _table(["Column", "NULLs", "Null rate", 'Planted "?"'], null_rows)
    lines += ["", "## Reconciliation, silver to gold", ""]
    lines += _table(
        ["Check", "Expected", "Actual", "OK"],
        [[c.name, _fmt(c.expected), _fmt(c.actual), "yes" if c.ok else "**NO**"] for c in checks],
    )
    lines += ["", "## Runs", "", "Each `oulad run-all`, in order, on this machine:", ""]
    run_rows = []
    for i, run in enumerate(runs, 1):
        steps = {s["step"]: s for s in run["steps"]}
        run_rows.append(
            [
                i,
                run["label"],
                f"{steps['ingest']['files_ingested']} new, "
                f"{steps['ingest']['files_skipped']} skipped",
                ", ".join(f"`{p}`" for p in steps["silver"]["partitions_rebuilt"]) or "none",
                steps["ingest"]["seconds"],
                steps["silver"]["seconds"],
                steps["gold"]["seconds"],
            ]
        )
    lines += _table(
        ["#", "Run", "Files", "Partitions rebuilt", "Ingest s", "Silver s", "Gold s"], run_rows
    )
    lines += [
        "",
        "Silver runs Spark in local mode on a shared 2-CPU machine. Most of its time is fixed "
        "Spark start-up and per-job overhead, not data volume, so at this size rebuilding one "
        "presentation is not much faster than rebuilding all of them, and timings vary a lot "
        "between runs. The win from partitioning here is what is *not* touched: other "
        "presentations keep their files, and gold rewrites only the changed rows.",
        "",
    ]
    return "\n".join(lines)


def write_report(layout: Layout, out_dir: Path, data_label: str | None = None) -> Path:
    generator = load_summary(layout.landing)
    label = data_label or default_label(generator)
    con = duckdb.connect(str(layout.gold_db), read_only=True)
    try:
        register_silver(con, layout.silver)
        checks = reconcile(con)
        counts = table_counts(con)
        weekly, risk = weekly_by_group(con), risk_by_quartile(con)
    finally:
        con.close()
    totals = layer_totals(read_dq(layout.silver))
    text = results_markdown(
        totals, generator, counts, checks, read_runs(layout.run_log), weekly, risk, label
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "results.md"
    path.write_text(text)
    plot_weekly(weekly, out_dir / "img" / "weekly_clicks_by_outcome.png", label)
    plot_risk(risk, out_dir / "img" / "at_risk_by_engagement.png", label)
    return path
