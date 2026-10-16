"""Seeded synthetic OULAD: the real seven CSVs, the real quirks, and one planted effect.

Why synthetic: the pipeline (and CI) must run without downloading anything. The files have the
exact OULAD names and columns, so `data/landing/` can hold the real dataset instead.

What is planted on purpose, so the pipeline has something real to find:
- Each enrolment has a hidden engagement level `z`. It drives daily VLE activity, assessment
  submission and scores, and the final result. Students who pass therefore click more, and the
  gap grows as failing students fade out. The report chart shows this effect.
- OULAD quirks: "?" for unknown imd_band and score, the 10-20 IMD band written both as "10-20"
  and "10-20%", blank date_unregistration, dates relative to the module start (negative before
  it opens), exact duplicate studentVle rows.
- A handful of broken rows per presentation, each with the exact reason silver should give.
  Everything planted is counted in `_generator.json`, so tests and the report can check that the
  pipeline caught exactly what was planted.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from . import contracts as c
from .tables import TABLE_COLUMNS

# (code_module, code_presentation, length in days). One module per presentation keeps
# the partitions (2013J, 2014B, 2014J) independent and easy to follow.
PRESENTATIONS: tuple[tuple[str, str, int], ...] = (
    ("AAA", "2013J", 268),
    ("BBB", "2014B", 241),
    ("CCC", "2014J", 269),
)
ACTIVITY_TYPES = ("homepage", "oucontent", "forumng", "quiz", "resource", "subpage", "url")
ACTIVITY_WEIGHTS = (0.10, 0.30, 0.20, 0.12, 0.15, 0.08, 0.05)
REGIONS = (
    "East Anglian Region",
    "Scotland",
    "North Western Region",
    "South East Region",
    "West Midlands Region",
    "Wales",
    "North Region",
    "South Region",
    "Ireland",
    "South West Region",
    "East Midlands Region",
    "Yorkshire Region",
    "London Region",
)
GENERATOR_FILE = "_generator.json"
SITES_PER_PRESENTATION = 40
PRE_START_DAYS = 14

# Ids used by planted bad rows; they never collide with generated ids.
UNKNOWN_SITE_ID = 999_999
UNKNOWN_ASSESSMENT_ID = 999_999
NOT_ENROLLED_STUDENT_ID = 99_999_999


@dataclass(frozen=True)
class Assessment:
    kind: str
    day: int | None
    weight: float


ASSESSMENT_PLAN = (
    Assessment("TMA", 25, 10),
    Assessment("CMA", 40, 0),
    Assessment("TMA", 70, 20),
    Assessment("TMA", 120, 30),
    Assessment("CMA", 150, 0),
    Assessment("TMA", 180, 40),
    Assessment("Exam", None, 100),
)


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


def _students(
    rng: np.random.Generator, module: str, pres: str, length: int, ids: np.ndarray
) -> pd.DataFrame:
    n = len(ids)
    z = rng.normal(0, 1, n)
    withdraws = rng.random(n) < _sigmoid(-1.4 - 1.3 * z)
    withdraw_day = np.where(withdraws, rng.integers(-20, int(length * 0.7), n), -9999)
    skill = z + rng.normal(0, 0.6, n)
    result = np.where(skill > 1.2, "Distinction", np.where(skill > -0.4, "Pass", "Fail"))
    result = np.where(withdraws, "Withdrawn", result)
    imd = rng.choice(c.IMD_BANDS, n).astype(object)
    # The real files write the 10-20 band without "%" in places, and "?" when unknown.
    imd[(imd == "10-20%") & (rng.random(n) < 0.5)] = "10-20"
    imd[rng.random(n) < 0.03] = "?"
    return pd.DataFrame(
        {
            "code_module": module,
            "code_presentation": pres,
            "id_student": ids,
            "gender": rng.choice(["M", "F"], n),
            "region": rng.choice(REGIONS, n),
            "highest_education": rng.choice(
                c.EDUCATION_LEVELS, n, p=[0.02, 0.40, 0.43, 0.14, 0.01]
            ),
            "imd_band": imd,
            "age_band": rng.choice(c.AGE_BANDS, n, p=[0.70, 0.29, 0.01]),
            "num_of_prev_attempts": rng.poisson(0.15, n),
            "studied_credits": rng.choice([30, 60, 60, 60, 90, 120], n),
            "disability": np.where(rng.random(n) < 0.1, "Y", "N"),
            "final_result": result,
            "_z": z,
            "_skill": skill,
            "_withdraw_day": withdraw_day,
        }
    )


def _registrations(rng: np.random.Generator, st: pd.DataFrame) -> pd.DataFrame:
    n = len(st)
    reg = (-rng.integers(1, 180, n)).astype(object)
    reg[rng.random(n) < 0.005] = "?"
    unreg = np.where(st["final_result"] == "Withdrawn", st["_withdraw_day"].astype(str), "")
    return pd.DataFrame(
        {
            "code_module": st["code_module"],
            "code_presentation": st["code_presentation"],
            "id_student": st["id_student"],
            "date_registration": reg,
            "date_unregistration": unreg,
        }
    )


def _vle_sites(rng: np.random.Generator, module: str, pres: str, first_id: int) -> pd.DataFrame:
    n = SITES_PER_PRESENTATION
    week_from = rng.integers(0, 30, n)
    has_weeks = rng.random(n) < 0.15
    return pd.DataFrame(
        {
            "id_site": np.arange(first_id, first_id + n),
            "code_module": module,
            "code_presentation": pres,
            "activity_type": rng.choice(ACTIVITY_TYPES, n, p=ACTIVITY_WEIGHTS),
            "week_from": np.where(has_weeks, week_from.astype(str), "?"),
            "week_to": np.where(has_weeks, (week_from + 1).astype(str), "?"),
        }
    )


def _clicks(
    rng: np.random.Generator, st: pd.DataFrame, sites: pd.DataFrame, length: int
) -> pd.DataFrame:
    days = np.arange(-PRE_START_DAYS, length)
    z = st["_z"].to_numpy()[:, None]
    late_fade = np.where(st["_skill"].to_numpy() < -0.4, 0.006, 0.0)[:, None]
    logit = -1.0 + 0.6 * z - late_fade * np.clip(days, 0, None)[None, :]
    logit = logit - np.where(days < 0, 1.2, 0.0)[None, :]
    active = rng.random(logit.shape) < _sigmoid(logit)
    # Nobody is active after the day they withdrew.
    withdrew = st["_withdraw_day"].to_numpy()[:, None]
    active &= ~((withdrew > -9999) & (days[None, :] >= withdrew))
    s_idx, d_idx = np.nonzero(active)
    per_day = 1 + np.minimum(rng.poisson(1.0, len(s_idx)), 3)
    s_idx, d_idx = np.repeat(s_idx, per_day), np.repeat(d_idx, per_day)
    weights = sites["activity_type"].map(dict(zip(ACTIVITY_TYPES, ACTIVITY_WEIGHTS, strict=True)))
    site = rng.choice(
        sites["id_site"].to_numpy(), len(s_idx), p=(weights / weights.sum()).to_numpy()
    )
    clicks = 1 + rng.poisson(np.exp(0.9 + 0.2 * st["_z"].to_numpy()[s_idx]))
    df = pd.DataFrame(
        {
            "code_module": st["code_module"].iloc[0],
            "code_presentation": st["code_presentation"].iloc[0],
            "id_student": st["id_student"].to_numpy()[s_idx],
            "id_site": site,
            "date": days[d_idx],
            "sum_click": clicks,
        }
    )
    # One row per student, site and day, like a daily click log.
    return df.drop_duplicates(["id_student", "id_site", "date"], ignore_index=True)


def _assessments(module: str, pres: str, length: int, first_id: int) -> pd.DataFrame:
    rows = [
        {
            "code_module": module,
            "code_presentation": pres,
            "id_assessment": first_id + i,
            "assessment_type": a.kind,
            "date": "" if a.day is None else str(a.day),
            "weight": a.weight,
        }
        for i, a in enumerate(ASSESSMENT_PLAN)
    ]
    out = pd.DataFrame(rows)
    # OULAD leaves some exam dates unknown ("?"); do the same for the last presentation.
    out.loc[out["assessment_type"] == "Exam", "date"] = "?" if pres == "2014J" else str(length - 4)
    return out


def _submissions(
    rng: np.random.Generator, st: pd.DataFrame, assessments: pd.DataFrame, length: int
) -> pd.DataFrame:
    frames = []
    skill, withdrew = st["_skill"].to_numpy(), st["_withdraw_day"].to_numpy()
    for _, a in assessments.iterrows():
        deadline = int(a["date"]) if a["date"] not in c.MISSING_TOKENS else length - 4
        still_in = (withdrew == -9999) | (withdrew > deadline)
        submits = still_in & (rng.random(len(st)) < _sigmoid(1.6 + 1.3 * skill))
        n = int(submits.sum())
        submitted = deadline + np.round(rng.normal(-2, 4, n)).astype(int)
        score = np.clip(np.round(62 + 16 * skill[submits] + rng.normal(0, 9, n)), 0, 100)
        banked = rng.random(n) < 0.01
        frames.append(
            pd.DataFrame(
                {
                    "id_assessment": a["id_assessment"],
                    "id_student": st["id_student"].to_numpy()[submits],
                    "date_submitted": np.where(banked, -1, submitted),
                    "is_banked": banked.astype(int),
                    "score": score.astype(int).astype(str),
                }
            )
        )
    out = pd.concat(frames, ignore_index=True)
    out.loc[rng.random(len(out)) < 0.01, "score"] = "?"
    return out


def _first_row(df: pd.DataFrame) -> dict[str, Any]:
    return {str(k): v for k, v in df.iloc[0].items()}


def _plant_bad_rows(tables: dict[str, pd.DataFrame], planted: dict[str, dict[str, int]]) -> None:
    """Append broken rows. Each one is a copy of a good row with exactly one defect."""

    def add(table: str, row: dict[str, Any], reason: str) -> None:
        tables[table] = pd.concat([tables[table], pd.DataFrame([row])], ignore_index=True)
        planted.setdefault(table, {})
        planted[table][reason] = planted[table].get(reason, 0) + 1

    vle = _first_row(tables["studentVle"])
    add("studentVle", {**vle, "sum_click": -4}, c.reason_range("sum_click", 1, None))
    add("studentVle", {**vle, "date": "12.5"}, c.reason_type("date", "int"))
    add("studentVle", {**vle, "id_site": UNKNOWN_SITE_ID}, c.UNKNOWN_SITE)
    add("studentVle", {**vle, "id_student": NOT_ENROLLED_STUDENT_ID}, c.NOT_ENROLLED)

    sub = _first_row(tables["studentAssessment"])
    add("studentAssessment", {**sub, "score": "105"}, c.reason_range("score", 0, 100))
    add("studentAssessment", {**sub, "id_assessment": UNKNOWN_ASSESSMENT_ID}, c.UNKNOWN_ASSESSMENT)

    info = _first_row(tables["studentInfo"])
    ghost = NOT_ENROLLED_STUDENT_ID - 1
    add(
        "studentInfo",
        {**info, "id_student": ghost, "final_result": "Passed"},
        c.reason_unexpected("final_result"),
    )

    reg = _first_row(tables["studentRegistration"])
    add("studentRegistration", {**reg, "id_student": "A1234"}, c.reason_type("id_student", "int"))


def _plant_duplicates(rng: np.random.Generator, vle: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    n = max(3, len(vle) // 100)
    dups = vle.iloc[rng.choice(len(vle), n, replace=False)]
    return pd.concat([vle, dups], ignore_index=True), n


def _write(tables: dict[str, pd.DataFrame], folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    for name, cols in TABLE_COLUMNS.items():
        tables[name].loc[:, list(cols)].to_csv(folder / f"{name}.csv", index=False)


def generate(landing: Path, students_per_presentation: int = 1000, seed: int = 42) -> dict:
    """Write one folder of seven CSVs per presentation and return what was planted."""
    rng = np.random.default_rng(seed)
    pool = rng.choice(np.arange(10_000, 2_700_000), students_per_presentation * 4, replace=False)
    summary: dict[str, Any] = {
        "seed": seed,
        "students_per_presentation": students_per_presentation,
        "presentations": [p for _, p, _ in PRESENTATIONS],
        "quarantine": {},
        "duplicates": {"studentVle": 0},
        "missing_tokens": {"studentInfo.imd_band": 0, "studentAssessment.score": 0},
        "late_files": [],
    }
    for i, (module, pres, length) in enumerate(PRESENTATIONS):
        # Draw from a shared pool so some students take more than one module, as in OULAD.
        ids = rng.choice(pool, students_per_presentation, replace=False)
        st = _students(rng, module, pres, length, ids)
        sites = _vle_sites(rng, module, pres, first_id=500_000 + 1000 * i)
        assessments = _assessments(module, pres, length, first_id=1_000 + 100 * i)
        tables = {
            "courses": pd.DataFrame(
                [
                    {
                        "code_module": module,
                        "code_presentation": pres,
                        "module_presentation_length": length,
                    }
                ]
            ),
            "assessments": assessments,
            "vle": sites,
            "studentInfo": st,
            "studentRegistration": _registrations(rng, st),
            "studentAssessment": _submissions(rng, st, assessments, length),
            "studentVle": _clicks(rng, st, sites, length),
        }
        summary["missing_tokens"]["studentInfo.imd_band"] += int((st["imd_band"] == "?").sum())
        summary["missing_tokens"]["studentAssessment.score"] += int(
            (tables["studentAssessment"]["score"] == "?").sum()
        )
        tables["studentVle"], n_dups = _plant_duplicates(rng, tables["studentVle"])
        summary["duplicates"]["studentVle"] += n_dups
        _plant_bad_rows(tables, summary["quarantine"])
        _write(tables, landing / pres)
    _save_summary(landing, summary)
    return summary


def generate_late(landing: Path, presentation: str, seed: int = 7) -> dict:
    """A late studentVle file for one presentation: new click rows plus re-sent old ones.

    Re-sent rows are exact copies of rows already delivered, so silver must drop them; the new
    rows must appear in silver and gold. Only this presentation's partition should rebuild.
    """
    summary = load_summary(landing)
    folder = landing / presentation
    existing = pd.read_csv(folder / "studentVle.csv", dtype=str)
    good = existing[
        (existing["sum_click"].str.fullmatch(r"\d+"))
        & (existing["date"].str.fullmatch(r"-?\d+"))
        & (existing["id_site"] != str(UNKNOWN_SITE_ID))
        & (existing["id_student"] != str(NOT_ENROLLED_STUDENT_ID))
    ].drop_duplicates()
    rng = np.random.default_rng(seed)
    n_resent = min(50, len(good) // 10)
    resent = good.iloc[rng.choice(len(good), n_resent, replace=False)]
    new = good.iloc[rng.choice(len(good), n_resent * 2, replace=False)].copy()
    new["sum_click"] = (new["sum_click"].astype(int) + 100).astype(str)
    new = new.merge(existing, how="left", indicator=True)
    new = new[new["_merge"] == "left_only"].drop(columns="_merge")
    k = len(summary["late_files"]) + 1
    late = pd.concat([new, resent], ignore_index=True)
    late.loc[:, list(TABLE_COLUMNS["studentVle"])].to_csv(
        folder / f"studentVle__late_{k}.csv", index=False
    )
    summary["late_files"].append(
        {"presentation": presentation, "new_rows": len(new), "resent_rows": n_resent}
    )
    summary["duplicates"]["studentVle"] += n_resent
    _save_summary(landing, summary)
    return summary


def load_summary(landing: Path) -> dict:
    path = landing / GENERATOR_FILE
    return json.loads(path.read_text()) if path.exists() else {}


def _save_summary(landing: Path, summary: dict) -> None:
    landing.mkdir(parents=True, exist_ok=True)
    (landing / GENERATOR_FILE).write_text(json.dumps(summary, indent=2, sort_keys=True))
