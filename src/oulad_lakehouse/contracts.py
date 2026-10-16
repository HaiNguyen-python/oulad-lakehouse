"""Schema contracts for the silver layer, written as plain data.

A contract says, per column, what a valid value looks like (type, allowed values, format,
range, nullability) and, per table, how rows are de-duplicated and which foreign keys must
resolve. Keeping contracts declarative means the rules are easy to review in one file and the
reason strings are stable, so the DQ report can count them and tests can assert on them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# OULAD writes "?" for unknown values; some exports leave the cell empty.
MISSING_TOKENS: tuple[str, ...] = ("", "?")

IMD_BANDS: tuple[str, ...] = tuple(f"{lo}-{lo + 10}%" for lo in range(0, 100, 10))
FINAL_RESULTS: tuple[str, ...] = ("Pass", "Fail", "Withdrawn", "Distinction")
ASSESSMENT_TYPES: tuple[str, ...] = ("TMA", "CMA", "Exam")
EDUCATION_LEVELS: tuple[str, ...] = (
    "No Formal quals",
    "Lower Than A Level",
    "A Level or Equivalent",
    "HE Qualification",
    "Post Graduate Qualification",
)
AGE_BANDS: tuple[str, ...] = ("0-35", "35-55", "55<=")


@dataclass(frozen=True)
class Field:
    name: str
    dtype: Literal["int", "float", "str"] = "str"
    nullable: bool = False
    allowed: tuple[str, ...] | None = None
    pattern: str | None = None
    min: float | None = None
    max: float | None = None
    # "percent_band" turns "10-20" into "10-20%" (OULAD writes this band both ways).
    normalize: Literal["percent_band"] | None = None


@dataclass(frozen=True)
class Reference:
    columns: tuple[str, ...]
    parent: str
    reason: str


@dataclass(frozen=True)
class Contract:
    table: str
    fields: tuple[Field, ...]
    primary_key: tuple[str, ...]
    # "latest": one row per primary key, the most recently ingested wins (corrections).
    # "exact": drop rows identical in every business column (re-delivered log lines).
    dedup: Literal["latest", "exact"]
    references: tuple[Reference, ...] = ()

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(f.name for f in self.fields)


def reason_missing(col: str) -> str:
    return f"{col}: missing"


def reason_type(col: str, dtype: str) -> str:
    return f"{col}: not an integer" if dtype == "int" else f"{col}: not a number"


def reason_format(col: str) -> str:
    return f"{col}: bad format"


def reason_unexpected(col: str) -> str:
    return f"{col}: unexpected value"


def reason_range(col: str, lo: float | None, hi: float | None) -> str:
    if lo is not None and hi is not None:
        return f"{col}: outside [{lo:g}, {hi:g}]"
    return f"{col}: below {lo:g}" if lo is not None else f"{col}: above {hi:g}"


NOT_ENROLLED = "id_student: not enrolled in this presentation"
UNKNOWN_ASSESSMENT = "id_assessment: unknown assessment"
UNKNOWN_SITE = "id_site: unknown VLE site"

_MODULE = Field("code_module", pattern=r"^[A-Z]{3}$")
_PRESENTATION = Field("code_presentation", pattern=r"^\d{4}[BJ]$")
_STUDENT = Field("id_student", "int", min=0)
_ENROLMENT = ("code_module", "code_presentation", "id_student")
_ENROLLED = Reference(_ENROLMENT, "studentInfo", NOT_ENROLLED)

CONTRACTS: dict[str, Contract] = {
    c.table: c
    for c in (
        Contract(
            "courses",
            (_MODULE, _PRESENTATION, Field("module_presentation_length", "int", min=1, max=400)),
            ("code_module", "code_presentation"),
            "latest",
        ),
        Contract(
            "assessments",
            (
                _MODULE,
                _PRESENTATION,
                Field("id_assessment", "int", min=0),
                Field("assessment_type", allowed=ASSESSMENT_TYPES),
                # Exams often have no date: they take place at the end of the presentation.
                Field("date", "int", nullable=True, min=-100, max=400),
                Field("weight", "float", min=0, max=100),
            ),
            ("id_assessment",),
            "latest",
        ),
        Contract(
            "vle",
            (
                Field("id_site", "int", min=0),
                _MODULE,
                _PRESENTATION,
                Field("activity_type"),
                Field("week_from", "int", nullable=True, min=0, max=60),
                Field("week_to", "int", nullable=True, min=0, max=60),
            ),
            ("id_site",),
            "latest",
        ),
        Contract(
            "studentInfo",
            (
                _MODULE,
                _PRESENTATION,
                _STUDENT,
                Field("gender", allowed=("M", "F")),
                Field("region"),
                Field("highest_education", allowed=EDUCATION_LEVELS),
                Field("imd_band", nullable=True, allowed=IMD_BANDS, normalize="percent_band"),
                Field("age_band", allowed=AGE_BANDS),
                Field("num_of_prev_attempts", "int", min=0, max=20),
                Field("studied_credits", "int", min=1, max=700),
                Field("disability", allowed=("Y", "N")),
                Field("final_result", allowed=FINAL_RESULTS),
            ),
            _ENROLMENT,
            "latest",
        ),
        Contract(
            "studentRegistration",
            (
                _MODULE,
                _PRESENTATION,
                _STUDENT,
                Field("date_registration", "int", nullable=True, min=-400, max=400),
                # Blank means the student never unregistered.
                Field("date_unregistration", "int", nullable=True, min=-400, max=400),
            ),
            _ENROLMENT,
            "latest",
            (_ENROLLED,),
        ),
        Contract(
            "studentAssessment",
            (
                Field("id_assessment", "int", min=0),
                _STUDENT,
                Field("date_submitted", "int", min=-400, max=700),
                Field("is_banked", "int", min=0, max=1),
                # "?" scores are kept as NULL: the submission happened, the mark is unknown.
                Field("score", "float", nullable=True, min=0, max=100),
            ),
            ("id_assessment", "id_student"),
            "latest",
            (Reference(("id_assessment",), "assessments", UNKNOWN_ASSESSMENT), _ENROLLED),
        ),
        Contract(
            "studentVle",
            (
                _MODULE,
                _PRESENTATION,
                _STUDENT,
                Field("id_site", "int", min=0),
                # Days relative to the module start; negative = before the course opened.
                Field("date", "int", min=-100, max=400),
                Field("sum_click", "int", min=1),
            ),
            ("code_module", "code_presentation", "id_student", "id_site", "date", "sum_click"),
            "exact",
            (
                Reference(("code_module", "code_presentation", "id_site"), "vle", UNKNOWN_SITE),
                _ENROLLED,
            ),
        ),
    )
}
