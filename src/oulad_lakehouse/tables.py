"""The seven OULAD source tables: exact column names, as published by the Open University.

The synthetic generator writes exactly these headers, and bronze rejects any file whose header
differs, so the real dataset drops in without code changes.
"""

from __future__ import annotations

TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "courses": ("code_module", "code_presentation", "module_presentation_length"),
    "assessments": (
        "code_module",
        "code_presentation",
        "id_assessment",
        "assessment_type",
        "date",
        "weight",
    ),
    "vle": ("id_site", "code_module", "code_presentation", "activity_type", "week_from", "week_to"),
    "studentInfo": (
        "code_module",
        "code_presentation",
        "id_student",
        "gender",
        "region",
        "highest_education",
        "imd_band",
        "age_band",
        "num_of_prev_attempts",
        "studied_credits",
        "disability",
        "final_result",
    ),
    "studentRegistration": (
        "code_module",
        "code_presentation",
        "id_student",
        "date_registration",
        "date_unregistration",
    ),
    "studentAssessment": ("id_assessment", "id_student", "date_submitted", "is_banked", "score"),
    "studentVle": (
        "code_module",
        "code_presentation",
        "id_student",
        "id_site",
        "date",
        "sum_click",
    ),
}

# Processing order: parents before the tables whose foreign keys point at them.
TABLE_ORDER: tuple[str, ...] = tuple(TABLE_COLUMNS)

# Bronze adds these lineage columns to every row.
METADATA_COLUMNS: tuple[str, ...] = ("_ingested_at", "_source_file", "_file_hash", "_row_number")

# Rows whose presentation cannot be determined land in this partition (and in quarantine).
UNKNOWN_PARTITION = "__unknown__"


def table_for_file(stem: str) -> str | None:
    """Map a landing file name to its table: `studentVle.csv` or `studentVle__late_1.csv`."""
    name = stem.split("__", 1)[0]
    return name if name in TABLE_COLUMNS else None
