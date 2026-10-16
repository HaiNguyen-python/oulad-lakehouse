-- materialized: incremental
-- grain: one row per submitted assessment (student enrolment x assessment).
SELECT
    sa.code_module || '_' || sa.code_presentation || '_' || sa.id_student AS student_key,
    sa.code_module || '_' || sa.code_presentation AS module_presentation_key,
    sa.code_presentation,
    sa.id_assessment,
    a.assessment_type,
    a.weight,
    -- Exams without a date are held at the end of the presentation.
    coalesce(a.date, c.module_presentation_length) AS deadline_day,
    sa.date_submitted,
    CAST(floor(sa.date_submitted / 7.0) AS INTEGER) AS week_submitted,
    sa.date_submitted - coalesce(a.date, c.module_presentation_length) AS days_late,
    sa.date_submitted > coalesce(a.date, c.module_presentation_length) AS is_late,
    sa.is_banked = 1 AS is_banked,
    sa.score,
    sa.score >= 40 AS is_pass_mark
FROM silver_student_assessment sa
JOIN silver_assessments a ON a.id_assessment = sa.id_assessment
JOIN silver_courses c
    ON c.code_module = sa.code_module AND c.code_presentation = sa.code_presentation
WHERE list_contains($partitions, sa.code_presentation)
