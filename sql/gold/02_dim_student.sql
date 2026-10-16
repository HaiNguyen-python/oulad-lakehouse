-- materialized: incremental
-- grain: one row per student enrolment (student x module presentation). OULAD records
-- demographics and the final result per enrolment, so this is the natural student dimension.
SELECT
    i.code_module || '_' || i.code_presentation || '_' || i.id_student AS student_key,
    i.code_module || '_' || i.code_presentation AS module_presentation_key,
    i.id_student,
    i.code_module,
    i.code_presentation,
    i.gender,
    i.region,
    i.highest_education,
    i.imd_band,
    i.age_band,
    i.num_of_prev_attempts,
    i.studied_credits,
    i.disability,
    r.date_registration,
    r.date_unregistration,
    i.final_result,
    CASE WHEN i.final_result IN ('Pass', 'Distinction') THEN 'Pass/Distinction'
         ELSE 'Fail/Withdrawn' END AS outcome_group
FROM silver_student_info i
LEFT JOIN silver_student_registration r
    ON r.code_module = i.code_module
    AND r.code_presentation = i.code_presentation
    AND r.id_student = i.id_student
WHERE list_contains($partitions, i.code_presentation)
