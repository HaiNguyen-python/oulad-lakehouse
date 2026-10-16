-- materialized: incremental
-- grain: one row per module presentation (e.g. AAA 2013J)
SELECT
    c.code_module || '_' || c.code_presentation AS module_presentation_key,
    c.code_module,
    c.code_presentation,
    CAST(substr(c.code_presentation, 1, 4) AS INTEGER) AS presentation_year,
    -- OULAD: "B" presentations start in February, "J" presentations in October.
    CASE right(c.code_presentation, 1) WHEN 'B' THEN 'February' ELSE 'October' END
        AS start_month,
    c.module_presentation_length AS length_days,
    CAST(ceil(c.module_presentation_length / 7.0) AS INTEGER) AS length_weeks,
    -- Weeks with all 7 days inside the presentation; the last, partial week is left out of
    -- weekly averages because it would look like a sudden drop in activity.
    c.module_presentation_length // 7 AS full_weeks,
    (SELECT count(*) FROM silver_student_info s
        WHERE s.code_module = c.code_module AND s.code_presentation = c.code_presentation)
        AS n_students,
    (SELECT count(*) FROM silver_assessments a
        WHERE a.code_module = c.code_module AND a.code_presentation = c.code_presentation)
        AS n_assessments,
    (SELECT count(*) FROM silver_vle v
        WHERE v.code_module = c.code_module AND v.code_presentation = c.code_presentation)
        AS n_vle_sites
FROM silver_courses c
WHERE list_contains($partitions, c.code_presentation)
