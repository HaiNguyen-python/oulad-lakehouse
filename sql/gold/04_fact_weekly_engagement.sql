-- materialized: incremental
-- grain: one row per student enrolment and course week with at least one VLE click.
-- n_vle_rows lets the reconciliation check prove that every silver click row is counted.
SELECT
    v.code_module || '_' || v.code_presentation || '_' || v.id_student AS student_key,
    v.code_module || '_' || v.code_presentation AS module_presentation_key,
    v.code_presentation,
    CAST(floor(v.date / 7.0) AS INTEGER) AS week,
    count(*) AS n_vle_rows,
    sum(v.sum_click) AS total_clicks,
    count(DISTINCT v.date) AS active_days,
    count(DISTINCT v.id_site) AS n_sites,
    coalesce(sum(v.sum_click) FILTER (WHERE s.activity_type = 'homepage'), 0) AS clicks_homepage,
    coalesce(sum(v.sum_click) FILTER (WHERE s.activity_type = 'oucontent'), 0) AS clicks_oucontent,
    coalesce(sum(v.sum_click) FILTER (WHERE s.activity_type = 'forumng'), 0) AS clicks_forumng,
    coalesce(sum(v.sum_click) FILTER (WHERE s.activity_type = 'quiz'), 0) AS clicks_quiz,
    coalesce(sum(v.sum_click) FILTER (WHERE s.activity_type = 'resource'), 0) AS clicks_resource,
    coalesce(sum(v.sum_click) FILTER (
        WHERE s.activity_type NOT IN ('homepage', 'oucontent', 'forumng', 'quiz', 'resource')
    ), 0) AS clicks_other
FROM silver_student_vle v
JOIN silver_vle s
    ON s.code_module = v.code_module
    AND s.code_presentation = v.code_presentation
    AND s.id_site = v.id_site
WHERE list_contains($partitions, v.code_presentation)
GROUP BY ALL
