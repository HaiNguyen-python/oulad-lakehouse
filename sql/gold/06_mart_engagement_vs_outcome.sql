-- materialized: incremental
-- grain: module presentation x final_result x complete course week (0 .. full_weeks - 1).
-- Every enrolled student counts in every week, with zero clicks when inactive or after
-- withdrawing; without this zero-fill, averages would only describe the students who showed up.
WITH weeks AS (
    SELECT module_presentation_key, unnest(generate_series(0, full_weeks - 1)) AS week
    FROM dim_module_presentation
    WHERE list_contains($partitions, code_presentation)
),
grid AS (
    SELECT s.student_key, s.module_presentation_key, s.code_presentation, s.final_result,
           s.outcome_group, w.week
    FROM dim_student s
    JOIN weeks w USING (module_presentation_key)
)
SELECT
    g.module_presentation_key,
    g.code_presentation,
    g.final_result,
    g.outcome_group,
    g.week,
    count(*) AS n_students,
    count(f.student_key) AS n_active_students,
    coalesce(sum(f.total_clicks), 0) AS total_clicks,
    coalesce(sum(f.active_days), 0) AS total_active_days,
    coalesce(sum(f.total_clicks), 0) / count(*) AS mean_clicks_per_student,
    count(f.student_key) / count(*) AS share_active
FROM grid g
LEFT JOIN fact_weekly_engagement f ON f.student_key = g.student_key AND f.week = g.week
GROUP BY ALL
