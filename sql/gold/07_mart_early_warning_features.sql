-- materialized: incremental
-- grain: student enrolment x as_of_week. One row = what was known at the START of course
-- week N (cutoff_day = 7 * N): only events with day < cutoff_day are used. Consumed by the
-- companion ML project fair-early-intervention.
-- Columns starting with label_ are the outcome and must never be used as features.
-- Assumption: a score is known on the day it was submitted (real marking takes longer;
-- see docs/decisions.md).
WITH cutoffs AS (SELECT unnest($as_of_weeks) AS as_of_week),
base AS (
    SELECT s.*, c.as_of_week, 7 * c.as_of_week AS cutoff_day
    FROM dim_student s
    CROSS JOIN cutoffs c
    WHERE list_contains($partitions, s.code_presentation)
),
clicks AS (
    SELECT
        b.student_key,
        b.as_of_week,
        sum(v.sum_click) AS clicks_to_date,
        count(DISTINCT v.date) AS active_days_to_date,
        count(DISTINCT floor(v.date / 7.0)) AS active_weeks_to_date,
        coalesce(sum(v.sum_click) FILTER (WHERE v.date >= b.cutoff_day - 14), 0)
            AS clicks_last_14_days,
        coalesce(sum(v.sum_click) FILTER (WHERE v.date < 0), 0) AS clicks_before_start,
        coalesce(sum(v.sum_click) FILTER (WHERE s.activity_type = 'forumng'), 0)
            AS forum_clicks_to_date,
        coalesce(sum(v.sum_click) FILTER (WHERE s.activity_type = 'quiz'), 0)
            AS quiz_clicks_to_date,
        min(b.cutoff_day - v.date) AS days_since_last_click
    FROM base b
    JOIN silver_student_vle v
        ON v.code_module = b.code_module
        AND v.code_presentation = b.code_presentation
        AND v.id_student = b.id_student
        AND v.date < b.cutoff_day
    JOIN silver_vle s
        ON s.code_module = v.code_module
        AND s.code_presentation = v.code_presentation
        AND s.id_site = v.id_site
    GROUP BY 1, 2
),
submissions AS (
    SELECT
        b.student_key,
        b.as_of_week,
        count(*) AS n_submitted_to_date,
        count(*) FILTER (WHERE f.is_late) AS n_submitted_late_to_date,
        avg(f.score) AS mean_score_to_date,
        sum(f.score * f.weight) / nullif(sum(f.weight) FILTER (WHERE f.score IS NOT NULL), 0)
            AS weighted_score_to_date
    FROM base b
    JOIN fact_assessment f
        ON f.student_key = b.student_key AND f.date_submitted < b.cutoff_day
    GROUP BY 1, 2
),
missed AS (
    -- Assessments whose deadline passed before the cutoff with no submission before it.
    -- Deadlines are published in advance, so using them is not leakage.
    SELECT b.student_key, b.as_of_week, count(*) AS n_missed_to_date
    FROM base b
    JOIN silver_assessments a
        ON a.code_module = b.code_module
        AND a.code_presentation = b.code_presentation
        AND a.assessment_type <> 'Exam'
        AND a.date < b.cutoff_day
    LEFT JOIN fact_assessment f
        ON f.student_key = b.student_key
        AND f.id_assessment = a.id_assessment
        AND f.date_submitted < b.cutoff_day
    WHERE f.student_key IS NULL
    GROUP BY 1, 2
)
SELECT
    b.student_key,
    b.module_presentation_key,
    b.code_presentation,
    b.id_student,
    b.as_of_week,
    b.cutoff_day,
    b.gender,
    b.region,
    b.highest_education,
    b.imd_band,
    b.age_band,
    b.num_of_prev_attempts,
    b.studied_credits,
    b.disability,
    -b.date_registration AS days_registered_before_start,
    -- Known at the cutoff: has the student already unregistered?
    b.date_unregistration IS NULL OR b.date_unregistration >= b.cutoff_day
        AS is_enrolled_at_cutoff,
    coalesce(c.clicks_to_date, 0) AS clicks_to_date,
    coalesce(c.active_days_to_date, 0) AS active_days_to_date,
    coalesce(c.active_weeks_to_date, 0) AS active_weeks_to_date,
    coalesce(c.clicks_last_14_days, 0) AS clicks_last_14_days,
    coalesce(c.clicks_before_start, 0) AS clicks_before_start,
    coalesce(c.forum_clicks_to_date, 0) AS forum_clicks_to_date,
    coalesce(c.quiz_clicks_to_date, 0) AS quiz_clicks_to_date,
    c.days_since_last_click,
    coalesce(s.n_submitted_to_date, 0) AS n_submitted_to_date,
    coalesce(s.n_submitted_late_to_date, 0) AS n_submitted_late_to_date,
    coalesce(m.n_missed_to_date, 0) AS n_missed_to_date,
    s.mean_score_to_date,
    s.weighted_score_to_date,
    b.final_result AS label_final_result,
    b.final_result IN ('Fail', 'Withdrawn') AS label_at_risk
FROM base b
LEFT JOIN clicks c USING (student_key, as_of_week)
LEFT JOIN submissions s USING (student_key, as_of_week)
LEFT JOIN missed m USING (student_key, as_of_week)
