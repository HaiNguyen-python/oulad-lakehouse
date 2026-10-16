-- materialized: full
-- grain: one row per course day. OULAD dates are days relative to the module start
-- (day 0), not calendar dates, so the date dimension is relative too.
-- floor(day / 7.0), not day // 7: integer division truncates toward zero in DuckDB, which
-- would put day -1 in week 0 together with the first days of the course.
WITH bounds AS (
    SELECT
        least(
            (SELECT min(date) FROM silver_student_vle),
            (SELECT min(date_submitted) FROM silver_student_assessment),
            (SELECT min(date_registration) FROM silver_student_registration),
            (SELECT min(date) FROM silver_assessments)
        ) AS lo,
        greatest(
            (SELECT max(date) FROM silver_student_vle),
            (SELECT max(date_submitted) FROM silver_student_assessment),
            (SELECT max(date_unregistration) FROM silver_student_registration),
            (SELECT max(module_presentation_length) FROM silver_courses)
        ) AS hi
),
days AS (SELECT unnest(generate_series(lo, hi)) AS day FROM bounds)
SELECT
    CAST(day AS INTEGER) AS day,
    CAST(floor(day / 7.0) AS INTEGER) AS week,
    CAST(day - 7 * floor(day / 7.0) AS INTEGER) AS day_of_week,
    day < 0 AS is_before_start
FROM days
ORDER BY day
