# Results

Data: **Synthetic data (seed 42, 3,000 students), effect planted**. Written by `oulad report` from the run log, the silver DQ records and the gold DuckDB file; no number on this page is typed by hand.

## Findings

- Pass/Distinction students made **5.1x** as many VLE clicks per week as Fail/Withdrawn students. The gap grows: 18.8 vs 8.1 clicks per student in the first 4 weeks, 18.7 vs 1.2 in the last 4.
- Among students still enrolled at the start of week 4, the at-risk share (later Fail or Withdrawn) is **81%** in the lowest click quartile and **8%** in the highest.
- Both effects were planted by the synthetic generator (a hidden engagement level drives clicks and results). They show the pipeline carries a real signal end to end, not a discovery about students.

## Row counts per layer

| Table | Bronze rows | Silver rows | Quarantined | Duplicates removed |
| --- | --- | --- | --- | --- |
| courses | 3 | 3 | 0 | 0 |
| assessments | 21 | 21 | 0 | 0 |
| vle | 120 | 120 | 0 | 0 |
| studentInfo | 3,003 | 3,000 | 3 | 0 |
| studentRegistration | 3,003 | 3,000 | 3 | 0 |
| studentAssessment | 13,948 | 13,942 | 6 | 0 |
| studentVle | 355,664 | 352,084 | 12 | 3,568 |

Every bronze row is accounted for: bronze = silver + quarantined + duplicates.

| Gold table | Rows |
| --- | --- |
| dim_date | 454 |
| dim_module_presentation | 3 |
| dim_student | 3,000 |
| fact_assessment | 13,942 |
| fact_weekly_engagement | 78,081 |
| mart_early_warning_features | 12,000 |
| mart_engagement_vs_outcome | 440 |

## Data quality issues caught

| Table | Reason | Rows caught | Rows planted |
| --- | --- | --- | --- |
| studentInfo | `final_result: unexpected value` | 3 | 3 |
| studentRegistration | `id_student: not an integer` | 3 | 3 |
| studentAssessment | `id_assessment: unknown assessment` | 3 | 3 |
| studentAssessment | `score: outside [0, 100]` | 3 | 3 |
| studentVle | `date: not an integer` | 3 | 3 |
| studentVle | `id_site: unknown VLE site` | 3 | 3 |
| studentVle | `id_student: not enrolled in this presentation` | 3 | 3 |
| studentVle | `sum_click: below 1` | 3 | 3 |
| studentVle | exact duplicate (dropped, not quarantined) | 3,568 | 3,568 |

Missing values kept as NULL (not errors), with their null rate in silver:

| Column | NULLs | Null rate | Planted "?" |
| --- | --- | --- | --- |
| assessments.date | 1 | 4.8% | - |
| vle.week_from | 105 | 87.5% | - |
| vle.week_to | 105 | 87.5% | - |
| studentInfo.imd_band | 88 | 2.9% | 88 |
| studentRegistration.date_registration | 16 | 0.5% | - |
| studentRegistration.date_unregistration | 2,248 | 74.9% | - |
| studentAssessment.score | 139 | 1.0% | 139 |

## Reconciliation, silver to gold

| Check | Expected | Actual | OK |
| --- | --- | --- | --- |
| dim_student rows = silver studentInfo rows | 3,000 | 3,000 | yes |
| dim_module_presentation rows = silver courses rows | 3 | 3 | yes |
| fact_weekly_engagement covers every silver studentVle row | 352,084 | 352,084 | yes |
| fact_weekly_engagement clicks = silver studentVle clicks | 1,353,271 | 1,353,271 | yes |
| activity-type columns add up to total clicks | 1,353,271 | 1,353,271 | yes |
| fact_assessment rows = silver studentAssessment rows | 13,942 | 13,942 | yes |
| early-warning rows = students x as-of weeks | 12,000 | 12,000 | yes |
| engagement mart students = students x complete weeks | 110,000 | 110,000 | yes |

## Runs

Each `oulad run-all`, in order, on this machine:

| # | Run | Files | Partitions rebuilt | Ingest s | Silver s | Gold s |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | initial load | 21 new, 0 skipped | `2013J`, `2014B`, `2014J`, `__unknown__` | 0.98 | 52.54 | 1.6 |
| 2 | rerun, nothing new | 0 new, 21 skipped | none | 0.18 | 0.16 | 0.39 |
| 3 | late studentVle file for 2014B | 1 new, 21 skipped | `2014B` | 0.43 | 45.47 | 0.92 |

Silver runs Spark in local mode on a shared 2-CPU machine. Most of its time is fixed Spark start-up and per-job overhead, not data volume, so at this size rebuilding one presentation is not much faster than rebuilding all of them, and timings vary a lot between runs. The win from partitioning here is what is *not* touched: other presentations keep their files, and gold rewrites only the changed rows.
