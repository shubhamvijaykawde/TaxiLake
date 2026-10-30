# Expected values for the Power BI validation queries

Source: read-only SQL against `workspace.gold.*`, `workspace.silver.trips_quarantine` and `workspace.ops.month_reconciliation`, run 2026-10-25 after Phase 4 (data window 2025-01 to 2025-06). These values were measured on the production-named schemas, which the model reads with `Environment = prod`. With `Environment = dev` after a full dev backfill of all six months (`workspace.dev_*`), the data-derived values (trips, revenue, quarantine counts and rates, dimension sizes, the 1,414,800 `fact_trips_hourly` rows) are *expected* to be the same because the dev run processes the same source files, but that has **not been measured**. Values that depend on how many times a month was run are different by construction: the `dq_reconciliation` row count (8 here: 6 months plus two re-runs; a fresh dev backfill gives one row per run), `Last Reconciliation Check` and the run-count measures. With fewer months loaded, the totals are smaller by design, so do not compare them. Compare these with the output of `validate_measures.dax`. The first mismatch is investigated, never adjusted.

Tolerances: revenue, fare, distance and tip sums to the cent (Power BI sums doubles, so a difference of up to 0.01 in a large total is floating-point noise); ratios to 4 decimals (MoM to 6).

## 1. Row counts of model tables

| Table | Expected rows | Source of the number |
|---|---:|---|
| `fact_trips_hourly` | 1,414,800 | distinct date x hour x pickup zone x payment type groups in `gold.fact_trips` (measured) |
| `dim_zone` | 265 | `gold.dim_zone` |
| `dim_date` | 181 | `gold.dim_date`, 2025-01-01 to 2025-06-30 |
| `dim_time_of_day` | 24 | `gold.dim_time_of_day` |
| `dim_payment_type` | 7 | `gold.dim_payment_type` (codes 0-6; code 6 has no trips) |
| `agg_hourly_demand` | 4,343 | `gold.agg_hourly_demand` (181 x 24 = 4,344; one hour has no trips) |
| `agg_daily_revenue_by_borough` | 1,428 | `gold.agg_daily_revenue_by_borough` |
| `dq_reconciliation` | 8 | 6 months + the January and April reruns |
| `dq_quarantine_by_rule` | 24 | 6 months x 4 reason codes |
| `dim_month` | 6 | distinct `year_month` in `dim_date` |

## 2. Orphans: all four expected 0

## 3. Overall core measures

| Measure | Expected |
|---|---:|
| Trips | 22,215,869 |
| Total Revenue | 626,084,920.56 |
| Fare Revenue | 430,600,796.02 |
| Total Distance (miles) | 144,060,161.47 |
| Tip Revenue | 67,811,039.07 |
| Average Fare | 19.3826 |
| Revenue per Mile | 4.3460 |

## 4. Per month

| Month | Trips | Total Revenue | Fare Revenue | Distance (miles) | Average Fare | Revenue per Mile | Revenue Prior Month | Revenue MoM % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 2025-01 | 3,254,520 | 88,129,538.63 | 59,289,144.52 | 17,943,219.04 | 18.2175 | 4.9116 | blank | blank |
| 2025-02 | 3,311,525 | 88,208,812.71 | 59,820,245.65 | 19,315,824.39 | 18.0643 | 4.5667 | 88,129,538.63 | 0.000900 (0.1%) |
| 2025-03 | 3,849,463 | 107,498,734.15 | 73,753,179.01 | 23,407,094.90 | 19.1593 | 4.5926 | 88,208,812.71 | 0.218685 (21.9%) |
| 2025-04 | 3,706,768 | 104,717,670.28 | 71,488,576.64 | 24,949,118.47 | 19.2860 | 4.1972 | 107,498,734.15 | -0.025871 (-2.6%) |
| 2025-05 | 4,156,487 | 121,486,052.69 | 84,486,428.26 | 30,055,192.06 | 20.3264 | 4.0421 | 104,717,670.28 | 0.160129 (16.0%) |
| 2025-06 | 3,937,106 | 116,044,112.10 | 81,763,221.94 | 28,389,712.61 | 20.7673 | 4.0875 | 121,486,052.69 | -0.044795 (-4.5%) |

## 5. By payment type (Trips, Total Revenue)

| Payment type | Trips | Total Revenue |
|---|---:|---:|
| Flex Fare trip (0) | 4,174,145 | 106,753,890.11 |
| Credit card (1) | 15,483,985 | 454,086,655.04 |
| Cash (2) | 2,208,748 | 54,487,414.82 |
| No charge (3) | 84,211 | 2,240,866.79 |
| Dispute (4) | 264,779 | 8,516,021.81 |
| Unknown (5) | 1 | 71.99 |
| Voided trip (6) | no rows | no rows (absent from `SUMMARIZECOLUMNS`) |

## 6. By pickup borough (Trips, Total Revenue)

| Borough | Trips | Total Revenue |
|---|---:|---:|
| Manhattan | 19,342,209 | 463,213,311.23 |
| Queens | 2,010,063 | 136,429,315.29 |
| Brooklyn | 660,755 | 19,867,469.85 |
| Bronx | 154,914 | 4,807,319.96 |
| Unknown | 41,590 | 1,293,971.87 |
| N/A | 4,037 | 344,889.11 |
| Staten Island | 1,792 | 80,109.32 |
| EWR | 509 | 48,533.93 |

## 7. Top 5 pickup zones (trips)

Upper East Side South (Manhattan) 1,020,811; Midtown Center (Manhattan) 1,004,664; Upper East Side North (Manhattan) 913,010; JFK Airport (Queens) 895,546; Times Sq/Theatre District (Manhattan) 725,831.

## 8. Busiest 3 hours of day (trips)

18:00 = 1,533,627; 17:00 = 1,463,883; 19:00 = 1,364,997.

## 9. Weather: hours and average trips per hour by precipitation band

| Band | Hours Observed | Avg Trips per Hour |
|---|---:|---:|
| 1 Dry | 3,672 | 4,957.70 (shown as 4,958) |
| 2 Light (under 1 mm) | 500 | 5,907.67 (5,908) |
| 3 Moderate (1 to 4 mm) | 144 | 6,238.43 (6,238) |
| 4 Heavy (4 mm and over) | 27 | 5,889.56 (5,890) |

No hour has a null temperature or precipitation. This is an association only: rainy hours are not a controlled comparison (time of day and weekday differ between bands).

## 10. Data quality per month (latest reconciliation run per month)

| Month | Bronze Rows | Accepted Rows | Quarantined Rows | Quarantine Rate | Duplicates Removed |
|---|---:|---:|---:|---:|---:|
| 2025-01 | 3,475,226 | 3,254,520 | 220,706 | 6.35% (0.063508) | 0 |
| 2025-02 | 3,577,543 | 3,311,525 | 266,018 | 7.44% (0.074358) | 0 |
| 2025-03 | 4,145,257 | 3,849,463 | 295,794 | 7.14% (0.071357) | 0 |
| 2025-04 | 3,970,553 | 3,706,768 | 263,785 | 6.64% (0.066435) | 0 |
| 2025-05 | 4,591,845 | 4,156,487 | 435,358 | 9.48% (0.094811) | 0 |
| 2025-06 | 4,322,960 | 3,937,106 | 385,854 | 8.93% (0.089257) | 0 |

## 11. Data quality overall

Bronze Rows 24,083,384; Accepted Rows 22,215,869; Quarantined Rows 1,867,515; Quarantine Rate 7.75% (0.077544); Months Reconciled 6; Reconciliation Status "All PASS". Last Reconciliation Check = 2026-10-25 18:11:30 (the 2025-01 rerun is the newest run; stored in UTC and loaded as UTC-naive by the query, so the display should match. If Power BI shows a different clock time, record the offset instead of editing the expected value).

## 12-13. Quarantine rows hit by rule

| Rule | Rows hit (total) | Rule Hit Rate (of 24,083,384 bronze rows) |
|---|---:|---:|
| DROPOFF_BEFORE_PICKUP | 793 | 0.0033% |
| NEGATIVE_FARE | 1,323,048 | 5.4936% |
| PICKUP_OUTSIDE_MONTH | 137 | 0.0006% |
| ZERO_DISTANCE | 661,842 | 2.7481% |

Rule totals add up to 1,985,820, which is 118,305 more than the 1,867,515 quarantined rows, because a row can hit more than one rule. Do not present these as shares of quarantined rows. NEGATIVE_DISTANCE and NULL_TIMESTAMP matched no rows in this window and so do not appear.

Per month (month, rule, rows) is the 24-row table below (read-only SQL, also recorded in `docs/RUNLOG.md`): Jan 124/144,118/22/90,893; Feb 93/182,655/31/99,771; Mar 81/208,724/33/103,722; Apr 163/186,720/7/91,439; May 101/325,266/24/141,121; Jun 231/275,565/20/134,896 (order: DROPOFF_BEFORE_PICKUP / NEGATIVE_FARE / PICKUP_OUTSIDE_MONTH / ZERO_DISTANCE).

## 14. Reconciliation differences

Agg vs Fact Revenue Difference = 0.00 (both aggregates sum to 626,084,920.56). Agg vs Fact Trips Difference = 0 (both 22,215,869).

## 15. Guard behaviour

`Revenue MoM %` and `Revenue Prior Month` with no month selected are blank by design (a multi-month range has no single prior month). January is blank because no December 2024 data exists in the model.

## Definitions and assumptions (do not change silently)

- **Total Revenue** = sum of `total_amount` (as in the gold aggregate `total_revenue`). It includes 1,592 trips with a negative `total_amount`.
- **Average Fare** = sum of `fare_amount` / trips (base fare, not total amount).
- **Revenue per Mile** = Total Revenue / total trip distance (ratio of sums, not an average of per-trip ratios).
- **Quarantine Rate** = quarantined rows / bronze rows (the definition used in `docs/RUNLOG.md`: 7.75% overall).
- Pickup-zone attributes only: the aggregated fact has no dropoff zone.
