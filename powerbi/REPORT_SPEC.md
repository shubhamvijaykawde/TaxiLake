# TaxiLake report specification (three pages)

Build these pages in Power BI Desktop after the model is applied and validated (`docs/POWERBI_SETUP.md`). Every field below exists in `powerbi/model/taxilake_model.tmdl`; every number shown must match `powerbi/validation/expected_values.md`. Do not add measures that are not defined there.

## Global settings

- Canvas 16:9 (1280 x 720). Theme: one accent colour plus neutral greys; red/green only for MoM direction. Font Segoe UI. Page titles as a text box at the top left: **Demand**, **Revenue and payments**, **Data quality**. Rename the page tabs the same way.
- Slicers (all synced across pages with View > Sync slicers): **Month** = `dim_month[year_month]` (tile or dropdown, multi-select). Always use `dim_month` for months, never `dim_date[month_name]`, otherwise the data-quality tables are not filtered.
- Every visual gets a clear title that says what and the unit ("Daily trips", "Revenue ($)"). Turn on alt text for each visual. Number formats come from the measures (no extra formatting needed).
- Disable cross-filtering from the data-quality table and from the weather scatter to other visuals (Format > Edit interactions) to avoid confusing filters.
- Pickup-zone view only: the model has no dropoff zone. Say so in a footer text box on pages 1 and 2: "Pickup zone only. Source: NYC TLC yellow taxi, Jan-Jun 2025, via Databricks gold layer."

## Page 1: Demand

| # | Visual | Fields | Notes |
|---|---|---|---|
| 1 | Card | `[Trips]` | title "Trips" |
| 2 | Card | `[Avg Trips per Hour]` | title "Average trips per hour" |
| 3 | Slicers | `dim_month[year_month]`; `dim_zone[borough]`; `dim_date[is_weekend]` | the weekend slicer shows True/False; rename to "Weekend?" |
| 4 | Line chart "Daily trips" | X `dim_date[date]`, Y `[Trips]` | tooltip adds `dim_date[day_name]` |
| 5 | Bar chart "Trips by pickup borough" | Y `dim_zone[borough]`, X `[Trips]` | sort by Trips descending |
| 6 | Bar chart "Top 15 pickup zones" | Y `dim_zone[zone]`, X `[Trips]` | Top N filter: top 15 by `[Trips]`; tooltip adds `dim_zone[borough]` |
| 7 | Column chart "Trips by hour of day" | X `dim_time_of_day[hour_label]`, Y `[Trips]`, legend `dim_time_of_day[time_band]` | hour labels are sorted by hour already |
| 8 | Matrix (heatmap) "Trips by weekday and hour" | rows `dim_date[day_name]`, columns `dim_time_of_day[hour_label]`, values `[Trips]` | background colour scale (white to accent); weekday order follows `day_of_week` |
| 9 | Column chart "Average trips per hour by rainfall" | X `agg_hourly_demand[precip_band]`, Y `[Avg Trips per Hour]` | add subtitle "Association only: rain hours differ in time of day and weekday" |
| 10 | Scatter "Daily temperature vs trips per hour" | Details `dim_date[date]`, X `[Avg Temperature (C)]`, Y `[Avg Trips per Hour]` | one point per day (181) |

Expected anchors (no slicers selected): Trips 22,215,869; busiest hour 18:00 (1,533,627 trips); top zone Upper East Side South (1,020,811); precipitation bands 3,672 / 500 / 144 / 27 hours.

## Page 2: Revenue and payments

| # | Visual | Fields | Notes |
|---|---|---|---|
| 1 | Cards (3) | `[Total Revenue]`, `[Average Fare]`, `[Revenue per Mile]` | titles "Revenue", "Average fare (base fare per trip)", "Revenue per mile" |
| 2 | Slicers | `dim_month[year_month]`; `dim_payment_type[payment_type_name]`; `dim_zone[borough]` | |
| 3 | Line and clustered column "Monthly revenue and month-over-month change" | X `dim_month[year_month]`, columns `[Total Revenue]`, line `[Revenue MoM %]` (secondary axis) | January shows no MoM (no December data) |
| 4 | Line chart "Daily revenue" | X `dim_date[date]`, Y `[Total Revenue]` | |
| 5 | Line chart "Average fare by day" | X `dim_date[date]`, Y `[Average Fare]` | |
| 6 | Bar chart "Revenue per mile by pickup borough" | Y `dim_zone[borough]`, X `[Revenue per Mile]` | tooltip adds `[Trips]`; small boroughs (EWR 509 trips, Staten Island 1,792) are noisy, say so in the subtitle |
| 7 | Bar chart "Revenue by payment type" | Y `dim_payment_type[payment_type_name]`, X `[Total Revenue]` | the voided-trip type has no rows and does not appear |
| 8 | Matrix "Month-over-month comparison" | rows `dim_month[year_month]`; values `[Total Revenue]`, `[Revenue Prior Month]`, `[Revenue MoM Change]`, `[Revenue MoM %]`, `[Trips]`, `[Average Fare]`, `[Revenue per Mile]` | conditional font colour on MoM (green positive, red negative); grand total row off or blank |

Expected anchors (no slicers selected): Revenue $626,084,920.56; Average Fare $19.38; Revenue per Mile $4.35 (4.3460); monthly MoM: Feb +0.1%, Mar +21.9%, Apr -2.6%, May +16.0%, Jun -4.5%. Revenue includes 1,592 trips with a negative total amount (documented in the footer: "Revenue = sum of total_amount, including refunds/adjustments").

## Page 3: Data quality

| # | Visual | Fields | Notes |
|---|---|---|---|
| 1 | Cards | `[Bronze Rows]`, `[Accepted Rows]`, `[Quarantined Rows]`, `[Quarantine Rate]`, `[Months Reconciled]` | |
| 2 | Cards | `[Reconciliation Status]`, `[Last Reconciliation Check]` | title the second one "Last reconciliation check (timestamp stored in UTC)". It is the newest `ops.month_reconciliation` row, not a source-system freshness feed |
| 3 | Slicer | `dim_month[year_month]` | |
| 4 | Stacked column "Accepted vs quarantined rows by month" | X `dim_month[year_month]`, Y `[Accepted Rows]`, `[Quarantined Rows]` | data labels on |
| 5 | Line chart "Quarantine rate by month" | X `dim_month[year_month]`, Y `[Quarantine Rate]` | |
| 6 | Bar chart "Quarantined rows by rule" | Y `dq_quarantine_by_rule[reason_code]`, X `[Rule Rows Hit]` | subtitle: "A row can break several rules, so rule counts overlap and exceed quarantined rows" |
| 7 | Table "Rows processed per load" | `dq_reconciliation[source_month]`, `run_batch_id`, `checked_at`, `bronze_rows`, `silver_accepted`, `quarantined`, `fact_rows`, `status` | shows all 8 runs including the two reruns; this table is not affected by `is_latest_run` |
| 8 | Cards (2) | `[Agg vs Fact Revenue Difference]`, `[Agg vs Fact Trips Difference]` | title "Gold aggregates vs fact (expected 0)" |

Expected anchors (no slicers selected): Bronze 24,083,384; Accepted 22,215,869; Quarantined 1,867,515; Quarantine Rate 7.75%; 6 months reconciled; status "All PASS"; rule rows NEGATIVE_FARE 1,323,048, ZERO_DISTANCE 661,842, DROPOFF_BEFORE_PICKUP 793, PICKUP_OUTSIDE_MONTH 137; both reconciliation differences 0.

## Screenshots (for the README)

Export each page after setting the Month slicer to "all months": `docs/img/page1_demand.png`, `docs/img/page2_revenue_payments.png`, `docs/img/page3_data_quality.png` (Windows Snipping Tool, region capture of the report canvas). Do not include the Power BI title bar, parameter dialogs, or anything showing the workspace hostname or your account name.
