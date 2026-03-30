/* @bruin

name: reports.revenue_trends
type: duckdb.sql

depends:
  - reports.trips_report

materialization:
  type: table

columns:
  - name: trip_date
    type: DATE
    description: Date of the aggregated trips
    primary_key: true
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi
    primary_key: true
  - name: daily_trips
    type: BIGINT
    description: Number of trips on this day
    checks:
      - name: non_negative
  - name: daily_revenue
    type: DOUBLE
    description: Sum of total_amount on this day
  - name: revenue_7day_avg
    type: DOUBLE
    description: 7-day rolling average of daily revenue (current day inclusive)
  - name: prev_day_revenue
    type: DOUBLE
    description: Revenue on the previous calendar day (same taxi type)
  - name: day_over_day_delta
    type: DOUBLE
    description: Absolute revenue change versus previous day
  - name: day_over_day_pct
    type: DOUBLE
    description: Percentage revenue change versus previous day (NULL on first day)

@bruin */

-- Daily revenue trend with 7-day rolling average and day-over-day delta.
-- Builds on top of reports.trips_report, collapsing payment_type_name to get
-- a single revenue figure per (date, taxi_type).
WITH daily AS (
    SELECT
        trip_date,
        taxi_type,
        SUM(trip_count)    AS daily_trips,
        SUM(total_revenue) AS daily_revenue
    FROM reports.trips_report
    GROUP BY 1, 2
)

SELECT
    trip_date,
    taxi_type,
    daily_trips,
    daily_revenue,

    -- 7-day rolling average (window includes current row)
    AVG(daily_revenue) OVER (
        PARTITION BY taxi_type
        ORDER BY trip_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                                            AS revenue_7day_avg,

    -- Previous day's revenue for the same taxi type
    LAG(daily_revenue) OVER (
        PARTITION BY taxi_type
        ORDER BY trip_date
    )                                                            AS prev_day_revenue,

    -- Absolute day-over-day change
    daily_revenue - LAG(daily_revenue) OVER (
        PARTITION BY taxi_type
        ORDER BY trip_date
    )                                                            AS day_over_day_delta,

    -- Percentage day-over-day change
    CASE
        WHEN LAG(daily_revenue) OVER (
                 PARTITION BY taxi_type ORDER BY trip_date
             ) > 0
        THEN ROUND(
            100.0 * (daily_revenue - LAG(daily_revenue) OVER (
                         PARTITION BY taxi_type ORDER BY trip_date
                     ))
            / LAG(daily_revenue) OVER (
                  PARTITION BY taxi_type ORDER BY trip_date
              ),
            2
        )
        ELSE NULL
    END                                                          AS day_over_day_pct

FROM daily
ORDER BY trip_date, taxi_type
