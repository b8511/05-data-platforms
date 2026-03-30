/* @bruin

name: analytics.time_patterns
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: day_of_week
    type: INTEGER
    description: "Day of week (0=Sunday … 6=Saturday)"
    primary_key: true
  - name: hour_of_day
    type: INTEGER
    description: "Hour of day in 24-hour format (0–23)"
    primary_key: true
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow, green, etc.)
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Number of trips in this time bucket
    checks:
      - name: non_negative
  - name: total_revenue
    type: DOUBLE
    description: Total revenue for this time bucket
  - name: avg_fare
    type: DOUBLE
    description: Average fare amount
  - name: avg_distance_miles
    type: DOUBLE
    description: Average trip distance in miles
  - name: avg_passengers
    type: DOUBLE
    description: Average passengers per trip
  - name: avg_tip_rate
    type: DOUBLE
    description: Average tip as a fraction of the fare

@bruin */

-- Demand, revenue, and tip patterns broken down by hour-of-day, day-of-week, and taxi type.
-- Useful for identifying rush-hour peaks, weekend vs weekday splits, and off-peak pricing trends.
SELECT
    EXTRACT(DOW  FROM pickup_datetime)::INTEGER   AS day_of_week,   -- 0=Sun, 6=Sat
    EXTRACT(HOUR FROM pickup_datetime)::INTEGER   AS hour_of_day,

    taxi_type,

    COUNT(*)                                      AS trip_count,
    SUM(total_amount)                             AS total_revenue,

    AVG(fare_amount)                              AS avg_fare,
    AVG(trip_distance)                            AS avg_distance_miles,
    AVG(COALESCE(passenger_count, 0))             AS avg_passengers,

    AVG(
        CASE
            WHEN fare_amount > 0 THEN tip_amount / fare_amount
            ELSE 0
        END
    )                                             AS avg_tip_rate

FROM staging.trips
WHERE pickup_datetime IS NOT NULL

GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
