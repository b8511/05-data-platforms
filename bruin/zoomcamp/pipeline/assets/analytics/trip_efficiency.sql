/* @bruin

name: analytics.trip_efficiency
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: trip_date
    type: DATE
    description: Date of the trip
    primary_key: true
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow, green, etc.)
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Number of trips in the sample
    checks:
      - name: non_negative
  - name: avg_duration_minutes
    type: DOUBLE
    description: Average trip duration in minutes
  - name: avg_distance_miles
    type: DOUBLE
    description: Average trip distance in miles
  - name: avg_speed_mph
    type: DOUBLE
    description: Average estimated speed (distance / duration)
  - name: avg_fare
    type: DOUBLE
    description: Average fare amount
  - name: avg_fare_per_mile
    type: DOUBLE
    description: Average fare charged per mile
  - name: avg_tip
    type: DOUBLE
    description: Average tip amount
  - name: avg_tip_rate
    type: DOUBLE
    description: Average tip as a fraction of fare (0-1)

custom_checks:
  - name: avg_speed_plausible
    description: Average speed should be under 100 mph
    query: SELECT COUNT(*) FROM analytics.trip_efficiency WHERE avg_speed_mph > 100
    value: 0

@bruin */

-- Aggregate per-trip efficiency metrics by date and taxi type.
-- Filters out implausible trips (zero distance, zero fare, or sub-60-second / multi-day durations).
SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,

    COUNT(*)                                                            AS trip_count,

    -- Duration
    AVG(EPOCH(dropoff_datetime - pickup_datetime) / 60.0)              AS avg_duration_minutes,

    -- Distance
    AVG(trip_distance)                                                  AS avg_distance_miles,

    -- Speed: miles per hour
    AVG(
        trip_distance / (EPOCH(dropoff_datetime - pickup_datetime) / 3600.0)
    )                                                                   AS avg_speed_mph,

    -- Fare metrics
    AVG(fare_amount)                                                    AS avg_fare,
    AVG(fare_amount / trip_distance)                                    AS avg_fare_per_mile,

    -- Tip metrics
    AVG(tip_amount)                                                     AS avg_tip,
    AVG(tip_amount / fare_amount)                                       AS avg_tip_rate

FROM staging.trips
WHERE
    -- Only well-formed trips
    trip_distance > 0
    AND fare_amount > 0
    AND EPOCH(dropoff_datetime - pickup_datetime) BETWEEN 60 AND 86400

GROUP BY 1, 2
ORDER BY 1, 2
