/* @bruin

name: reports.trips_report
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
  - name: payment_type_name
    type: VARCHAR
    description: Payment method name
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Total number of trips
    checks:
      - name: non_negative
  - name: total_passengers
    type: BIGINT
    description: Total number of passengers
  - name: total_distance
    type: DOUBLE
    description: Total trip distance
  - name: total_fare
    type: DOUBLE
    description: Total fare amount
  - name: total_tips
    type: DOUBLE
    description: Total tip amount
  - name: total_revenue
    type: DOUBLE
    description: Total revenue
  - name: avg_fare
    type: DOUBLE
    description: Average fare per trip
  - name: avg_trip_distance
    type: DOUBLE
    description: Average trip distance
  - name: avg_passengers
    type: DOUBLE
    description: Average passengers per trip

@bruin */

-- Aggregate trips by date, taxi type, and payment type
SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    payment_type_name,

    -- Count metrics
    COUNT(*) AS trip_count,
    SUM(COALESCE(passenger_count, 0)) AS total_passengers,

    -- Distance metrics
    SUM(COALESCE(trip_distance, 0)) AS total_distance,

    -- Revenue metrics
    SUM(COALESCE(fare_amount, 0)) AS total_fare,
    SUM(COALESCE(tip_amount, 0)) AS total_tips,
    SUM(COALESCE(total_amount, 0)) AS total_revenue,

    -- Average metrics
    AVG(COALESCE(fare_amount, 0)) AS avg_fare,
    AVG(COALESCE(trip_distance, 0)) AS avg_trip_distance,
    AVG(COALESCE(passenger_count, 0)) AS avg_passengers

FROM staging.trips
GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    payment_type_name
