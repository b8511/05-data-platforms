/* @bruin

name: analytics.zone_performance
type: duckdb.sql

depends:
  - staging.trips
  - staging.zones

materialization:
  type: table

columns:
  - name: pickup_borough
    type: VARCHAR
    description: NYC borough of the pickup location
    primary_key: true
  - name: pickup_zone
    type: VARCHAR
    description: Taxi zone of the pickup location
    primary_key: true
  - name: dropoff_borough
    type: VARCHAR
    description: NYC borough of the dropoff location
    primary_key: true
  - name: dropoff_zone
    type: VARCHAR
    description: Taxi zone of the dropoff location
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Number of trips on this origin-destination pair
    checks:
      - name: non_negative
  - name: total_revenue
    type: DOUBLE
    description: Total revenue (total_amount) for this pair
  - name: avg_revenue_per_trip
    type: DOUBLE
    description: Average revenue per trip
  - name: avg_distance_miles
    type: DOUBLE
    description: Average trip distance for this origin-destination pair
  - name: avg_duration_minutes
    type: DOUBLE
    description: Average trip duration in minutes
  - name: total_passengers
    type: BIGINT
    description: Total passengers transported on this pair

custom_checks:
  - name: has_known_zones
    description: At least one zone should be resolvable (no fully unmatched join)
    query: SELECT COUNT(*) FROM analytics.zone_performance WHERE pickup_borough IS NOT NULL
    value: 1

@bruin */

-- Revenue, volume, and distance aggregated by pickup → dropoff zone pair.
-- Only includes trips where both location IDs resolve to a named zone.
SELECT
    pz.borough                                                          AS pickup_borough,
    pz.zone                                                             AS pickup_zone,
    dz.borough                                                          AS dropoff_borough,
    dz.zone                                                             AS dropoff_zone,

    COUNT(*)                                                            AS trip_count,

    SUM(t.total_amount)                                                 AS total_revenue,
    AVG(t.total_amount)                                                 AS avg_revenue_per_trip,

    AVG(t.trip_distance)                                                AS avg_distance_miles,
    AVG(EPOCH(t.dropoff_datetime - t.pickup_datetime) / 60.0)          AS avg_duration_minutes,

    SUM(COALESCE(t.passenger_count, 0))                                 AS total_passengers

FROM staging.trips t
INNER JOIN staging.zones pz ON t.pickup_location_id  = pz.location_id
INNER JOIN staging.zones dz ON t.dropoff_location_id = dz.location_id

WHERE t.trip_distance > 0
  AND t.total_amount  > 0

GROUP BY 1, 2, 3, 4
ORDER BY trip_count DESC
