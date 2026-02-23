/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  type: table

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: row_count_positive
    description: Ensures that table is not empty
    query: SELECT COUNT(*) > 0 FROM staging.trips
    value: 1

@bruin */

-- Staging layer: clean, normalize, deduplicate, and enrich trip data
-- Joins with payment_lookup for human-readable payment types

WITH deduplicated AS (
    SELECT DISTINCT
        t.vendor_id,
        t.tpep_pickup_datetime AS pickup_datetime,
        t.tpep_dropoff_datetime AS dropoff_datetime,
        t.passenger_count,
        t.trip_distance,
        t.ratecode_id AS rate_code_id,
        t.store_and_fwd_flag,
        t.pu_location_id AS pickup_location_id,
        t.do_location_id AS dropoff_location_id,
        t.payment_type AS payment_type_id,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.total_amount,
        t.congestion_surcharge,
        t.airport_fee,
        t.taxi_type,
        t.extracted_at
    FROM ingestion.trips t
    WHERE t.tpep_pickup_datetime IS NOT NULL
      AND t.tpep_dropoff_datetime IS NOT NULL
      AND t.trip_distance >= 0
      AND t.total_amount >= 0
)

SELECT
    d.*,
    COALESCE(p.payment_type_name, 'unknown') AS payment_type_name
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
    ON d.payment_type_id = p.payment_type_id
