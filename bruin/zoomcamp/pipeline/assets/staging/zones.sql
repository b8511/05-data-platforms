/* @bruin

name: staging.zones
type: duckdb.sql

depends:
  - ingestion.zone_lookup

materialization:
  type: table

columns:
  - name: location_id
    type: INTEGER
    description: Unique taxi zone location identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: borough
    type: VARCHAR
    description: NYC borough name
    checks:
      - name: not_null
  - name: zone
    type: VARCHAR
    description: Taxi zone name within the borough
    checks:
      - name: not_null
  - name: service_zone
    type: VARCHAR
    description: TLC service zone grouping (Yellow Zone, Boro Zone, EWR, etc.)

@bruin */

-- Normalize taxi zone lookup: lowercase column names, clean whitespace
SELECT
    LocationID         AS location_id,
    TRIM(Borough)      AS borough,
    TRIM(Zone)         AS zone,
    TRIM(service_zone) AS service_zone
FROM ingestion.zone_lookup
WHERE LocationID IS NOT NULL
