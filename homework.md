Question 1. Bruin Pipeline Structure
In a Bruin project, what are the required files/directories?
A Bruin project requires a root .bruin.yml for environment/config, a pipeline.yml (in pipeline/ or root) to define pipeline and scheduling, and an assets/ folder alongside it containing the Python, SQL, and YAML asset files.

Question 2. Materialization Strategies
You're building a pipeline that processes NYC taxi data organized by month based on pickup_datetime. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?
The time_interval strategy in Bruin is a time-based delete-and-insert approach for partitioned data, used to refresh data for a specific time window (e.g., NYC taxi data by pickup_datetime).

Question 3. Pipeline Variables
You have the following variable defined in pipeline.yml:
```
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```
How do you override this when running the pipeline to only process yellow taxis?
Because taxi_types is an array in pipeline.yml, you have to override it as a JSON array, so you run bruin run --var 'taxi_types=["yellow"]' instead of passing a plain string.
bruin run --select ingestion.trips+ runs the ingestion.trips asset and everything that depends on it (all downstream assets).


Question 5. Quality Checks
You want to ensure the pickup_datetime column in your trips table never has NULL values. Which quality check should you add to your asset definition?
not_null is a quality check that verifies a column has no NULL values.
https://getbruin.com/docs/bruin/quality/overview.html

Question 6. Lineage and Dependencies
After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?
bruin lineage
https://getbruin.com/docs/bruin/overview.html#command-lineage


Question 7. First-Time Run
You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?
--full-refresh Drop and recreate tables (overrides incremental)