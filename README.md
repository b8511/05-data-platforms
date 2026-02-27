# 05-data-platforms

In this module, we learned about data platforms and how they manage the entire data lifecycle from ingestion to analytics. Using Bruin as an example, we saw how data ingestion, transformation, orchestration, data quality checks, and metadata management are handled within a single platform.
Practice workspace for Bruin-based data platform exercises.

## What is in this repo

- `homework.md`: answers for the data platforms homework.
- `bruin/bruin-pipeline/`: starter/sample Bruin pipeline (`players`, `player_stats`, Python asset).
- `bruin/zoomcamp/`: NYC taxi end-to-end Bruin project (ingestion, staging, reports).

## Prerequisites

- Git
- [Bruin CLI](https://getbruin.com/docs/bruin/getting-started/installation)
- Python 3.10+

Verify Bruin is installed:

```bash
bruin version
```

## Quick start

From the repository root:

```bash
cd bruin
```

### 1) Run the sample pipeline

```bash
cd bruin-pipeline
bruin validate .
bruin run .
```

### 2) Run the NYC taxi pipeline

```bash
cd ../zoomcamp
bruin validate ./pipeline/pipeline.yml
bruin run ./pipeline/pipeline.yml --start-date 2022-01-01 --end-date 2022-03-01
```

## Useful commands

```bash
# show dependencies/lineage
bruin lineage ./pipeline/pipeline.yml

# run only one asset and its downstreams
bruin run ./pipeline/assets/ingestion/trips.py --downstream

# force rebuild
bruin run ./pipeline/pipeline.yml --full-refresh
```

## Notes

- Keep `.bruin.yml` out of version control (it may contain credentials).
- Start with a small date range while developing, then backfill once everything works.
