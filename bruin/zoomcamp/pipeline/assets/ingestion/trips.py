"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

@bruin"""

import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd


def materialize():
    """
    Ingest NYC TLC taxi trip data for the configured date window and taxi types.
    """
    # Get date window from Bruin environment
    start_date = os.environ.get("BRUIN_START_DATE", "2022-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2022-01-31")

    # Get pipeline variables
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", '{"taxi_types": ["yellow"]}'))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    # Generate list of year-month combinations in the date range
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    months = []
    current = start_dt.replace(day=1)
    while current <= end_dt:
        months.append(current.strftime("%Y-%m"))
        current += relativedelta(months=1)

    # Base URL for NYC TLC trip data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    # Fetch data for each taxi type and month
    dataframes = []
    for taxi_type in taxi_types:
        for month in months:
            url = f"{base_url}/{taxi_type}_tripdata_{month}.parquet"
            try:
                df = pd.read_parquet(url)
                df["taxi_type"] = taxi_type
                dataframes.append(df)
                print(f"Fetched {len(df)} rows from {url}")
            except Exception as e:
                print(f"Warning: Could not fetch {url}: {e}")

    if not dataframes:
        return pd.DataFrame()

    # Concatenate all dataframes
    final_df = pd.concat(dataframes, ignore_index=True)

    # Add extraction timestamp for lineage
    final_df["extracted_at"] = datetime.utcnow()

    return final_df


