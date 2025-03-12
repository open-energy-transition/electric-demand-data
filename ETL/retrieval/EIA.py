#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from DATA_SOURCE...

    Source: ...
"""

import json
import os
import urllib.request
from pathlib import Path

import pandas as pd
import util.time_series as time_series_utilities
from dotenv import load_dotenv


def download_and_extract_data_of_period(
    start_date_and_time: pd.Timestamp, end_date_and_time: pd.Timestamp, region_code: str
) -> pd.Series:
    """
    Retrieve the electricity demand data from the Energy Information Administration (EIA) for a specific region and period.

    Parameters
    ----------
    start_date_and_time : pd.Timestamp
        The start date and time of the data retrieval
    end_date_and_time : pd.Timestamp
        The end date and time of the data retrieval
    region_code : str
        The code of the region of interest

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Extract the region code.
    region_code = region_code.split("_")[1]

    # Load the environment variables.
    load_dotenv(dotenv_path=Path(".") / ".env")

    # Get the API key.
    api_key = os.getenv("EIA_API_KEY")

    # Convert the start and end dates and times to the required format.
    start = start_date_and_time.strftime("%Y-%m-%dT%H")
    end = end_date_and_time.strftime("%Y-%m-%dT%H")

    # Define the URL.
    url = f"https://api.eia.gov/v2/electricity/rto/region-data/data/?api_key={api_key}&facets[type][]=D&facets[respondent][]={region_code}&start={start}&end={end}&frequency=hourly&data[0]=value&sort[0][column]=period&sort[0][direction]=asc&offset=0&length=5000"

    # Retrieve the data.
    response = urllib.request.urlopen(url).read().decode("utf-8")

    # Convert the data to a JSON object.
    region_data = json.loads(response)

    # Extract the data.
    index = [item["period"] for item in region_data["response"]["data"]]
    values = [item["value"] for item in region_data["response"]["data"]]

    # Create the electricity demand time series.
    electricity_demand_time_series = pd.Series(
        values, index=pd.to_datetime(index)
    ).tz_localize("UTC")

    return electricity_demand_time_series


def download_and_extract_data(region_code: str) -> pd.Series:
    """
    Retrieve the electricity demand data from the Energy Information Administration (EIA).

    Parameters
    ----------
    region_code : str
        The code of the region of interest

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Define the start and end date according to the data availability.
    start_date_and_time = pd.Timestamp("2020-01-01 00:00:00")
    end_date_and_time = pd.Timestamp.today()

    # Define start and end dates and times for six-month retrieval periods.
    start_date_and_time_of_period = pd.date_range(
        start_date_and_time, end_date_and_time, freq="6MS"
    )
    end_date_and_time_of_period = start_date_and_time_of_period[1:].union(
        pd.to_datetime([end_date_and_time])
    )

    # Retrieve the electricity demand time series of all periods.
    electricity_demand_time_series_list = [
        download_and_extract_data_of_period(period_start, period_end, region_code)
        for period_start, period_end in zip(
            start_date_and_time_of_period, end_date_and_time_of_period
        )
    ]

    # Concatenate the electricity demand time series of all periods.
    electricity_demand_time_series = pd.concat(electricity_demand_time_series_list)

    # Clean the data.
    electricity_demand_time_series = time_series_utilities.clean_data(
        electricity_demand_time_series
    )

    return electricity_demand_time_series
