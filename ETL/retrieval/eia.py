# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of the US Energy Information Administration (EIA).

    The data is retrieved for the years from 2020 to the current year. The data is retrieved in six-month intervals.

    Source: https://www.eia.gov/opendata/browser/electricity/rto/region-data
"""

import logging
import os
from pathlib import Path

import pandas
import util.fetcher
from dotenv import load_dotenv


def get_available_requests(
    code: str | None = None,
) -> list[tuple[pandas.Timestamp, pandas.Timestamp]]:
    """
    Get the list of available requests to retrieve the electricity demand data from the EIA website.

    Parameters
    ----------
    code : str, optional
        The code of the country or region (not used in this function)

    Returns
    -------
    list[pandas.Timestamp, pandas.Timestamp]
        The list of available requests
    """

    # Define the start and end date according to the data availability.
    start_date_and_time = pandas.Timestamp("2020-01-01 00:00:00")
    end_date_and_time = pandas.Timestamp.today()

    # Define start and end dates and times for six-month retrieval periods. A six-month period avoids the limitation of the API to retrieve a maximum of 5000 data points.
    start_date_and_time_of_period = pandas.date_range(
        start_date_and_time, end_date_and_time, freq="6MS"
    )
    end_date_and_time_of_period = start_date_and_time_of_period[1:].union(
        pandas.to_datetime([end_date_and_time])
    )

    # Return the available requests, which are the beginning and end of each six-month period.
    return list(zip(start_date_and_time_of_period, end_date_and_time_of_period))


def get_url(
    start_date_and_time: pandas.Timestamp,
    end_date_and_time: pandas.Timestamp,
    region_code: str,
) -> str:
    """
    Get the URL of the electricity demand data on the EIA website.

    Parameters
    ----------
    start_date_and_time : pandas.Timestamp
        The start date and time of the data retrieval
    end_date_and_time : pandas.Timestamp
        The end date and time of the data retrieval
    region_code : str
        The code of the region of interest

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    # Check that the number of time points is less than 5000.
    assert (end_date_and_time - start_date_and_time).days * 24 < 5000, (
        "The number of time points is greater than 5000."
    )

    # Check that the beginning of the period is on or after 2020-01-01.
    assert start_date_and_time >= pandas.Timestamp("2020-01-01 00:00:00"), (
        "The beginning of the data availability is 2020-01-01."
    )

    # Load the environment variables.
    load_dotenv(dotenv_path=Path(".") / ".env")

    # Get the API key.
    api_key = os.getenv("EIA_API_KEY")

    # Convert the start and end dates and times to the required format.
    start = start_date_and_time.strftime("%Y-%m-%dT%H")
    end = end_date_and_time.strftime("%Y-%m-%dT%H")

    # Return the URL of the electricity demand data.
    return f"https://api.eia.gov/v2/electricity/rto/region-data/data/?api_key={api_key}&facets[type][]=D&facets[respondent][]={region_code}&start={start}&end={end}&frequency=hourly&data[0]=value&sort[0][column]=period&sort[0][direction]=asc&offset=0&length=5000"


def download_and_extract_data_for_request(
    start_date_and_time: pandas.Timestamp,
    end_date_and_time: pandas.Timestamp,
    region_code: str,
) -> pandas.Series:
    """
    Download and extract the electricity demand data from the EIA website.

    Parameters
    ----------
    start_date_and_time : pandas.Timestamp
        The start date and time of the data retrieval
    end_date_and_time : pandas.Timestamp
        The end date and time of the data retrieval
    region_code : str
        The code of the region of interest

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Check that the number of time points is less than 5000.
    assert (end_date_and_time - start_date_and_time).days * 24 < 5000, (
        "The number of time points is greater than 5000."
    )

    # Check that the beginning of the period is on or after 2020-01-01.
    assert start_date_and_time >= pandas.Timestamp("2020-01-01 00:00:00"), (
        "The beginning of the data availability is 2020-01-01."
    )

    logging.info(f"Retrieving data from {start_date_and_time} to {end_date_and_time}.")

    # Extract the region code.
    region_code = region_code.split("_")[1]

    # Get the URL of the electricity demand data.
    url = get_url(start_date_and_time, end_date_and_time, region_code)

    # Fetch the electricity demand data from the URL.
    dataset = util.fetcher.fetch_data(url, "json", json_keys=["response", "data"])

    # Create the electricity demand time series.
    electricity_demand_time_series = pandas.Series(
        dataset["value"].values, index=pandas.to_datetime(dataset["period"])
    ).tz_localize("UTC")

    return electricity_demand_time_series
