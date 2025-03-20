#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the transparency platform of the European Network of Transmission System Operators for Electricity (ENTSO-E).

    The data is retrieved for the years from 2014 (end of year) to the current year. The data is retrieved in one-year intervals.

    The data is saved in CSV and Parquet formats.

    Source: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
    Source: https://github.com/EnergieID/entsoe-py
"""

import logging
import os
from pathlib import Path

import pandas as pd
import util.fetcher as fetcher
import util.time_series as time_series_utilities
from dotenv import load_dotenv


def get_available_requests() -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Get the list of available requests to retrieve the electricity load data on the ENTSO-E transparency platform.

    Returns
    -------
    available_requests : list[tuple[pd.Timestamp, pd.Timestamp]]
        The list of available requests
    """

    # Define the start and end date according to the data availability.
    start_date_and_time = pd.Timestamp("2014-01-01 00:00:00")
    end_date_and_time = pd.Timestamp.today()

    # Define start and end dates and times for one-year retrieval periods. A one-year period is the maximum available on the platform.
    start_date_and_time_of_period = pd.date_range(
        start_date_and_time, end_date_and_time, freq="YS"
    )
    end_date_and_time_of_period = start_date_and_time_of_period[1:].union(
        pd.to_datetime([end_date_and_time])
    )

    # The available requests are the beginning and end of each one-year period.
    available_requests = list(
        zip(start_date_and_time_of_period, end_date_and_time_of_period)
    )

    return available_requests


def get_url(
    start_date_and_time: pd.Timestamp,
    end_date_and_time: pd.Timestamp,
    iso_alpha_2_code: str = "",
) -> str:
    """
    Get the URL of the electricity load data on the ENTSO-E transparency platform.

    Parameters
    ----------
    start_date_and_time : pd.Timestamp
        The start date and time of the data retrieval
    end_date_and_time : pd.Timestamp
        The end date and time of the data retrieval
    iso_alpha_2_code : str
        The ISO Alpha-2 code of the country

    Returns
    -------
    url : str
        The URL of the electricity load data
    """

    # Convert the start and end dates and times to the required format.
    start = start_date_and_time.strftime("%Y%m%d%H00")
    end = end_date_and_time.strftime("%Y%m%d%H00")

    # Define the domain of the country.
    domain = "10YBE----------2"  # Belgium

    # Load the environment variables.
    load_dotenv(dotenv_path=Path(".") / ".env")

    # Get the ENTSO-E API client.
    api_key = os.getenv("ENTSOE_API_KEY")

    # Set some parameters for the API request.
    document_type = "A65"  # System total load
    process_type = "A16"  # Realised

    # Define the URL of the electricity load data.
    url = f"https://web-api.tp.entsoe.eu/api?securityToken={api_key}&documentType={document_type}&processType={process_type}&outBiddingZone_Domain={domain}&periodStart={start}&periodEnd={end}"

    return url


def download_and_extract_data_of_period(
    start_date_and_time: pd.Timestamp,
    end_date_and_time: pd.Timestamp,
    iso_alpha_2_code: str,
) -> pd.Series:
    """
    Download the electricity demand time series from the ENTSO-E API for a specific country and year.

    Parameters
    ----------
    start_date_and_time : pd.Timestamp
        The start date and time of the data retrieval period
    end_date_and_time : pd.Timestamp
        The end date and time of the data retrieval period
    iso_alpha_2_code : str
        The ISO Alpha-2 code of the country

    Returns
    -------
    pandas.Series
        The electricity demand time series in MW
    """

    logging.info(f"Retrieving data from {start_date_and_time} to {end_date_and_time}.")

    # Load the environment variables.
    load_dotenv(dotenv_path=Path(".") / ".env")

    # Get the ENTSO-E API client.
    api_key = os.getenv("ENTSOE_API_KEY")

    # Add the time zone to the start date and time.
    start_date_and_time = start_date_and_time.tz_localize("UTC")
    end_date_and_time = end_date_and_time.tz_localize("UTC")

    # Download the electricity demand time series from the ENTSO-E API.
    electricity_demand_time_series = fetcher.fetch_entsoe_demand(
        api_key, iso_alpha_2_code, start_date_and_time, end_date_and_time
    )

    if not electricity_demand_time_series.empty:
        # The time values are provided at the beginning of the time step. Set them at the end of the time step for consistency.
        if len(electricity_demand_time_series) > 1:
            # Calculate the time difference between the time values.
            time_difference = (
                electricity_demand_time_series.index.to_series().diff().min()
            )
        else:
            # Assume a one-hour time difference if there is only one time value.
            time_difference = pd.Timedelta("1h")

        # Add the time difference to the time values.
        electricity_demand_time_series.index = (
            electricity_demand_time_series.index + time_difference
        )

    return electricity_demand_time_series


def download_and_extract_data(iso_alpha_2_code: str) -> pd.Series:
    """
    Download the electricity demand time series from the ENTSO-E API for a specific country and year.

    Parameters
    ----------
    iso_alpha_2_code : str
        The ISO Alpha-2 code of the country

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Get the list of available requests.
    requests = get_available_requests()

    # Retrieve the electricity demand time series of all periods.
    electricity_demand_time_series_list = [
        download_and_extract_data_of_period(*request, iso_alpha_2_code)
        for request in requests
    ]

    # Remove empty time series.
    electricity_demand_time_series_list = [
        time_series
        for time_series in electricity_demand_time_series_list
        if not time_series.empty
    ]

    # Concatenate the electricity demand time series of all periods.
    electricity_demand_time_series = pd.concat(electricity_demand_time_series_list)

    # Clean the data.
    electricity_demand_time_series = time_series_utilities.clean_data(
        electricity_demand_time_series
    )

    return electricity_demand_time_series
