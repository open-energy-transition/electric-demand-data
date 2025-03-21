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
import time
from pathlib import Path

import pandas as pd
import util.time_series as time_series_utilities
from dotenv import load_dotenv
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from requests.exceptions import ConnectionError


def fetch_entsoe_demand(
    client: EntsoePandasClient,
    iso_alpha_2_code: str,
    start_date_and_time: pd.Timestamp,
    end_date_and_time: pd.Timestamp,
    max_attempts: int = 3,
    retry_delay: int = 5,
) -> pd.Series:
    """
    Fetches the hourly electricity demand time series from ENTSO-E with retry logic.

    Parameters
    ----------
    client : entsoe.EntsoePandasClient
        The ENTSO-E API client
    iso_alpha_2_code : str
        The ISO Alpha-2 code of the country
    start_date_and_time : pd.Timestamp
        The start date and time of the data retrieval
    end_date_and_time : pd.Timestamp
        The end date and time of the data retrieval
    max_attempts : int, optional
        The maximum number of retry attempts (default is 3)
    retry_delay : int, optional
        The delay between retry attempts in seconds (default is 5)

    Returns
    -------
    pandas.Series
        The electricity demand time series in MW
    """

    attempts = 0

    while attempts < max_attempts:
        try:
            return client.query_load(
                iso_alpha_2_code, start=start_date_and_time, end=end_date_and_time
            )["Actual Load"]
        except ConnectionError:
            attempts += 1
            logging.warning(
                f"Connection error. Retrying ({attempts}/{max_attempts})..."
            )
            if attempts < max_attempts:
                time.sleep(retry_delay)

    raise ConnectionError(
        "Failed to connect to the ENTSO-E API after multiple attempts."
    )


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

    # Load the environment variables.
    load_dotenv(dotenv_path=Path(".") / ".env")

    # Define the ENTSO-E API client.
    client = EntsoePandasClient(api_key=os.getenv("ENTSOE_API_KEY"))

    # Add the time zone to the start date and time.
    start_date_and_time = start_date_and_time.tz_localize("UTC")
    end_date_and_time = end_date_and_time.tz_localize("UTC")

    try:
        # Download the electricity demand time series from the ENTSO-E API.
        electricity_demand_time_series = fetch_entsoe_demand(
            client, iso_alpha_2_code, start_date_and_time, end_date_and_time
        )

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

    except NoMatchingDataError:
        # If the data is not available, skip to the next country.
        logging.warning(
            f"No data available for {iso_alpha_2_code} between {start_date_and_time} and {end_date_and_time}."
        )

        return pd.Series()


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

    # Define the start and end date according to the data availability.
    start_date_and_time = pd.Timestamp("2014-01-01 00:00:00")
    end_date_and_time = pd.Timestamp.today()

    # Define start and end dates and times for one-year retrieval periods.
    start_date_and_time_of_period = pd.date_range(
        start_date_and_time, end_date_and_time, freq="YS"
    )
    end_date_and_time_of_period = start_date_and_time_of_period[1:].union(
        pd.to_datetime([end_date_and_time])
    )

    # Create a flag to check if the dataset has been created.
    dataset_created = False

    # Initialize the electricity demand time series.
    electricity_demand_time_series = pd.Series()

    # Loop over the retrieval periods.
    for period_start, period_end in zip(
        start_date_and_time_of_period, end_date_and_time_of_period
    ):
        logging.info(f"Retrieving data from {period_start} to {period_end}.")

        # Download the electricity demand time series from the ENTSO-E API.
        electricity_demand_time_series_of_period = download_and_extract_data_of_period(
            period_start, period_end, iso_alpha_2_code
        )

        if not electricity_demand_time_series_of_period.empty:
            if dataset_created:
                electricity_demand_time_series = pd.concat(
                    [
                        electricity_demand_time_series,
                        electricity_demand_time_series_of_period,
                    ]
                )
            else:
                electricity_demand_time_series = (
                    electricity_demand_time_series_of_period
                )
                dataset_created = True

    # Clean the data.
    electricity_demand_time_series = time_series_utilities.clean_data(
        electricity_demand_time_series
    )

    return electricity_demand_time_series
