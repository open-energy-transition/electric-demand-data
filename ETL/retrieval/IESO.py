#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the website of Ontario's Independent Electricity System Operator (IESO).

    The data is retrieved for the years from 1994 to current year.

    The data is saved in CSV and Parquet formats.

    Source: https://www.ieso.ca/Power-Data/Data-Directory
    Source: https://reports-public.ieso.ca/public/Demand/
"""

import logging
import ssl
from urllib.request import urlopen

import pandas as pd
import util.time_series as time_series_utilities


def read_csv_file_1994_2002() -> pd.Series:
    """
    Read the CSV file from Ontario's Independent Electricity System Operator website for the years 1994 to 2002.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    logging.info("Retrieving electricity demand data for the years 1994 to 2002.")

    # Construct the request URL.
    url = "https://www.ieso.ca/-/media/Files/IESO/Power-Data/data-directory/HourlyDemands_1994-2002.csv"

    # Ignore SSL certificate errors.
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # Fetch HTML content from the URL.
    dataset = pd.read_csv(urlopen(url, context=ctx))

    # Extract the electricity demand time series.
    electricity_demand_time_series = pd.Series(
        dataset["OntarioDemand"].values, index=pd.to_datetime(dataset["DateTime"])
    )

    # Convert the time zone of the electricity demand time series to UTC.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize(
            "America/Toronto", ambiguous="NaT", nonexistent="NaT"
        )
    )

    # Add one hour to the time index because the time values appear to be provided at the beginning of the time interval.
    electricity_demand_time_series.index += pd.Timedelta(hours=1)

    return electricity_demand_time_series


def read_csv_file(year: int) -> pd.Series:
    """
    Read the CSV file from Ontario's Independent Electricity System Operator website for a year between 2002 and the current year.

    Parameters
    ----------
    year : int
        The year of the electricity demand data

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    logging.info(f"Retrieving electricity demand data for the year {year}.")

    # Define the URL of the electricity demand data.
    url = f"https://reports-public.ieso.ca/public/Demand/PUB_Demand_{year}.csv"

    # Fetch HTML content from the URL.
    dataset = pd.read_csv(url, skiprows=3)

    # Extract the index of the electricity demand time series.
    index = pd.to_datetime(
        [
            date + " " + str(time - 1) + ":00"
            for date, time in zip(dataset["Date"], dataset["Hour"])
        ]
    ).tz_localize("America/Toronto", ambiguous="NaT", nonexistent="NaT")

    # Extract the electricity demand time series.
    electricity_demand_time_series = pd.Series(
        dataset["Ontario Demand"].values, index=index
    )

    return electricity_demand_time_series


def download_and_extract_data() -> pd.Series:
    """
    Retrieve the electricity demand data from Ontario's Independent Electricity System Operator.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Retrieve the electricity demand time series from the csv files.
    electricity_demand_time_series_list = [read_csv_file_1994_2002()] + [
        read_csv_file(year) for year in range(2002, pd.Timestamp.now().year + 1)
    ]

    # Concatenate the electricity demand time series.
    electricity_demand_time_series = pd.concat(electricity_demand_time_series_list)

    # Clean the data.
    electricity_demand_time_series = time_series_utilities.clean_data(
        electricity_demand_time_series
    )

    return electricity_demand_time_series
