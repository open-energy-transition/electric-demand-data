#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data for the National Electricity Market (NEM) from the Australian Energy Market Operator (AEMO) website.

    The data is retrieved for the years from December of 1998 to the current month. The data is retrieved from the available CSV files on the AEMO website.

    Source: https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data
"""

import logging

import pandas
import util.fetcher


def get_available_requests() -> list[tuple[int, int]]:
    """
    Get the list of available requests to retrieve the electricity demand data on the AEMO website.

    Returns
    -------
    available_requests : list[tuple[int, int]]
        The list of available requests
    """

    # Define the start and end date according to the data availability.
    start_date = pandas.Timestamp("1998-12-01")
    end_date = pandas.Timestamp.today()

    # The available requests are the years and months from December of 1998 to the current month.
    available_requests = [
        (year, month)
        for year in range(start_date.year, end_date.year + 1)
        for month in range(1, 13)
        if year != end_date.year or month < end_date.month
    ]

    return available_requests


def get_url(year: int, month: int, region_code: str) -> str:
    """
    Get the URL of the electricity demand data on the AEMO website.

    Parameters
    ----------
    month : int
        The month of the data to retrieve
    year : int
        The year of the data to retrieve
    region_code : str
        The region code of the data to retrieve

    Returns
    -------
    url : str
        The URL of the electricity demand data
    """

    # Check if the year and month are supported.
    assert (year, month) in get_available_requests(), (
        f"Year {year} and month {month} are not supported."
    )

    # Define the URL of the electricity demand data.
    url = f"https://aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_{year}{month:02d}_{region_code}1.csv"

    return url


def download_and_extract_data_for_request(
    year: int, month: int, region_code: str
) -> pandas.Series:
    """
    Download and extract the electricity demand data from the AEMO website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data
    month : int
        The month of the electricity demand data
    region_code : str
        The region code of the electricity demand data

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Check if the year and month are supported.
    assert (year, month) in get_available_requests(), (
        f"Year {year} and month {month} are not supported."
    )

    logging.info(
        f"Retrieving electricity demand data for the year {year} and month {month}."
    )

    # Extract the region code.
    region_code = region_code.split("_")[1]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }

    # Get the URL of the electricity demand data.
    url = get_url(year, month, region_code)

    # Fetch the electricity demand data from the URL.
    dataset = util.fetcher.fetch_data(
        url, "text", output="tabular", header_params=headers
    )

    # Extract the electricity demand data from the dataset.
    electricity_demand_time_series = pandas.Series(
        dataset["TOTALDEMAND"].values,
        index=pandas.to_datetime(dataset["SETTLEMENTDATE"]),
    )

    # Add the time zone information to the index.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize(
            "Australia/Sydney", ambiguous="NaT", nonexistent="NaT"
        )
    )

    return electricity_demand_time_series
