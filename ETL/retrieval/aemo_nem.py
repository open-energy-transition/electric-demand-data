#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the website of the Australian Energy Market Operator (AEMO) for the National Electricity Market (NEM) in Australia.

    The data is retrieved for the years from December of 1998 to the current month. The data is retrieved from the available CSV files on the AEMO website.

    Source: https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data
"""

import logging

import pandas
import util.fetcher


def get_available_requests(code: str | None) -> list[tuple[int, int]]:
    """
    Get the list of available requests to retrieve the electricity demand data on the AEMO website.

    Parameters
    ----------
    code : str, optional
        The code of the country or region

    Returns
    -------
    available_requests : list[tuple[int, int]]
        The list of available requests
    """

    if code is None:
        raise ValueError("The region code must be provided.")
    else:
        # Get the region code.
        if "_" in code:
            # Extract the region code from the code.
            region_code = code.split("_")[1]
        else:
            region_code = code

        # For Tasmania, the data starts from May 2005.
        if region_code == "TAS":
            start_date = pandas.Timestamp("2005-05-01")
        else:
            start_date = pandas.Timestamp("1998-12-01")

    # Get the list of year, month from December of 1998 to today.
    values_list = (
        pandas.date_range(start=start_date, end=pandas.Timestamp.today(), freq="ME")
        .strftime("%Y-%m")
        .str.split("-")
        .tolist()
    )

    # Return the available requests, which are are tuples in the format (year, month).
    return [(int(year), int(month)) for year, month in values_list]


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
    assert (year, month) in get_available_requests(region_code), (
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
    assert (year, month) in get_available_requests(region_code), (
        f"Year {year} and month {month} are not supported."
    )

    logging.info(
        f"Retrieving electricity demand data for the year {year} and month {month}."
    )

    # Extract the region code.
    region_code = region_code.split("_")[1]

    # Define the headers for the request.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }

    # Get the URL of the electricity demand data.
    url = get_url(year, month, region_code)

    # Fetch the electricity demand data from the URL.
    dataset = util.fetcher.fetch_data(
        url, "text", output_content_type="tabular", header_params=headers
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
