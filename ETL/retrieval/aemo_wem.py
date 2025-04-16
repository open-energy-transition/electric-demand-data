#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:
    This script retrieves the electricity load data for the Wholesale Electricity Market (WEM) from the Australian Energy Market Operator (AEMO) website.

    The data is retrieved for the dates from September 26, 2023, to the current date. The data is fetched from the available JSON files on the AEMO website.

    Source: https://data.wa.aemo.com.au/public/market-data/wemde/operationalDemandWithdrawal/dailyFiles/
"""

import logging
import pandas as pd
import util.fetcher


def get_available_requests() -> list[tuple[int, int, int]]:
    """
    Get the list of available requests to retrieve the electricity demand data from the AEMO website.

    Returns
    -------
    available_requests : list[tuple[int, int, int]]
        List of tuples in the format (year, month, day)
    """
    start_date = pd.Timestamp("2023-09-26")
    end_date = pd.Timestamp.today().normalize()

    available_requests = []
    current_date = start_date

    while current_date <= end_date:
        available_requests.append(
            (current_date.year, current_date.month, current_date.day)
        )
        current_date += pd.Timedelta(days=1)

    return available_requests


def get_url(year: int, month: int, day: int) -> str:
    """
    Get the URL of the electricity demand data on the AEMO website.

    Parameters
    ----------
    year : int
        The year of the data to retrieve
    month : int
        The month of the data to retrieve
    day : int
        The day of the data to retrieve

    Returns
    -------
    url : str
        The URL of the electricity demand data
    """
    # Check if the year, month, and day are supported.
    assert (year, month, day) in get_available_requests(), (
        f"Date {year}-{month:02d}-{day:02d} is not available."
    )

    # URL format for WEM
    url = (
        f"https://data.wa.aemo.com.au/public/market-data/wemde/operationalDemandWithdrawal/dailyFiles/"
        f"OperationalDemandAndWithdrawal_{year}-{month:02d}-{day:02d}.json"
    )

    return url


def download_and_extract_data_for_request(year: int, month: int, day: int) -> pd.DataFrame:
    """
    Download and extract the electricity demand data for a specific date from the AEMO website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data
    month : int
        The month of the electricity demand data
    day : int
        The day of the electricity demand data

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the extracted data
    """

  
    
     # Check if the year, month, and day are supported.
    assert (year, month, day) in get_available_requests(), (
        f"Year {year} and month {month} and day {day} are not supported."
    )

    logging.info(
        f"Retrieving electricity demand data for the year {year}, month {month}, and day {day}."
    )

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }

    url = get_url(year, month, day)

  
    dataset = util.fetcher.fetch_data(url, "json", output="tabular", header_params=headers)

    # Extract the electricity demand data from the JSON response.
    if isinstance(dataset, dict) and "data" in dataset and "data" in dataset["data"]:
        dataset = pd.DataFrame(dataset["data"]["data"])
    else:
        raise ValueError(f"Unexpected data format for {year}-{month:02d}-{day:02d}")

    # Extract the time series for electricity demand
    electricity_demand_time_series = pd.Series(
        dataset["TOTALDEMAND"].values,
        index=pd.to_datetime(dataset["SETTLEMENTDATE"]),
    )

    # Add the time zone information to the index.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize(
            "Australia/Perth", ambiguous="NaT", nonexistent="NaT"
        )
    )

    return electricity_demand_time_series
