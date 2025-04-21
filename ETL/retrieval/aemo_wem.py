#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:
    This script retrieves the electricity load data from the website of the Australian Energy Market Operator (AEMO) for the Wholesale Electricity Market (WEM) in Western Australia.

    The data is retrieved for the dates from 2006, to the current date. The data is fetched from the available CSV files on the AEMO website for the dates before 2023 and JSON files for the dates after 2023.

    Source: https://data.wa.aemo.com.au/#operational-demand

    Source: https://data.wa.aemo.com.au/public/market-data/wemde/operationalDemandWithdrawal/dailyFiles/
"""

import logging

import pandas
import util.fetcher


def get_available_requests() -> list[tuple[int, int, int]]:
    """
    Get the list of available requests to retrieve the electricity demand data from September 26, 2023 to the current date.

    Returns
    -------
    list[tuple[int, int, int]]
        List of tuples in the format (year, month, day)
    """

    # Get the list of year, month, and day from 2023-09-26 to the current date.
    values_list = (
        pandas.date_range(
            start="2023-09-26",
            end=pandas.Timestamp.today(),
            freq="D",
        )
        .strftime("%Y-%m-%d")
        .str.split("-")
        .tolist()
    )

    # Return the available requests, which are tuples in the format (year, month, day).
    return [(int(year), int(month), int(day)) for year, month, day in values_list]


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

    # Before September 2023: URL to fetch .csv files for data before September 2023
    if year < 2023 or (year == 2023 and month < 9):
        url = f"https://data.wa.aemo.com.au/datafiles/operational-demand/operational-demand-{year}.csv"

    # After September 2023: URL to fetch .json files for data from September 2023 onward
    else:
        url = f"https://data.wa.aemo.com.au/public/market-data/wemde/operationalDemandWithdrawal/dailyFiles/OperationalDemandAndWithdrawal_{year}-{month:02d}-{day:02d}.json"

    return url


def download_and_extract_data_for_request(
    year: int, month: int, day: int
) -> pandas.DataFrame:
    """
    Download and extract the electricity demand data from the AEMO website.

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
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Check if the year, month, and day are supported.
    assert (year, month, day) in get_available_requests(), (
        f"Year {year} and month {month} and day {day} are not supported."
    )

    logging.info(
        f"Retrieving electricity demand data for {year}-{month:02d}-{day:02d}."
    )

    # Get the URL of the electricity demand data.
    url = get_url(year, month, day)

    # Fetch the data from the URL.
    if year < 2023:
        # For years before 2023, the data is in CSV format
        dataset = util.fetcher.fetch_data(url, "csv")
        electricity_demand_time_series = pandas.read_csv(
            dataset, parse_dates=["timestamp"], index_col="timestamp"
        )
    else:
        # For 2023 and after, the data is in JSON format
        dataset = util.fetcher.fetch_data(url, "json", json_keys=["data", "data"])
        electricity_demand_time_series = pandas.Series(
            dataset["operationalDemand"].values,
            index=pandas.to_datetime(dataset["dispatchInterval"]),
        )

    return electricity_demand_time_series
