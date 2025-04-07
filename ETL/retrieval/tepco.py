#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the Tokyo Electric Power Company (TEPCO) website.

    The data is retrieved for the years from 2016 to the current year. The data is retrieved from the available CSV files on the TEPCO website.

    Source: https://www4.tepco.co.jp/en/forecast/html/download-e.html
"""

import logging

import pandas
import util.fetcher


def get_available_requests() -> list[int]:
    """
    Get the available requests for the electricity demand data on the Tokyo Electric Power Company website.

    Returns
    -------
    available_requests : list[int]
        The available requests for the electricity demand data
    """

    # The available requests are the years from 2016 to current year.
    available_requests = [year for year in range(2016, pandas.Timestamp.now().year + 1)]

    return available_requests


def get_url(year: int) -> str:
    """
    Get the URL of the electricity demand data on the Tokyo Electric Power Company website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data

    Returns
    -------
    url : str
        The URL of the electricity demand data
    """

    assert year in get_available_requests(), f"The year {year} is not available."

    # Define the URL of the electricity demand data.
    url = f"https://www4.tepco.co.jp/forecast/html/images/juyo-{year}.csv"

    return url


def download_and_extract_data_for_request(year: int) -> pandas.Series:
    """
    Read the CSV files from the Tokyo Electric Power Company website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    assert year in get_available_requests(), f"The year {year} is not available."

    logging.info(f"Retrieving electricity demand data for the year {year}.")

    # Get the URL of the electricity demand data.
    url = get_url(year)

    # Fetch the data from the URL.
    dataset = util.fetcher.fetch_data(url, "text", csv_kwargs={"skiprows": 2})

    # Define the index of the time series.
    index = pandas.to_datetime(
        [date + " " + time for date, time in zip(dataset["DATE"], dataset["TIME"])]
    ).tz_localize("Asia/Tokyo")

    # Extract the electricity demand time series. Multiply by 10 to convert from 10,000 kW (Japanese way of expressing unit of power) to MW.
    electricity_demand_time_series = (
        pandas.Series(dataset["ÀÑ(kW)"].values, index=index) * 10
    )

    # Add one hour to the time index because the time values appear to be provided at the beginning of the time interval.
    electricity_demand_time_series.index += pandas.Timedelta(hours=1)

    return electricity_demand_time_series
