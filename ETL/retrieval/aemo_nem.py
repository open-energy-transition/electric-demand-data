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
import util.entities
import util.fetcher


def _check_input_parameters(
    year: int | None = None, month: int | None = None, code: str | None = None
) -> None:
    """
    Check if the year and month are valid.

    Parameters
    ----------
    year : int
        The year of the data to retrieve
    month : int
        The month of the data to retrieve
    code : str
        The code of the subdivision
    """

    if code is not None:
        # Check if the code is valid.
        util.entities.check_code(code, "aemo_nem")

    if year is not None and month is not None and code is not None:
        # Check if the year and month are valid.
        assert (year, month) in get_available_requests(code), (
            f"Year {year} and month {month} are not supported."
        )


def get_available_requests(code: str) -> list[tuple[int, int]]:
    """
    Get the list of available requests to retrieve the electricity demand data on the AEMO website.

    Parameters
    ----------
    code : str, optional
        The code of the subdivision

    Returns
    -------
    available_requests : list[tuple[int, int]]
        The list of available requests
    """

    # Check if input parameters are valid.
    _check_input_parameters(code=code)

    # Read the start and end date of the available data.
    start_date, end_date = util.entities.read_date_ranges(data_source="aemo_nem")[code]

    # Get the list of available requests, which are the years and months.
    values_list = (
        pandas.date_range(start=start_date, end=end_date, freq="ME")
        .strftime("%Y-%m")
        .str.split("-")
        .tolist()
    )

    # Return the available requests, which are tuples in the format (year, month).
    return [(int(year), int(month)) for year, month in values_list]


def get_url(year: int, month: int, code: str) -> str:
    """
    Get the URL of the electricity demand data on the AEMO website.

    Parameters
    ----------
    month : int
        The month of the data to retrieve
    year : int
        The year of the data to retrieve
    code : str
        The code of the subdivision

    Returns
    -------
    url : str
        The URL of the electricity demand data
    """

    # Check if the input parameters are valid.
    _check_input_parameters(year=year, month=month, code=code)

    # Extract the subdivision code.
    subdivision_code = code.split("_")[1]

    # Define the URL of the electricity demand data.
    url = f"https://aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_{year}{month:02d}_{subdivision_code}1.csv"

    return url


def download_and_extract_data_for_request(
    year: int, month: int, code: str
) -> pandas.Series:
    """
    Download and extract the electricity demand data from the AEMO website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data
    month : int
        The month of the electricity demand data
    subdivision_code : str
        The subdivision code of the electricity demand data

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Check if the input parameters are valid.
    _check_input_parameters(year=year, month=month, code=code)

    logging.info(
        f"Retrieving electricity demand data for the year {year} and month {month}."
    )

    # Get the URL of the electricity demand data.
    url = get_url(year, month, code)

    # Fetch the electricity demand data from the URL.
    dataset = util.fetcher.fetch_data(
        url,
        "html",
        read_with="requests.get",
        header_params={"User-Agent": "Mozilla/5.0"},
        read_as="tabular",
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
