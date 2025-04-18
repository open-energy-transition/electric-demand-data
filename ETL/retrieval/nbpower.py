# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of the New Brunswick Power Corporation (NB Power) in Canada.

    The data is retrieved for the years from 2018 to current year. The data is retrieved in one-month intervals.

    Source: https://tso.nbpower.com/Public/en/system_information_archive.aspx
"""

import logging

import pandas
import util.fetcher


def get_available_requests(code: str | None = None) -> list[tuple[int, int]]:
    """
    Get the list of available requests to retrieve the electricity demand data from the NB Power website.

    Parameters
    ----------
    code : str, optional
        The code of the country or region (not used in this function)

    Returns
    -------
    list[tuple[int, int]]
        The list of available requests
    """

    # Define the start and end date according to the data availability.
    start_date = pandas.Timestamp("2018-01-01")
    end_date = pandas.Timestamp.now()

    # Return the available requests, which are the years and months from 2018 to the current year.
    return [
        (year, month)
        for year in range(start_date.year, end_date.year + 1)
        for month in range(1, 13)
        if year != end_date.year or month < end_date.month
    ]


def get_url() -> str:
    """
    Get the URL of the electricity demand data on the NB Power website.

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    # Return the URL of the electricity demand data.
    return "https://tso.nbpower.com/Public/en/system_information_archive.aspx"


def download_and_extract_data_for_request(year: int, month: int) -> pandas.Series:
    """
    Download and extract the electricity demand data from the NB Power website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data
    month : int
        The month of the electricity demand data

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Check if the year and month are supported.
    assert (year, month) in get_available_requests(), (
        f"Year {year} and month {month} are not available."
    )

    logging.info(
        f"Retrieving electricity demand data for the year {year} and month {month}."
    )

    # Get the URL of the electricity demand data.
    url = get_url()

    # Fetch HTML content from the URL.
    dataset = util.fetcher.fetch_data(
        url,
        "query",
        query_event_target="ctl00$cphMainContent$lbGetData",
        query_params={
            "ctl00$cphMainContent$ddlMonth": month,
            "ctl00$cphMainContent$ddlYear": year,
        },
    )

    # Extract the electricity demand time series.
    # It is unclear whether the time values represent the start or end of the hour. Most likely, they represent the start of the hour but this is not confirmed.
    electricity_demand_time_series = pandas.Series(
        dataset["NB_LOAD"].values,
        index=pandas.to_datetime(dataset["HOUR"].values, format="%Y-%m-%d %H:%M"),
    )

    # Convert the time zone of the electricity demand time series to UTC.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize(
            "America/Moncton", ambiguous="infer"
        )
    )

    return electricity_demand_time_series
