# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of Ontario's Independent Electricity System Operator (IESO) in Canada.

    The data is retrieved for the years from 1994 to current year. The data is retrieved from the available CSV files on the IESO website.

    Source: https://www.ieso.ca/Power-Data/Data-Directory
    Source: https://reports-public.ieso.ca/public/Demand/
"""

import logging

import pandas
import util.fetcher


def get_available_requests() -> list[tuple[int, bool] | tuple[None, bool]]:
    """
    Get the list of available requests to retrieve the electricity demand data from the IESO website.

    Returns
    -------
    list[tuple[int, bool] | tuple[None, bool]]
        The list of available requests
    """

    # Return the available requests, which are a combination of a year number and a boolean indicating whether the data is before April 2002.
    # available_requests = [(year = None, before_Apr_2002 = True)
    #                       (year = 2002, before_Apr_2002 = False),
    #                       (year = 2003, before_Apr_2002 = False),
    #                       ...
    #                       (year = current year, before_Apr_2002 = False)]
    return [(None, True)] + [
        (year, False) for year in range(2002, pandas.Timestamp.now().year + 1)
    ]


def get_url(year: int | None, before_Apr_2002: bool) -> str:
    """
    Get the URL of the electricity demand data on the IESO website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data
    before_Apr_2002 : bool
        Whether the url is for the time period before April 2002

    Returns
    -------
    url : str
        The URL of the electricity demand data
    """

    assert (year, before_Apr_2002) in get_available_requests(), (
        "The request is not available."
    )

    # Define the URL of the electricity demand data.
    if before_Apr_2002:
        url = "https://www.ieso.ca/-/media/Files/IESO/Power-Data/data-directory/HourlyDemands_1994-2002.csv"
    elif year is not None and year >= 2002 and year <= pandas.Timestamp.now().year:
        url = f"https://reports-public.ieso.ca/public/Demand/PUB_Demand_{year}.csv"

    return url


def download_and_extract_data_for_request(
    year: int | None, before_Apr_2002: bool
) -> pandas.Series:
    """
    Download and extract the electricity demand data from the IESO website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data
    before_Apr_2002 : bool
        Whether the url is for the time period before April 2002

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    assert (year, before_Apr_2002) in get_available_requests(), (
        "The request is not available."
    )

    # Get the URL of the electricity demand data.
    url = get_url(year=year, before_Apr_2002=before_Apr_2002)

    if before_Apr_2002:
        logging.info("Retrieving electricity demand data for the years 1994 to 2002.")

        # Fetch HTML content from the URL.
        dataset = util.fetcher.fetch_data(url, "text", verify_ssl=False)

        # Extract the electricity demand time series.
        electricity_demand_time_series = pandas.Series(
            dataset["OntarioDemand"].values,
            index=pandas.to_datetime(dataset["DateTime"]),
        )

        # Convert the time zone of the electricity demand time series to UTC.
        electricity_demand_time_series.index = (
            electricity_demand_time_series.index.tz_localize(
                "America/Toronto", ambiguous="NaT", nonexistent="NaT"
            )
        )

        # Add one hour to the time index because the time values appear to be provided at the beginning of the time interval.
        electricity_demand_time_series.index += pandas.Timedelta(hours=1)

    else:
        logging.info(f"Retrieving electricity demand data for the year {year}.")

        # Fetch HTML content from the URL.
        dataset = util.fetcher.fetch_data(url, "csv", csv_kwargs={"skiprows": 3})

        # Extract the index of the electricity demand time series.
        index = pandas.to_datetime(
            [
                date + " " + str(time - 1) + ":00"
                for date, time in zip(dataset["Date"], dataset["Hour"])
            ]
        ).tz_localize("America/Toronto", ambiguous="NaT", nonexistent="NaT")

        # Extract the electricity demand time series.
        electricity_demand_time_series = pandas.Series(
            dataset["Ontario Demand"].values, index=index
        )

    return electricity_demand_time_series
