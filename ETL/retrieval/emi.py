#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
License: AGPL-3.0

Description:
    This script retrieves electricity demand data from the website of the Electricity Market Information (EMI) in New Zealand.

    The data is downloaded from Jan 1, 2005 up to the current date. The data is retrieved all at once.

    Source: https://www.emi.ea.govt.nz/Wholesale/Reports/W_GD_C
"""

import logging

import pandas
import util.fetcher


def get_available_requests(
    code: str | None = None,
) -> list[tuple[pandas.Timestamp, pandas.Timestamp]]:
    """
    Get the list of available requests to retrieve the electricity demand data from the EMI website.

    Parameters
    ----------
    code : str, optional
        The code of the country or region (not used in this function)

    Returns
    -------
    list[tuple[pandas.Timestamp, pandas.Timestamp]]
        The list of available requests
    """

    # Define the start and end date according to the data availability.
    start_date_and_time = pandas.to_datetime("2005-01-01")
    end_date_and_time = pandas.Timestamp.today()

    # Define start and end dates and times for one-year retrieval periods.
    start_date_and_time_of_period = pandas.date_range(
        start_date_and_time, end_date_and_time, freq="YS"
    )
    end_date_and_time_of_period = (
        start_date_and_time_of_period[1:] - pandas.Timedelta("24h")
    ).union(pandas.to_datetime([end_date_and_time]))

    # Return the available requests, which are the beginning and end of each one-year period.
    return list(zip(start_date_and_time_of_period, end_date_and_time_of_period))


def get_url(start_date: pandas.Timestamp, end_date: pandas.Timestamp) -> str:
    """
    Get the URL of the electricity demand data on the EMI website.

    Parameters
    ----------
    start_date : pandas.Timestamp
        The start date and time of the data retrieval
    end_date : pandas.Timestamp
        The end date and time of the data retrieval

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    return (
        "https://www.emi.ea.govt.nz/Wholesale/Download/DataReport/CSV/W_GD_C"
        f"?DateFrom={start_date.strftime('%Y%m%d')}"
        f"&DateTo={end_date.strftime('%Y%m%d')}"
        "&RegionType=NZ"
    )


def download_and_extract_data_for_request(
    start_date: pandas.Timestamp, end_date: pandas.Timestamp
) -> pandas.Series:
    """
    Download and extract the electricity demand data from the EMI website.

    Parameters
    ----------
    start_date : pandas.Timestamp
        The start date and time of the data retrieval
    end_date : pandas.Timestamp
        The end date and time of the data retrieval

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    logging.info(f"Retrieving data from {start_date.date()} to {end_date.date()}.")

    # Get the URL of the electricity demand data.
    url = get_url(start_date, end_date)

    # Fetch the electricity demand data from the URL.
    dataset = util.fetcher.fetch_data(
        url,
        target_content_type="csv",
        csv_kwargs={"skiprows": 11},
    )

    # Extract the electricity demand time series. Convert GWh to MW considering a 0.5-hour time step.
    electricity_demand_time_series = pandas.Series(
        dataset["Demand (GWh)"].values * 1000 / 0.5,
        index=pandas.to_datetime(dataset["Period end"], format="%d/%m/%Y %H:%M:%S"),
    )

    # Add the time zone information to the time series.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize(
            "Pacific/Auckland", ambiguous="NaT", nonexistent="NaT"
        )
    )

    return electricity_demand_time_series
