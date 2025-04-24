#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
License: AGPL-3.0

Description:
This script retrieves cumulative electricity demand data from the website of the Electricity Market Information (EMI) for New Zealand.

The data is downloaded as a CSV file from Jan 1, 2005 up to the current date. The data is retrieved all at once.
Source: https://www.emi.ea.govt.nz/Wholesale/Reports/W_GD_C
"""

import logging
import pandas
import util.fetcher


def get_available_requests(code: str | None = None) -> None:
    """
    This script retrieves all data at once. No need to provide date inputs.
    """
    logging.info("The data is retrieved all at once.")
    return None


def get_url(year: int, month: int, day: int) -> str:
    """
    Get the URL of the electricity demand data on the EMI website.

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
    return (
        f"https://www.emi.ea.govt.nz/Wholesale/Reports/W_GD_C"
        f"?DateFrom={year}{month:02d}{day:02d}&DateTo={year}{month:02d}{day:02d}"
        f"&RegionType=NZ&_rsdr=D1&_si=_dr_DateFrom|20050101,"
        f"_dr_DateTo|{year}{month:02d}{day:02d},_dr_RegionType|NZ,v|4"
    )


def download_and_extract_data(year: int, month: int, day: int) -> pandas.Series:
    """
    Download and extract the electricity demand data from the EMI website.

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
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in GWh
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    url = get_url(year, month, day)

    dataset = util.fetcher.fetch_data(
        url, "text", output_content_type="tabular", header_params=headers
    )

    electricity_demand_time_series = pandas.Series(
        dataset["Demand (GWh)"].values,
        index=pandas.to_datetime(dataset["Period start"]),
    )

    return electricity_demand_time_series
