#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the website of New Brunswick Power Corporation (NB Power).

    The data is retrieved for the years from 2018 to current year. The data is retrieved in one-month intervals.

    The data is saved in CSV and Parquet formats.

    Source: https://tso.nbpower.com/Public/en/system_information_archive.aspx
"""

from io import StringIO

import pandas as pd
import requests
import util.time_series as time_series_utilities
from bs4 import BeautifulSoup


def download_and_extract_data_of_month(year: int, month: int) -> pd.Series:
    """
    Retrieve the electricity demand data of a specific month from the website of NB Power.

    Parameters
    ----------
    year : int
        The year of the electricity demand data
    month : int
        The month of the electricity demand data

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Define the URL of the electricity demand data.
    url = "https://tso.nbpower.com/Public/en/system_information_archive.aspx"

    # Start a session.
    session = requests.Session()

    # Get the page to extract necessary form data (VIEWSTATE, EVENTVALIDATION).
    response = session.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract hidden input values (needed for form submission).
    viewstate = soup.find("input", {"id": "__VIEWSTATE"})["value"]
    eventvalidation = soup.find("input", {"id": "__EVENTVALIDATION"})["value"]

    # Prepare the payload.
    payload = {
        "__VIEWSTATE": viewstate,
        "__EVENTVALIDATION": eventvalidation,
        "__EVENTTARGET": "ctl00$cphMainContent$lbGetData",
        "ctl00$cphMainContent$ddlMonth": month,
        "ctl00$cphMainContent$ddlYear": year,
    }

    # Submit the form to get the data and read the response.
    response = session.post(url, data=payload)
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract the data from the response.
    dataset = pd.read_csv(StringIO(soup.text))

    # Extract the electricity demand time series.
    electricity_demand_time_series = pd.Series(
        dataset["NB_LOAD"].values,
        index=pd.to_datetime(dataset["HOUR"].values, format="%Y-%m-%d %H:%M"),
    )

    # Convert the time zone of the electricity demand time series to UTC.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize(
            "America/Moncton", ambiguous="infer"
        )
    )

    return electricity_demand_time_series


def download_and_extract_data() -> pd.Series:
    """
    Retrieve the electricity demand data from the website of Hydro-Québec.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    start_date = pd.Timestamp("2018-01-01")
    end_date = pd.Timestamp.now()

    # Retrieve the electricity demand data for each month.
    electricity_demand_time_series_list = [
        download_and_extract_data_of_month(year, month)
        for year in range(start_date.year, end_date.year + 1)
        for month in range(1, 13)
        if year != end_date.year or month < end_date.month
    ]

    # Concatenate the electricity demand time series.
    electricity_demand_time_series = pd.concat(electricity_demand_time_series_list)

    # Clean the data.
    electricity_demand_time_series = time_series_utilities.clean_data(
        electricity_demand_time_series
    )

    return electricity_demand_time_series
