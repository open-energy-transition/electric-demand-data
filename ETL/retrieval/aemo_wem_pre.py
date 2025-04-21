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

import pandas as pd
import util.fetcher


def get_available_requests() -> list[tuple[int]]:
    """
    Get the list of available requests to retrieve the electricity demand data from the AEMO website.

    Returns
    -------
    list[tuple[int]]
        The list of available requests (from 2006 to current year)
    """

    # Return the available requests for years 2006 to current year
    return [(year,) for year in range(2006, pd.Timestamp.now().year + 1)]


def get_url(year: int) -> str:
    """
    Get the URL of the electricity demand data on the AEMO website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data

    Returns
    -------
    url : str
        The URL of the electricity demand data
    """

    # Define the URL of the electricity demand data for the given year
    assert 2006 <= year <= pd.Timestamp.now().year, "Year is out of range"

    url = f"https://data.wa.aemo.com.au/datafiles/operational-demand/operational-demand-{year}.csv"

    return url


def download_and_extract_data_for_request(year: int) -> pd.Series:
    """
    Download and extract the electricity demand data from the AEMO website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data

    Returns
    -------
    electricity_demand_time_series : pd.Series
        The electricity demand time series in MW
    """

    assert 2006 <= year <= pd.Timestamp.now().year, "Year is out of range"

    # Get the URL of the electricity demand data.
    url = get_url(year)

    logging.info(f"Retrieving electricity demand data for the year {year}.")

    # Fetch the CSV data
    dataset = util.fetcher.fetch_data(
        url, "csv", csv_kwargs={"skiprows": 1}
    )  # Skip header if necessary

    # Adjust the column names based on the data structure (modify as needed)
    dataset.columns = [
        col.strip() for col in dataset.columns
    ]  # Clean column names if needed

    # Extract the electricity demand time series.
    index = pd.to_datetime(
        dataset["Date"] + " " + dataset["Time"], format="%d/%m/%Y %H:%M"
    )

    # Assuming the column name for demand is 'Demand' (check the actual CSV structure)
    electricity_demand_time_series = pd.Series(dataset["Demand"].values, index=index)

    # Optionally, localize the time zone if needed (set correct timezone based on the dataset's time zone)
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize(
            "Australia/Perth", ambiguous="NaT", nonexistent="NaT"
        )
    )

    return electricity_demand_time_series
