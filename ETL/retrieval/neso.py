#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the website of the UK's National Energy System Operator (NESO).

    The data is retrieved for the years from 2009 to the current year. The data is retrieved in one-year intervals.

    Source: https://www.neso.energy/data-portal/historic-demand-data
"""

import logging

import pandas
import util.fetcher
import util.general


def get_available_requests() -> list[int]:
    """
    Get the list of available requests to retrieve the electricity demand data on the UK's National Energy System Operator website.

    Returns
    -------
    available_requests : list[int]
        The list of available requests
    """

    # The available requests are the years from 2009 to the current year.
    available_requests = list(range(2009, pandas.Timestamp.now().year + 1))

    return available_requests


def get_url(year: int) -> str:
    """
    Get the URL of the electricity demand data on the UK's National Energy System Operator website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data

    Returns
    -------
    url : str
        The URL of the electricity demand data
    """

    # Check if the year is supported.
    assert year in get_available_requests(), f"The year {year} is not available."

    # Define the dataset names for the electricity demand data.
    dataset_name = {
        2009: "ed8a37cb-65ac-4581-8dbc-a3130780da3a",
        2010: "b3eae4a5-8c3c-4df1-b9de-7db243ac3a09",
        2011: "01522076-2691-4140-bfb8-c62284752efd",
        2012: "4bf713a2-ea0c-44d3-a09a-63fc6a634b00",
        2013: "2ff7aaff-8b42-4c1b-b234-9446573a1e27",
        2014: "b9005225-49d3-40d1-921c-03ee2d83a2ff",
        2015: "cc505e45-65ae-4819-9b90-1fbb06880293",
        2016: "3bb75a28-ab44-4a0b-9b1c-9be9715d3c44",
        2017: "2f0f75b8-39c5-46ff-a914-ae38088ed022",
        2018: "fcb12133-0db0-4f27-a4a5-1669fd9f6d33",
        2019: "dd9de980-d724-415a-b344-d8ae11321432",
        2020: "33ba6857-2a55-479f-9308-e5c4c53d4381",
        2021: "18c69c42-f20d-46f0-84e9-e279045befc6",
        2022: "bb44a1b5-75b1-4db2-8491-257f23385006",
        2023: "bf5ab335-9b40-4ea4-b93a-ab4af7bce003",
        2024: "f6d02c0f-957b-48cb-82ee-09003f2ba759",
        2025: "b2bde559-3455-4021-b179-dfe60c0337b0",
    }

    # Define the URL of the electricity demand data.
    url = f"https://api.neso.energy/api/3/action/datastore_search_sql?sql=SELECT%20*%20FROM%20%22{dataset_name[year]}%22%20ORDER%20BY%20%22_id%22%20ASC%20LIMIT%20100000"

    return url


def download_and_extract_data_for_request(year: int) -> pandas.Series:
    """
    Download the electricity demand time series from the website of the UK's National Energy System Operator.

    Parameters
    ----------
    year : int
        The year of the electricity demand data

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Check if the year is supported.
    assert year in get_available_requests(), f"The year {year} is not available."

    logging.info(f"Retrieving electricity demand data for the year {year}.")

    # Get the URL of the electricity demand data.
    url = get_url(year)

    # Fetch the electricity demand data from the URL.
    dataset = util.fetcher.fetch_data(url, "json", json_keys=["result", "records"])

    # Get the time zone of the country.
    local_time_zone = util.general.get_time_zone("GB")

    # Extract the electricity demand time series.
    electricity_demand_time_series = pandas.Series(
        dataset["ND"].values,
        index=pandas.date_range(
            start=f"{year}-01-01 00:30",
            periods=len(dataset),
            freq="30min",
            tz=local_time_zone,
        ),
    )

    return electricity_demand_time_series
