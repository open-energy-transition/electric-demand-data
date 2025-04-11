#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of the Alberta Electric System Operator (AESO) in Canada.

    The data is retrieved for the years from 2011 to 2024. The data is retrieved from the available Excel files on the AESO website.

    Source: https://www.aeso.ca/market/market-and-system-reporting/data-requests/hourly-load-by-area-and-region
"""

import logging

import pandas
import util.fetcher


def get_available_requests() -> list[int]:
    """
    Get the list of available requests to retrieve the electricity demand data from the AESO website.

    Returns
    -------
    list[int]
        The list of available requests
    """

    # Return the available requests, which are the numbers of the Excel files available on the AESO website.
    return [1, 2, 3, 4]


def get_url(file_number: int) -> str:
    """
    Get the URL of the electricity demand data on the AESO website.

    Parameters
    ----------
    file_number : int
        The number of the file to read

    Returns
    -------
    url : str
        The URL of the electricity demand data
    """

    # Check if the file number is supported.
    assert file_number in get_available_requests(), (
        f"File number {file_number} is not supported."
    )

    # Define the URL of the electricity demand data.
    if file_number == 1:
        url = "https://www.aeso.ca/assets/Uploads/Hourly-load-by-area-and-region-2011-to-2017-.xlsx"
    elif file_number == 2:
        url = "https://www.aeso.ca/assets/Uploads/Hourly-load-by-area-and-region-2017-2020.xlsx"
    elif file_number == 3:
        url = "https://www.aeso.ca/assets/Uploads/data-requests/Hourly-load-by-area-and-region-May-2020-to-Oct-2023.xlsx"
    elif file_number == 4:
        url = "https://www.aeso.ca/assets/Uploads/data-requests/Hourly-load-by-area-and-region-Nov-2023-to-Dec-2024.xlsx"

    return url


def _get_excel_information(file_number: int) -> tuple[str, int, list[str], list[str]]:
    """
    Get the Excel information of the electricity demand data on the AESO website.

    Parameters
    ----------
    file_number : int
        The number of the file to read

    Returns
    -------
    sheet_name : str
        The name of the sheet in the Excel file
    rows_to_skip : int
        The number of rows to skip in the Excel file
    index_columns : list[str]
        The names of the index columns in the Excel file
    load_columns : list[str]
        The names of the load columns in the Excel file
    """

    # Check if the file number is supported.
    assert file_number in get_available_requests(), (
        f"File number {file_number} is not supported."
    )

    # Define the excel information.
    if file_number == 1:
        sheet_name = "Load by AESO Planning Area"
        rows_to_skip = 1
        index_columns = ["DATE", "HOUR ENDING"]
        load_columns = [
            "SOUTH",
            "NORTHWEST",
            "NORTHEAST",
            "EDMONTON",
            "CALGARY",
            "CENTRAL",
        ]
    elif file_number == 2:
        sheet_name = "Load by Area and Region"
        rows_to_skip = 0
        index_columns = ["DATE", "HOUR ENDING"]
        load_columns = [
            "SOUTH",
            "NORTHWEST",
            "NORTHEAST",
            "EDMONTON",
            "CALGARY",
            "CENTRAL",
        ]
    elif file_number == 3:
        sheet_name = "Sheet1"
        rows_to_skip = 0
        index_columns = ["DT_MST"]
        load_columns = [
            "Calgary",
            "Central",
            "Edmonton",
            "Northeast",
            "Northwest",
            "South",
        ]
    elif file_number == 4:
        sheet_name = "Sheet1"
        rows_to_skip = 0
        index_columns = ["DT_MST"]
        load_columns = [
            "Calgary",
            "Central",
            "Edmonton",
            "Northeast",
            "Northwest",
            "South",
        ]

    return sheet_name, rows_to_skip, index_columns, load_columns


def download_and_extract_data_for_request(file_number: int) -> pandas.Series:
    """
    Download and extract the electricity demand data from the AESO website.
    There seem to be some inconsistencies in the data between the years before 2020 and the years after 2020.

    Parameters
    ----------
    file_number : int
        The number of the file to read

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Check if the file number is supported.
    assert file_number in get_available_requests(), (
        f"File number {file_number} is not supported."
    )

    logging.info(
        f"Retrieving electricity demand data from the file number {file_number}."
    )

    # Get the URL of the electricity demand data.
    url = get_url(file_number)

    # Get the Excel information of the electricity demand data.
    sheet_name, rows_to_skip, index_columns, load_columns = _get_excel_information(
        file_number
    )

    # Fetch the electricity demand data from the URL.
    dataset = util.fetcher.fetch_data(
        url,
        "excel",
        excel_kwargs={
            "sheet_name": sheet_name,
            "skiprows": rows_to_skip,
            "usecols": index_columns + load_columns,
        },
    )

    if file_number == 1 or file_number == 2:
        # Define starting time index.
        first_local_time_index = (
            dataset["DATE"][0] + pandas.Timedelta(hours=int(dataset["HOUR ENDING"][0]))
        ).tz_localize("America/Edmonton")

    elif file_number == 3 or file_number == 4:
        # Define starting time index.
        first_local_time_index = (
            dataset["DT_MST"].iloc[0].tz_localize("America/Edmonton")
        )

        # The Excel files seem to report all times in standard time, so we need to add the daylight saving time to the first time index, if necessary.
        first_local_time_index = first_local_time_index + first_local_time_index.dst()

        # The Excel files seem to report the beginning of the hour, so we need to add 1 hour.
        first_local_time_index = first_local_time_index + pandas.Timedelta(hours=1)

    # Define the local time index.
    local_time_index = pandas.date_range(
        start=first_local_time_index,
        periods=len(dataset),
        freq="1h",
        tz="America/Edmonton",
    )

    # Extract the electricity demand time series in the local time zone.
    electricity_demand_time_series = pandas.Series(
        data=dataset[load_columns].sum(axis=1).values, index=local_time_index
    )

    return electricity_demand_time_series
