#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the website of the Alberta Electric System Operator (AESO).

    The data is retrieved for the years from 2011 to 2024. The data is retrieved from the available Excel files on the AESO website.

    The data is saved in CSV and Parquet formats.

    Source: https://www.aeso.ca/market/market-and-system-reporting/data-requests/hourly-load-by-area-and-region
"""

import logging

import pandas as pd
import util.time_series as time_series_utilities


def read_excel_file(file_number: int) -> pd.Series:
    """
    Read the Excel files on the Alberta Electric System Operator website.

    Parameters
    ----------
    file_number : int
        The number of the file to read

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    logging.info(
        f"Retrieving electricity demand data from the file number {file_number}."
    )

    # Define the URL of the electricity demand data and excel information.
    if file_number == 1:
        url = "https://www.aeso.ca/assets/Uploads/Hourly-load-by-area-and-region-2011-to-2017-.xlsx"
        sheet_name = "Load by AESO Planning Area"
        rows_to_skip = 1
        index_to_keep = ["DATE", "HOUR ENDING"]
        columns_to_keep = [
            "SOUTH",
            "NORTHWEST",
            "NORTHEAST",
            "EDMONTON",
            "CALGARY",
            "CENTRAL",
        ]
    elif file_number == 2:
        url = "https://www.aeso.ca/assets/Uploads/Hourly-load-by-area-and-region-2017-2020.xlsx"
        sheet_name = "Load by Area and Region"
        rows_to_skip = 0
        index_to_keep = ["DATE", "HOUR ENDING"]
        columns_to_keep = [
            "SOUTH",
            "NORTHWEST",
            "NORTHEAST",
            "EDMONTON",
            "CALGARY",
            "CENTRAL",
        ]
    elif file_number == 3:
        url = "https://www.aeso.ca/assets/Uploads/data-requests/Hourly-load-by-area-and-region-May-2020-to-Oct-2023.xlsx"
        sheet_name = "Sheet1"
        rows_to_skip = 0
        index_to_keep = ["DT_MST"]
        columns_to_keep = [
            "Calgary",
            "Central",
            "Edmonton",
            "Northeast",
            "Northwest",
            "South",
        ]
    elif file_number == 4:
        url = "https://www.aeso.ca/assets/Uploads/data-requests/Hourly-load-by-area-and-region-Nov-2023-to-Dec-2024.xlsx"
        sheet_name = "Sheet1"
        rows_to_skip = 0
        index_to_keep = ["DT_MST"]
        columns_to_keep = [
            "Calgary",
            "Central",
            "Edmonton",
            "Northeast",
            "Northwest",
            "South",
        ]

    # Fetch the electricity demand data from the URL.
    dataset = pd.read_excel(
        url,
        sheet_name=sheet_name,
        skiprows=rows_to_skip,
        usecols=index_to_keep + columns_to_keep,
    )

    if file_number == 1 or file_number == 2:
        # Define starting time index.
        first_local_time_index = (
            dataset["DATE"][0] + pd.Timedelta(hours=int(dataset["HOUR ENDING"][0]))
        ).tz_localize("America/Edmonton")

    elif file_number == 3 or file_number == 4:
        # Define starting time index.
        first_local_time_index = (
            dataset["DT_MST"].iloc[0].tz_localize("America/Edmonton")
        )

        # The Excel files seem to report all times in standard time, so we need to add the daylight saving time to the first time index, if necessary.
        first_local_time_index = first_local_time_index + first_local_time_index.dst()

        # The Excel files seem to report the beginning of the hour, so we need to add 1 hour.
        first_local_time_index = first_local_time_index + pd.Timedelta(hours=1)

    # Define the local time index.
    local_time_index = pd.date_range(
        start=first_local_time_index,
        periods=len(dataset),
        freq="1h",
        tz="America/Edmonton",
    )

    # Extract the electricity demand time series in the local time zone.
    electricity_demand_time_series = pd.Series(
        data=dataset[columns_to_keep].sum(axis=1).values, index=local_time_index
    )

    return electricity_demand_time_series


def download_and_extract_data() -> pd.Series:
    """
    Retrieve the electricity demand data of Alberta from the Alberta Electric System Operator.
    There seem to be some inconsistencies in the data between the years before 2020 and the years after 2020.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Retrieve the electricity demand data from each Excel file.
    electricity_demand_time_series_list = [
        read_excel_file(file_number) for file_number in range(1, 5)
    ]

    # Concatenate the electricity demand time series of all files.
    electricity_demand_time_series = pd.concat(electricity_demand_time_series_list)

    # Clean the data.
    electricity_demand_time_series = time_series_utilities.clean_data(
        electricity_demand_time_series
    )

    return electricity_demand_time_series
