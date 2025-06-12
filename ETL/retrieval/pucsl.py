# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of Public Utilities Commission of Sri Lanka (PUCSL) in Sri Lanka.

    The data is retrieved for the years from 2023-01-01 to today. The data is retrieved in one-week intervals.

    Source: https://gendata.pucsl.gov.lk/generation-profile
"""

import logging
import os

import pandas
import util.directories


def get_available_requests() -> None:
    """
    Get the list of available requests to retrieve the electricity demand data from the PUCSL website.
    """

    logging.debug("The data is retrieved manually in one-week intervals.")


def get_url() -> str:
    """
    Get the URL of the electricity demand data from the PUCSL website.

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    # Return the URL of the electricity demand data.
    return "https://gendata.pucsl.gov.lk/generation-profile"


def download_and_extract_data() -> pandas.Series:
    """
    Extract the electricity demand data retrieved from the PUCSL website.

    This function assumes that the data has been downloaded and is available in the specified folder.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Get the data folder.
    data_directory = util.directories.read_folders_structure()[
        "manually_downloaded_data_folder"
    ]

    # Get the paths of the downloaded files. Each file starts with "PUC".
    downloaded_file_paths = [
        os.path.join(data_directory, file)
        for file in os.listdir(data_directory)
        if file.startswith("PUC")
    ]

    # Load the data from the downloaded files into a pandas DataFrame.
    dataset = pandas.concat(
        [pandas.read_csv(file_path) for file_path in downloaded_file_paths]
    )

    # List of relevant generation columns
    generation_cols = [
        "Solar (Telemetered) (Power in MW)",
        "Mini Hydro (Telemetered) (Power in MW)",
        "Biomass and Waste Heat (Power in MW)",
        "Wind (Power in MW)",
        "Major Hydro (Power in MW)",
        "Oil (IPP) (Power in MW)",
        "Oil (CEB) (Power in MW)",
        "Coal (Power in MW)",
    ]

    # Convert relevant columns to numeric, coercing errors (e.g. 'Data N/A') to NaN
    dataset[generation_cols] = dataset[generation_cols].apply(
        lambda col: pandas.to_numeric(col, errors="coerce")
    )

    # Sum the generation columns row-wise to get total generation
    total_generation = dataset[generation_cols].sum(axis=1)

    # Create a time series indexed by the datetime column
    electricity_demand_time_series = pandas.Series(
        total_generation.values, index=pandas.to_datetime(dataset["Date (GMT+5:30)"])
    )

    # Add 15 minutes to the index to reflect data timestamp adjustment
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index + pandas.Timedelta(minutes=15)
    )

    # Localize the index to 'Asia/Colombo' timezone, handle ambiguous/nonexistent times by setting NaT
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize(
            "Asia/Colombo", ambiguous="NaT", nonexistent="NaT"
        )
    )

    return electricity_demand_time_series
