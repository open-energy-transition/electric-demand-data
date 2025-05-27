# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data the website of EEpias in Turkey.

    The data is retrieved by registering through email in the epias website.

    The user then login into the website and download the data.

    Source: https://giris.epias.com.tr/cas/login?service=https://seffaflik.epias.com.tr
"""

import logging
import os

import pandas
import util.directories


def get_available_requests() -> None:
    """
    Get the list of available requests to retrieve the electricity demand data from the Epias website.
    """

    logging.debug("TThe data is retrieved in one-year intervals.")


def get_url() -> str:
    """
    Get the URL of the electricity demand data from the Epias website.

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    # Return the URL of the electricity demand data.
    return "https://seffaflik.epias.com.tr/electricity/electricity-consumption/ex-post-consumption/real-time-consumption"


def download_and_extract_data() -> pandas.Series:
    """
    Extract the electricity demand data retrieved from the Epias website.

    This function assumes that the data has been downloaded and is available in the specified folder.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Get the data folder.
    data_directory = util.directories.read_folders_structure()["data_folder"]

    # Get the paths of the downloaded files. Each file starts with "EPI".
    downloaded_file_paths = [
        os.path.join(data_directory, file)
        for file in os.listdir(data_directory)
        if file.startswith("EPI")
    ]

    # Load the data from the downloaded files into a pandas DataFrame.
    dataset = pandas.concat(
        [pandas.read_csv(file_path) for file_path in downloaded_file_paths]
    )
    
    # Combine date and time columns
    dataset["Tarih Saat"] = dataset["Tarih"] + " " + dataset["Saat"]


    # Extract the electricity demand time series.
    electricity_demand_time_series = pandas.Series(
        dataset["Tüketim Miktarı(MWh)"].values,
        index=pandas.to_datetime(
            dataset["Tarih Saat"], format="%d.%m.%Y %H:%M"
        ),
    )

    # Add one hour to the index because the electricity demand seems to be provided at the beginning of the hour.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index + pandas.Timedelta(hours=1)
    )

    # Add the timezone information to the index.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize("Europe/Istanbul")
    )

    return electricity_demand_time_series
