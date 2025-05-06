# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of Kaggle in Turkey.

    The data is retrieved for the years from 2020 to 2022. 
    
    The data is retrieved by logging into the Kaggle website.

    Source: https://www.kaggle.com/datasets/dharanikra/electrical-power-demand-in-turkey


"""

import logging
import os

import pandas
import util.general


def get_available_requests() -> None:
    """
    Get the list of available requests to retrieve the electricity demand data from the Kaggle website.
    """

    logging.info("The data is retrieved all at once.")
    return None


def get_url() -> str:
    """
    Get the URL of the electricity demand data from the Kaggle website.

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    # Return the URL of the electricity demand data.
    return "https://www.kaggle.com/datasets/dharanikra/electrical-power-demand-in-turkey?resource=download"


def download_and_extract_data() -> pandas.Series:
    """
    Extract the electricity demand data retrieved from the Kaggle website.

    This function assumes that the data has been downloaded and is available in the specified folder.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Get the data folder.
    data_directory = util.general.read_folders_structure()["data_folder"]

    # Get the paths of the downloaded files. Each file starts with "KAG".
    downloaded_file_paths = [
        os.path.join(data_directory, file)
        for file in os.listdir(data_directory)
        if file.startswith("KAG")
    ]

    # Load the data from the downloaded files into a pandas DataFrame.
    dataset = pandas.concat(
        [pandas.read_csv(file_path) for file_path in downloaded_file_paths]
    )

    # Extract the electricity demand time series.
    electricity_demand_time_series = pandas.Series(
        dataset["onsumption (MWh)"].values,
        index=pandas.to_datetime(
            dataset["Date_Time"], format="%Y-%m-%d %H:%M:%S %p"
        ),
    )


    # Add the timezone information to the index.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize("Europe/Istanbul")
    )

    return electricity_demand_time_series
