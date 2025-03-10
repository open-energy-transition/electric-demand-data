#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the website of Hydro-Québec.

    The data is retrieved for the years from 2019 to 2023.

    The data is saved in CSV and Parquet formats.

    Source: https://donnees.hydroquebec.com/explore/dataset/historique-demande-electricite-quebec/information/
"""

import pandas as pd
import util.time_series as time_series_utilities


def download_and_extract_data() -> pd.Series:
    """
    Retrieve the electricity demand data from the website of Hydro-Québec.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Define the URL of the electricity demand data.
    url = "https://donnees.hydroquebec.com/api/explore/v2.1/catalog/datasets/historique-demande-electricite-quebec/exports/csv?lang=en&timezone=America%2FToronto&use_labels=true&delimiter=%2C"

    # Fetch the electricity demand data.
    electricity_demand_time_series = pd.read_csv(url)

    # Set the date as the index.
    electricity_demand_time_series = electricity_demand_time_series.set_index(
        "date", drop=True
    ).squeeze()

    # Convert the index to a datetime object.
    electricity_demand_time_series.index = pd.to_datetime(
        electricity_demand_time_series.index, format="%Y-%m-%dT%H:%M:%S%z", utc=True
    )

    # Sort the index.
    electricity_demand_time_series = electricity_demand_time_series.sort_index()

    # Clean the data.
    electricity_demand_time_series = time_series_utilities.clean_data(
        electricity_demand_time_series
    )

    return electricity_demand_time_series
