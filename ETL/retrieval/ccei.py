#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the website of the Canadian Centre for Energy Information (CCEI).

    The data is retrieved from different starting dates depending on the region until the current date. The data has various time resolutions.

    The data is saved in CSV and Parquet formats.

    Source: https://energy-information.canada.ca/en/resources/high-frequency-electricity-data
"""

import pandas as pd
import util.time_series as time_series_utilities


def download_and_extract_data(region_code: str) -> pd.Series:
    """
    Retrieve the electricity demand data of a Canadian province or territory from the Canadian Centre for Energy Information.

    Parameters
    ----------
    region_code : str
        The code of the region of interest

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Extract the region code.
    region_code = region_code.split("_")[1]

    # Define the mapping between the region codes and API variable names.
    variable_names = {
        "AB": ["AB", "INTERNAL_LOAD"],
        "BC": ["BC", "LOAD"],
        "NB": ["NB", "LOAD"],
        "NL": ["NL", "DEMAND"],
        "NS": ["NS", "LOAD"],
        "ON": ["ON", "ONTARIO_DEMAND"],
        "PE": ["PE", "ON_ISL_LOAD"],
        "QC": ["QC", "DEMAND"],
        "SK": ["SK", "SYSTEM_DEMAND"],
        "YT": ["YK", "TOTAL"],
    }

    # Construct the request URL.
    url = f"https://api.statcan.gc.ca/hfed-dehf/sdmx/rest/data/CCEI,DF_HFED_{variable_names[region_code][0]},1.0/N...{variable_names[region_code][1]}?&dimensionAtObservation=AllDimensions&format=csv"

    # Fetch HTML content from the URL.
    dataset = pd.read_csv(url)

    if region_code == "NB":
        # Remove unknown code from the time step values.
        dataset["TIME_PERIOD"] = dataset["TIME_PERIOD"].str.replace(".000Z", "")
    if region_code == "ON":
        # Remove dummy time steps where the time is equal to 06:59:59 right before the daylight saving time change.
        dataset = dataset[~dataset["TIME_PERIOD"].str.contains("06:59:59")]

    # Extract the electricity demand time series with UTC time zone.
    electricity_demand_time_series = pd.Series(
        data=dataset["OBS_VALUE"].values, index=pd.to_datetime(dataset["TIME_PERIOD"])
    ).tz_localize("UTC")

    # Clean the data.
    electricity_demand_time_series = time_series_utilities.clean_data(
        electricity_demand_time_series
    )

    return electricity_demand_time_series
