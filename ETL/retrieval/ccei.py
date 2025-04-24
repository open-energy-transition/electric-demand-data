# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of the Canadian Centre for Energy Information (CCEI).

    The data is retrieved from different starting dates depending on the region until the current date. The data has various time resolutions.

    Source: https://energy-information.canada.ca/en/resources/high-frequency-electricity-data
"""

import logging

import pandas
import util.fetcher


def get_available_requests(code: str | None = None) -> None:
    """
    Get the list of available requests to retrieve the electricity demand data from the CCEI website.

    Parameters
    ----------
    code : str, optional
        The code of the country or region (not used in this function)
    """

    logging.info("The data is retrieved all at once.")
    return None


def get_url(region_code: str) -> str:
    """
    Get the URL of the electricity demand data on the CCEI website.

    Parameters
    ----------
    region_code : str
        The code of the Province or Territory of interest

    Returns
    -------
    str
        The URL of the electricity demand data
    """

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

    assert region_code in variable_names.keys(), (
        f"Region code {region_code} is not supported."
    )

    # Return the URL of the electricity demand data.
    return f"https://api.statcan.gc.ca/hfed-dehf/sdmx/rest/data/CCEI,DF_HFED_{variable_names[region_code][0]},1.0/N...{variable_names[region_code][1]}?&dimensionAtObservation=AllDimensions&format=csv"


def download_and_extract_data(region_code: str) -> pandas.Series:
    """
    Download and extract the electricity demand data from the CCEI website.

    Parameters
    ----------
    region_code : str
        The code of the region of interest

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Extract the region code.
    if "_" in region_code:
        region_code = region_code.split("_")[1]
    else:
        raise ValueError(
            f"Invalid region_code format: '{region_code}'. Expected a combination of ISO Alpha-2 code and region code separated by an underscore"
        )

    # Get the URL of the electricity demand data.
    url = get_url(region_code)

    # Fetch HTML content from the URL.
    dataset = util.fetcher.fetch_data(url, "csv")

    if region_code == "NB":
        # Remove unknown code from the time step values.
        dataset["TIME_PERIOD"] = dataset["TIME_PERIOD"].str.replace(".000Z", "")
    if region_code == "ON":
        # Remove dummy time steps where the time is equal to 06:59:59 right before the daylight saving time change.
        dataset = dataset[~dataset["TIME_PERIOD"].str.contains("06:59:59")]

    # Extract the electricity demand time series with UTC time zone.
    electricity_demand_time_series = pandas.Series(
        data=dataset["OBS_VALUE"].values,
        index=pandas.to_datetime(dataset["TIME_PERIOD"]),
    ).tz_localize("UTC")

    return electricity_demand_time_series
