# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of the Comité de Operación Económica (COES) of Peru.

    The data is retrieved from 1997 to current date. The data is retrieved in one-year intervals.

    Source: https://www.coes.org.pe/Portal/portalinformacion/demanda
"""

import logging

import pandas
import util.fetcher


def get_available_requests() -> list[int]:
    """
    Get the list of available requests to retrieve the electricity demand data from the COES website.

    Returns
    -------
    list[int]
        The list of available requests
    """

    # Return the available requests, which are the years from 2000 to current date.
    return [year for year in range(1997, pandas.Timestamp.today().year + 1)]


def get_url() -> str:
    """
    Get the URL of the electricity demand data on the COES website.

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    # Return the URL of the electricity demand data.
    return "https://www.coes.org.pe/Portal/portalinformacion/demanda"


def download_and_extract_data_for_request(year: int) -> pandas.Series:
    """
    Download and extract the electricity demand data from the COES website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    assert year in get_available_requests(), f"The year {year} is not available."

    logging.info(f"Retrieving electricity demand data for {year}.")

    # Get the URL of the electricity demand data.
    url = get_url()

    # Define the request parameters.
    request_params = {"fechaInicial": f"01/01/{year}", "fechaFinal": f"31/12/{year}"}

    # Define the json keys to extract the data.
    json_keys = ["Chart", "Series"]

    # Fetch the data from the URL.
    dataset = util.fetcher.fetch_data(
        url,
        "json",
        request_type="post",
        request_params=request_params,
        json_keys=json_keys,
    )

    # Extract the electricity demand data from the dataset.
    dataset = pandas.DataFrame(dataset[dataset["Name"] == "Ejecutado"]["Data"][0])

    # Extract the electricity demand time series.
    electricity_demand_time_series = pandas.Series(
        dataset["Valor"].values, index=pandas.to_datetime(dataset["Nombre"])
    )

    # Add timezone information to the index.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize("America/Lima")
    )

    return electricity_demand_time_series
