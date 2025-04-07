#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the website of the Compañía Administradora del Mercado Mayorista Eléctrico S.A. (CAMMESA) in Argentina.

    The data is retrieved for the years

    Source: https://api.cammesa.com/demanda-svc/swagger-ui.html#/demanda-ws
    Source: https://api.cammesa.com/demanda-svc/demanda/RegionesDemanda
    Source: https://microfe.cammesa.com/demandaregionchart/assets/data/regionesCammesa.geojson.json
"""

import logging

import pandas
import util.fetcher

region_id = {
    "AR": 1002,  # Argentina
    "BAS": [425, 426],  # Provincia de Buenos Aires
    "CEN": 422,  # Centro
    "COM": 420,  # Comahue
    "CUY": 429,  # Cuyo
    "LIT": 417,  # Litoral
    "NEA": 418,  # Nordeste Argentino
    "NOA": 419,  # Noroeste Argentino
    "PAT": 111,  # Patagonia
}


def get_available_requests() -> list[str]:
    """
    Get the available requests for the electricity demand data on the Tokyo Electric Power Company website.

    Returns
    -------
    available_requests : list[str]
        The available requests for the electricity demand data
    """

    # The available requests are the days from 2024-08-01 to current date.
    available_requests = (
        pandas.date_range(start="2024-08-01", end=pandas.Timestamp.today(), freq="D")
        .strftime("%Y-%m-%d")
        .to_list()
    )

    return available_requests


def get_url(date: str) -> str:
    """
    Get the URL of the electricity demand data on the CAMMESA website.

    Parameters
    ----------
    date : str
        The date of the electricity demand data in the format YYYY-MM-DD

    Returns
    -------
    url : str
        The URL of the electricity demand data
    """

    # Check if the date is supported.
    assert date in get_available_requests(), f"The date {date} is not available."

    # Define the URL of the electricity demand data.
    url = f"https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegionByFecha?id_region=1002&fecha={date}"

    return url


def download_and_extract_data_for_request(date: str) -> pandas.Series:
    """
    Download and extract the electricity demand data from the CAMMESA website.

    Parameters
    ----------
    date : str
        The date of the electricity demand data in the format YYYY-MM-DD

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    assert date in get_available_requests(), f"The date {date} is not available."

    logging.info(f"Retrieving electricity demand data for {date}.")

    # Get the URL of the electricity demand data.
    url = get_url(date)

    # Fetch the data from the URL.
    dataset = util.fetcher.fetch_data(url, "json")

    # Remove rows with NaN values for demand.
    dataset = dataset.dropna(subset=["dem"])

    # Extract the electricity demand time series.
    electricity_demand_time_series = pandas.Series(
        dataset["dem"].values, index=pandas.to_datetime(dataset["fecha"])
    )

    # Drop the last value of the time series because it belongs to the following day.
    electricity_demand_time_series = electricity_demand_time_series[:-1]

    return electricity_demand_time_series
