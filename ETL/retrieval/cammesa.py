# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of the Compañía Administradora del Mercado Mayorista Eléctrico S.A. (CAMMESA) in Argentina.

    The data is retrieved from 2024-08-01 to current date. The data is retrieved in one-day intervals.

    Source: https://api.cammesa.com/demanda-svc/swagger-ui.html#/demanda-ws
    Source: https://api.cammesa.com/demanda-svc/demanda/RegionesDemanda
    Source: https://microfe.cammesa.com/demandaregionchart/assets/data/regionesCammesa.geojson.json
"""

import logging

import pandas
import util.entities
import util.fetcher

province_id = {
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


def _check_input_parameters(date: str) -> None:
    """
    Check if the input parameters are valid.

    Parameters
    ----------
    date : str
        The date of the electricity demand data in the format YYYY-MM-DD
    """

    # Check if the date is supported.
    assert date in get_available_requests(), (
        f"The date {date} is not in the supported range."
    )


def get_available_requests() -> list[str]:
    """
    Get the list of available requests to retrieve the electricity demand data from the CAMMESA website.

    Returns
    -------
    list[str]
        The list of available requests
    """

    # Read the start and end date of the available data.
    start_date, end_date = util.entities.read_date_ranges(data_source="cammesa")["AR"]

    # Return the available requests, which are the dates in the format YYYY-MM-DD.
    return (
        pandas.date_range(start=start_date, end=end_date, freq="D")
        .strftime("%Y-%m-%d")
        .to_list()
    )


def get_url(date: str) -> str:
    """
    Get the URL of the electricity demand data on the CAMMESA website.

    Parameters
    ----------
    date : str
        The date of the electricity demand data in the format YYYY-MM-DD

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    # Check if the input parameters are valid.
    _check_input_parameters(date)

    # Return the URL of the electricity demand data.
    return f"https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegionByFecha?id_region=1002&fecha={date}"


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
        The electricity demand time series in MW
    """

    # Check if the input parameters are valid.
    _check_input_parameters(date)

    logging.info(f"Retrieving electricity demand data for {date}.")

    # Get the URL of the electricity demand data.
    url = get_url(date)

    # Fetch the data from the URL.
    dataset = util.fetcher.fetch_data(
        url, "html", read_with="requests.get", read_as="json"
    )

    # Remove rows with NaN values for demand.
    dataset = dataset.dropna(subset=["dem"])

    # Extract the electricity demand time series.
    electricity_demand_time_series = pandas.Series(
        dataset["dem"].values, index=pandas.to_datetime(dataset["fecha"])
    )

    # Drop the last value of the time series because it belongs to the following day.
    electricity_demand_time_series = electricity_demand_time_series[:-1]

    return electricity_demand_time_series
