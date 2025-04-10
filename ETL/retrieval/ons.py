#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of the Operador Nacional do Sistema Elétrico (ONS) in Brazil.

    The data is retrieved for the years from 2000 to the current year. The data is retrieved from the available CSV files on the ONS website.

    Source: https://dados.ons.org.br/dataset/curva-carga
"""

import logging

import pandas
import util.fetcher


def get_available_requests() -> list[int]:
    """
    Get the list of available requests to retrieve the electricity demand data from the ONS website.

    Returns
    -------
    list[int]
        The list of available requests
    """

    # Return the available requests, which are the years from 2000 to current year.
    return [year for year in range(2000, pandas.Timestamp.now().year + 1)]


def get_url(year: int) -> str:
    """
    Get the URL of the electricity demand data on the ONS website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    assert year in get_available_requests(), f"The year {year} is not available."

    # Return the URL of the electricity demand data.
    return f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/curva-carga-ho/CURVA_CARGA_{year}.csv"


def download_and_extract_data_for_request(year: int, region_code: str) -> pandas.Series:
    """
    Download and extract the electricity generation data from the ONS website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data
    region_code : str
        The code of the region of interest

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    assert year in get_available_requests(), f"The year {year} is not available."

    logging.info(f"Retrieving electricity demand data for the year {year}.")

    # Extract the region code.
    region_code = region_code.split("_")[1]

    # Get the URL of the electricity demand data.
    url = get_url(year)

    # Fetch the data from the URL.
    dataset = util.fetcher.fetch_data(url, "csv", csv_kwargs={"sep": ";"})

    # Filter the dataset for the region of interest.
    dataset = dataset[dataset["id_subsistema"] == region_code]

    # Extract the electricity demand time series.
    electricity_demand_time_series = pandas.Series(
        dataset["val_cargaenergiahomwmed"].values,
        index=pandas.to_datetime(dataset["din_instante"]),
    ).tz_localize("America/Sao_Paulo", ambiguous="NaT", nonexistent="NaT")

    # Add one hour to the time index because the time values appear to be provided at the beginning of the time interval.
    electricity_demand_time_series.index += pandas.Timedelta(hours=1)

    return electricity_demand_time_series
