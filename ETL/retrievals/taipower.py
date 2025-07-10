#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This module provides functions to retrieve electricity demand
    data for Taiwan from the official Taiwan Power Company (Taipower)
    open data portal. Historical data from Jan 1, 2017 to Jul 1, 2022
    is retrieved as a consolidated CSV file from a public research
    archive. More recent data (latest three months) is retrieved in
    JSON format directly from Taipower’s official open data.

    Source: https://zenodo.org/records/7537890
    Source: https://data.gov.tw/dataset/37331
"""

import logging

import pandas
import utils.fetcher


def _check_input_parameters(pre_reform: bool) -> None:
    """
    Check if the input parameters are valid.

    Parameters
    ----------
    pre_reform : bool
        A boolean flag to indicate if the request is for the pre-reform
        period (until Jul 1, 2022).
    """
    # Check if the input parameters are valid.
    assert (pre_reform) in get_available_requests(), (
        "The request is not supported."
    )


def get_available_requests() -> list[bool]:
    """
    Get the available requests.

    This function retrieves the available requests for the electricity
    demand data for Taiwan.

    Returns
    -------
    list[bool]
        List of allowed values for pre_reform parameter.
    """
    # Return the boolean flag to indicate if the request is
    # for the pre-reform period.
    return [True, False]


def get_url(pre_reform: bool) -> str:
    """
    Get the URL of the electricity demand data.

    Parameters
    ----------
    pre_reform : bool
        Flag indicating if the data is for the pre-reform period.

    Returns
    -------
    str
        The URL of the electricity demand data.
    """
    # Check if the input parameters are valid.
    _check_input_parameters(pre_reform)

    if pre_reform:
        # If the request is for the pre-reform period, set the URL to
        # fetch .csv files for data from Jan 1, 2017 to Jul 1, 2022.
        return "https://zenodo.org/records/7537890/files/loadarea_10min_2017Jan_2022Jun.csv?download=1"
    else:
        # If the request is for the post-reform period, set the URL to
        # fetch JSON data for the most recent three months.
        return "https://service.taipower.com.tw/data/opendata/apply/file/d006010/001.json"


def download_and_extract_data_for_request(pre_reform: bool) -> pandas.Series:
    """
    Download and extract electricity demand data.

    This function downloads and extracts the electricity demand data
    for Taiwan.

    Parameters
    ----------
    pre_reform : bool
        A boolean flag to indicate if the request is for the pre-reform
        period (until Jul 1 2022).

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW.

    Raises
    ------
    ValueError
        If the extracted data is not a pandas DataFrame.
    """
    # Check if the input parameters are valid.
    _check_input_parameters(pre_reform)

    if pre_reform:
        logging.info(
            "Retrieving electricity demand data from Jan 1, 2017 to Jul 1, 2022."
        )

        # Get the URL of the electricity demand data.
        url = get_url(pre_reform)

        # Fetch the data from the URL.
        dataset = utils.fetcher.fetch_data(url, "csv")

        # Make sure the dataset is a pandas DataFrame.
        if not isinstance(dataset, pandas.DataFrame):
            raise ValueError(
                f"The extracted data is a {type(dataset)} object, expected a pandas DataFrame."
            )

        # Sum regional columns to get national demand
        dataset["National Demand"] = (
            dataset["south"]
            + dataset["north"]
            + dataset["east"]
            + dataset["central"]
        )

        # Extract the electricity demand data from the dataset.
        electricity_demand_time_series = pandas.Series(
            dataset["National Demand"].values,
            index=pandas.to_datetime(dataset["datetime"]),
        )

        # Add 10 minutes to the index because the demand data seems
        # to be reported at the beginning of the trading interval.
        electricity_demand_time_series.index = (
            electricity_demand_time_series.index + pandas.Timedelta(minutes=10)
        )

        # Add the time zone information to the time series.
        electricity_demand_time_series.index = (
            electricity_demand_time_series.index.tz_localize("Asia/Taipei")
        )

    else:
        logging.info(
            "Retrieving electricity demand data for the most recent three months."
        )

        # Get the URL of the electricity demand data.
        url = get_url(pre_reform)

        # Fetch the data from the URL.
        dataset = utils.fetcher.fetch_data(
            url,
            content_type="html",
            read_as="json",
            encoding_type="utf-8-sig",
            json_keys=["records", "NET_P"],
        )

        # Make sure the dataset is a pandas DataFrame.
        if not isinstance(dataset, pandas.DataFrame):
            raise ValueError(
                f"The extracted data is a {type(dataset)} object, expected a pandas DataFrame."
            )

        # Convert NET_P column to numeric, coercing errors to NaN
        dataset["NET_P"] = pandas.to_numeric(dataset["NET_P"], errors="coerce")

        # Group by DATETIME and sum NET_P to aggregate across units
        grouped = dataset.groupby("DATETIME")["NET_P"].sum()

        # Extract the electricity demand data from the dataset.
        electricity_demand_time_series = pandas.Series(
            grouped.values,
            index=pandas.to_datetime(grouped.index, utc=True),
        )

        # Add 10 minutes to the index because the demand data seems
        # to be reported at the beginning of the trading interval.
        electricity_demand_time_series.index += pandas.Timedelta(minutes=10)

        # Add the time zone information to the time series.
        electricity_demand_time_series.index = (
            electricity_demand_time_series.index.tz_convert("Asia/Taipei")
        )

    return electricity_demand_time_series
