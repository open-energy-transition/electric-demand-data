#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the website of the UK's National Energy System Operator (NESO).

    The data is retrieved for the years from 2009 to the current year. The data is retrieved in one-year intervals.

    The data is saved in CSV and Parquet formats.

    Source: https://www.neso.energy/data-portal/historic-demand-data
"""

import logging
from urllib import parse

import pandas as pd
import requests
import util.general as general_utilities
import util.time_series as time_series_utilities

# Define the dataset names for the electricity demand data.
dataset_name = {
    2009: "ed8a37cb-65ac-4581-8dbc-a3130780da3a",
    2010: "b3eae4a5-8c3c-4df1-b9de-7db243ac3a09",
    2011: "01522076-2691-4140-bfb8-c62284752efd",
    2012: "4bf713a2-ea0c-44d3-a09a-63fc6a634b00",
    2013: "2ff7aaff-8b42-4c1b-b234-9446573a1e27",
    2014: "b9005225-49d3-40d1-921c-03ee2d83a2ff",
    2015: "cc505e45-65ae-4819-9b90-1fbb06880293",
    2016: "3bb75a28-ab44-4a0b-9b1c-9be9715d3c44",
    2017: "2f0f75b8-39c5-46ff-a914-ae38088ed022",
    2018: "fcb12133-0db0-4f27-a4a5-1669fd9f6d33",
    2019: "dd9de980-d724-415a-b344-d8ae11321432",
    2020: "33ba6857-2a55-479f-9308-e5c4c53d4381",
    2021: "18c69c42-f20d-46f0-84e9-e279045befc6",
    2022: "bb44a1b5-75b1-4db2-8491-257f23385006",
    2023: "bf5ab335-9b40-4ea4-b93a-ab4af7bce003",
    2024: "f6d02c0f-957b-48cb-82ee-09003f2ba759",
    2025: "b2bde559-3455-4021-b179-dfe60c0337b0",
}


def download_and_extract_data_of_year(year: int) -> pd.Series:
    """
    Download the electricity demand time series for a specific year.

    Parameters
    ----------
    year : int
        The year of the electricity demand data

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Get the time zone of the country.
    local_time_zone = general_utilities.get_time_zone("GB")

    # Define the SQL query.
    sql_query = (
        f"""SELECT * FROM  "{dataset_name[year]}" ORDER BY "_id" ASC LIMIT 100000"""
    )
    params = {"sql": sql_query}

    try:
        # Retrieve and extract the electricity demand data.
        response = requests.get(
            "https://api.neso.energy/api/3/action/datastore_search_sql",
            params=parse.urlencode(params),
        )
        data = response.json()["result"]
        data = pd.DataFrame(data["records"])

        # Extract the electricity demand time series.
        electricity_demand_time_series = pd.Series(
            data["ND"].values,
            index=pd.date_range(
                start=f"{year}-01-01 00:30",
                periods=len(data),
                freq="30min",
                tz=local_time_zone,
            ),
        )

    except requests.exceptions.RequestException as e:
        print(e.response.text)

    return electricity_demand_time_series


def download_and_extract_data() -> pd.Series:
    """
    Download the electricity demand time series from the website of the UK's National Energy System Operator.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Define the years of the data retrieval according to the data availability.
    start_year = 2009
    end_year = pd.Timestamp.today().year

    # Define the years for the data retrieval.
    years = range(start_year, end_year + 1)

    # Initialize the flag for the dataset creation.
    dataset_created = False

    # Initialize the electricity demand time series.
    electricity_demand_time_series = pd.Series()

    # Loop over the years.
    for year in years:
        logging.info(f"Retrieving electricity demand data for the year {year}.")

        # Retrieve the electricity demand time series.
        electricity_demand_time_series_of_year = download_and_extract_data_of_year(year)

        if dataset_created:
            electricity_demand_time_series = pd.concat(
                [electricity_demand_time_series, electricity_demand_time_series_of_year]
            )
        else:
            electricity_demand_time_series = electricity_demand_time_series_of_year
            dataset_created = True

    # Clean the data.
    electricity_demand_time_series = time_series_utilities.clean_data(
        electricity_demand_time_series
    )

    return electricity_demand_time_series
