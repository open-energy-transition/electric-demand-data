#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity load data from the website of British Columbia Hydro and Power Authority (BC Hydro).

    The data is retrieved for the years from 2001 to current year.

    The data is saved in CSV and Parquet formats.

    Source: https://www.bchydro.com/energy-in-bc/operations/transmission/transmission-system/balancing-authority-load-data/historical-transmission-data.html
"""

import logging

import numpy as np
import pandas as pd
import util.time_series as time_series_utilities


def read_excel_file(year: int) -> pd.Series:
    """
    Read the Excel file from BC Hydro website for the year of interest.

    Parameters
    ----------
    year : int
        The year of the electricity demand data

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    logging.info(f"Retrieving electricity demand data for the year {year}.")

    # Define the URL of the electricity demand data.
    url = "https://www.bchydro.com/content/dam/BCHydro/customer-portal/documents/corporate/suppliers/transmission-system/balancing_authority_load_data/Historical%20Transmission%20Data/"
    if year == 2001:
        url += "BalancingAuthorityLoadApr-Dec2001.xls"
    elif (
        (year >= 2002 and year <= 2006)
        or year == 2013
        or (year >= 2015 and year <= 2023)
    ):
        url += f"BalancingAuthorityLoad{year}.xls"
    elif year >= 2007 and year <= 2008 or year == 2014:
        url += f"{year}controlareaload.xls"
    elif year >= 2009 and year <= 2012:
        url += f"jandec{year}controlareaload.xls"
    elif year == 2024:
        url += "BalancingAuthorityLoad%202024.xls"
    elif year == 2025:
        url = "https://www.bchydro.com/content/dam/BCHydro/customer-portal/documents/corporate/suppliers/transmission-system/actual_flow_data/historical_data/BalancingAuthorityLoad%202025.xls"

    # Define the number of rows to skip.
    if (year >= 2001 and year <= 2006) or (year >= 2014 and year <= 2021):
        rows_to_skip = 1
    elif year >= 2007 and year <= 2011:
        rows_to_skip = 2
    elif (year >= 2012 and year <= 2013) or (year >= 2022 and year <= 2025):
        rows_to_skip = 3

    # Define the header of the Excel file.
    if year == 2007:
        header = None
    else:
        header = 0

    # Define the index columns of the Excel file.
    index_columns: list[str] | list[int]
    if (
        (year >= 2001 and year <= 2006)
        or (year >= 2008 and year <= 2011)
        or (year >= 2016 and year <= 2020)
    ):
        index_columns = ["Date", "HE"]
    elif year == 2007:
        index_columns = [0, 1]
    elif year >= 2012 and year <= 2015:
        index_columns = ["Date ▲", "HE"]
    elif year >= 2021 and year <= 2024:
        index_columns = ["Date ?", "HE"]
    elif year == 2025:
        index_columns = ["Date ", "HE"]

    # Define the column of the electricity demand data.
    load_column: list[str] | list[int]
    if (year >= 2001 and year <= 2006) or (year >= 2015 and year <= 2020):
        load_column = ["Balancing Authority Load"]
    elif year == 2007:
        load_column = [2]
    elif year >= 2008 and year <= 2011:
        load_column = ["MWh"]
    elif year >= 2012 and year <= 2014 or year >= 2021 and year <= 2025:
        load_column = ["Control Area Load"]

    # Fetch HTML content from the URL.
    dataset = pd.read_excel(
        url, skiprows=rows_to_skip, header=header, usecols=index_columns + load_column
    )

    # Extract the first and last time steps of the electricity demand time series.
    first_time_step = pd.to_datetime(
        str(dataset[index_columns[0]].iloc[0])
        + " "
        + str(int(dataset[index_columns[1]].iloc[0]) - 1)
        + ":00"
    ) + pd.Timedelta("1h")
    last_time_step = pd.to_datetime(
        str(dataset[index_columns[0]].iloc[-1])
        + " "
        + str(int(dataset[index_columns[1]].iloc[-1]) - 1)
        + ":00"
    ) + pd.Timedelta("1h")

    # Remove NaN and zero values where daylight saving time switch occurs. The other data points are typically nice and clean.
    available_data = dataset[load_column[0]][
        (np.logical_and(dataset[load_column[0]] != 0, dataset[load_column[0]].notna()))
    ]

    # Construct the index of the electricity demand time series.
    timestamps = pd.date_range(
        start=first_time_step, end=last_time_step, freq="h", tz="America/Vancouver"
    )

    # Extract the electricity demand time series.
    electricity_demand_time_series = pd.Series(available_data.values, index=timestamps)

    return electricity_demand_time_series


def download_and_extract_data() -> pd.Series:
    """
    Retrieve the electricity demand data from Ontario's Independent Electricity System Operator.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Retrieve the electricity demand time series for the years from 2001 to current year.
    electricity_demand_time_series_list = [
        read_excel_file(year) for year in range(2001, pd.Timestamp.now().year + 1)
    ]

    # Concatenate the electricity demand time series of all years.
    electricity_demand_time_series = pd.concat(electricity_demand_time_series_list)

    # Clean the data.
    electricity_demand_time_series = time_series_utilities.clean_data(
        electricity_demand_time_series
    )

    return electricity_demand_time_series
