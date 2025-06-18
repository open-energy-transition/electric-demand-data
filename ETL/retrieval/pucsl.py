# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of Public Utilities Commission of Sri Lanka (PUCSL) in Sri Lanka.

    The data is retrieved for the years from 2023-01-01 to today. The data is retrieved in one-week intervals.

    Source: https://gendata.pucsl.gov.lk/generation-profile
"""

from datetime import datetime, timedelta

import pandas
import requests


def get_available_requests():
    """
    Get the list of available requests to retrieve the electricity demand data from the PUCSL website.
    """
    start_date = datetime(2023, 1, 1)
    end_limit = datetime.now()
    delta = timedelta(days=7)

    requests_list = []

    while start_date < end_limit:
        end_date = min(start_date + delta, end_limit)
        requests_list.append((start_date, end_date))
        start_date = end_date  # advance to next interval

    return requests_list


def get_url(start_date: datetime, end_date: datetime) -> str:
    """
    Get the URL of the electricity demand data on the PUCSL website.

    Returns
    -------
    str
        The API URL of the electricity demand data
    """

    from_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    to_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    return (
        "https://gendata.pucsl.gov.lk/api/actual-system-dispatch"
        f"?dateAggregation=15min"
        f"&from={from_str}"
        f"&to={to_str}"
    )


def download_and_extract_data_for_request(start_date, end_date) -> pandas.Series:
    """
    Download and extract the electricity generation data from the PUCSL website.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Get the URL of the electricity demand data.
    url = get_url(start_date, end_date)

    # Fetch the data from the URL.
    response = requests.get(url)
    data = response.json()["data"]

    dataset = pandas.DataFrame(data)
    dataset["reportTimestamp"] = pandas.to_datetime(
        dataset["reportTimestamp"], utc=True
    )

    # Aggregate total generation (in MW) across all power plants for each timestamp
    dataset_grouped = dataset.groupby("reportTimestamp", as_index=False)[
        "dispatchValueInMW"
    ].sum()

    # Format as pandas.Series
    electricity_demand_time_series = pandas.Series(
        dataset_grouped["dispatchValueInMW"].values,
        index=dataset_grouped["reportTimestamp"],
    )

    # Add 15 minutes to the index because the electricity demand seems to be provided at the beginning of the hour.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index + pandas.Timedelta(minutes=15)
    )

    # Add the timezone information to the index.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_convert("Asia/Colombo")
    )

    return electricity_demand_time_series
