# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data for Nigeria from a publicly available repository developed for research purposes only.

    The data represents the electricity demand that would have occurred if there were no outage events.

    Source: https://data.mendeley.com/datasets/pxvdm26rn7/2
    Source: https://doi.org/10.1016/j.esd.2020.08.009 (not the model used to generate the data but a related one)
"""

import logging

import pandas
import util.fetcher


def get_available_requests() -> None:
    """
    Get the list of available requests to retrieve the electricity demand data for Nigeria.
    """

    logging.debug("The data is retrieved all at once.")


def get_url() -> str:
    """
    Get the URL of the electricity demand data for Nigeria.

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    # Return the URL of the electricity demand data.
    return "https://prod-dcd-datasets-public-files-eu-west-1.s3.eu-west-1.amazonaws.com/0766cdca-14a5-4522-9380-a15094e0c4c6"


def download_and_extract_data() -> pandas.Series:
    """
    Download and extract the electricity demand data for Nigeria.

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Get the URL of the electricity demand data.
    url = get_url()

    # Fetch the data from the URL.
    dataset = util.fetcher.fetch_data(
        url, "excel", excel_kwargs={"sheet_name": "Demand Timeseries", "skiprows": 3}
    )

    # Extract the electricity demand time series.
    electricity_demand_time_series = pandas.Series(
        dataset["National Unsuppressed Demand"].values,
        index=pandas.to_datetime(dataset["date time"]),
    )

    # Round the index to the nearest second.
    electricity_demand_time_series.index = electricity_demand_time_series.index.round(
        "s"
    )

    # Add one hour to the index because the electricity demand seems to be provided at the beginning of the hour.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index + pandas.Timedelta(hours=1)
    )

    # Add the timezone information to the index.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize("Africa/Lagos")
    )

    return electricity_demand_time_series
