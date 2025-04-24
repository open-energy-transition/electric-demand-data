# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of the Coordinador Eléctrico Nacional (CEN) in Chile.

    The data is retrieved for the years from 1999 to the current year. The data is retrieved in one-year intervals.

    Source: https://www.coordinador.cl/operacion/graficos/operacion-real/demanda-real/
"""

import logging

import pandas
import util.fetcher


def get_available_requests(
    code: str | None = None,
) -> list[tuple[pandas.Timestamp, pandas.Timestamp]]:
    """
    Get the list of available requests to retrieve the electricity demand data from the CEN website.

    Parameters
    ----------
    code : str, optional
        The code of the country or region (not used in this function)

    Returns
    -------
    list[tuple[pandas.Timestamp, pandas.Timestamp]]
        The list of available requests
    """

    # Define the start and end date according to the data availability.
    start_date_and_time = pandas.Timestamp("1999-01-01 00:00:00")
    end_date_and_time = pandas.Timestamp.today()

    # Define start and end dates and times for one-year retrieval periods. A one-year period is the maximum available on the platform.
    start_date_and_time_of_period = pandas.date_range(
        start_date_and_time, end_date_and_time, freq="YS"
    )
    end_date_and_time_of_period = (
        start_date_and_time_of_period[1:] - pandas.Timedelta("24h")
    ).union(pandas.to_datetime([end_date_and_time]))

    # Return the available requests, which are the beginning and end of each one-year period.
    return list(zip(start_date_and_time_of_period, end_date_and_time_of_period))


def get_url(start_date: pandas.Timestamp, end_date: pandas.Timestamp) -> str:
    """
    Get the URL of the electricity demand data on the CEN website.

    Parameters
    ----------
    start_date : pandas.Timestamp
        The start date and time of the data retrieval
    end_date : pandas.Timestamp
        The end date and time of the data retrieval

    Returns
    -------
    str
        The URL of the electricity demand data
    """

    # Check that the retrieval period is less than one year.
    assert (end_date - start_date).days < 366, (
        "The retrieval period is greater than 1 year. Please reduce the period to 1 year."
    )

    # Check that the start date and time is greater than or equal to the beginning of the data availability.
    assert start_date >= pandas.Timestamp("1999-01-01 00:00:00"), (
        "The beginning of the data availability is 1999-01-01 00:00:00."
    )

    # Convert the start and end date and time to string format.
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    # Return the URL of the electricity demand data.
    return f"https://sipub.coordinador.cl/api/v1/recursos/demandasistemareal?fecha__gte={start_date}&fecha__lte={end_date}"


def download_and_extract_data_for_request(
    start_date_and_time: pandas.Timestamp, end_date_and_time: pandas.Timestamp
) -> pandas.Series:
    """
    Download and extract the electricity demand data from the CEN website.

    Parameters
    ----------
    start_date_and_time : pandas.Timestamp
        The start date and time of the data retrieval
    end_date_and_time : pandas.Timestamp
        The end date and time of the data retrieval

    Returns
    -------
    electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW
    """

    # Check that the retrieval period is less than one year.
    assert (end_date_and_time - start_date_and_time).days < 366, (
        "The retrieval period is greater than 1 year. Please reduce the period to 1 year."
    )

    # Check that the start date and time is greater than or equal to the beginning of the data availability.
    assert start_date_and_time >= pandas.Timestamp("1999-01-01 00:00:00"), (
        "The beginning of the data availability is 1999-01-01 00:00:00."
    )

    logging.info(f"Retrieving data from {start_date_and_time} to {end_date_and_time}.")

    # Get the URL of the electricity demand data.
    url = get_url(start_date_and_time, end_date_and_time)

    # Define the headers for the request.
    header_params = {
        "Referer": "https://www.coordinador.cl/operacion/graficos/operacion-real/demanda-real/",
        "Origin": "https://www.coordinador.cl",
    }

    # Fetch the data from the URL.
    dataset = util.fetcher.fetch_data(
        url, "json", header_params=header_params, json_keys=["data"]
    )

    # Merge the date and time columns into a single column. Consider that the time is given at the end of the hour. In some years where there is the switch to or from daylight saving time, there is a 25th hour.
    dataset["date and time"] = [
        date + f" {(min(time, 24) - 1):02d}:00"
        for date, time in zip(dataset["fecha"], dataset["hora"])
    ]

    # Sort the dataset by date and time.
    dataset = dataset.sort_values(by="date and time", ascending=True)

    # Extract the electricity demand time series.
    electricity_demand_time_series = pandas.Series(
        dataset["demanda"].values, index=pandas.to_datetime(dataset["date and time"])
    )

    # Add the timezone to the index.
    electricity_demand_time_series.index = (
        electricity_demand_time_series.index.tz_localize(
            "America/Santiago", ambiguous="NaT", nonexistent="NaT"
        )
    )

    # Add 1 hour to the index to match the original dataset.
    electricity_demand_time_series.index += pandas.Timedelta(hours=1)

    return electricity_demand_time_series
