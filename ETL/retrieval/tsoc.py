# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity demand data from the website of the Transmission System Operator of Cyprus (TSOC).

    The data seems to represent the total electricity generation in MW, which can be considered a proxy for the electricity demand.

    The data is retrieved for the years from 2008 to the current year. The data is retrieved in 15-day intervals.

    Source: https://tsoc.org.cy/electrical-system/archive-total-daily-system-generation-on-the-transmission-system/
"""

import logging
import re

import pandas
import util.fetcher


def get_available_requests() -> list[pandas.Timestamp]:
    """
    Get the list of available requests to retrieve the electricity generation data from the TSOC website.

    Returns
    -------
    list[pandas.Timestamp]
        The list of available requests
    """

    # Define the start and end date according to the data availability.
    start_date_and_time = pandas.Timestamp(
        "2008-01-01 00:00:00"
    )  # This is in local time (Asia/Nicosia).
    end_date_and_time = pandas.Timestamp.today()

    # Return the available requests, which are the start dates of the retrieval periods. We use 15-day intervals (the maximum available on the website) to minimize the number of requests.
    return list(pandas.date_range(start_date_and_time, end_date_and_time, freq="15D"))


def get_url(start_date: pandas.Timestamp) -> str:
    """
    Get the URL of the electricity generation data on the TSOC website.

    Parameters
    ----------
    starting_date : pandas.Timestamp
        The starting date for the data retrieval period

    Returns
    -------
    str
        The URL of the electricity generation data
    """

    # Check that the beginning of the period is on or after 2008-01-01.
    assert start_date >= pandas.Timestamp("2008-01-01 00:00:00"), (
        "The beginning of the data availability is 2008-01-01."
    )

    # Convert the start and end dates and times to the required format.
    start_date = start_date.strftime("%d-%m-%Y")

    # Return the URL of the electricity generation data.
    return f"https://tsoc.org.cy/electrical-system/archive-total-daily-system-generation-on-the-transmission-system/?startdt={start_date}&enddt=%2B15days"


def _read_generation(generation_step):
    """
    Process a single generation data entry and determine the total generation.

    Parameters
    ----------
    generation_step : tuple of str
        A tuple containing the wind, solar, total, and conventional generation values

    Returns
    -------
    float | None
        The total generation in MW, or None if data is unavailable
    """

    # Extract the wind, solar, total, and conventional generation values from the tuple.
    wind, solar, total, conventional = generation_step

    # If total generation is null, it usually means no data is available.
    if total == "null":
        return None

    # If total generation is 0, attempt to compute it from wind, solar, and conventional values.
    if total == "0":
        wind = float(wind) if wind != "null" else 0
        solar = float(solar) if solar != "null" else 0
        conventional = float(conventional) if conventional != "null" else 0
        total_estimated = wind + solar + conventional

        # If the sum is still 0, return None.
        return total_estimated if total_estimated > 0 else None

    # Otherwise, return the total generation as a float.
    return float(total)


def _read_timestamp_and_generation(
    page: str,
) -> tuple[list[str], list[str], list[str], list[float | None]]:
    """
    Extract dates, hours, minutes, and generation data from an HTML file.

    Parameters
    ----------
    page : str
        The HTML file content

    Returns
    -------
    dates : list of str
        The dates in the format "YYYY-MM-DD"
    hours : list of str
        The hours as strings
    minutes : list of str
        The minutes as strings
    total_generation : list of float | None
        The total power generation in MW. If data is unavailable, None is returned
    """

    # Extract the dates, hours, minutes, and generation data from the HTML content using regex patterns.
    dates = re.findall(r'var dateStr = "(\d{4}-\d{2}-\d{2})";', page)
    hours = re.findall(r'var hourStr = "(\d{2})";', page)
    minutes = re.findall(r'var minutesStr = "(\d{2})";', page)
    generation_matches = re.findall(
        r"\[dateStrFormat, (\d+|null), (\d+|null), (\d+|null), (\d+|null)\]", page
    )  # The values represent wind, solar, total, and conventional generation, respectively.

    # Process the generation data to determine the total generation.
    total_generation = [_read_generation(g) for g in generation_matches]

    return dates, hours, minutes, total_generation


def download_and_extract_data_for_request(
    start_date: pandas.Timestamp,
) -> tuple[list[str], list[str], list[str], list[float | None]]:
    """
    Download and extract the electricity generation data from the TSOC website.

    Parameters
    ----------
    start_date : pandas.Timestamp
        The starting date for the data retrieval

    Returns
    -------
    electricity_generation_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Check that the beginning of the period is on or after 2008-01-01.
    assert start_date >= pandas.Timestamp("2008-01-01 00:00:00"), (
        "The beginning of the data availability is 2008-01-01."
    )

    logging.info(f"Retrieving data for the 15-day period starting from {start_date}.")

    # Get the URL of the electricity generation data.
    url = get_url(start_date)

    # Fetch HTML content from the URL.
    page = util.fetcher.fetch_data(url, "text", output_content_type="text")

    # Extract time and generation data.
    dates, hours, minutes, generation = _read_timestamp_and_generation(page)

    # Construct datetime index with time zone.
    date_time = pandas.to_datetime(
        [f"{date} {hour}:{minute}" for date, hour, minute in zip(dates, hours, minutes)]
    ).tz_localize("Asia/Nicosia", nonexistent="NaT", ambiguous="NaT")

    # Create a Pandas Series for the electricity generation data.
    electricity_generation_time_series = pandas.Series(data=generation, index=date_time)

    return electricity_generation_time_series
