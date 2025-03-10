#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves the electricity generation data from the website of the Transmission System Operator of Cyprus (TSOC).

    The data seems to represent the total electricity generation in MW, which can be considered a proxy for the electricity demand.

    The data is retrieved for the years from 2008 to the current year. The data is retrieved in 15-day intervals.

    The data is saved in Parquet and CSV formats.

    Source: https://tsoc.org.cy/electrical-system/archive-total-daily-system-generation-on-the-transmission-system/
"""

import logging
import re
import time
from urllib.error import URLError
from urllib.request import Request, urlopen

import pandas as pd
import util.time_series as time_series_utilities


def query_tsoc_website(
    starting_date: pd.Timestamp,
    days: int = 15,
    max_attempts: int = 3,
    retry_delay: int = 5,
) -> str:
    """
    Query the website of the Transmission System Operator of Cyprus for the electricity generation data.

    Parameters
    ----------
    starting_date : pandas.Timestamp
        The starting date for the data retrieval period
    days : int, optional
        The number of days of the retrieval period (it can be 1, 3, 7, or 15 days)
    max_attempts : int, optional
        The maximum number of retry attempts (default is 3)
    retry_delay : int, optional
        The delay between retry attempts in seconds (default is 5)

    Returns
    -------
    str
        The HTML content of the website
    """

    # Format date in "YYYY-MM-DD".
    date_for_query = starting_date.strftime("%d-%m-%Y")

    # Construct the request URL.
    url = f"https://tsoc.org.cy/electrical-system/archive-total-daily-system-generation-on-the-transmission-system/?startdt={date_for_query}&enddt=%2B{days}days"

    # Fetch HTML content.
    attempts = 0

    while attempts < max_attempts:
        try:
            with urlopen(
                Request(url=url, headers={"User-Agent": "Mozilla/5.0"})
            ) as response:
                return response.read().decode("utf-8")
        except URLError as e:
            attempts += 1
            logging.warning(
                f"Error fetching URL: {e}. Retrying ({attempts}/{max_attempts})..."
            )
            if attempts < max_attempts:
                time.sleep(retry_delay)

    raise URLError("Failed to retrieve data after multiple attempts.")


def read_generation(generation_step):
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


def read_timestamp_and_generation(
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
    total_generation = [read_generation(g) for g in generation_matches]

    return dates, hours, minutes, total_generation


def read_all_timestamps_and_generation(
    retrieval_start_dates: pd.DatetimeIndex | pd.Timestamp, interval: int
) -> tuple[list[str], list[str], list[str], list[float | None]]:
    """
    Extract dates, hours, minutes, and generation data from an HTML file.

    Parameters
    ----------
    retrieval_start_dates : pandas.DatetimeIndex | pandas.Timestamp
        The starting dates for the data retrieval

    Returns
    -------
    all_dates : list of str
        The dates in the format "YYYY-MM-DD"
    all_hours : list of str
        The hours as strings
    all_minutes : list of str
        The minutes as strings
    all_generation : list of float | None
        The total power generation in MW. If data is unavailable, None is returned
    """

    # Initialize lists to store results.
    all_dates, all_hours, all_minutes, all_generation = [], [], [], []

    if isinstance(retrieval_start_dates, pd.Timestamp):
        retrieval_start_dates = pd.DatetimeIndex([retrieval_start_dates])

    # Loop through date ranges and fetch data.
    for current_date in retrieval_start_dates:
        logging.info(
            f"Retrieving data for the time period starting from {current_date}."
        )

        # Fetch the page content.
        page = query_tsoc_website(current_date, days=interval)

        # Extract time and generation data.
        dates, hours, minutes, generation = read_timestamp_and_generation(page)

        # Store results.
        all_dates.extend(dates)
        all_hours.extend(hours)
        all_minutes.extend(minutes)
        all_generation.extend(generation)

    return all_dates, all_hours, all_minutes, all_generation


def download_and_extract_data() -> pd.Series:
    """
    Download and extract the electricity generation data from the website of the Transmission System Operator of Cyprus.

    Returns
    -------
    electricity_generation_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Define the start and end date according to the data availability.
    start_date_and_time = pd.Timestamp("2008-01-01 00:00:00")
    end_date_and_time = pd.Timestamp.today()

    # Define the interval for the data retrievals. It can be 1, 3, 7 or 15 days.
    interval = 15

    # Define the starting dates for the data retrievals.
    retrieval_start_dates = pd.date_range(
        start_date_and_time, end_date_and_time, freq=str(interval) + "D"
    )

    # Retrieve all timestamps and generation data.
    all_dates, all_hours, all_minutes, all_generation = (
        read_all_timestamps_and_generation(retrieval_start_dates, interval)
    )

    # Construct datetime index with time zone.
    date_time = pd.to_datetime(
        [
            f"{date} {hour}:{minute}"
            for date, hour, minute in zip(all_dates, all_hours, all_minutes)
        ]
    ).tz_localize("Asia/Nicosia", nonexistent="NaT", ambiguous="NaT")

    # Create a Pandas Series for the electricity generation data.
    electricity_generation_time_series = pd.Series(data=all_generation, index=date_time)

    # Clean the data.
    electricity_generation_time_series = time_series_utilities.clean_data(
        electricity_generation_time_series
    )

    return electricity_generation_time_series
