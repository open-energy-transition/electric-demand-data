#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:
    This script retrieves the electricity load data for the Wholesale Electricity Market (WEM)
    from the Australian Energy Market Operator (AEMO) website.

    The data is retrieved for the dates from September 26, 2023, to the current date.
    The data is fetched from the available JSON files on the AEMO website.

    Source: https://data.wa.aemo.com.au/public/market-data/wemde/operationalDemandWithdrawal/dailyFiles/
"""

import logging
import os
from datetime import date, timedelta

import pandas as pd
import util.fetcher

logging.basicConfig(level=logging.INFO)


def get_available_requests() -> list[tuple[int, int, int]]:
    """
    Get the list of available requests to retrieve the electricity demand data from the AEMO website.

    Returns
    -------
    available_requests : list[tuple[int, int, int]]
        List of tuples in the format (year, month, day)
    """
    start_date = pd.Timestamp("2023-09-26")
    end_date = pd.Timestamp.today().normalize()

    available_requests = []
    current_date = start_date

    while current_date <= end_date:
        available_requests.append(
            (current_date.year, current_date.month, current_date.day)
        )
        current_date += timedelta(days=1)

    return available_requests


def get_url(year: int, month: int, day: int) -> str:
    """
    Get the URL of the electricity demand data on the AEMO website.

    Parameters
    ----------
    year : int
        The year of the data to retrieve
    month : int
        The month of the data to retrieve
    day : int
        The day of the data to retrieve

    Returns
    -------
    url : str
        The URL of the electricity demand data
    """

    assert (year, month, day) in get_available_requests(), (
        f"Date {year}-{month:02d}-{day:02d} is not available."
    )

    url = (
        f"https://data.wa.aemo.com.au/public/market-data/wemde/operationalDemandWithdrawal/dailyFiles/"
        f"OperationalDemandAndWithdrawal_{year}-{month:02d}-{day:02d}.json"
    )

    return url


def download_and_save_data(year: int, month: int, day: int) -> None:
    """
    Download and save the electricity demand data from the AEMO website.

    Parameters
    ----------
    year : int
        The year of the electricity demand data
    month : int
        The month of the electricity demand data
    day : int
        The day of the electricity demand data
    """
    url = get_url(year, month, day)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/58.0.3029.110 Safari/537.36"
    }

    try:
        dataset = util.fetcher.fetch_data(
            url, "json", output="tabular", header_params=headers
        )
        # The JSON has a nested format: extract the actual table from ["data"]["data"]
        if (
            isinstance(dataset, dict)
            and "data" in dataset
            and "data" in dataset["data"]
        ):
            dataset = pd.DataFrame(dataset["data"]["data"])
        else:
            raise ValueError("Unexpected data format from AEMO server.")

        os.makedirs("aemo_csv", exist_ok=True)
        filename = f"aemo_csv/OperationalDemand_WEM_{year}-{month:02d}-{day:02d}.csv"
        dataset.to_csv(filename, index=False)
        logging.info(f"Saved: {filename}")

    except Exception as e:
        logging.error(f"Error downloading/saving {year}-{month:02d}-{day:02d}: {e}")


def get_available_requests_for_range(start_date: date, end_date: date):
    current_date = start_date
    while current_date <= end_date:
        download_and_save_data(current_date.year, current_date.month, current_date.day)
        current_date += timedelta(days=1)


# --- Main execution: Download data for all dates from 2023-09-26 to today ---
if __name__ == "__main__":
    from datetime import datetime

    start_date = datetime(2023, 9, 26).date()
    end_date = datetime.today().date()

    get_available_requests_for_range(start_date, end_date)
