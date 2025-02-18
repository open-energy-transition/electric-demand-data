# Source: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
# Source: https://github.com/EnergieID/entsoe-py

import logging
import os
import time
from pathlib import Path

import pandas as pd
import util.general_utilities as general_utilities
from dotenv import load_dotenv
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from requests.exceptions import ConnectionError


def fetch_entsoe_demand(
    client: EntsoePandasClient,
    iso_alpha_2_code: str,
    start_date_and_time: pd.Timestamp,
    end_date_and_time: pd.Timestamp,
    max_attempts: int = 3,
    retry_delay: int = 5,
) -> pd.Series | None:
    """
    Fetches the hourly electricity demand time series from ENTSO-E with retry logic.

    Parameters
    ----------
    client : entsoe.EntsoePandasClient
        The ENTSO-E API client
    iso_alpha_2_code : str
        The ISO Alpha-2 code of the country
    start_date_and_time : pd.Timestamp
        The start date and time of the data retrieval
    end_date_and_time : pd.Timestamp
        The end date and time of the data retrieval
    max_attempts : int, optional
        The maximum number of retry attempts (default is 3)
    retry_delay : int, optional
        The delay between retry attempts in seconds (default is 5)

    Returns
    -------
    pandas.Series
        The electricity demand time series in MW
    """

    attempts = 0

    while attempts < max_attempts:
        try:
            return client.query_load(
                iso_alpha_2_code, start=start_date_and_time, end=end_date_and_time
            )["Actual Load"]
        except ConnectionError:
            attempts += 1
            logging.warning(
                f"Connection error. Retrying ({attempts}/{max_attempts})..."
            )
            if attempts < max_attempts:
                time.sleep(retry_delay)

    logging.error("Failed to retrieve data after multiple attempts.")

    return None


def download_electricity_demand_from_entsoe(
    year: int, iso_alpha_2_code: str
) -> pd.Series | None:
    """
    Download the electricity demand time series from the ENTSO-E API for a specific country and year.

    Parameters
    ----------
    year : int
        The year of the data retrieval
    iso_alpha_2_code : str
        The ISO Alpha-2 code of the country

    Returns
    -------
    pandas.Series
        The electricity demand time series in MW
    """

    # Load the environment variables.
    load_dotenv(dotenv_path=Path(".") / ".env")

    # Define the ENTSO-E API client.
    client = EntsoePandasClient(api_key=os.getenv("ENTSOE_API_KEY"))

    # Get the time zone of the country.
    country_time_zone = general_utilities.get_time_zone(iso_alpha_2_code)

    # Define the start and end dates for the data retrieval.
    start_date_and_time = pd.Timestamp(str(year), tz=country_time_zone)
    end_date_and_time = pd.Timestamp(str(year + 1), tz=country_time_zone)

    # Try to download the electricity demand time series and handle connection and data availability errors.
    try:
        return fetch_entsoe_demand(
            client, iso_alpha_2_code, start_date_and_time, end_date_and_time
        )
    except NoMatchingDataError:
        # If the data is not available, skip to the next country.
        logging.error(f"No data available for {iso_alpha_2_code} in {year}.")
        return None
