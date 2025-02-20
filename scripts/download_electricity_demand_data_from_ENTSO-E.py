# Source: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
# Source: https://github.com/EnergieID/entsoe-py

import logging
import os
import time
from pathlib import Path

import pandas as pd
import util.general as general_utilities
import util.time_series as time_series_utilities
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


def run_electricity_demand_data_retrieval() -> None:
    """
    Run the electricity demand data retrieval from the ENTSO-E API for the countries of interest and the years of interest.
    """

    # Set up the logging configuration.
    log_files_directory = general_utilities.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
    log_file_name = "electricity_demand_data_from_ENTSO-E.log"
    logging.basicConfig(
        filename=log_files_directory + "/" + log_file_name,
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Create a directory to store the electricity demand time series.
    result_directory = general_utilities.read_folders_structure()[
        "electricity_demand_folder"
    ]
    os.makedirs(result_directory, exist_ok=True)

    # Read the ISO Alpha-2 codes of the countries of interest.
    settings_directory = general_utilities.read_folders_structure()["settings_folder"]
    iso_alpha_2_codes = general_utilities.read_countries_from_file(
        settings_directory + "/gegis__countries_on_entsoe_platform.txt"
    )

    # Define the target file type.
    file_type = ".parquet"

    # Define the years of the data retrieval.
    start_year = 2015
    end_year = 2015
    years = range(start_year, end_year + 1)

    # Loop over the years.
    for year in years:
        logging.info(f"Year {year}.")

        # Loop over the countries.
        for iso_alpha_2_code in iso_alpha_2_codes:
            logging.info(f"Retrieving data for {iso_alpha_2_code}...")

            # Define the file path of the electricity demand time series.
            electricity_demand_file_path = (
                result_directory
                + f"/electricity_demand_{iso_alpha_2_code}_{year}"
                + file_type
            )

            # Check if the file does not exist.
            if not os.path.exists(electricity_demand_file_path):
                # Download the electricity demand time series from the ENTSO-E API.
                entsoe_electricity_demand_time_series = (
                    download_electricity_demand_from_entsoe(year, iso_alpha_2_code)
                )

                if entsoe_electricity_demand_time_series is not None:
                    # Get the local time zone of the country.
                    country_time_zone = general_utilities.get_time_zone(
                        iso_alpha_2_code
                    )

                    # Harmonize the electricity demand time series.
                    entsoe_electricity_demand_time_series = (
                        time_series_utilities.harmonize_time_series(
                            entsoe_electricity_demand_time_series,
                            country_time_zone,
                            interpolate_missing_values=False,
                        )
                    )

                    # Save the electricity demand time series to a parquet file.
                    time_series_utilities.save_time_series(
                        entsoe_electricity_demand_time_series,
                        electricity_demand_file_path,
                        "Electricity demand [MW]",
                    )


if __name__ == "__main__":
    run_electricity_demand_data_retrieval()
