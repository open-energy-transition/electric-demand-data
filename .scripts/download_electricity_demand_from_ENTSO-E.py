# Source: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
# Source: https://github.com/EnergieID/entsoe-py

import os
from pathlib import Path

import utilities
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from requests.exceptions import ConnectionError


def download_electricity_demand_from_entsoe(log_file_name, year, country_code):
    """
    Download the electricity demand time series from the ENTSO-E API for a specific country and year.

    Parameters
    ----------
    log_file_name : str
        The name of the log file.
    year : int
        The year of the data retrieval.
    country_code : str
        The ISO Alpha-2 code of the country.

    Returns
    -------
    entsoe_electricity_demand_time_series : pandas.Series
        The electricity demand time series in MW.
    """

    # Read the ENTSO-E API key
    with open(Path.home() / ".entsoerc", "r") as file:
        entsoe_api_key = file.read().replace("\n", "")

    # Define the ENTSO-E API client.
    client = EntsoePandasClient(api_key=entsoe_api_key)

    # Get the timezone, start date, and end date of the data retrieval for the country and year.
    __, start_date_and_time, end_date_and_time = utilities.get_time_information(
        year, country_code
    )

    # Try to download the electricity demand time series and handle connection and data availability errors.
    try:
        entsoe_electricity_demand_time_series = None

        while entsoe_electricity_demand_time_series is None:
            try:
                # Retrieve the hourly electricity demand time series in MW.
                entsoe_electricity_demand_time_series = client.query_load(
                    country_code, start=start_date_and_time, end=end_date_and_time
                )["Actual Load"]

                return entsoe_electricity_demand_time_series

            except ConnectionError:
                # If there is a connection error, try again.
                utilities.write_to_log_file(
                    log_file_name, "Connection error. Retrying...\n"
                )

                pass

    except NoMatchingDataError:
        # If the data is not available, skip to the next country.
        utilities.write_to_log_file(
            log_file_name, f"No data available for {country_code} in {year}.\n"
        )

        return None


def run_electricity_demand_data_retrieval():
    """
    Run the electricity demand data retrieval from the ENTSO-E API for the countries of interest and the years of interest.
    """

    # Define the log file name.
    log_file_name = "electricity_demand_from_ENTSO-E.log"

    # Create a directory to store the electricity demand time series.
    result_directory = "Retrieved electricity demand data"
    if not os.path.exists(result_directory):
        os.makedirs(result_directory)

    # Define the ISO Alpha-2 codes of the countries of interest.
    country_codes = [
        "AT",  # Austria
        "BE",  # Belgium
        "BA",  # Bosnia and Herzegovina
        "BG",  # Bulgaria
        "HR",  # Croatia
        "CY",  # Cyprus
        "CZ",  # Czech Republic
        "DK",  # Denmark
        "EE",  # Estonia
        "FI",  # Finland
        "FR",  # France
        "DE",  # Germany
        "GR",  # Greece
        "HU",  # Hungary
        # 'IS', # Iceland
        "IE",  # Ireland
        "IT",  # Italy
        "LV",  # Latvia
        "LT",  # Lithuania
        "MK",  # Macedonia
        "NL",  # Netherlands
        "NO",  # Norway
        "PL",  # Poland
        "PT",  # Portugal
        "RO",  # Romania
        "RS",  # Serbia
        "SK",  # Slovakia
        "SI",  # Slovenia
        "ES",  # Spain
        "SE",  # Sweden
        "CH",  # Switzerland
        "GB",  # United Kingdom
    ]

    # Define the start and end years of the data retrieval.
    start_year = 2015
    end_year = 2015

    # Loop over the years.
    for year in range(start_year, end_year + 1):
        utilities.write_to_log_file(
            log_file_name,
            ("" if year == start_year else "\n") + f"Year: {year}\n",
            new_file=(year == start_year),
        )

        # Loop over the countries.
        for country_code in country_codes:
            utilities.write_to_log_file(log_file_name, "\n")
            utilities.write_to_log_file(
                log_file_name,
                f"Retrieving data for {country_code}...\n",
                write_time=True,
            )

            # Define the file name of the electricity demand time series.
            file_name = f"/electricity_demand_{country_code}_{year}.parquet"

            # Check if the file does not exist.
            if not os.path.exists(result_directory + "/" + file_name):
                # Download the electricity demand time series from the ENTSO-E API.
                entsoe_electricity_demand_time_series = (
                    download_electricity_demand_from_entsoe(
                        log_file_name, year, country_code
                    )
                )

                if entsoe_electricity_demand_time_series is not None:
                    # Harmonize the electricity demand time series.
                    entsoe_electricity_demand_time_series = (
                        utilities.harmonize_time_series(
                            log_file_name,
                            country_code,
                            entsoe_electricity_demand_time_series,
                        )
                    )

                    # Save the electricity demand time series to a parquet file.
                    utilities.save_time_series(
                        entsoe_electricity_demand_time_series,
                        result_directory + "/" + file_name,
                        "Electricity demand [MW]",
                    )


if __name__ == "__main__":
    run_electricity_demand_data_retrieval()
