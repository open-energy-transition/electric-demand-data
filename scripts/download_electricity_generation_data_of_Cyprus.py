import logging
import os
import re
from urllib.request import Request, urlopen

import pandas as pd
import util.general as general_utilities
import util.time_series as time_series_utilities


def read_time_and_generation(
    page: str,
) -> tuple[list[str], list[str], list[str], list[str]]:
    """
    Read the dates, hours, minutes, and generation data from the html file.

    Parameters
    ----------
    page : str
        The html file

    Returns
    -------
    dates : list of str
        The dates in the format "YYYY-MM-DD"
    hours : list of str
        The hours
    minutes : list of str
        The minutes
    generation : list of str
        The generation data
    """

    # Extract the dates, hours, minutes, and generation data from the html file according to the regex pattern.
    dates = re.findall(r"var dateStr = \"(\d{4}-\d{2}-\d{2})\";", page)
    hours = re.findall(r"var hourStr = \"(\d{2})\";", page)
    minutes = re.findall(r"var minutesStr = \"(\d{2})\";", page)
    generation = re.findall(
        r"\[dateStrFormat, (\d+|null), (\d+|null), (\d+|null), (\d+|null)\]", page
    )

    # Compute the total generation based on the available data.
    total_generation = []
    for generation_step in generation:
        if generation_step[2] == "null":
            # When the value for total generation is null, typically the values for wind, solar, and conventional generation are also null.
            total_generation.append(None)
        elif generation_step[2] == "0":
            # When the value for total generation is 0, typically the values for wind, solar, and/or conventional generation are not null, and these can be used to estimate the total demand.
            wind_generation = (
                float(generation_step[0]) if generation_step[0] != "null" else 0
            )
            solar_generation = (
                float(generation_step[1]) if generation_step[1] != "null" else 0
            )
            conventional_generation = (
                float(generation_step[3]) if generation_step[3] != "null" else 0
            )
            total_generation.append(
                wind_generation + solar_generation + conventional_generation
            )
            # If the total demand is still 0, set it to None.
            if total_generation[-1] == 0:
                total_generation[-1] = None
        else:
            total_generation.append(float(generation_step[2]))

    return dates, hours, minutes, total_generation


def download_electricity_generation_from_tsoc(year: int) -> pd.Series:
    """
    Retrieve the electricity generation data for Cyprus from the website of the Transmission System Operator of Cyprus.

    Parameters
    ----------
    year : int
        The year of interest

    Returns
    -------
    electricity_generation_time_series : pandas.Series
        The electricity generation time series in MW
    """

    # Define the start and end date.
    start_date = pd.Timestamp(year=year, month=1, day=1)
    end_date = pd.Timestamp(year=year, month=12, day=31)

    # Define the lists that will store the data.
    dates = []
    hours = []
    minutes = []
    total_generation = []

    # Define the increment that corresponds to the maximum number of days that can be requested.
    increment = pd.Timedelta(days=15)

    # Define the current date.
    current_date = start_date

    # Loop through the dates.
    while current_date <= end_date:
        # Define the date in the format "DD-MM-YYYY".
        date_for_query = current_date.strftime("%d-%m-%Y")

        # Define the url of the current request.
        url = f"https://tsoc.org.cy/electrical-system/archive-total-daily-system-generation-on-the-transmission-system/?startdt={date_for_query}&enddt=%2B15days"

        # Read the html file.
        page = (
            urlopen(Request(url=url, headers={"User-Agent": "Mozilla/5.0"}))
            .read()
            .decode("utf-8")
        )

        # Extract the dates and generation data from the html file.
        queried_dates, queried_hours, queried_minutes, queried_total_generation = (
            read_time_and_generation(page)
        )

        dates.extend(queried_dates)
        hours.extend(queried_hours)
        minutes.extend(queried_minutes)
        total_generation.extend(queried_total_generation)

        current_date += increment

    # From the date, hour and minute lists, create a datetime object and set the time zone to Asia/Nicosia.
    date_time = pd.to_datetime(
        [f"{date} {hour}:{minute}" for date, hour, minute in zip(dates, hours, minutes)]
    ).tz_localize("Asia/Nicosia", nonexistent="NaT", ambiguous="NaT")

    # Create a series with the time and generation data.
    electricity_generation_time_series = pd.Series(
        data=total_generation, index=date_time
    )

    # Remove timesteps with NaT values.
    electricity_generation_time_series = electricity_generation_time_series[
        electricity_generation_time_series.index.notnull()
    ]

    # Remove timesteps outside the year of interest.
    electricity_generation_time_series = electricity_generation_time_series[
        electricity_generation_time_series.index.year == year
    ]

    return electricity_generation_time_series


def run_electricity_generation_data_retrieval() -> None:
    """
    Run the electricity generation data retrieval from the website of the Transmission System Operator of Cyprus.
    """

    # Set up the logging configuration.
    log_files_directory = general_utilities.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
    log_file_name = "electricity_generation_data_of_Cyprus.log"
    logging.basicConfig(
        filename=log_files_directory + "/" + log_file_name,
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Create a directory to store the electricity generation time series.
    result_directory = general_utilities.read_folders_structure()[
        "electricity_generation_folder"
    ]
    os.makedirs(result_directory, exist_ok=True)

    # Define the target file type.
    file_type = ".parquet"

    # Define the years of the data retrieval.
    start_year = 2015
    end_year = 2015
    years = range(start_year, end_year + 1)

    # Loop over the years.
    for year in years:
        logging.info(f"Year {year}.")

        # Define the file path of the electricity generation time series.
        electricity_generation_file_path = (
            result_directory + f"/electricity_generation_CY_{year}" + file_type
        )

        # Check if the file does not exist.
        if not os.path.exists(electricity_generation_file_path):
            # Retrieve the electricity generation time series.
            electricity_generation_time_series = (
                download_electricity_generation_from_tsoc(year)
            )

            # Get the time zone of the country.
            country_time_zone = general_utilities.get_time_zone("CY")

            # Harmonize the electricity generation time series.
            electricity_generation_time_series = (
                time_series_utilities.harmonize_time_series(
                    electricity_generation_time_series,
                    country_time_zone,
                    interpolate_missing_values=False,
                )
            )

            # Save the electricity generation time series to a parquet file.
            time_series_utilities.save_time_series(
                electricity_generation_time_series,
                electricity_generation_file_path,
                "Electricity generation [MW]",
            )


if __name__ == "__main__":
    run_electricity_generation_data_retrieval()
