import logging
import os
import re
from urllib.request import Request, urlopen

import pandas as pd
import util.general as general_utilities
import util.time_series as time_series_utilities


def parse_generation(generation_step):
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


def read_time_and_generation(
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

    # Precompile regex patterns for improved performance when processing large files.
    date_pattern = re.compile(r'var dateStr = "(\d{4}-\d{2}-\d{2})";')
    hour_pattern = re.compile(r'var hourStr = "(\d{2})";')
    minute_pattern = re.compile(r'var minutesStr = "(\d{2})";')
    generation_pattern = re.compile(
        r"\[dateStrFormat, (\d+|null), (\d+|null), (\d+|null), (\d+|null)\]"
    )  # The values represent wind, solar, total, and conventional generation, respectively.

    # Extract the dates, hours, minutes, and generation data from the HTML content using regex patterns.
    dates = date_pattern.findall(page)
    hours = hour_pattern.findall(page)
    minutes = minute_pattern.findall(page)
    generation_matches = generation_pattern.findall(page)

    # Process the generation data to determine the total generation.
    total_generation = [parse_generation(g) for g in generation_matches]

    return dates, hours, minutes, total_generation


def download_electricity_generation_from_tsoc(year: int) -> pd.Series:
    """
    Retrieve the electricity generation data of Cyprus from the Transmission System Operator of Cyprus.

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

    # Define the increment for data requests (15 days).
    date_ranges = pd.date_range(start_date, end_date, freq="15D")

    # Initialize lists to store results.
    all_dates, all_hours, all_minutes, all_generation = [], [], [], []

    # Loop through date ranges and fetch data.
    for current_date in date_ranges:
        # Format date in "DD-MM-YYYY".
        date_for_query = current_date.strftime("%d-%m-%Y")

        # Construct the request URL.
        url = f"https://tsoc.org.cy/electrical-system/archive-total-daily-system-generation-on-the-transmission-system/?startdt={date_for_query}&enddt=%2B15days"

        # Fetch HTML content.
        page = (
            urlopen(Request(url=url, headers={"User-Agent": "Mozilla/5.0"}))
            .read()
            .decode("utf-8")
        )

        # Extract time and generation data.
        dates, hours, minutes, generation = read_time_and_generation(page)

        # Store results.
        all_dates.extend(dates)
        all_hours.extend(hours)
        all_minutes.extend(minutes)
        all_generation.extend(generation)

    # Construct datetime index with time zone.
    date_time = pd.to_datetime(
        [
            f"{date} {hour}:{minute}"
            for date, hour, minute in zip(all_dates, all_hours, all_minutes)
        ]
    ).tz_localize("Asia/Nicosia", nonexistent="NaT", ambiguous="NaT")

    # Create a Pandas Series for the electricity generation data.
    electricity_generation_time_series = pd.Series(data=all_generation, index=date_time)

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
