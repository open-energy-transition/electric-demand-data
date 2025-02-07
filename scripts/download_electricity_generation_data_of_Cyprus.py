import logging
import os

import pandas as pd
import util.cyprus_utilities as cyprus_utilities
import util.general_utilities as general_utilities
import util.time_series_utilities as time_series_utilities


def download_electricity_generation_from_tsoc(year: int) -> pd.Series:
    """
    Retrieve the electricity generation data for Cyprus from the website of the Transmission System Operator of Cyprus.

    Parameters
    ----------
    year : int
        The year of interest.

    Returns
    -------
    electricity_generation_time_series : pandas.Series
        The electricity generation time series in MW.
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
        # Define the dates of the current request.
        dates_in_interval = [
            (current_date + pd.Timedelta(days=dd)).strftime("%Y-%m-%d")
            for dd in range(0, 15, 1)
        ]

        # Query the website for the current request.
        lines = cyprus_utilities.query_website(dates_in_interval[0])

        # Set the line count to 0.
        line_count = 0

        # Loop through the lines.
        while line_count < len(lines) - 1:
            # Check if the current line contains the date.
            if any(
                ('var dateStr = "' + date) in lines[line_count]
                for date in dates_in_interval
            ):
                dates.append(cyprus_utilities.read_date(lines, line_count))
                hours.append(cyprus_utilities.read_hour(lines, line_count))
                minutes.append(cyprus_utilities.read_minute(lines, line_count))
                total_generation.append(
                    cyprus_utilities.read_total_generation(lines, line_count)
                )

                line_count += 22

            else:
                line_count += 1

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
            electricity_generation_time_series = (
                download_electricity_generation_from_tsoc(year)
            )

            # Harmonize the electricity generation time series.
            electricity_generation_time_series = (
                time_series_utilities.harmonize_time_series(
                    "CY",
                    electricity_generation_time_series,
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
