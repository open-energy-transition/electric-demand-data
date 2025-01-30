import os

import pandas as pd
import utilities
import utilities_for_Cyprus_website as cyprus_utilities


def download_electricity_generation_from_tsoc(year):
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

    # From the date, hour and minute lists, create a datetime object and set the timezone to Asia/Nicosia.
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


def run_electricity_generation_data_retrieval():
    # Define the log file name.
    log_file_name = "electricity_generation_of_Cyprus.log"

    # Create a directory to store the electricity generation time series.
    result_directory = "Retrieved electricity generation data"
    if not os.path.exists(result_directory):
        os.makedirs(result_directory)

    # Define the start and end years of the data retrieval.
    start_year = 2015
    end_year = 2015

    for year in range(start_year, end_year + 1):
        utilities.write_to_log_file(
            log_file_name,
            ("" if year == start_year else "\n") + f"Year: {year}\n\n",
            new_file=(year == start_year),
        )

        # Define the file name of the electricity generation time series.
        file_name = f"/electricity_generation_CY_{year}.parquet"

        # Check if the file does not exist.
        if not os.path.exists(result_directory + "/" + file_name):
            electricity_generation_time_series = (
                download_electricity_generation_from_tsoc(year)
            )

            # Harmonize the electricity generation time series.
            electricity_generation_time_series = utilities.harmonize_time_series(
                log_file_name, "CY", electricity_generation_time_series
            )

            # Save the electricity generation time series to a parquet file.
            utilities.save_time_series(
                electricity_generation_time_series,
                result_directory + "/" + file_name,
                "Electricity generation [MW]",
            )


if __name__ == "__main__":
    run_electricity_generation_data_retrieval()
