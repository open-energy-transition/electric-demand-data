import os
from datetime import datetime

import pandas as pd
import pytz


def write_to_log_file(filename, message, new_file=False, write_time=False):
    """
    Write a message to a log file.

    Parameters
    ----------
    filename : str
        Name of the log file without the extension
    message : str
        Message to write to the log file
    new_file : bool, optional
        If True, the log file is created
    write_time : bool, optional
        If True, the current time is written before the message
    """

    # Get the working directory.
    working_directory = os.getcwd()

    # Create the log file if it does not exist.
    if not os.path.exists(working_directory + "/log_files"):
        os.makedirs(working_directory + "/log_files")

    # Determine whether to append or overwrite the log file.
    mode = "w" if new_file else "a"

    # Write the message to the log file.
    with open(working_directory + "/log_files/" + filename, mode) as output_file:
        if write_time:
            # Write the current time to the log file.
            now = datetime.now()
            prefix_time = now.strftime("%H:%M:%S") + " - "
            output_file.write(prefix_time + message)
        else:
            output_file.write(message)


def get_time_information(year, country_code):
    """
    Get the timezone, start date, and end date of the data retrieval for a specific country and year.

    Parameters
    ----------
    year : int
        The year of the data retrieval
    country_code : str
        The ISO Alpha-2 code of the country

    Returns
    -------
    country_timezone : pytz.timezone
        The timezone of the country
    start_date_and_time : pandas.Timestamp
        The start date and time of the data retrieval
    end_date_and_time : pandas.Timestamp
        The end date and time of the data retrieval
    """

    # Get the timezone of the country.
    country_timezone = pytz.timezone(pytz.country_timezones[country_code][0])

    # Define the start and end dates for the data retrieval.
    start_date_and_time = pd.Timestamp(str(year), tz=country_timezone)
    end_date_and_time = pd.Timestamp(str(year + 1), tz=country_timezone)

    return country_timezone, start_date_and_time, end_date_and_time


def add_missing_time_steps(log_file_name, country_code, time_series):
    """
    Add the missing time steps to a time series.

    Parameters
    ----------
    log_file_name : str
        Name of the log file
    country_code : str
        The ISO Alpha-2 code of the country
    time_series : pandas.Series
        Original time series

    Returns
    -------
    time_series : pandas.Series
        Time series of interest with the missing time steps added
    """

    # Get the time resolution of the time series.
    time_resolution = time_series.index.to_series().diff().min()

    # Get the number of time steps in the time series.
    number_of_time_steps = len(time_series)

    # Get the year of the time series.
    year = time_series.index.year[0]

    # Calculate the expected number of time steps in the time series.
    expected_number_of_time_steps = int(
        (8760 if year % 4 != 0 else 8784) * pd.Timedelta("1h") / time_resolution
    )

    # Check if there are less time steps than expected.
    if number_of_time_steps < expected_number_of_time_steps:
        # Get the timezone, start date, and end date of the data retrieval for the country and year.
        country_timezone, start_date_and_time, end_date_and_time = get_time_information(
            year, country_code
        )

        # Define the full time index for the time series in the local timezone.
        full_local_time_index = pd.date_range(
            start=start_date_and_time,
            end=end_date_and_time,
            freq=time_resolution,
            tz=country_timezone,
        )[:-1]

        # Reindex the time series to include the missing time steps.
        time_series = time_series.reindex(full_local_time_index)

        write_to_log_file(
            log_file_name,
            f"Added {expected_number_of_time_steps-number_of_time_steps} missing time steps out of {expected_number_of_time_steps}.\n",
        )

    return time_series


def resample_time_resolution(log_file_name, time_series, target_time_resolution="1h"):
    """
    Resample the time resolution of a time series to a given time resolution.

    Parameters
    ----------
    log_file_name : str
        Name of the log file
    time_series : pandas.Series
        Original time series
    target_time_resolution : str, optional
        Target time resolution of the time series

    Returns
    -------
    time_series : pandas.Series
        Resampled time series
    """

    # Get the time resolution of the time series.
    time_resolution = time_series.index.to_series().diff().min()

    # Check if the time resolution of the time series is less than the target time resolution.
    if time_resolution < pd.Timedelta(target_time_resolution):
        # Resample the time series to the target time resolution.
        time_series = time_series.resample(target_time_resolution).mean()

        # Write the resampling information to the log file.
        write_to_log_file(
            log_file_name,
            f"Resampled the time series from {time_resolution.total_seconds()/3600}h to {target_time_resolution}.\n",
        )

    return time_series


def linearly_interpolate(log_file_name, time_series):
    """
    Linearly interpolate the missing values in a time series only if they are isolated.

    Parameters
    ----------
    log_file_name : str
        Name of the log file
    time_series : pandas.Series
        Original time series

    Returns
    -------
    time_series : pandas.Series
        Time series of interest with the missing values interpolated
    """

    # Get the number of original non-null values.
    original_non_null_values = time_series.notnull().sum()

    # Where there is a NaN value, replace it with the average of the previous and next values. This takes care of replacing isolated NaN values.
    time_series[time_series.isnull()] = (
        time_series.shift(-1) + time_series.shift(1)
    ) / 2

    # Get the number of interpolated values.
    interpolated_values = time_series.notnull().sum() - original_non_null_values

    if interpolated_values > 0:
        # Write the number of interpolated values to the log file.
        write_to_log_file(
            log_file_name,
            f"Interpolated {interpolated_values} isolated missing values.\n",
        )

    return time_series


def harmonize_time_series(
    log_file_name,
    country_code,
    time_series,
    resample=True,
    target_time_resolution="1h",
    interpolate_missing_values=True,
):
    """
    Harmonize a given time series by adding the missing time steps, resampling the time resolution, and interpolating the missing values.

    Parameters
    ----------
    log_file_name : str
        Name of the log file
    country_code : str
        The ISO Alpha-2 code of the country
    time_series : pandas.Series
        The original time series
    resample : bool, optional
        If True, resample the time series to the target time resolution
    target_time_resolution : str, optional
        Target time resolution of the time series
    interpolate_missing_values : bool, optional
        If True, interpolate the missing values in the time series

    Returns
    -------
    time_series : pandas.Series
        The harmonized time series
    """

    # Add the missing time steps to the time series.
    time_series = add_missing_time_steps(log_file_name, country_code, time_series)

    # Resample the time resolution of the time series.
    if resample:
        time_series = resample_time_resolution(
            log_file_name, time_series, target_time_resolution=target_time_resolution
        )

    # Linearly interpolate the isolated missing values in the time series.
    if interpolate_missing_values:
        time_series = linearly_interpolate(log_file_name, time_series)

    return time_series


def save_time_series(time_series, full_file_name, variable_name):
    """
    Save the time series to a parquet or csv file.

    Parameters
    ----------
    time_series : pandas.Series
        The time series to save.
    full_file_name : str
        The file name where the data will be saved.
    """

    # Get the year of the time series.
    year = time_series.index.year[0]

    # Create an empty DataFrame to store the time series and set the index to UTC time.
    time_series_data = pd.DataFrame(
        index=time_series.index.tz_convert("UTC").tz_localize(None)
    ).rename_axis("Time (UTC)")

    # Add the hour of the day, day of the week, month of the year, and year to the DataFrame.
    time_series_data["Local hour of the day"] = time_series.index.hour
    time_series_data["Local day of the week"] = time_series.index.dayofweek
    time_series_data["Local month of the year"] = time_series.index.month
    time_series_data["Local year"] = year

    # Convert the time to UTC time and remove the timezone information.
    time_series.index = time_series.index.tz_convert("UTC").tz_localize(None)

    # Add the time series to the DataFrame.
    time_series_data[variable_name] = time_series

    # Save the time series.
    if full_file_name.endswith(".parquet"):
        time_series_data.to_parquet(full_file_name)
    elif full_file_name.endswith(".csv"):
        time_series_data.to_csv(full_file_name)
    else:
        raise ValueError("The file name must end with '.parquet' or '.csv'.")
