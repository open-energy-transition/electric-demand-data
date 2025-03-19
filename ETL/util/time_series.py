import logging

import pandas as pd
import pytz


def add_missing_time_steps(
    time_series: pd.Series, local_time_zone: pytz.timezone
) -> pd.Series:
    """
    Add the missing time steps to a time series.

    Parameters
    ----------
    time_series : pandas.Series
        Original time series
    local_time_zone : pytz.timezone
        Local time zone of the time series

    Returns
    -------
    time_series : pandas.Series
        Time series of interest with the missing time steps added
    """

    # Get the time resolution of the time series.
    time_resolution = time_series.index.to_series().diff().min()

    # Get the year of the time series.
    year = time_series.index.year[0]

    # Calculate the expected number of time steps in the time series.
    expected_number_of_time_steps = int(
        (8760 if year % 4 != 0 else 8784) * pd.Timedelta("1h") / time_resolution
    )

    # Check if there are less time steps than expected.
    if len(time_series) < expected_number_of_time_steps:
        logging.warning(
            f"Added {expected_number_of_time_steps - len(time_series)} missing time steps out of {expected_number_of_time_steps}."
        )

        # Define the full time index for the time series in the local time zone.
        full_local_time_index = pd.date_range(
            start=str(year), end=str(year + 1), freq=time_resolution, tz=local_time_zone
        )[:-1]

        # Reindex the time series to include the missing time steps.
        time_series = time_series.reindex(full_local_time_index)

    return time_series


def resample_time_resolution(
    time_series: pd.Series, target_time_resolution: str = "1h"
) -> pd.Series:
    """
    Resample the time resolution of a time series to a given time resolution.

    Parameters
    ----------
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
        logging.warning(
            f"Resampled the time series from {time_resolution.total_seconds() / 3600}h to {target_time_resolution}."
        )

    return time_series


def linearly_interpolate(time_series: pd.Series) -> pd.Series:
    """
    Linearly interpolate the missing values in a time series only if they are isolated.

    Parameters
    ----------
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
        logging.warning(f"Interpolated {interpolated_values} isolated missing values.")

    return time_series


def harmonize_time_series(
    time_series: pd.Series,
    local_time_zone: pytz.timezone,
    resample: bool = True,
    target_time_resolution: str = "1h",
    interpolate_missing_values: bool = True,
) -> pd.Series:
    """
    Harmonize a given time series by adding the missing time steps, resampling the time resolution, and interpolating the missing values.

    Parameters
    ----------
    time_series : pandas.Series
        The original time series
    local_time_zone : pytz.timezone
        Local time zone of the time series
    resample : bool, optional
        If True, resample the time series to the target time resolution
    target_time_resolution : str, optional
        Target time resolution of the time series
    interpolate_missing_values : bool, optional
        If True, interpolate isolated missing values in the time series

    Returns
    -------
    time_series : pandas.Series
        The harmonized time series
    """

    # Add the missing time steps to the time series.
    time_series = add_missing_time_steps(time_series, local_time_zone)

    # Resample the time resolution of the time series.
    if resample:
        time_series = resample_time_resolution(
            time_series, target_time_resolution=target_time_resolution
        )

    # Linearly interpolate the isolated missing values in the time series.
    if interpolate_missing_values:
        time_series = linearly_interpolate(time_series)

    return time_series


def save_time_series(
    time_series: pd.Series | pd.DataFrame,
    full_file_name: str,
    variable_name: str | list[str],
    local_time_zone: pytz.timezone = None,
) -> None:
    """
    Save the time series to a parquet or csv file.

    Parameters
    ----------
    time_series : pandas.Series or pandas.DataFrame
        The time series in the local time zone. If the time zone is not specified, the time series is assumed to be in UTC time
    full_file_name : str
        The file name where the data will be saved
    variable_name : str or list of str
        The name of the variable in the time series
    local_time_zone : pytz.timezone, optional
        The local time zone of the time series
    """

    if time_series.index.tz is None:
        if local_time_zone is not None:
            # Assume the time series is in UTC time and convert it to the local time zone.
            time_series.index = time_series.index.tz_localize("UTC").tz_convert(
                local_time_zone
            )
        else:
            raise ValueError("The time zone of the time series must be specified.")

    # Create an empty DataFrame to store the time series and set the index to UTC time.
    time_series_data = pd.DataFrame(
        index=time_series.index.tz_convert("UTC").tz_localize(None)
    )

    # Add the hour of the day, day of the week, month of the year, and year to the DataFrame.
    time_series_data["Local hour of the day"] = time_series.index.hour
    time_series_data["Local day of the week"] = time_series.index.dayofweek
    time_series_data["Local month of the year"] = time_series.index.month
    time_series_data["Local year"] = time_series.index.year

    # Convert the time to UTC time and remove the time zone information.
    time_series.index = time_series.index.tz_convert("UTC").tz_localize(None)

    # Add the time series to the DataFrame.
    if isinstance(time_series, pd.Series):
        time_series_data[variable_name] = time_series
    elif isinstance(time_series, pd.DataFrame):
        time_series_data = pd.concat([time_series_data, time_series], axis=1)

    # Rename the index.
    time_series_data.index.name = "Time (UTC)"

    # Save the time series.
    if full_file_name.endswith(".parquet"):
        time_series_data.to_parquet(full_file_name)
    elif full_file_name.endswith(".csv"):
        time_series_data.to_csv(full_file_name)
    else:
        raise ValueError("The file name must end with .parquet or .csv.")
