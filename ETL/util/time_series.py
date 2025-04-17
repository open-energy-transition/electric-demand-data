import os

import google.auth
import pandas
import pytz
import util.general
from google.cloud import storage


def save_time_series(
    time_series: pandas.Series | pandas.DataFrame,
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
    time_series_data = pandas.DataFrame(
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
    if isinstance(time_series, pandas.Series):
        time_series_data[variable_name] = time_series
    elif isinstance(time_series, pandas.DataFrame):
        time_series_data = pandas.concat([time_series_data, time_series], axis=1)

    # Rename the index.
    time_series_data.index.name = "Time (UTC)"

    # Save the time series.
    if full_file_name.endswith(".parquet"):
        time_series_data.to_parquet(full_file_name)
    elif full_file_name.endswith(".csv"):
        time_series_data.to_csv(full_file_name)
    else:
        raise ValueError("The file name must end with .parquet or .csv.")


def clean_data(time_series: pandas.Series, variable_name: str) -> pandas.Series:
    """
    Clean the time series data by setting the time zone to UTC, removing NaN and zero values, removing duplicated time steps, and setting consistent names.

    Parameters
    ----------
    time_series : pandas.Series
        Original time series
    variable_name : str
        The name of the variable in the time series

    Returns
    -------
    time_series : pandas.Series
        Time series of interest without NaN and zero values
    """

    # Check if the time series is timezone-aware.
    if time_series.index.tz is None:
        raise ValueError("The time series must be timezone-aware.")
    else:
        # Convert the time zone of the electricity demand time series to UTC and remove the time zone information.
        time_series.index = time_series.index.tz_convert("UTC").tz_localize(None)

    # Set the name of the index and the series.
    time_series.index.name = "Time (UTC)"
    time_series.name = variable_name

    # Remove timestamps with NaT values.
    time_series = time_series[time_series.index.notnull()]

    # Remove NaN and zero values from the time series.
    time_series = time_series[time_series.notnull() & (time_series != 0)]

    # Remove duplicated time steps from the time series.
    time_series = time_series[~time_series.index.duplicated()]

    return time_series


def upload_to_gcs(
    file_path: str,
    bucket_name: str,
    destination_blob_name: str,
) -> None:
    """
    Upload a file to Google Cloud Storage (GCS).

    Parameters
    ----------
    file_path : str
        The path to the file to be uploaded
    bucket_name : str
        The name of the GCS bucket
    destination_blob_name : str
        The name of the blob in the GCS bucket
    """

    # Get the root directory of the project.
    root_directory = util.general.read_folders_structure()["root_folder"]

    # Get the path to the credentials file.
    credentials_path = os.path.normpath(
        os.path.join(root_directory, "..", "application_default_credentials.json")
    )

    # Load the credentials from the file.
    credentials, __ = google.auth.load_credentials_from_file(credentials_path)

    # Create a GCS client.
    storage_client = storage.Client(
        credentials=credentials, project="electric-demand-data"
    )

    # Get the bucket.
    bucket = storage_client.bucket(bucket_name)

    # Create a new blob and upload the file.
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

    return None
