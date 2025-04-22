import os

import numpy
import pandas
import pytest
import pytz
from time_series import (
    add_missing_time_steps,
    harmonize_time_series,
    linearly_interpolate,
    resample_time_resolution,
    save_time_series,
)

local_time_zone = pytz.timezone("America/New_York")


@pytest.fixture
def sample_time_series():
    # Define a one-year-long time series with 30-minute resolution.
    dates = pandas.date_range("2023", "2024", freq="30min", tz=local_time_zone)[:-1]

    # Define the data.
    data = list(range(len(dates)))

    # Remove three data points, two of which are consecutive.
    data[2] = None
    data[3] = None
    data[10] = None

    return pandas.Series(data, index=dates)


def test_add_missing_time_steps(sample_time_series):
    # Store the original length of the time series.
    original_length = len(sample_time_series)

    # Remove a time step.
    time_series = sample_time_series.drop(sample_time_series.index[20])

    # Count the number of missing data points.
    missing_data_points = time_series.isna().sum()

    # Add the missing time step back.
    filled_time_series = add_missing_time_steps(time_series, local_time_zone)

    # Check if the missing time step is added.
    assert len(filled_time_series) == original_length

    # Ensure that there is one additional missing data point.
    assert filled_time_series.isna().sum() == missing_data_points + 1


def test_resample_time_resolution(sample_time_series):
    # Resample to hourly resolution.
    resampled_time_series = resample_time_resolution(sample_time_series, "1h")

    # The one-year-long time series resampled to hourly resolution should have 8760 time steps.
    assert len(resampled_time_series) == 8760


def test_linearly_interpolate(sample_time_series):
    # Check the data point expected to be interpolated.
    assert numpy.isnan(sample_time_series.iloc[10])

    # Count the number of missing data points.
    missing_data_points = sample_time_series.isna().sum()

    # Interpolate missing values where the two surrounding values are known.
    interpolated_time_series = linearly_interpolate(sample_time_series)

    # Check if only isolated missing values are interpolated.
    assert interpolated_time_series.isna().sum() == missing_data_points - 1

    # Check correct interpolation.
    assert numpy.isnan(interpolated_time_series.iloc[2])
    assert numpy.isnan(interpolated_time_series.iloc[3])
    assert interpolated_time_series.iloc[10] == 10


def test_harmonize_time_series(sample_time_series):
    # Harmonize the time series by adding missing time steps, resampling the time resolution, and interpolating missing values.
    harmonized_time_series = harmonize_time_series(sample_time_series, local_time_zone)

    # Should have no missing values.
    assert harmonized_time_series.isna().sum() == 0

    # Check total number of time steps.
    assert len(harmonized_time_series) == 8760


def test_save_time_series(sample_time_series, tmp_path):
    # Define the file paths where the time series will be saved.
    csv_file_path = os.path.join(tmp_path, "test_output.csv")
    parquet_file_path = os.path.join(tmp_path, "test_output.parquet")
    unsupported_file_path = os.path.join(tmp_path, "test_output.txt")

    # Save the time series to a csv file by passing a DataFrame.
    save_time_series(
        sample_time_series.copy().to_frame(), csv_file_path, "TestVariable"
    )

    # Save the time series to a csv file using a time zone aware series.
    save_time_series(sample_time_series.copy(), csv_file_path, "TestVariable")

    # Save the time series to a parquet file using a time zone naive series.
    save_time_series(
        sample_time_series.copy().tz_localize(None),
        parquet_file_path,
        "TestVariable",
        local_time_zone,
    )

    # Save the time series to a csv without time zone information.
    try:
        save_time_series(
            sample_time_series.copy().tz_localize(None), csv_file_path, "TestVariable"
        )
    except ValueError:
        pass

    # Save the time series to an unsupported file format.
    try:
        save_time_series(
            sample_time_series.copy(), unsupported_file_path, "TestVariable"
        )
    except ValueError:
        pass

    # Check if the file is created.
    assert os.path.exists(csv_file_path)
    assert os.path.exists(parquet_file_path)
