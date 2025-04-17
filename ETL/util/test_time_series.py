import os

import pandas
import pytest
import pytz
from time_series import (
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
