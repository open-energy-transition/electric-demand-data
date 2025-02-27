import logging
import pandas as pd

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
