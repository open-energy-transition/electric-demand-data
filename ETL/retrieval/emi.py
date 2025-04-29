#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
License: AGPL-3.0

Description:
This script retrieves cumulative electricity demand data from the website of the Electricity Market Information (EMI) for New Zealand.

The data is downloaded as a CSV file from Jan 1, 2005 up to the current date. The data is retrieved all at once.
Source: https://www.emi.ea.govt.nz/Wholesale/Reports/W_GD_C
"""

import logging

import pandas as pd
import util.fetcher


def get_available_requests(code: str | None = None) -> None:
    logging.info("The data is retrieved all at once.")
    return None


def get_url(start_date: pd.Timestamp, end_date: pd.Timestamp) -> str:
    """
    Build the EMI URL for demand data download.
    """
    return (
        "https://www.emi.ea.govt.nz/Wholesale/Download/DataReport/CSV/W_GD_C"
        f"?DateFrom={start_date.strftime('%Y%m%d')}"
        f"&DateTo={end_date.strftime('%Y%m%d')}"
        "&RegionType=NZ&_rsdr=L7423D&_si=v|4"
    )


def download_and_extract_data(
    start_date: str = "2005-01-01", end_date: str | None = None
) -> pd.Series:
    """
    Download and extract demand data from EMI.
    Returns a timezone-aware pandas.Series indexed by datetime.
    """
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date) if end_date else pd.Timestamp.today()

    url = get_url(start_ts, end_ts)

    dataset = util.fetcher.fetch_data(
        url,
        target_content_type="csv",
        csv_kwargs={"skiprows": 11},
    )

    # Parse date columns with proper format
    try:
        index = pd.to_datetime(dataset["Period start"], dayfirst=True, errors="coerce")
    except Exception as e:
        raise ValueError(f"Date parsing failed: {e}")

    # Ensure timezone-aware using Pacific/Auckland, handle DST ambiguity
    index = index.dt.tz_localize(
        "Pacific/Auckland", ambiguous="NaT", nonexistent="shift_forward"
    )

    # Convert from GWh to MW (multiply by 1000)
    electricity_demand_time_series = pd.Series(
        dataset["Demand (GWh)"].values
        * 1000,  # Convert GWh to MW by multiplying by 1000
        index=index,
    )

    return electricity_demand_time_series
