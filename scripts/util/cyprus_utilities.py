import logging
from urllib.request import Request, urlopen

import pandas as pd


def query_website(date: str) -> list[str]:
    """
    Query the website for the electricity demand data for 15-day period starting from the given date.

    Parameters
    ----------
    date : str
        The date in the format "YYYY-MM-DD"

    Returns
    -------
    lines : list of str
        The list of lines from the html file
    """

    # Define the url of the current request.
    url = f"https://tsoc.org.cy/electrical-system/archive-total-daily-system-generation-on-the-transmission-system/?startdt={date[8:]}-{date[5:7]}-{date[0:4]}&enddt=%2B15days"

    # Read the html file.
    page = (
        urlopen(Request(url=url, headers={"User-Agent": "Mozilla/5.0"}))
        .read()
        .decode("utf-8")
    )

    # Convert the html file to a list of lines.
    lines = page.split("\n")

    return lines


def read_date(lines: list[str], line_count: int) -> str:
    """
    Read the date from the html file.

    Parameters
    ----------
    lines : list of str
        The list of lines from the html file
    line_count : int
        The current line count

    Returns
    -------
    date : str
        The date in the format "YYYY-MM-DD"
    """

    # Extract the date from the line.
    date = (
        lines[line_count]
        .split(" = ")[1]
        .replace('"', "")
        .replace(";", "")
        .replace("\r", "")
    )

    return date


def read_hour(lines: list[str], line_count: int) -> int:
    """
    Read the hour from the html file.

    Parameters
    ----------
    lines : list of str
        The list of lines from the html file
    line_count : int
        The current line count

    Returns
    -------
    hour : int
        The hour
    """

    # Extract the line that contains the hour data.
    hour_line = lines[line_count + 1]

    if "hourStr" in hour_line:
        # Extract the hour from the line.
        hour = int(hour_line.split(" = ")[1].replace('"', "").replace(";", ""))
    else:
        logging.error("Hour not found")

    return hour


def read_minute(lines: list[str], line_count: int) -> int:
    """
    Read the minute from the html file.

    Parameters
    ----------
    lines : list of str
        The list of lines from the html file
    line_count : int
        The current line count

    Returns
    -------
    minute : int
        The minute
    """

    # Extract the line that contains the minute data.
    minute_line = lines[line_count + 2]

    if "minutesStr" in minute_line:
        # Extract the minute from the line.
        minute = int(minute_line.split(" = ")[1].replace('"', "").replace(";", ""))
    else:
        logging.error("Minute not found")

    return minute


def read_wind_generation(lines: list[str], line_count: int) -> float:
    """
    Read the wind generation from the html file.

    Parameters
    ----------
    lines : list of str
        The list of lines from the html file
    line_count : int
        The current line count

    Returns
    -------
    wind_generation : float
        The wind generation in MW
    """

    # Extract the line that contains the generation data.
    generation_line = lines[line_count + 22]

    if "[dateStrFormat," in generation_line:
        if generation_line.split(", ")[1] == "null":
            wind_generation = None
        else:
            # Extract the wind generation from the line.
            wind_generation = float(generation_line.split(", ")[1])

    else:
        logging.error("Generation not found")

    return wind_generation


def read_solar_generation(lines: list[str], line_count: int) -> float:
    """
    Read the solar generation from the html file.

    Parameters
    ----------
    lines : list of str
        The list of lines from the html file
    line_count : int
        The current line count

    Returns
    -------
    solar_generation : float
        The solar generation in MW
    """

    # Extract the line that contains the generation data.
    generation_line = lines[line_count + 22]

    if "[dateStrFormat," in generation_line:
        if generation_line.split(", ")[2] == "null":
            solar_generation = None
        else:
            # Extract the solar generation from the line.
            solar_generation = float(generation_line.split(", ")[2])

    else:
        logging.error("Generation not found")

    return solar_generation


def read_conventional_generation(lines: list[str], line_count: int) -> float:
    """
    Read the conventional generation from the html file.

    Parameters
    ----------
    lines : list of str
        The list of lines from the html file
    line_count : int
        The current line count

    Returns
    -------
    conventional_generation : float
        The conventional generation in MW
    """

    # Extract the line that contains the generation data.
    generation_line = lines[line_count + 22]

    if "[dateStrFormat," in generation_line:
        if generation_line.split(", ")[4].replace("]", "") == "null\r":
            conventional_generation = None
        else:
            # Extract the conventional generation from the line.
            conventional_generation = float(
                generation_line.split(", ")[4].replace("]", "")
            )

    else:
        logging.error("Generation not found")

    return conventional_generation


def read_total_generation(lines: list[str], line_count: int) -> float:
    """
    Read the total generation from the html file.

    Parameters
    ----------
    lines : list of str
        The list of lines from the html file
    line_count : int
        The current line count

    Returns
    -------
    total_generation : float
        The total generation in MW
    """

    # Extract the line that contains the generation data.
    generation_line = lines[line_count + 22]

    if "[dateStrFormat," in generation_line:
        if generation_line.split(", ")[3] == "null":
            # When the value for total generation is null, typically the values for wind, solar, and conventional generation are also null.
            total_generation = None
        elif generation_line.split(", ")[3] == "0":
            # When the value for total generation is 0, typically the values for wind, solar, and/or conventional generation are not null, and these can be used to estimate the total demand.
            wind_generation = float(read_wind_generation(lines, line_count) or 0)
            solar_generation = float(read_solar_generation(lines, line_count) or 0)
            conventional_generation = float(
                read_conventional_generation(lines, line_count) or 0
            )
            total_generation = (
                wind_generation + solar_generation + conventional_generation
            )
            # If the total demand is still 0, set it to None.
            if total_generation == 0:
                total_generation = None
        else:
            # Extract the total generation from the line.
            total_generation = float(generation_line.split(", ")[3])

    else:
        logging.error("Generation not found")

    return total_generation


def download_electricity_generation_from_tsoc(year: int) -> pd.Series:
    """
    Retrieve the electricity generation data for Cyprus from the website of the Transmission System Operator of Cyprus.

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
        lines = query_website(dates_in_interval[0])

        # Set the line count to 0.
        line_count = 0

        # Loop through the lines.
        while line_count < len(lines) - 1:
            # Check if the current line contains the date.
            if any(
                ('var dateStr = "' + date) in lines[line_count]
                for date in dates_in_interval
            ):
                dates.append(read_date(lines, line_count))
                hours.append(read_hour(lines, line_count))
                minutes.append(read_minute(lines, line_count))
                total_generation.append(read_total_generation(lines, line_count))

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
