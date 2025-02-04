import os
from datetime import datetime

import pandas as pd
import pycountry
import pytz
from countryinfo import CountryInfo
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder


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


def read_countries_from_file(file_name):
    """
    Read the countries from a file and get their ISO Alpha-2 codes.

    Parameters
    ----------
    file_name : str
        The name of the file to read the countries from

    Returns
    -------
    list
        The ISO Alpha-2 codes of the countries
    """

    # Read the countries from the file.
    with open(file_name, "r") as file:
        countries = file.read().splitlines()

    country_codes = []
    for country in countries:
        try:
            country_codes.append(pycountry.countries.lookup(country).alpha_2)
        except LookupError:
            print(f"{country} not found.")

    return country_codes


def get_time_zone(country_code=None, latitude=None, longitude=None):
    """
    Get the time zone of a country or a specific location.

    Parameters
    ----------
    country_code : str, optional
        The ISO Alpha-2 code of the country
    latitude : float, optional
        The latitude of the location
    longitude : float, optional
        The longitude of the location

    Returns
    -------
    time_zone : pytz.timezone
        The time zone of the country or location
    """

    if country_code is not None:
        # Get the list of time zones for the country.
        time_zones = pytz.country_timezones[country_code]

        if len(time_zones) > 1:
            # Get the country name.
            country = pycountry.countries.get(alpha_2=country_code)

            # Get the capital city name.
            capital = CountryInfo(country.name).capital()

            # Get the capital city coordinates.
            geolocator = Nominatim(user_agent="time_zone_finder")
            location = geolocator.geocode(capital)

            # Find time zone based on capital city coordinates.
            tf = TimezoneFinder()
            time_zone = tf.timezone_at(lng=location.longitude, lat=location.latitude)
        else:
            # Get the time zone of the country.
            time_zone = time_zones[0]

    elif latitude is not None and longitude is not None:
        # Find time zone based on the provided coordinates.
        tf = TimezoneFinder()
        time_zone = tf.timezone_at(lng=longitude, lat=latitude)

    else:
        raise ValueError(
            "Either the country code or the latitude and longitude must be provided."
        )

    return time_zone


def calculate_time_difference(local_time_zone, year=2020):
    """
    Calculate the time shift between UTC and the local time zone.

    Parameters
    ----------
    local_time_zone : str
        Local time zone

    Returns
    -------
    time_difference : float
        Time difference in hours
    """

    # Get an arbitrary time expressed both in UTC and the local time zone.
    datetime1 = pd.Timestamp(f"{year}-01-01 00:00:00", tz="UTC")
    datetime2 = pd.Timestamp(f"{year}-01-01 00:00:00", tz=local_time_zone)

    # Calculate the time shift between UTC and the local time zone.
    time_difference = (datetime2 - datetime1).total_seconds() / 3600

    return time_difference
