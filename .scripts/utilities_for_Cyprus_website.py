from urllib.request import Request, urlopen


def query_website(date):
    """
    Query the website for the electricity demand data for 15-day period starting from the given date.

    Parameters
    ----------
    date : str
        The date in the format "YYYY-MM-DD".

    Returns
    -------
    lines : list
        The list of lines from the html file.
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


def read_date(lines, line_count):
    """
    Read the date from the html file.

    Parameters
    ----------
    lines : list
        The list of lines from the html file.
    line_count : int
        The current line count.

    Returns
    -------
    date : str
        The date in the format "YYYY-MM-DD".
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


def read_hour(lines, line_count):
    """
    Read the hour from the html file.

    Parameters
    ----------
    lines : list
        The list of lines from the html file.
    line_count : int
        The current line count.

    Returns
    -------
    hour : int
        The hour.
    """

    # Extract the line that contains the hour data.
    hour_line = lines[line_count + 1]

    if "hourStr" in hour_line:
        # Extract the hour from the line.
        hour = int(hour_line.split(" = ")[1].replace('"', "").replace(";", ""))
    else:
        print("Hour not found")

    return hour


def read_minute(lines, line_count):
    """
    Read the minute from the html file.

    Parameters
    ----------
    lines : list
        The list of lines from the html file.
    line_count : int
        The current line count.

    Returns
    -------
    minute : int
        The minute.
    """

    # Extract the line that contains the minute data.
    minute_line = lines[line_count + 2]

    if "minutesStr" in minute_line:
        # Extract the minute from the line.
        minute = int(minute_line.split(" = ")[1].replace('"', "").replace(";", ""))
    else:
        print("Minute not found")

    return minute


def read_wind_generation(lines, line_count):
    """
    Read the wind generation from the html file.

    Parameters
    ----------
    lines : list
        The list of lines from the html file.
    line_count : int
        The current line count.

    Returns
    -------
    wind_generation : float
        The wind generation in MW.
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
        print("Generation not found")

    return wind_generation


def read_solar_generation(lines, line_count):
    """
    Read the solar generation from the html file.

    Parameters
    ----------
    lines : list
        The list of lines from the html file.
    line_count : int
        The current line count.

    Returns
    -------
    solar_generation : float
        The solar generation in MW.
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
        print("Generation not found")

    return solar_generation


def read_conventional_generation(lines, line_count):
    """
    Read the conventional generation from the html file.

    Parameters
    ----------
    lines : list
        The list of lines from the html file.
    line_count : int
        The current line count.

    Returns
    -------
    conventional_generation : float
        The conventional generation in MW.
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
        print("Generation not found")

    return conventional_generation


def read_total_generation(lines, line_count):
    """
    Read the total generation from the html file.

    Parameters
    ----------
    lines : list
        The list of lines from the html file.
    line_count : int
        The current line count.

    Returns
    -------
    total_generation : float
        The total generation in MW.
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
        print("Generation not found")

    return total_generation
