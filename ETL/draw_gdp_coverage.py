# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script generates a bar plot showing the years of available electricity demand data falling into different GDP per capita levels, grouped by continent.

    It fetches GDP per capita data from the World Bank, categorizes it into income levels, and plots the number of years in which each country or subdivision falls into these income levels, grouped by continent.

    Source: https://data.worldbank.org/indicator/NY.GDP.PCAP.PP.CD?view=map
"""

import os
import zipfile
from io import BytesIO

import matplotlib.pyplot
import numpy
import pandas
import requests
import util.directories
import util.entities

# Create a directory to store the figures.
figure_directory = util.directories.read_folders_structure()["figures_folder"]
os.makedirs(figure_directory, exist_ok=True)

# Read the codes of all countries and subdivisions.
codes = util.entities.read_all_codes()

# Get the ISO alpha-3 codes for all countries and subdivisions.
alpha_3_codes = {}
for code in codes:
    alpha_3_codes[code] = util.entities.get_iso_alpha_3_code(code)

# Get the continent for each country.
continent_codes = {}
for code in codes:
    continent_codes[code] = util.entities.get_continent_code(code)

# Get the data time range for all countries and subdivisions.
data_time_ranges = util.entities.read_all_date_ranges()

# Initialize a dictionary to store the fractions of years for which data is available for each country or subdivision.
fractions_of_years: dict[str, dict[str, float]] = {}

# Loop over the codes of all countries and subdivisions.
for code in codes:
    # Define a series containing the days in the time range of the data for the country or subdivision.
    days = pandas.date_range(
        start=data_time_ranges[code][0],
        end=data_time_ranges[code][1],
        freq="d",
    )

    # Initialize the dictionary for the current country or subdivision.
    fractions_of_years[code] = {}

    # Loop over the available years for the country or subdivision.
    for year in days.year.unique():
        # Define the days in the current year.
        total_days_in_year = 366 if pandas.Timestamp(year, 1, 1).is_leap_year else 365

        # Append the fraction of year for which data is available.
        fractions_of_years[code][str(year)] = (
            len(days[days.year == year]) / total_days_in_year
        )

# Fetch the GDP data from the World Bank.
response = requests.get(
    "https://api.worldbank.org/v2/en/indicator/NY.GDP.PCAP.PP.CD?downloadformat=csv"
)

# Extract the archive from the response.
archive = zipfile.ZipFile(BytesIO(response.content), "r")

# Get the name of the GDP data file in the archive. It is the file that does not start with "Metadata" and ends with ".csv".
gdp_file_name = [
    name
    for name in archive.namelist()
    if not name.startswith("Metadata") and name.endswith(".csv")
][0]

# Read the GDP data from the compressed CSV file.
gdp_data = pandas.read_csv(archive.open(gdp_file_name), skiprows=4)

# Define the GDP groups.
gdp_levels = {
    "Low income": (0, 10000),
    "Lower middle income": (10000, 30000),
    "Upper middle income": (30000, 60000),
    "High income": (60000, float("inf")),
}

# Define the labels for the continents.
continent_names = {
    "AF": "Africa",
    "AS": "Asia",
    "EU": "Europe",
    "NA": "North America",
    "SA": "South America",
    "OC": "Oceania",
}

# Initialize the GDP occurrence in the defined GDP levels and by continent.
gdp_occurrence = {
    continent: {level: 0.0 for level in gdp_levels.keys()}
    for continent in continent_names.keys()
}

# Loop over the countries and subdivisions.
for code in codes:
    # Get the GDP data for the country or subdivision.
    gdp_series = gdp_data[gdp_data["Country Code"] == alpha_3_codes[code]].iloc[:, 4:]

    # Extract the GDP values for the available years.
    gdp_series = gdp_series.iloc[
        0, gdp_series.columns.isin(fractions_of_years[code].keys())
    ]

    # Remove any NaN values from the GDP series.
    gdp_series = gdp_series.dropna()

    # Loop over the available years for the country or subdivision.
    for year in gdp_series.index:
        # Loop over the GDP levels and continents and add the occurrence to the corresponding level and continent.
        for level, (min_val, max_val) in gdp_levels.items():
            if min_val <= gdp_series[year] < max_val:
                gdp_occurrence[continent_codes[code]][level] += fractions_of_years[
                    code
                ][year]

# Define the colors for the continents.
colors = {
    "AF": "crimson",  # Africa
    "AS": "teal",  # Asia
    "EU": "gold",  # Europe
    "NA": "indigo",  # North America
    "SA": "coral",  # South America
    "OC": "slategray",  # Oceania
}

# Create a figure to plot the GDP coverage.
fig, ax = matplotlib.pyplot.subplots(figsize=(10, 8))

# Initialize the cumulative height for the stacked bars.
cumulative_height = numpy.zeros(len(gdp_levels))

# Create a bar plot for the GDP occurrences with continents as stacked bars.
for continent_code in gdp_occurrence.keys():
    # Create a bar plot for the current continent.
    bars = ax.bar(
        gdp_occurrence[continent_code].keys(),
        gdp_occurrence[continent_code].values(),
        bottom=cumulative_height,
        label=continent_names[continent_code],
        color=colors[continent_code],
        alpha=0.7,
    )

    # Update the cumulative height for the next iteration.
    cumulative_height += numpy.array(list(gdp_occurrence[continent_code].values()))

# Set the y-axis limit to the maximum cumulative height.
ax.set_ylim(0, max(cumulative_height) * 1.05)

# Set the title and labels.
ax.set_title("Available electricity demand data", fontsize=16, pad=15, weight="bold")
ax.set_xlabel("GDP per capita, PPP (current international $)", fontsize=14)
ax.set_ylabel("Number of years", fontsize=14)
ax.set_xticklabels(
    [
        f"{min_val}+" if numpy.isinf(max_val) else f"{min_val} - {max_val}"
        for min_val, max_val in gdp_levels.values()
    ]
)

# Add a legend to the plot.
ax.legend(loc="upper left", fontsize=12)

# Save the figure.
fig.savefig(
    os.path.join(figure_directory, "gdp_coverage.png"), dpi=300, bbox_inches="tight"
)
