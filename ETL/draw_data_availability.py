# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This script generates a figure showing the availability of hourly
    and sub-hourly electricity demand data by GDP and annual electricity
    demand per capita. It uses data from Ember and the World Bank to
    visualize the coverage of electricity demand data across different
    countries and continents. Both sources are used to improve the
    coverage of the data.

    Source: https://data.worldbank.org/indicator/NY.GDP.PCAP.PP.CD?view=map
    Source: https://data.worldbank.org/indicator/EG.USE.ELEC.KH.PC?view=map
    Source: https://ember-energy.org/data/yearly-electricity-data/
"""  # noqa: W505

import os
import zipfile
from io import BytesIO

import matplotlib.patches
import matplotlib.pyplot
import matplotlib.ticker
import numpy
import pandas
import requests
import utils.directories
import utils.entities


def _get_year_fractions(codes: list[str]) -> dict[str, dict[str, float]]:
    """
    Get the fractions of years for which data is available.

    Parameters
    ----------
    codes : list[str]
        A list of ISO alpha-2 or a combination of ISO alpha-2 and
        subdivision codes.

    Returns
    -------
    dict[str, dict[str, float]]
        A dictionary where the keys are entity codes and the values are
        dictionaries with years as keys and fractions of years as
        values.
    """
    # Get the data time range for all countries and subdivisions.
    data_time_ranges = utils.entities.read_all_date_ranges()

    # Initialize a dictionary to store the fractions of years for which
    # data is available for each country or subdivision.
    fractions_of_years: dict[str, dict[str, float]] = {}

    # Loop over the codes of all countries and subdivisions.
    for code in codes:
        # Define a series containing the days in the time range of the
        # data for the country or subdivision.
        days = pandas.date_range(
            start=data_time_ranges[code][0],
            end=data_time_ranges[code][1],
            freq="d",
        )

        # Initialize the dictionary for the current country or
        # subdivision.
        fractions_of_years[code] = {}

        # Loop over the available years for the country or subdivision.
        for year in days.year.unique():
            # Define the days in the current year.
            total_days_in_year = (
                366 if pandas.Timestamp(year, 1, 1).is_leap_year else 365
            )

            # Append the fraction of year for which data is available.
            fractions_of_years[code][str(year)] = (
                len(days[days.year == year]) / total_days_in_year
            )

    return fractions_of_years


def _retrieve_ember_electricity_data() -> pandas.DataFrame:
    """
    Fetch the electricity demand data from Ember.

    Returns
    -------
    pandas.DataFrame
        The electricity demand data from Ember.
    """
    # Fetch the data from the Ember dataset.
    ember_data = pandas.read_csv(
        "https://storage.googleapis.com/emb-prod-bkt-publicdata/"
        "public-downloads/yearly_full_release_long_format.csv"
    )

    # Extract and return the data for the "Demand per capita" variable.
    return ember_data[(ember_data["Variable"] == "Demand per capita")]


def _retrieve_world_bank_data(variable: str) -> pandas.DataFrame:
    """
    Fetch the data from the World Bank.

    Returns
    -------
    pandas.DataFrame
        The data from the World Bank.
    """
    if variable == "electricity_demand_per_capita":
        url = (
            "https://api.worldbank.org/v2/en/indicator/"
            "EG.USE.ELEC.KH.PC?downloadformat=csv"
        )
    elif variable == "gdp_per_capita":
        url = (
            "https://api.worldbank.org/v2/en/indicator/"
            "NY.GDP.PCAP.PP.CD?downloadformat=csv"
        )

    # Fetch the data from the World Bank.
    response = requests.get(url)

    # Extract the archive from the response.
    archive = zipfile.ZipFile(BytesIO(response.content), "r")

    # Get the name of data file in the archive. It is the file that does
    # not start with "Metadata" and ends with ".csv".
    world_bank_file_name = [
        name
        for name in archive.namelist()
        if not name.startswith("Metadata") and name.endswith(".csv")
    ][0]

    # Return the data from the compressed CSV file.
    return pandas.read_csv(archive.open(world_bank_file_name), skiprows=4)


def _extract_ember_electricity_data(
    ember_data: pandas.DataFrame,
    alpha_3_code: str,
    years_of_interest: list[str],
) -> pandas.Series:
    """
    Extract the data for a specific country from the Ember dataset.

    Parameters
    ----------
    alpha_3_code : str
        The ISO alpha-3 code of the country.
    ember_data : pandas.DataFrame
        The Ember dataset.

    Returns
    -------
    pandas.Series
        The data for the specified country.
    """
    # Get the data for the country in the Ember dataset.
    ember_series = ember_data[(ember_data["Country code"] == alpha_3_code)]

    # Extract the values for the available years in the Ember dataset.
    ember_series = ember_series[
        ember_series["Year"].astype(str).isin(years_of_interest)
    ]

    # Convert to a Series with years as index, convert MWh to kWh, and
    # return it.
    return pandas.Series(
        ember_series["Value"].to_numpy() * 1000,
        index=ember_series["Year"].astype(str),
    )


def _extract_world_bank_data(
    world_bank_data: pandas.DataFrame,
    alpha_3_code: str,
    years_of_interest: list[str],
) -> pandas.Series:
    """
    Extract the data from the World Bank dataset.

    Parameters
    ----------
    alpha_3_code : str
        The ISO alpha-3 code of the country.
    dataset : pandas.DataFrame
        The World Bank dataset.

    Returns
    -------
    pandas.Series
        The data for the specified country and for the years of
        interest.
    """
    # Get the data for the country in the World Bank dataset.
    world_bank_series = world_bank_data[
        world_bank_data["Country Code"] == alpha_3_code
    ].iloc[:, 4:]

    # Extract the values for the available years in the World Bank
    # dataset.
    world_bank_series = world_bank_series.iloc[
        0, world_bank_series.columns.isin(years_of_interest)
    ]

    # Return the series without NaN values.
    return world_bank_series.dropna()


def _get_electricity_demand_data(
    codes: list[str],
    alpha_3_codes: dict[str, str],
    years_of_interest: dict[str, list[str]],
) -> pandas.Series:
    """
    Get the electricity demand data.

    Parameters
    ----------
    codes : list[str]
        A list of ISO alpha-2 or a combination of ISO alpha-2 and
        subdivision codes.
    alpha_3_codes : dict[str, str]
        A list of ISO alpha-3 codes of the countries.
    years_of_interest : dict[str, list[str]]
        A dictionary where the keys are entitiy codes and the values are
        lists of strings representing the years of interest.

    Returns
    -------
    dict[str, dict[str, pandas.Series]]
        The annual electricity demand data for the specified country and
        for the years of interest.
    """
    # Fetch the electricity demand data from Ember.
    ember_electricity_data = _retrieve_ember_electricity_data()

    # Fetch the electricity demand per capita data from the World Bank.
    world_bank_electricity_data = _retrieve_world_bank_data(
        "electricity_demand_per_capita"
    )

    # Initialize the electricity demand data series for each country or
    # subdivision.
    electricity_demand_data: dict[str, dict[str, pandas.Series]] = {}

    # Loop over the ISO alpha-3 codes.
    for code in codes:
        # Get the electricity demand data for the country and for the
        # available years in the World Bank dataset.
        world_bank_series = _extract_world_bank_data(
            world_bank_electricity_data,
            alpha_3_codes[code],
            years_of_interest[code],
        )

        # Get the electricity demand data for the country and for the
        # available years in the Ember dataset.
        ember_series = _extract_ember_electricity_data(
            ember_electricity_data,
            alpha_3_codes[code],
            years_of_interest[code],
        )

        # Combine the World Bank and Ember datasets, with an average of
        # the two datasets for each year if both are available.
        combined_series = (world_bank_series + ember_series) / 2

        # Where the combined series is NaN because one of the datasets
        # is missing, use the other dataset.
        combined_series = combined_series.fillna(world_bank_series).fillna(
            ember_series
        )

        # If the country is already in the dictionary, add a new key for
        # the code.
        if alpha_3_codes[code] in electricity_demand_data:
            electricity_demand_data[alpha_3_codes[code]][code] = (
                combined_series
            )
        else:
            # If the country is not in the dictionary, add it.
            electricity_demand_data[alpha_3_codes[code]] = {
                code: combined_series
            }

    return electricity_demand_data


def _get_world_bank_gdp_data(
    codes: list[str],
    alpha_3_codes: dict[str, str],
    years_of_interest: dict[str, list[str]],
) -> pandas.Series:
    """
    Get the GDP per capita data.

    Parameters
    ----------
    codes : list[str]
        A list of ISO alpha-2 or a combination of ISO alpha-2 and
        subdivision codes.
    alpha_3_codes : dict[str, str]
        A list of ISO alpha-3 codes of the countries.
    years_of_interest : dict[str, list[str]]
        A dictionary where the keys are entitiy codes and the values are
        lists of strings representing the years of interest.

    Returns
    -------
    dict[str, dict[str, pandas.Series]]
        The GDP per capita data for the specified country and for the
        years of interest.
    """
    # Fetch the GDP per capita data from the World Bank.
    world_bank_gdp_data = _retrieve_world_bank_data("gdp_per_capita")

    # Initialize the GDP data series for each country or subdivision.
    gdp_data: dict[str, dict[str, pandas.Series]] = {}

    # Loop over the ISO alpha-3 codes.
    for code in codes:
        # Get the GDP data for the country and for the available years
        # in the World Bank dataset.
        gdp_series = _extract_world_bank_data(
            world_bank_gdp_data,
            alpha_3_codes[code],
            years_of_interest[code],
        )

        # If the country is already in the dictionary, add a new key for
        # the code.
        if alpha_3_codes[code] in gdp_data:
            gdp_data[alpha_3_codes[code]][code] = gdp_series
        else:
            # If the country is not in the dictionary, add it.
            gdp_data[alpha_3_codes[code]] = {code: gdp_series}

    return gdp_data


def _get_occurrences(
    data: dict[str, pandas.Series],
    codes: list[str],
    alpha_3_codes: dict[str, str],
    continent_codes: dict[str, str],
    fractions_of_years: dict[str, dict[str, float]],
    levels: dict[str, tuple[int, int | float]],
) -> dict[str, dict[str, float]]:
    """
    Get the occurrences of electricity demand or GDP per capita data.

    Parameters
    ----------
    data : dict[str, pandas.Series]
        A dictionary where the keys are entity codes and the values are
        pandas Series with years as index and values as electricity
        demand or GDP per capita.
    codes : list[str]
        A list of ISO alpha-2 or a combination of ISO alpha-2 and
        subdivision codes.
    alpha_3_codes : dict[str, str]
        A dictionary where the keys are entity codes and the values are
        ISO alpha-3 codes.
    continent_codes : dict[str, str]
        A dictionary where the keys are entity codes and the values are
        continent codes.
    fractions_of_years : dict[str, dict[str, float]]
        A dictionary where the keys are entity codes and the values are
        dictionaries with years as keys and fractions of years as
        values.
    levels : dict[str, tuple[int, int | float]]
        A dictionary where the keys are level names and the values are
        tuples with minimum and maximum values for the level.

    Returns
    -------
    dict[str, dict[str, float]]
        A dictionary where the keys are entity codes and the values are
        dictionaries with levels as keys and occurrences as values.
    """
    # Initialize the occurrence in the defined levels and by continent.
    occurrence = {
        continent: {level: 0.0 for level in levels.keys()}
        for continent in continent_names.keys()
    }

    # Loop over the countries and subdivisions.
    for code in codes:
        # Loop over the available years for the country or subdivision.
        for year in data[alpha_3_codes[code]][code].index:
            # Loop over the levels and continents and add the occurrence
            # to the corresponding level and continent.
            for level, (min_val, max_val) in levels.items():
                if min_val <= data[alpha_3_codes[code]][code][year] < max_val:
                    occurrence[continent_codes[code]][level] += (
                        fractions_of_years[code][year]
                    )

    return occurrence


def _add_bar_chart(
    ax: matplotlib.axes.Axes,
    levels: dict[str, tuple[int, int | float]],
    occurrence: dict[str, dict[str, float]],
    xlabel: str,
) -> matplotlib.axes.Axes:
    """
    Add a bar chart to the given axes.

    Parameters
    ----------
    ax : matplotlib.axes.Axes
        The axes to which the bar chart will be added.
    levels : dict[str, tuple[int, int | float]]
        A dictionary where the keys are level names and the values are
        tuples with minimum and maximum values for the level.
    occurrence : dict[str, dict[str, float]]
        A dictionary where the keys are continent codes and the values
        are dictionaries with levels as keys and occurrences as values.
    xlabel : str
        The label for the x-axis.

    Returns
    -------
    matplotlib.axes.Axes
        The axes with the added bar chart.
    """
    # Initialize the cumulative height for the stacked bars.
    cumulative_height = numpy.zeros(len(levels))

    # Create a bar plot for the GDP occurrences with continents as
    # stacked bars.
    for continent_code in occurrence.keys():
        # Create a bar plot for the current continent.
        ax.bar(
            occurrence[continent_code].keys(),
            occurrence[continent_code].values(),
            bottom=cumulative_height,
            label=continent_names[continent_code],
            color=colors[continent_code],
            alpha=0.7,
        )

        # Update the cumulative height for the next iteration.
        cumulative_height += numpy.array(
            list(occurrence[continent_code].values())
        )

    # Set the title and labels.
    ax.set_xlabel(xlabel, fontsize=14)
    ax.set_xticks(range(len(levels)))
    ax.set_xticklabels(
        [
            f">{int(min_val / 1000)}"
            if numpy.isinf(max_val)
            else f"{int(min_val / 1000)} - {int(max_val / 1000)}"
            for min_val, max_val in levels.values()
        ]
    )

    return ax


# Create a directory to store the figures.
figure_directory = utils.directories.read_folders_structure()["figures_folder"]
os.makedirs(figure_directory, exist_ok=True)

# Read the codes of all countries and subdivisions.
codes = utils.entities.read_all_codes()

# Get the ISO alpha-3 codes for all countries and subdivisions.
alpha_3_codes = {}
for code in codes:
    alpha_3_codes[code] = utils.entities.get_iso_alpha_3_code(code)

# Get the continent for each country.
continent_codes = {}
for code in codes:
    continent_codes[code] = utils.entities.get_continent_code(code)

# Get the fractions of years for which data is available for each
# country or subdivision.
fractions_of_years = _get_year_fractions(codes)

# Extract the available years for each country or subdivision.
available_years = {
    code: list(fractions_of_years[code].keys()) for code in codes
}

# Get the electricity demand data from Ember and the World Bank.
electricity_data = _get_electricity_demand_data(
    codes,
    alpha_3_codes,
    available_years,
)

# Get the GDP per capita data from the World Bank.
gdp_data = _get_world_bank_gdp_data(
    codes,
    alpha_3_codes,
    available_years,
)

# Define the electricity demand groups.
electricity_demand_levels = {
    "Low demand": (0, 2000),
    "Lower middle demand": (2000, 5000),
    "Upper middle demand": (5000, 12000),
    "High demand": (12000, float("inf")),
}

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

# Get the electricity demand occurrence in the defined electricity
# demand levels and by continent.
electricity_demand_occurrence = _get_occurrences(
    electricity_data,
    codes,
    alpha_3_codes,
    continent_codes,
    fractions_of_years,
    electricity_demand_levels,
)

# Get the GDP occurrence in the defined GDP levels and by continent.
gdp_occurrence = _get_occurrences(
    gdp_data,
    codes,
    alpha_3_codes,
    continent_codes,
    fractions_of_years,
    gdp_levels,
)

# Define the colors for the continents.
colors = {
    "AF": "crimson",  # Africa
    "AS": "teal",  # Asia
    "EU": "gold",  # Europe
    "NA": "indigo",  # North America
    "SA": "coral",  # South America
    "OC": "slategray",  # Oceania
}

# Set the font size.
matplotlib.pyplot.rc("font", size=12)

# Create a figure to plot the GDP coverage.
fig, ax0 = matplotlib.pyplot.subplots(figsize=(10, 15))
ax0.set_axis_off()

# Add the bar chart for the electricity demand coverage.
ax = fig.add_axes([0.05, 0.6, 0.4, 0.35])
ax = _add_bar_chart(
    ax,
    gdp_levels,
    gdp_occurrence,
    "GDP per capita, PPP\n(current international k$)",
)

# Set the y-axis limit to the maximum cumulative height and the label.
ax.set_ylim(0, 410)
ax.set_ylabel("Number of years", fontsize=14)

# Add the bar chart for the GDP coverage.
ax = fig.add_axes([0.5, 0.6, 0.4, 0.35])
ax = _add_bar_chart(
    ax,
    electricity_demand_levels,
    electricity_demand_occurrence,
    "Annual electricity demand\nper capita (MWh)",
)
# Set the y-axis limit to the maximum cumulative height.
ax.set_ylim(0, 410)

# Add scatter plot for the GDP and annual demand per capita data.
ax = fig.add_axes([0.05, 0.05, 0.85, 0.48])

# Make the x and y axes logarithmic.
ax.set_xscale("log", base=2)
ax.set_yscale("log", base=2)
ax.xaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
ax.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())

# Initialize the GDP and electricity demand data to plot.
gdp_data_to_plot: dict[str, pandas.Series] = {}
electricity_data_to_plot: dict[str, pandas.Series] = {}

# Loop over the ISO alpha-3 codes and plot the data.
for alpha_3_code in set(alpha_3_codes.values()):
    # Get the codes belonging to the current alpha-3 code.
    local_codes = [
        code for code in codes if alpha_3_codes[code] == alpha_3_code
    ]

    # Initialize the GDP and electricity demand data to plot.
    gdp_data_to_plot[alpha_3_code] = pandas.Series(dtype=float)
    electricity_data_to_plot[alpha_3_code] = pandas.Series(dtype=float)

    # Get the GDP and electricity demand data for the current alpha-3
    # code with the longest available time range.
    for code in local_codes:
        local_gdp_data = gdp_data[alpha_3_code][code]
        local_electricity_data = electricity_data[alpha_3_code][code]
        if len(local_gdp_data) > len(gdp_data_to_plot[alpha_3_code]):
            gdp_data_to_plot[alpha_3_code] = local_gdp_data
        if len(local_electricity_data) > len(
            electricity_data_to_plot[alpha_3_code]
        ):
            electricity_data_to_plot[alpha_3_code] = local_electricity_data

    if (
        not gdp_data_to_plot[alpha_3_code].empty
        and not electricity_data_to_plot[alpha_3_code].empty
    ):
        # Make sure the GDP and electricity demand data are aligned by
        # year.
        common_index = gdp_data_to_plot[alpha_3_code].index.intersection(
            electricity_data_to_plot[alpha_3_code].index
        )
        gdp_data_to_plot[alpha_3_code] = gdp_data_to_plot[alpha_3_code][
            common_index
        ]
        electricity_data_to_plot[alpha_3_code] = electricity_data_to_plot[
            alpha_3_code
        ][common_index]

        # Get the the first and last values of the GDP and electricity
        # demand data.
        gdp_data_to_plot[alpha_3_code] = gdp_data_to_plot[alpha_3_code].iloc[
            [0, -1]
        ]
        electricity_data_to_plot[alpha_3_code] = electricity_data_to_plot[
            alpha_3_code
        ].iloc[[0, -1]]

        # Plot the of GDP per capita and annual electricity demand per
        # capita data.
        ax.plot(
            gdp_data_to_plot[alpha_3_code] / 1000,
            electricity_data_to_plot[alpha_3_code] / 1000,
            "o",
            alpha=0.7,
            color=colors[continent_codes[local_codes[0]]],
            markeredgecolor="none",
            markersize=10,
            label=continent_names[continent_codes[local_codes[0]]],
        )

        # Add an arrow from the first to the last point.
        ax.annotate(
            text="",
            xy=(
                gdp_data_to_plot[alpha_3_code].iloc[1] / 1000,
                electricity_data_to_plot[alpha_3_code].iloc[1] / 1000,
            ),
            xytext=(
                gdp_data_to_plot[alpha_3_code].iloc[0] / 1000,
                electricity_data_to_plot[alpha_3_code].iloc[0] / 1000,
            ),
            arrowprops=dict(
                facecolor=colors[continent_codes[local_codes[0]]],
                edgecolor=(0, 0, 0, 0.7),
                linewidth=0.5,
                alpha=0.7,
            ),
        )

# Add sample points for the GDP and electricity demand data to explain
# the plot.
ax.plot(
    [60, 110],
    [0.3, 0.3],
    "o",
    alpha=0.7,
    color=(0, 0, 0, 0.7),
    markeredgecolor="none",
    markersize=10,
)
ax.annotate(
    text="",
    xy=(110, 0.3),
    xytext=(60, 0.3),
    arrowprops=dict(
        facecolor=(0, 0, 0, 0.5),
        edgecolor=(0, 0, 0, 0.7),
        linewidth=0.5,
        alpha=0.7,
    ),
)
ax.annotate(text="First year\nof data", xy=(60, 0.36), ha="center")
ax.annotate(text="Last year\nof data", xy=(110, 0.36), ha="center")

# Add the names of a few countries to the plot.
ax.annotate(
    text="Nigeria",
    xy=(
        gdp_data_to_plot["NGA"].iloc[0] * 1.08 / 1000,
        electricity_data_to_plot["NGA"].iloc[0] / 1000,
    ),
    ha="left",
    va="center",
    fontsize=12,
    color=colors["AF"],
)
ax.annotate(
    text="Peru",
    xy=(
        gdp_data_to_plot["PER"].iloc[0] * 1.1 / 1000,
        electricity_data_to_plot["PER"].iloc[0] / 1000,
    ),
    ha="left",
    va="top",
    fontsize=12,
    color=colors["SA"],
)
ax.annotate(
    text="Colombia",
    xy=(
        gdp_data_to_plot["COL"].iloc[0] / 1000,
        electricity_data_to_plot["COL"].iloc[0] * 1.13 / 1000,
    ),
    ha="center",
    va="bottom",
    fontsize=12,
    color=colors["SA"],
)
ax.annotate(
    "Algeria",
    xy=(
        gdp_data_to_plot["DZA"].iloc[0] / 1000,
        electricity_data_to_plot["DZA"].iloc[0] * 0.9 / 1000,
    ),
    ha="center",
    va="top",
    fontsize=12,
    color=colors["AF"],
)
ax.annotate(
    text="Canada",
    xy=(
        gdp_data_to_plot["CAN"].iloc[0] * 1.05 / 1000,
        electricity_data_to_plot["CAN"].iloc[0] * 1.05 / 1000,
    ),
    ha="left",
    va="bottom",
    fontsize=12,
    color=colors["NA"],
)
ax.annotate(
    text="Norway",
    xy=(
        gdp_data_to_plot["NOR"].iloc[0] * 1.05 / 1000,
        electricity_data_to_plot["NOR"].iloc[0] * 0.94 / 1000,
    ),
    ha="left",
    va="top",
    fontsize=12,
    color=colors["EU"],
)
ax.annotate(
    text="Luxembourg",
    xy=(
        gdp_data_to_plot["LUX"].iloc[0] * 0.95 / 1000,
        electricity_data_to_plot["LUX"].iloc[0] * 1.08 / 1000,
    ),
    ha="left",
    va="bottom",
    fontsize=12,
    color=colors["EU"],
)

# Add a legend to the plot.
for count, continent_code in enumerate(continent_names.keys()):
    ax.add_patch(
        matplotlib.patches.Rectangle(
            (
                0.02,
                0.925 - 0.25 * (count) / (len(continent_names.values()) - 1),
            ),
            0.19,
            0.05,
            facecolor=colors[continent_code],
            edgecolor="none",
            alpha=0.7,
            transform=ax.transAxes,
        )
    )
    ax.annotate(
        text=continent_names[continent_code],
        xy=(0.03, 0.94 - 0.25 * (count) / (len(continent_names.values()) - 1)),
        xycoords="axes fraction",
        color=(0, 0, 0, 1),
        fontsize=14,
    )

# Add the axis titles.
ax.set_xlabel("GDP per capita, PPP (current international k$)", fontsize=14)
ax.set_ylabel("Annual electricity demand per capita (MWh)", fontsize=14)

# Add a title to the figure.
matplotlib.pyplot.suptitle(
    (
        "Availability of hourly and sub-hourly electricity demand data\n"
        "by GDP and annual electricity demand per capita"
    ),
    x=0.45,
    y=1.02,
    fontsize=18,
    weight="bold",
)

# Save the figure.
fig.savefig(
    os.path.join(
        figure_directory, "data_availability_by_gpd_and_electricity_demand.png"
    ),
    dpi=300,
    bbox_inches="tight",
)
