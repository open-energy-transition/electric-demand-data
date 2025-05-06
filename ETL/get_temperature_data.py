import argparse
import logging
import os

import geopandas
import numpy
import pandas
import pytz
import util.directories
import util.entities
import util.geospatial
import util.shapes
import xarray


def read_command_line_arguments() -> argparse.Namespace:
    """
    Create a parser for the command line arguments and read them.

    Returns
    -------
    args : argparse.Namespace
        The command line arguments
    """

    # Create a parser for the command line arguments.
    parser = argparse.ArgumentParser(description="")

    # Add the command line arguments.
    parser.add_argument(
        "-c",
        "--code",
        type=str,
        help='The ISO Alpha-2 code (example: "FR") or a combination of ISO Alpha-2 code and subdivision code (example: "US_CAL")',
        required=False,
    )
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        help="The path to the yaml file containing the list of codes of the countries and subdivisions of interest",
        required=False,
    )
    parser.add_argument(
        "-y",
        "--year",
        type=int,
        help="Year of the weather data to use",
        required=False,
    )

    # Read the arguments from the command line.
    args = parser.parse_args()

    return args


def get_temperature_in_largest_population_density_areas(
    year: int,
    entity_shape: geopandas.GeoDataFrame,
    entity_time_zone: pytz.timezone,
    number_of_grid_cells: int = 1,
) -> pandas.Series:
    """
    Get the temperature data for the largest population density areas in the given country or subdivision.

    Parameters
    ----------
    year : int
        The year of the temperature data
    entity_shape : geopandas.GeoDataFrame
        The shape of the country or subdivision of interest
    number_of_grid_cells : int
        The number of grid cells to consider

    Returns
    -------
    temperature_time_series : pandas.Series
        Temperature data for the largest population density areas in the given country or subdivision
    """

    # Read the temperature data downloaded from the Copernicus Climate Data Store (CDS).
    temperature_data_directory = util.directories.read_folders_structure()[
        "weather_folder"
    ]
    temperature_data = xarray.open_mfdataset(
        os.path.join(
            temperature_data_directory,
            f"{entity_shape.index[0]}_2m_temperature_*.nc",
        )
    )

    # Harmonize the temperature data.
    temperature_data = util.geospatial.harmonize_coords(temperature_data)

    # Extract the temperature data for the given year in local time.
    start_date = (
        pandas.Timestamp(str(year) + "-01-01 00:00:00", tz=entity_time_zone)
        .tz_convert("UTC")
        .tz_localize(None)
    )
    end_date = (
        pandas.Timestamp(str(year) + "-12-31 23:59:59", tz=entity_time_zone)
        .tz_convert("UTC")
        .tz_localize(None)
    )
    temperature_data = temperature_data.sel(valid_time=slice(start_date, end_date))[
        "t2m"
    ].load()

    # Define the years for which population density data is available.
    available_years = numpy.array([2020])  # [2000, 2005, 2010, 2015, 2020]

    # Find the year of the population density data that is closest to the year of the temperature data.
    population_density_year = available_years[
        numpy.abs(available_years - year).argmin()
    ]

    # Read the population density data of the country or subdivision of interest.
    population_density_directory = util.directories.read_folders_structure()[
        "population_density_folder"
    ]
    population_density = xarray.open_dataarray(
        os.path.join(
            population_density_directory,
            f"{entity_shape.index[0]}_0.25_deg_{population_density_year}.nc",
        )
    )

    # Harmonize the population density data.
    population_density = util.geospatial.harmonize_coords(population_density)

    # Get the grid cells with the largest population densities in the given shape.
    largest_population_densities = util.geospatial.get_largest_values_in_shape(
        entity_shape, population_density, number_of_grid_cells
    )

    # Get the temperature data for the grid cells with the largest population densities.
    temperature_time_series_in_largest_population_densities = temperature_data.sel(
        y=largest_population_densities["y"].values,
        x=largest_population_densities["x"].values,
    )

    # Calculate the average temperature for the grid cells with the largest population densities.
    average_temperature_time_series_in_largest_population_densities = (
        temperature_time_series_in_largest_population_densities.mean(dim=("y", "x"))
    )

    # Convert the temperature data to a pandas Series.
    temperature_time_series = (
        average_temperature_time_series_in_largest_population_densities.to_series()
    )

    return temperature_time_series


def build_temperature_database(
    temperature_time_series_top_1: pandas.Series,
    temperature_time_series_top_3: pandas.Series,
    entity_time_zone: pytz.timezone,
) -> pandas.DataFrame:
    """
    Build the temperature database for the given country or subdivision.

    Parameters
    ----------
    temperature_time_series_top_1 : pandas.Series
        The temperature time series for the largest population density area
    temperature_time_series_top_3 : pandas.Series
        The temperature time series for the 3 largest population density areas
    entity_time_zone : pytz.timezone
        Time zone of the country or subdivision of interest

    Returns
    -------
    temperature_time_series : pandas.DataFrame
        Temperature time series with added statistics
    """

    # Create an empty DataFrame to store the temperature data.
    temperature_database = pandas.DataFrame(index=temperature_time_series_top_1.index)

    # Convert the temperature time series to the local time zone.
    temperature_time_series_top_1 = temperature_time_series_top_1.tz_localize(
        "UTC"
    ).tz_convert(entity_time_zone)
    temperature_time_series_top_3 = temperature_time_series_top_3.tz_localize(
        "UTC"
    ).tz_convert(entity_time_zone)

    # Get the montly average temperature.
    monthly_average_temperature = temperature_time_series_top_1.resample("ME").mean()
    monthly_average_temperature.index = monthly_average_temperature.index.month

    # Get the rank of the monthly average temperature.
    monthly_average_temperature_rank = monthly_average_temperature.rank(ascending=False)

    # Map the monthly average temperature to the original temperature time series.
    monthly_average_temperature = temperature_time_series_top_1.index.month.map(
        monthly_average_temperature
    ).to_series()
    monthly_average_temperature.index = temperature_time_series_top_1.index

    # Map the monthly average temperature rank to the original temperature time series.
    monthly_average_temperature_rank = temperature_time_series_top_1.index.month.map(
        monthly_average_temperature_rank
    ).to_series()
    monthly_average_temperature_rank.index = temperature_time_series_top_1.index

    # Get the annual average temperature.
    annual_average_temperature = pandas.Series(
        temperature_time_series_top_1.resample("YE").mean().values[0],
        index=temperature_time_series_top_1.index,
    )

    # Get the 5 and 95 percentiles of the temperature.
    temperature_5_percentile = pandas.Series(
        temperature_time_series_top_1.quantile(0.05),
        index=temperature_time_series_top_1.index,
    )
    temperature_95_percentile = pandas.Series(
        temperature_time_series_top_1.quantile(0.95),
        index=temperature_time_series_top_1.index,
    )

    # Add the hour of the day, day of the week, month of the year, and year to the DataFrame.
    temperature_database["Local hour of the day"] = (
        temperature_time_series_top_1.index.hour
    )
    temperature_database["Local day of the week"] = (
        temperature_time_series_top_1.index.dayofweek
    )
    temperature_database["Local month of the year"] = (
        temperature_time_series_top_1.index.month
    )
    temperature_database["Local year"] = temperature_time_series_top_1.index.year

    # Add the temperature statistics to the temperature time series.
    temperature_database["Temperature - Top 1 (K)"] = (
        temperature_time_series_top_1.values
    )
    temperature_database["Temperature - Top 3 (K)"] = (
        temperature_time_series_top_3.values
    )
    temperature_database["Monthly average temperature - Top 1 (K)"] = (
        monthly_average_temperature.values
    )
    temperature_database["Monthly average temperature rank - Top 1"] = (
        monthly_average_temperature_rank.values
    )
    temperature_database["Annual average temperature - Top 1 (K)"] = (
        annual_average_temperature.values
    )
    temperature_database["5 percentile temperature - Top 1 (K)"] = (
        temperature_5_percentile.values
    )
    temperature_database["95 percentile temperature - Top 1 (K)"] = (
        temperature_95_percentile.values
    )
    temperature_database.index.name = "Time (UTC)"

    return temperature_database


def run_temperature_calculation(args: argparse.Namespace) -> None:
    """
    Run the calculation of the temperature data according to the population density.

    Parameters
    ----------
    args : argparse.Namespace
        The command line arguments
    """

    # Create a directory to store the weather data.
    result_directory = util.directories.read_folders_structure()["temperature_folder"]
    os.makedirs(result_directory, exist_ok=True)

    # Get the list of codes of the countries and subdivisions of interest.
    codes = util.entities.check_and_get_codes(args)

    # Loop over the countries and subdivisions of interest.
    for code in codes:
        logging.info(f"Extracting temperature data for {code}.")

        if args.year is not None:
            # If the year is provided, use it.
            years = [args.year]
        else:
            # Get the years of available data for the country or subdivision of interest.
            years = util.entities.get_available_years(code)

        # Get the shape of the country or subdivision.
        entity_shape = util.shapes.get_entity_shape(code, make_plot=False)

        # Get the time zone information for the country or subdivision.
        entity_time_zone = util.entities.get_time_zone(code)

        # Loop over the years.
        for year in years:
            logging.info(f"Year {year}.")

            # Define the file paths of the temperature time series.
            file_path = os.path.join(
                result_directory,
                f"{code}_temperature_time_series_{year}.parquet",
            )

            # Check if the file of temperature time series for the largest population density area does not exist.
            if not os.path.exists(file_path) or (
                os.path.exists(file_path) and year == pandas.Timestamp.now().year
            ):
                # Get the temperature data for the largest population density area in the given country or subdivision.
                temperature_time_series_top_1 = (
                    get_temperature_in_largest_population_density_areas(
                        year, entity_shape, entity_time_zone, number_of_grid_cells=1
                    )
                )

                # Get the temperature data for the 3 largest population density areas in the given country or subdivision.
                temperature_time_series_top_3 = (
                    get_temperature_in_largest_population_density_areas(
                        year, entity_shape, entity_time_zone, number_of_grid_cells=3
                    )
                )

                # Add temperature statistics to the time series.
                temperature_database = build_temperature_database(
                    temperature_time_series_top_1,
                    temperature_time_series_top_3,
                    entity_time_zone,
                )

                # Save the temperature time series.
                temperature_database.to_parquet(file_path)
                temperature_database.to_csv(file_path.replace(".parquet", ".csv"))

                logging.info(
                    f"Temperature time series for {code} has been successfully extracted and saved."
                )
            else:
                logging.info(f"Temperature time series for {code} already exists.")


if __name__ == "__main__":
    # Read the command line arguments.
    args = read_command_line_arguments()

    # Set up the logging configuration.
    log_files_directory = util.directories.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
    log_file_name = "temperature_data.log"
    logging.basicConfig(
        filename=os.path.join(log_files_directory, log_file_name),
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Run the temperature calculation.
    run_temperature_calculation(args)
