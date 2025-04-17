import logging
import os

import geopandas
import pandas
import pytz
import util.general
import util.geospatial
import util.time_series
import xarray


def get_largest_population_densities_in_region(
    region_shape: geopandas.GeoDataFrame,
    population_density: xarray.DataArray,
    number_of_grid_cells: int,
) -> xarray.DataArray:
    """
    Get the population density data in the given region, ready to be sorted.

    Parameters
    ----------
    region_shape : geopandas.GeoDataFrame
        The shape of the region of interest
    population_density : xarray.DataArray
        Population density data
    number_of_grid_cells : int
        The number of grid cells to consider

    Returns
    -------
    largest_population_densities : xarray.DataArray
        Population density in the largest population density areas
    """

    # Calculate the fraction of each grid cell that is in the given shapes.
    fraction_of_grid_cells_in_shape = (
        util.geospatial.get_fraction_of_grid_cells_in_shape(region_shape)
    )

    # Rearrange the population density data to sort the values.
    population_density_rearranged = (
        population_density.where(fraction_of_grid_cells_in_shape > 0.0)
        .stack(z=("y", "x"))
        .dropna(dim="z")
    )

    # Get the grid cells with the largest population densities.
    largest_population_densities = population_density_rearranged.sortby(
        population_density_rearranged
    ).tail(number_of_grid_cells)

    return largest_population_densities


def get_temperature_in_largest_population_density_areas(
    year: int, region_shape: geopandas.GeoDataFrame, number_of_grid_cells: int
) -> pandas.Series:
    """
    Get the temperature data for the largest population density areas in the given country.

    Parameters
    ----------
    year : int
        The year of the temperature data
    region_shape : geopandas.GeoDataFrame
        The shape of the region of interest
    number_of_grid_cells : int
        The number of grid cells to consider

    Returns
    -------
    temperature_time_series : pandas.Series
        Temperature data for the largest population density areas in the given country
    """

    # Read the temperature data from the Copernicus Climate Data Store (CDS).
    temperature_data_directory = util.general.read_folders_structure()["weather_folder"]
    temperature_data = util.geospatial.load_xarray(
        os.path.join(
            temperature_data_directory,
            f"2m_temperature_{region_shape.index[0]}_{year}.nc",
        )
    )

    # Read the regional population density data.
    population_density_directory = util.general.read_folders_structure()[
        "population_density_folder"
    ]
    population_density = util.geospatial.load_xarray(
        os.path.join(
            population_density_directory,
            f"population_density_0.25_deg_{region_shape.index[0]}_2015.nc",
        )
    )

    # Get the population density data in the given region, ready to be sorted.
    largest_population_densities = get_largest_population_densities_in_region(
        region_shape, population_density, number_of_grid_cells=number_of_grid_cells
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


def add_temperature_statistics(
    temperature_time_series: pandas.Series, country_time_zone: pytz.timezone
) -> pandas.DataFrame:
    """
    Add temperature statistics to the temperature time series.

    Parameters
    ----------
    temperature_time_series : pandas.Series
        Original temperature time series
    country_time_zone : pytz.timezone
        Time zone of the country

    Returns
    -------
    temperature_time_series : pandas.DataFrame
        Temperature time series with added statistics
    """

    # Convert the temperature time series to the local time zone.
    temperature_time_series = temperature_time_series.tz_localize("UTC").tz_convert(
        country_time_zone
    )

    # Get the montly average temperature.
    monthly_average_temperature = temperature_time_series.resample("ME").mean()
    monthly_average_temperature.index = monthly_average_temperature.index.month

    # Get the rank of the monthly average temperature.
    monthly_average_temperature_rank = monthly_average_temperature.rank(ascending=False)

    # Get the annual average temperature.
    annual_average_temperature = temperature_time_series.resample("YE").mean().values[0]

    # The the 5 and 95 percentiles of the temperature.
    temperature_5_percentile = temperature_time_series.quantile(0.05)
    temperature_95_percentile = temperature_time_series.quantile(0.95)

    # Map the monthly average temperature to the original temperature time series.
    monthly_average_temperature = temperature_time_series.index.month.map(
        monthly_average_temperature
    ).to_series()
    monthly_average_temperature.index = temperature_time_series.index

    # Map the monthly average temperature rank to the original temperature time series.
    monthly_average_temperature_rank = temperature_time_series.index.month.map(
        monthly_average_temperature_rank
    ).to_series()
    monthly_average_temperature_rank.index = temperature_time_series.index

    # Map the annual average temperature, 5 and 95 percentiles to the original temperature time series.
    annual_average_temperature = pandas.Series(
        annual_average_temperature, index=temperature_time_series.index
    )
    temperature_5_percentile = pandas.Series(
        temperature_5_percentile, index=temperature_time_series.index
    )
    temperature_95_percentile = pandas.Series(
        temperature_95_percentile, index=temperature_time_series.index
    )

    # Add the temperature statistics to the temperature time series.
    temperature_time_series = temperature_time_series.rename(
        "Temperature (K)"
    ).to_frame()
    temperature_time_series["Monthly average temperature (K)"] = (
        monthly_average_temperature
    )
    temperature_time_series["Monthly average temperature rank"] = (
        monthly_average_temperature_rank
    )
    temperature_time_series["Annual average temperature (K)"] = (
        annual_average_temperature
    )
    temperature_time_series["5 percentile temperature (K)"] = temperature_5_percentile
    temperature_time_series["95 percentile temperature (K)"] = temperature_95_percentile
    temperature_time_series.index.name = "Local time"

    return temperature_time_series


def run_temperature_calculation() -> None:
    """
    Run the calculation of the temperature data according to the population density.
    """

    # Set up the logging configuration.
    log_files_directory = util.general.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
    log_file_name = "temperature_data.log"
    logging.basicConfig(
        filename=os.path.join(log_files_directory, log_file_name),
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Create a directory to store the weather data.
    result_directory = util.general.read_folders_structure()["temperature_folder"]
    os.makedirs(result_directory, exist_ok=True)

    # Read the codes of the regions of interest.
    settings_directory = util.general.read_folders_structure()["settings_folder"]
    region_codes = util.general.read_codes_from_file(
        os.path.join(settings_directory, "gegis__all_countries.yaml")
    )

    # Define the target file type.
    file_type = ".parquet"

    # Define the years of the data retrieval.
    start_year = 2015
    end_year = 2015
    years = range(start_year, end_year + 1)

    # Loop over the years.
    for year in years:
        logging.info(f"Year {year}.")

        # Loop over the regions of interest.
        for region_code in region_codes:
            logging.info(f"Retrieving data for {region_code}.")

            # Define the file paths of the temperature time series.
            temperature_file_path_top_1 = os.path.join(
                result_directory,
                f"temperature_time_series_top_1_{region_code}_{year}" + file_type,
            )
            temperature_file_path_top_3 = os.path.join(
                result_directory,
                f"temperature_time_series_top_3_{region_code}_{year}" + file_type,
            )

            # Check if any of the files does not exist.
            if not os.path.exists(temperature_file_path_top_1) or not os.path.exists(
                temperature_file_path_top_3
            ):
                # Get the shape of the region of interest.
                region_shape = util.geospatial.get_region_shape(region_code)

                # Get the time zone information for the region.
                region_time_zone = util.general.get_time_zone(region_code)

            # Check if the file of temperature time series for the largest population density area does not exist.
            if not os.path.exists(temperature_file_path_top_1):
                # Get the temperature data for the largest population density area in the given region.
                temperature_time_series_top_1 = (
                    get_temperature_in_largest_population_density_areas(
                        year, region_shape, number_of_grid_cells=1
                    )
                )

                # Add temperature statistics to the time series.
                temperature_time_series_top_1 = add_temperature_statistics(
                    temperature_time_series_top_1, region_time_zone
                )

                # Save the temperature time series.
                util.time_series.save_time_series(
                    temperature_time_series_top_1,
                    temperature_file_path_top_1,
                    temperature_time_series_top_1.columns.values,
                    local_time_zone=region_time_zone,
                )

            # Check if the file of temperature time series for the 3 largest population density areas does not exist.
            if not os.path.exists(temperature_file_path_top_3):
                # Get the temperature data for the 3 largest population density areas in the given region.
                temperature_time_series_top_3 = (
                    get_temperature_in_largest_population_density_areas(
                        year, region_shape, number_of_grid_cells=3
                    )
                )

                # Save the temperature time series.
                util.time_series.save_time_series(
                    temperature_time_series_top_3,
                    temperature_file_path_top_3,
                    "Temperature (K)",
                    local_time_zone=region_time_zone,
                )


if __name__ == "__main__":
    run_temperature_calculation()
