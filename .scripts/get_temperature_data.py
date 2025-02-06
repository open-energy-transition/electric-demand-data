import os

import pandas as pd
import utilities.general_utilities as general_utilities
import utilities.geospatial_utilities as geospatial_utilities
import utilities.time_series_utilities as time_series_utilities


def get_largest_population_densities_in_region(
    region_shape, population_density, number_of_grid_cells=1
):
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
    population_density_rearranged : xarray.DataArray
        Population density data in the given region, ready to be sorted
    """

    # Calculate the fraction of each grid cell that is in the given shapes.
    fraction_of_grid_cells_in_shape = (
        geospatial_utilities.get_fraction_of_grid_cells_in_shape(region_shape)
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
    year, region_shape, number_of_grid_cells=1
):
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
    temperature_time_series : xarray.DataArray
        Temperature data for the largest population density areas in the given country
    """

    # Read the temperature data from the Copernicus Climate Data Store (CDS).
    temperature_data = geospatial_utilities.load_xarray(
        f"data__weather/2m_temperature_{region_shape.index[0]}_{year}.nc"
    )

    # Read the regional population density data.
    population_density = geospatial_utilities.load_xarray(
        f"data__population_density/population_density_0.25_deg_{region_shape.index[0]}_2015.nc"
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


def add_temperature_statistics(temperature_time_series, country_time_zone):
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
    annual_average_temperature = pd.Series(
        annual_average_temperature, index=temperature_time_series.index
    )
    temperature_5_percentile = pd.Series(
        temperature_5_percentile, index=temperature_time_series.index
    )
    temperature_95_percentile = pd.Series(
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


def run_temperature_calculation():
    """
    Run the calculation of the temperature data according to the population density.
    """

    # Define the log file name.
    log_file_name = "temperature_data.log"

    # Create a directory to store the weather data.
    result_directory = "data__temperature"
    if not os.path.exists(result_directory):
        os.makedirs(result_directory)

    # Read the ISO Alpha-2 codes of the countries of interest.
    country_codes = general_utilities.read_countries_from_file(
        "settings/gegis__all_countries.txt"
    )

    # Define the start and end years of the data retrieval.
    start_year = 2015
    end_year = 2015

    # Loop over the years.
    for year in range(start_year, end_year + 1):
        general_utilities.write_to_log_file(
            log_file_name,
            ("" if year == start_year else "\n") + f"Year: {year}\n",
            new_file=(year == start_year),
        )

        # Loop over the countries.
        for country_code in country_codes:
            general_utilities.write_to_log_file(log_file_name, "\n")
            general_utilities.write_to_log_file(
                log_file_name, f"Retrieving data for {country_code}.\n"
            )

            # Define the file name for the temperature time series in the largest population density areas
            file_name_top_1 = f"temperature_time_series_top_1_{country_code}_{year}.csv"
            file_name_top_3 = f"temperature_time_series_top_3_{country_code}_{year}.csv"

            # Check if the file does not exist.
            if not os.path.exists(result_directory + "/" + file_name_top_1):
                # Get the shape of the country of interest.
                region_shape = geospatial_utilities.get_geopandas_region(country_code)

                # Get the time zone information for the country.
                country_time_zone = general_utilities.get_time_zone(country_code)

                # Get the temperature data for the largest population density area in the given country.
                temperature_time_series_top_1 = (
                    get_temperature_in_largest_population_density_areas(
                        year, region_shape, number_of_grid_cells=1
                    )
                )

                # Add temperature statistics to the time series.
                temperature_time_series_top_1 = add_temperature_statistics(
                    temperature_time_series_top_1, country_time_zone
                )

                # Save the temperature time series.
                time_series_utilities.save_time_series(
                    temperature_time_series_top_1,
                    result_directory + "/" + file_name_top_1,
                    temperature_time_series_top_1.columns.values,
                    local_time_zone=country_time_zone,
                )

            # Check if the file does not exist.
            if not os.path.exists(result_directory + "/" + file_name_top_3):
                # Get the shape of the country of interest.
                region_shape = geospatial_utilities.get_geopandas_region(country_code)

                # Get the time zone information for the country.
                country_time_zone = general_utilities.get_time_zone(country_code)

                # Get the temperature data for the 3 largest population density areas in the given country.
                temperature_time_series_top_3 = (
                    get_temperature_in_largest_population_density_areas(
                        year, region_shape, number_of_grid_cells=3
                    )
                )

                # Save the temperature time series.
                time_series_utilities.save_time_series(
                    temperature_time_series_top_3,
                    result_directory + "/" + file_name_top_3,
                    "Temperature (K)",
                    local_time_zone=country_time_zone,
                )


if __name__ == "__main__":
    run_temperature_calculation()
