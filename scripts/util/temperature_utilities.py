import geopandas as gpd
import pandas as pd
import pytz
import util.general_utilities as general_utilities
import util.geospatial_utilities as geospatial_utilities
import xarray as xr


def get_largest_population_densities_in_region(
    region_shape: gpd.GeoDataFrame,
    population_density: xr.DataArray,
    number_of_grid_cells: int,
) -> xr.DataArray:
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
    year: int, region_shape: gpd.GeoDataFrame, number_of_grid_cells: int
) -> pd.Series:
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
    temperature_data_directory = general_utilities.read_folders_structure()[
        "weather_folder"
    ]
    temperature_data = geospatial_utilities.load_xarray(
        temperature_data_directory
        + f"/2m_temperature_{region_shape.index[0]}_{year}.nc"
    )

    # Read the regional population density data.
    population_density_directory = general_utilities.read_folders_structure()[
        "population_density_folder"
    ]
    population_density = geospatial_utilities.load_xarray(
        population_density_directory
        + f"/population_density_0.25_deg_{region_shape.index[0]}_2015.nc"
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
    temperature_time_series: pd.Series, country_time_zone: pytz.timezone
) -> pd.DataFrame:
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
