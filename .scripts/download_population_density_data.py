import os
from urllib.request import urlretrieve

import numpy as np
import utilities.figure_utilities as figure_utilities
import utilities.general_utilities as general_utilities
import utilities.geospatial_utilities as geospatial_utilities


def download_population_density_data_from_SEDAC(year, file_name):
    """
    Download the population density data from the Socioeconomic Data and Applications Center (SEDAC).

    Parameters
    ----------
    year : int
        The year of the population density data
    file_name : str
        The name of the file to store the population density data
    """

    if not os.path.exists(file_name):
        # Download the population density data.
        url = f"https://data.ghg.center/sedac-popdensity-yeargrid5yr-v4.11/gpw_v4_population_density_rev11_{year}_30_sec_{year}.tif"
        urlretrieve(url, file_name)


def coarsen_population_density(population_density, region_bounds):
    """
    Coarsen the population density data to the same resolution as the weather data. The population density resolution is 30 arc-seconds, while the resource data resolution is 900 arc-seconds (0.25 degrees).

    Parameters
    ----------
    population_density : xarray.DataArray
        The population density data
    region_bounds : tuple
        The lateral bounds of the region of interest (West, South, East, North)

    Returns
    -------
    population_density : xarray.DataArray
        The population density data coarsened to the same resolution as the weather data
    """

    # Define the new coarser resolution.
    x_list = np.linspace(-180, 180, int(360 / 0.25) + 1)
    y_list = np.linspace(-90, 90, int(180 / 0.25) + 1)

    # Define the bins where to aggregate the population density data of the finer resolution.
    # The next(...) function in this case calculates the first value that satisfies the specified condition.
    # The resulting bins are the first and last values of the x_list and y_list that are within the bounds of the region of interest.
    x_bins = np.arange(
        x_list[next(x for x, val in enumerate(x_list) if val >= region_bounds[0])]
        - 0.25 / 2,
        x_list[next(x for x, val in enumerate(x_list) if val >= region_bounds[2]) + 1]
        + 0.25 / 2,
        0.25,
    )
    y_bins = np.arange(
        y_list[next(x for x, val in enumerate(y_list) if val >= region_bounds[1])]
        - 0.25 / 2,
        y_list[next(x for x, val in enumerate(y_list) if val >= region_bounds[3]) + 1]
        + 0.25 / 2,
        0.25,
    )

    # Aggregate the population density data to the new coarser resolution, first in the x direction and then in the y direction.
    population_density = population_density.groupby_bins("x", x_bins).sum()
    population_density = population_density.groupby_bins("y", y_bins).sum()

    # For each coordinate, substitute the bin range with the middle of the bin.
    population_density["x_bins"] = np.arange(
        x_list[next(x for x, val in enumerate(x_list) if val >= region_bounds[0])],
        x_list[next(x for x, val in enumerate(x_list) if val >= region_bounds[2]) + 1],
        0.25,
    )
    population_density["y_bins"] = np.arange(
        y_list[next(x for x, val in enumerate(y_list) if val >= region_bounds[1])],
        y_list[next(x for x, val in enumerate(y_list) if val >= region_bounds[3]) + 1],
        0.25,
    )

    # Rename the bins to 'x' and 'y'.
    population_density = population_density.rename({"x_bins": "x", "y_bins": "y"})

    return population_density


def extract_population_density_of_region(
    population_density, region_shape, file_name, make_plot=True
):
    """
    Extract the population density of a region of interest, coarsen it to the same resolution as the weather data, and save it to a file.

    Parameters
    ----------
    population_density : xarray.DataArray
        The population density data.
    region_shape : geopandas.GeoDataFrame
        The shape of the region of interest
    file_name : str
        The name of the file to store the population density data
    make_plot : bool
        Whether to make a plot of the population density data
    """

    # Get the lateral bounds of the region of interest.
    region_bounds = geospatial_utilities.get_region_bounds(
        region_shape
    )  # West, South, East, North

    # Select the population density data in the bounding box of the country of interest.
    population_density = population_density.sel(
        x=slice(region_bounds[0], region_bounds[2]),
        y=slice(region_bounds[1], region_bounds[3]),
    )

    # Coarsen the population density data to the same resolution as the weather data.
    population_density = coarsen_population_density(population_density, region_bounds)

    # Clean the dataset.
    population_density = population_density.squeeze("band")
    population_density = population_density.drop_vars(["band", "spatial_ref"])
    population_density = population_density.drop_attrs()
    population_density = population_density.rename("population_density")

    # Save the population density data.
    population_density.to_netcdf(file_name)

    if make_plot:
        figure_utilities.plot(
            population_density, f"population_density_{region_shape.index[0]}"
        )


def run_population_density_data_retrieval():
    """
    Run the population density data retrieval from the SEDAC.
    """

    # Create a directory to store the weather data.
    result_directory = "data__population_density"
    if not os.path.exists(result_directory):
        os.makedirs(result_directory)

    # Define the year of the population density data.
    year = 2015

    # Define the file name of the global population density data.
    global_population_file_name = (
        f"{result_directory}/population_density_30_sec_{year}.tif"
    )

    # Download the population density data.
    download_population_density_data_from_SEDAC(year, global_population_file_name)

    # Load the population density data.
    population_density = geospatial_utilities.load_xarray(
        global_population_file_name, engine="rasterio"
    )

    # Read the ISO Alpha-2 codes of the countries of interest.
    country_codes = general_utilities.read_countries_from_file(
        "settings/gegis__all_countries.txt"
    )

    for country_code in country_codes:
        # Define the file name of the regional population density data.
        file_name = (
            f"{result_directory}/population_density_0.25_deg_{country_code}_{year}.nc"
        )

        if not os.path.exists(file_name):
            # Get the shape of the country of interest.
            region_shape = geospatial_utilities.get_geopandas_region(country_code)

            # Extract the population density of the region.
            extract_population_density_of_region(
                population_density, region_shape, file_name
            )


if __name__ == "__main__":
    run_population_density_data_retrieval()
