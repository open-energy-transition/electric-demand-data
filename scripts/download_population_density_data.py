import logging
import os
from urllib.request import urlretrieve

import geopandas as gpd
import numpy as np
import util.figures as figure_utilities
import util.general as general_utilities
import util.geospatial as geospatial_utilities
import xarray as xr


def download_population_density_data_from_SEDAC(year: int, file_path: str) -> None:
    """
    Download the population density data from the Socioeconomic Data and Applications Center (SEDAC).

    Parameters
    ----------
    year : int
        The year of the population density data
    file_path : str
        The path to store the population density data
    """

    if not os.path.exists(file_path):
        logging.info(
            f"Downloading population density data from SEDAC for the year {year}..."
        )

        # Download the population density data.
        url = f"https://data.ghg.center/sedac-popdensity-yeargrid5yr-v4.11/gpw_v4_population_density_rev11_{year}_30_sec_{year}.tif"
        urlretrieve(url, file_path)


def coarsen_population_density(
    population_density: xr.DataArray, region_bounds: list[float]
) -> xr.DataArray:
    """
    Coarsen the population density data to the same resolution as the weather data. The population density resolution is 30 arc-seconds, while the resource data resolution is 900 arc-seconds (0.25 degrees).

    Parameters
    ----------
    population_density : xarray.DataArray
        The population density data
    region_bounds : list of float
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

    # Rename the bins to "x" and "y".
    population_density = population_density.rename({"x_bins": "x", "y_bins": "y"})

    return population_density


def extract_population_density_of_region(
    population_density: xr.DataArray,
    region_shape: gpd.GeoDataFrame,
    file_path: str,
    make_plot: bool = True,
) -> None:
    """
    Extract the population density of a region of interest, coarsen it to the same resolution as the weather data, and save it to a file.

    Parameters
    ----------
    population_density : xarray.DataArray
        The population density data.
    region_shape : geopandas.GeoDataFrame
        The shape of the region of interest
    file_path : str
        The path to store the population density data
    make_plot : bool
        Whether to make a plot of the population density data
    """

    # Get the lateral bounds of the region of interest.
    region_bounds = geospatial_utilities.get_region_bounds(
        region_shape
    )  # West, South, East, North

    # Select the population density data in the bounding box of the region of interest.
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
    population_density.to_netcdf(file_path)

    if make_plot:
        figure_utilities.simple_plot(
            population_density, f"population_density_{region_shape.index[0]}"
        )


def run_population_density_data_retrieval() -> None:
    """
    Run the population density data retrieval from the SEDAC.
    """

    # Set up the logging configuration.
    log_files_directory = general_utilities.read_folders_structure()["log_files_folder"]
    os.makedirs(log_files_directory, exist_ok=True)
    log_file_name = "population_density_data.log"
    logging.basicConfig(
        filename=log_files_directory + "/" + log_file_name,
        level=logging.INFO,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Create a directory to store the population density data.
    result_directory = general_utilities.read_folders_structure()[
        "population_density_folder"
    ]
    os.makedirs(result_directory, exist_ok=True)

    # Define the year of the population density data.
    year = 2015

    # Define the file path of the global population density data.
    global_population_file_path = (
        result_directory + f"/population_density_30_sec_{year}.tif"
    )

    # Download the population density data.
    download_population_density_data_from_SEDAC(year, global_population_file_path)

    # Load the population density data.
    population_density = geospatial_utilities.load_xarray(
        global_population_file_path, engine="rasterio"
    )

    # Read the codes of the regions of interest.
    settings_directory = general_utilities.read_folders_structure()["settings_folder"]
    region_codes = general_utilities.read_codes_from_file(
        settings_directory + "/gegis__all_countries.yaml"
    )
    # region_codes = general_utilities.read_codes_from_file(settings_directory + "/us_eia_regions.yaml")

    # Loop over the regions of interest.
    for region_code in region_codes:
        # Define the file path of the regional population density data.
        regional_population_file_path = (
            result_directory + f"/population_density_0.25_deg_{region_code}_{year}.nc"
        )

        if not os.path.exists(regional_population_file_path):
            logging.info(f"Extracting population density of {region_code}...")

            # Get the shape of the region of interest.
            region_shape = geospatial_utilities.get_geopandas_region(region_code)

            # Extract the population density of the region.
            extract_population_density_of_region(
                population_density, region_shape, regional_population_file_path
            )


if __name__ == "__main__":
    run_population_density_data_retrieval()
