import logging
import os

import util.general_utilities as general_utilities
import util.geospatial_utilities as geospatial_utilities
import util.population_utilities as population_utilities


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
    population_utilities.download_population_density_data_from_SEDAC(
        year, global_population_file_path
    )

    # Load the population density data.
    population_density = geospatial_utilities.load_xarray(
        global_population_file_path, engine="rasterio"
    )

    # Read the codes of the regions of interest.
    settings_directory = general_utilities.read_folders_structure()["settings_folder"]
    # region_codes = general_utilities.read_countries_from_file(settings_directory+"/gegis__all_countries.txt")
    region_codes = general_utilities.read_us_regions_from_file(
        settings_directory + "/us_eia_regions.txt"
    )

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
            population_utilities.extract_population_density_of_region(
                population_density, region_shape, regional_population_file_path
            )


if __name__ == "__main__":
    run_population_density_data_retrieval()
