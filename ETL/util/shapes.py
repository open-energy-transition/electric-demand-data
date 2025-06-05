# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This module retrieves standard shapes of countries and subdivisions from the Natural Earth shapefile database.

    It also retrieves non-standard subdivision shapes from the shapes directory.
"""

import os

import cartopy.io.shapereader
import geopandas
import pandas
import pycountry
from shapely.geometry import Polygon

import util.directories
import util.figures


def _remove_islands(
    entity_shape: geopandas.GeoDataFrame, code: str
) -> geopandas.GeoDataFrame:
    """
    Remove small remote islands from the shape of some countries.

    Parameters
    ----------
    entity_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the country or subdivision of interest.
    code : str
        The code of the country or subdivision.

    Returns
    -------
    entity_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the country or subdivision of interest without small remote islands.
    """
    new_bounds = None

    # Create a GeoSeries containing the new bounds of the country or subdivision of interest.
    match code:
        case "CL":  # Chile
            new_bounds = geopandas.GeoSeries(
                Polygon([(-80, -60), (-60, -60), (-60, 0), (-80, 0)])
            )
        case "ES":  # Spain
            new_bounds = geopandas.GeoSeries(
                Polygon([(-10, 35), (5, 35), (5, 45), (-10, 45)])
            )
        case "FR":  # France
            new_bounds = geopandas.GeoSeries(
                Polygon([(-5, 40), (10, 40), (10, 55), (-5, 55)])
            )
        case "NL":  # Netherlands
            new_bounds = geopandas.GeoSeries(
                Polygon([(0, 50), (10, 50), (10, 55), (0, 55)])
            )
        case "NO":  # Norway
            new_bounds = geopandas.GeoSeries(
                Polygon([(0, 55), (35, 55), (35, 73), (0, 73)])
            )
        case "NZ":  # New Zealand
            new_bounds = geopandas.GeoSeries(
                Polygon([(165, -50), (180, -50), (180, -30), (165, -30)])
            )
        case "PT":  # Portugal
            new_bounds = geopandas.GeoSeries(
                Polygon([(-10, 35), (0, 35), (0, 45), (-10, 45)])
            )

    if new_bounds is not None:
        # Convert the GeoSeries to a GeoDataFrame and set the coordinate reference system to EPSG 4326.
        new_bounds = geopandas.GeoDataFrame.from_features(new_bounds, crs=4326)

        # Remove any area outside the new bounds.
        entity_shape = entity_shape.overlay(new_bounds, how="intersection")

    return entity_shape


def _get_standard_shape(
    code: str, remove_remote_islands: bool = True
) -> geopandas.GeoDataFrame:
    """
    Retrieve the shape of a country or subdivision from the Natural Earth shapefile database.

    Parameters
    ----------
    code : str
        The code of the entity (ISO Alpha-2 code or a combination of ISO Alpha-2 code and subdivision code).
    remove_remote_islands : bool
        Whether to remove small remote islands from the shape of some countries.

    Returns
    -------
    entity_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the shape of the country or subdivision.
    """
    # If there isn't an underscore in the code, it is the ISO Alpha-2 code of the country, and the entity is therefore the country.
    # If there is an underscore in the code, it is a combination of ISO Alpha-2 code and subdivision code, and the entity is a subdivision of the country.
    if "_" not in code:
        # Define the relevant parameters for the shapefile retrieval.
        shapefile_name = "admin_0_countries"
        main_keys = ["ISO_A2", "ISO_A2_EH"]
        secondary_keys = ["NAME", "NAME_LONG"]
        target_key = code
    else:
        # Split the code into the ISO Alpha-2 code and the subdivision code.
        iso_alpha_2_code, subdivision_code = code.split("_")

        # Define the relevant parameters for the shapefile retrieval.
        shapefile_name = "admin_1_states_provinces"
        main_keys = ["iso_3166_2"]
        secondary_keys = ["name"]
        target_key = iso_alpha_2_code + "-" + subdivision_code

    # Load the shapefile containing the subdivision shapes from the Natural Earth database.
    all_shapes = cartopy.io.shapereader.natural_earth(
        resolution="50m", category="cultural", name=shapefile_name
    )

    # Define a reader for the shapefile.
    reader = cartopy.io.shapereader.Reader(all_shapes)

    try:
        # Read the shape of the country or subdivision of interest by searching for its code.
        entity_shape = [
            shape
            for shape in list(reader.records())
            if target_key in [shape.attributes[key] for key in main_keys]
        ][0]
    except IndexError:
        # Get the name of the country or subdivision of interest based on its code.
        if "_" not in code:
            name = pycountry.countries.get(alpha_2=target_key).name
        else:
            name = pycountry.subdivisions.get(code=target_key).name

        # Read the shape of the country or subdivision of interest by searching for its name.
        entity_shape = [
            shape
            for shape in list(reader.records())
            if name in [shape.attributes[key] for key in secondary_keys]
        ][0]

    # Convert the shape to a GeoDataFrame.
    entity_shape = pandas.Series({"geometry": entity_shape.geometry})
    entity_shape = geopandas.GeoSeries(entity_shape)
    entity_shape = geopandas.GeoDataFrame.from_features(entity_shape, crs=4326)

    # Remove small remote islands from the shape of some countries or subdivisions.
    if remove_remote_islands:
        entity_shape = _remove_islands(entity_shape, code)

    return entity_shape


def _read_non_standard_shape_codes() -> dict[str, list[str]]:
    """
    Read the non-standard shapes contained in the shapes directory.

    Returns
    -------
    non_standard_shape_codes : dict[str, list[str]]
        Dictionary containing the non-standard shapes and their respective codes.
    """
    # Get the path to the shapes directory.
    shapes_directory = util.directories.read_folders_structure()["shapes_folder"]

    # Create a dictionary to store the non-standard shapes and their respective codes.
    non_standard_shape_codes = {}

    # Iterate over the folders in the shapes directory.
    for folder in os.listdir(shapes_directory):
        # Check if folder is a directory.
        if os.path.isdir(os.path.join(shapes_directory, folder)):
            # Define the path to the shapefile.
            shapefile_path = os.path.join(shapes_directory, folder, folder + ".shp")

            # Read the shapefile of the subdivisions of the data source.
            entity_shapes = geopandas.read_file(shapefile_path)

            # Get the codes of the subdivisions in the shapefile.
            entity_codes = entity_shapes["code"].unique()

            # Add the non-standard shapes and their respective codes to the dictionary.
            non_standard_shape_codes[folder] = list(entity_codes)

    return non_standard_shape_codes


def _get_non_standard_shape(code: str, data_source: str) -> geopandas.GeoDataFrame:
    """
    Retrieve the shape of a non-standard subdivision as defined by the data source.

    Parameters
    ----------
    code : str
        The combination of ISO Alpha-2 code and subdivision code.
    data_source : str
        The data source of the subdivision shape.

    Returns
    -------
    entity_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the shape of the subdivision.
    """
    # Get the path to the shapes directory.
    shapes_directory = util.directories.read_folders_structure()["shapes_folder"]

    # Define the path to the shapefile based on the data source.
    shapefile_path = os.path.join(shapes_directory, data_source, data_source + ".shp")

    # Read the shapefile of the subdivisions of the data source.
    entity_shapes = geopandas.read_file(shapefile_path)

    # Get the shape of the subdivision of interest.
    entity_shape = entity_shapes[entity_shapes["code"] == code]
    entity_shape = geopandas.GeoDataFrame.from_features(entity_shape["geometry"])

    return entity_shape


def get_entity_shape(
    code: str, make_plot: bool = True, remove_remote_islands: bool = True
) -> geopandas.GeoDataFrame:
    """
    Get the shape of a country or subdivision of interest.

    Parameters
    ----------
    code : str
        The code of the entity (ISO Alpha-2 code or a combination of ISO Alpha-2 code and subdivision code).
    make_plot : bool
        Whether to make a plot of the entity of interest.
    remove_remote_islands : bool
        Whether to remove small remote islands from the shape of some countries.

    Returns
    -------
    entity_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the country or subdivision of interest.
    """
    # If there isn't an underscore in the code, it is the ISO Alpha-2 code of the country.
    # If there is an underscore in the code, it is a combination of ISO Alpha-2 code and subdivision code.
    if "_" not in code:
        # Get the shape of the country based on the ISO Alpha-2 code.
        entity_shape = _get_standard_shape(code, remove_remote_islands)
    else:
        # Define a flag to check if the subdivision is in the list of non-standard shapes.
        is_non_standard_shape = False
        selected_data_source = ""

        # Read the codes of the non-standard shapes contained in the shapes directory.
        non_standard_shape_codes = _read_non_standard_shape_codes()

        # Iterate over the codes of the non-standard shapes and check if the subdivision code is in the list of non-standard shapes.
        for data_source, codes_of_data_source in non_standard_shape_codes.items():
            if code in codes_of_data_source:
                is_non_standard_shape = True
                selected_data_source = data_source
                break

        if is_non_standard_shape:
            # Get the shape of the subdivision based on its code and respective data source.
            entity_shape = _get_non_standard_shape(code, selected_data_source)
        else:
            # Get the shape of the subdivision based on the ISO Alpha-2 code and the subdivision code.
            entity_shape = _get_standard_shape(code, remove_remote_islands)

    # Add the code as index to the GeoDataFrame.
    entity_shape["code"] = code
    entity_shape = entity_shape.set_index("code")

    if make_plot:
        util.figures.simple_plot(entity_shape, f"entity_shape_{code}")

    return entity_shape


def get_entity_bounds(entity_shape: geopandas.GeoDataFrame) -> list[float]:
    """
    Get the lateral bounds of the country or subdivision of interest including a buffer layer of one degree.

    Parameters
    ----------
    entity_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the country or subdivision of interest.

    Returns
    -------
    entity_bounds : list[float]
        List containing the lateral bounds of the country or subdivision of interest.
    """
    # Get the lateral bounds of the country or subdivision of interest including a buffer layer of one degree.
    entity_bounds = (
        entity_shape.union_all().buffer(1).bounds
    )  # West, South, East, North

    # Round the bounds to the closest 0.25 degree.
    entity_bounds = [round(x * 4) / 4 for x in entity_bounds]

    return entity_bounds
