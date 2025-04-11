#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: AGPL-3.0

Description:

    This script retrieves standard shapes of countries and regions from the Natural Earth shapefile database.

    It also retrieves non-standard shapes of regions from the shapes directory.
"""

import os

import cartopy.io.shapereader
import geopandas
import pandas
import pycountry
import util.figures
from shapely.geometry import Polygon


def _remove_islands(
    region_shape: geopandas.GeoDataFrame, iso_alpha_2_code: str
) -> geopandas.GeoDataFrame:
    """
    Remove small remote islands from the shape of some countries.

    Parameters
    ----------
    region_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the region of interest
    iso_alpha_2_code : str
        The ISO Alpha-2 code of the country of interest

    Returns
    -------
    region_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the region of interest without small remote islands
    """

    new_bounds = None

    # Create a GeoSeries containing the new bounds of the region of interest.
    match iso_alpha_2_code:
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

        # Remove any region outside the new bounds.
        region_shape = region_shape.overlay(new_bounds, how="intersection")

    return region_shape


def _get_standard_shape(
    code: str, remove_remote_islands: bool = True
) -> geopandas.GeoDataFrame:
    """
    Retrieve the shape of a country or region from the Natural Earth shapefile database.

    Parameters
    ----------
    code : str
        The code of the region (ISO Alpha-2 code or a combination of ISO Alpha-2 code and region code)
    remove_remote_islands : bool
        Whether to remove small remote islands from the shape of some countries

    Returns
    -------
    region_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the shape of the country or region
    """

    # If there isn't an underscore in the code, it is the ISO Alpha-2 code of the country, and the region is therefore the country.
    # If there is an underscore in the code, it is a combination of ISO Alpha-2 code and region code, and the region is a subdivision of the country.
    if "_" not in code:
        # Define the relevant parameters for the shapefile retrieval.
        shapefile_name = "admin_0_countries"
        main_keys = ["ISO_A2", "ISO_A2_EH"]
        secondary_keys = ["NAME", "NAME_LONG"]
        target_key = code
    else:
        # Split the code into the ISO Alpha-2 code and the region code.
        iso_alpha_2_code, region_code = code.split("_")

        # Define the relevant parameters for the shapefile retrieval.
        shapefile_name = "admin_1_states_provinces"
        main_keys = ["iso_3166_2"]
        secondary_keys = ["name"]
        target_key = iso_alpha_2_code + "-" + region_code

    # Load the shapefile containing the shapes of the regions from the Natural Earth database.
    region_shapes = cartopy.io.shapereader.natural_earth(
        resolution="50m", category="cultural", name=shapefile_name
    )

    # Define a reader for the shapefile.
    reader = cartopy.io.shapereader.Reader(region_shapes)

    try:
        # Read the shape of the region of interest by searching for its code.
        region_shape = [
            ii
            for ii in list(reader.records())
            if target_key in [ii.attributes[key] for key in main_keys]
        ][0]
    except IndexError:
        # Get the name of the region of interest based on its code.
        if "_" not in code:
            name = pycountry.countries.get(alpha_2=target_key).name
        else:
            name = pycountry.subdivisions.get(code=target_key).name

        # Read the shape of the region of interest by searching for its name.
        region_shape = [
            ii
            for ii in list(reader.records())
            if name in [ii.attributes[key] for key in secondary_keys]
        ][0]

    # Convert the shape to a GeoDataFrame.
    region_shape = pandas.Series({"geometry": region_shape.geometry})
    region_shape = geopandas.GeoSeries(region_shape)
    region_shape = geopandas.GeoDataFrame.from_features(region_shape, crs=4326)

    # Remove small remote islands from the shape of some countries.
    if remove_remote_islands:
        region_shape = _remove_islands(region_shape, code)

    return region_shape


def _read_non_standard_shape_codes() -> dict[str, list[str]]:
    """
    Read the non-standard shapes contained in the shapes directory.

    Returns
    -------
    non_standard_shape_codes : dict
        Dictionary containing the non-standard shapes and their respective codes
    """

    # Define the path to the shapes directory.
    shapes_path = os.path.join(os.path.dirname(__file__), "..", "shapes")

    # Create a dictionary to store the non-standard shapes and their respective codes.
    non_standard_shape_codes = {}

    # Iterate over the folders in the shapes directory.
    for folder in os.listdir(shapes_path):
        # Check if folder is a directory.
        if os.path.isdir(os.path.join(shapes_path, folder)):
            # Define the path to the shapefile.
            shapefile_path = os.path.join(shapes_path, folder, folder + ".shp")

            # Read the shapefile of the regions of the data source.
            region_shapes = geopandas.read_file(shapefile_path)

            # Get the codes of the regions in the shapefile.
            region_codes = region_shapes["code"].unique()

            # Add the non-standard shapes and their respective codes to the dictionary.
            non_standard_shape_codes[folder] = list(region_codes)

    return non_standard_shape_codes


def _get_non_standard_shape(code: str, data_source: str) -> geopandas.GeoDataFrame:
    """
    Retrieve the shape of a non-standard region as defined by the data source.

    Parameters
    ----------
    code : str
        The combination of ISO Alpha-2 code and region code
    data_source : str
        The data source of the region shape

    Returns
    -------
    region_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the shape of the region
    """

    # Define the path to the shapefile based on the data source.
    shapefile_path = os.path.join(
        os.path.dirname(__file__), "..", "shapes", data_source, data_source + ".shp"
    )

    # Read the shapefile of the regions of the data source.
    region_shapes = geopandas.read_file(shapefile_path)

    # Get the shape of the region of interest.
    region_shape = region_shapes[region_shapes["code"] == code]
    region_shape = geopandas.GeoDataFrame.from_features(region_shape["geometry"])

    return region_shape


def get_region_shape(
    code: str, make_plot: bool = True, remove_remote_islands: bool = True
) -> geopandas.GeoDataFrame:
    """
    Get the region shape of a country or region of interest.

    Parameters
    ----------
    code : str
        The code of the region (ISO Alpha-2 code or a combination of ISO Alpha-2 code and region code)
    make_plot : bool
        Whether to make a plot of the region of interest
    remove_remote_islands : bool
        Whether to remove small remote islands from the shape of some countries

    Returns
    -------
    region_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the region of interest
    """

    # If there isn't an underscore in the code, it is the ISO Alpha-2 code of the country.
    # If there is an underscore in the code, it is a combination of ISO Alpha-2 code and region code.
    if "_" not in code:
        # Get the shape of the country based on the ISO Alpha-2 code.
        region_shape = _get_standard_shape(code, remove_remote_islands)
    else:
        # Define a flag to check if the region is in the list of non-standard shapes.
        is_non_standard_shape = False
        selected_data_source = ""

        # Read the codes of the non-standard shapes contained in the shapes directory.
        non_standard_shape_codes = _read_non_standard_shape_codes()

        # Iterate over the codes of the non-standard shapes and check if the region code is in the list of non-standard shapes.
        for data_source, codes_of_data_source in non_standard_shape_codes.items():
            if code in codes_of_data_source:
                is_non_standard_shape = True
                selected_data_source = data_source
                break

        if is_non_standard_shape:
            # Get the shape of the region based on the region code and the respective data source.
            region_shape = _get_non_standard_shape(code, selected_data_source)
        else:
            # Get the shape of the region based on the ISO Alpha-2 code and the region code.
            region_shape = _get_standard_shape(code, remove_remote_islands)

    # Add the code as index to the GeoDataFrame.
    region_shape["code"] = code
    region_shape = region_shape.set_index("code")

    if make_plot:
        util.figures.simple_plot(region_shape, f"region_shape_{code}")

    return region_shape


def get_region_bounds(region_shape: geopandas.GeoDataFrame) -> list[float]:
    """
    Get the lateral bounds of the region of interest including a buffer layer of one degree.

    Parameters
    ----------
    region_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the region of interest

    Returns
    -------
    region_bounds : list of float
        List containing the lateral bounds of the region of interest
    """

    # Get the lateral bounds of the region of interest including a buffer layer of one degree.
    region_bounds = (
        region_shape.union_all().buffer(1).bounds
    )  # West, South, East, North

    # Round the bounds to the closest 0.25 degree.
    region_bounds = [round(x * 4) / 4 for x in region_bounds]

    return region_bounds
