import os

import cartopy.io.shapereader
import geopandas
import pandas
import pycountry
import util.figures
from shapely.geometry import Polygon

non_standard_shapes = {
    "eia": [
        "US_CAL",
        "US_CAR",
        "US_CENT",
        "US_FLA",
        "US_MIDA",
        "US_MIDW",
        "US_NE",
        "US_NY",
        "US_NW",
        "US_SE",
        "US_SW",
        "US_TEN",
        "US_TEX",
    ],
    "tepco": ["JP_Kantō"],
}


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
) -> geopandas.GeoDataFrame | None:
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
    country_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the shape of the country
    """

    if "_" in code:
        # Split the code into the ISO Alpha-2 code and the region code.
        iso_alpha_2_code, region_code = code.split("_")

        # Define the relevant parameters for the shapefile retrieval.
        shapefile_name = "admin_1_states_provinces"
        main_key = "iso_3166_2"
        secondary_keys = "name"
        target_key = iso_alpha_2_code + "-" + region_code
    else:
        # Define the relevant parameters for the shapefile retrieval.
        shapefile_name = "admin_0_countries"
        main_key = "ISO_A2"
        secondary_keys = ["NAME", "NAME_LONG"]
        target_key = code

    # Load the shapefile containing the world countries.
    region_shapes = cartopy.io.shapereader.natural_earth(
        resolution="50m", category="cultural", name=shapefile_name
    )

    # Define a reader for the shapefile.
    reader = cartopy.io.shapereader.Reader(region_shapes)

    try:
        # Read the shape of the region of interest by searching for its code.
        region_shape = [
            ii for ii in list(reader.records()) if ii.attributes[main_key] == target_key
        ][0]
    except IndexError:
        # Read the shape of the region of interest by searching for its name.
        if "_" in code:
            name = pycountry.subdivisions.get(code=target_key).name
        else:
            name = pycountry.countries.get(alpha_2=target_key).name
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

    # Split the code into the ISO Alpha-2 code and the region code.
    iso_alpha_2_code, region_code = code.split("_")

    # Define the path to the shapefile based on the data source.
    shapefile_path = os.path.join(
        os.path.dirname(__file__), "..", "shapes", data_source, data_source + ".shp"
    )

    # Read the shapefile of the regions of the data source.
    region_shapes = geopandas.read_file(shapefile_path)

    # Get the shape of the region of interest.
    region_shape = region_shapes[region_shapes["code"] == region_code]
    region_shape = geopandas.GeoDataFrame.from_features(region_shape["geometry"])

    return region_shape


def get_geopandas_region(
    code: str, make_plot: bool = True, remove_remote_islands: bool = True
) -> geopandas.GeoDataFrame:
    """
    Get region shape, convert it to a geoDataFrame, and plot it.

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
        # Define a flag to check if the region code is in the list of non-standard shapes.
        is_non_standard_shape = False
        selected_data_source = None
        for data_source, codes in non_standard_shapes.items():
            if code in codes:
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
