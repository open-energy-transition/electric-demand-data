import logging

import atlite
import cartopy.io.shapereader as shpreader
import geopandas as gpd
import numpy as np
import pandas as pd
import pycountry
import util.figures as figure_utilities
import xarray as xr
from shapely.geometry import Polygon


def remove_islands(
    region_shape: gpd.GeoDataFrame, iso_alpha_2_code: str
) -> gpd.GeoDataFrame:
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
            new_bounds = gpd.GeoSeries(
                Polygon([(-80, -60), (-60, -60), (-60, 0), (-80, 0)])
            )
        case "ES":  # Spain
            new_bounds = gpd.GeoSeries(
                Polygon([(-10, 35), (5, 35), (5, 45), (-10, 45)])
            )
        case "FR":  # France
            new_bounds = gpd.GeoSeries(
                Polygon([(-5, 40), (10, 40), (10, 55), (-5, 55)])
            )
        case "NL":  # Netherlands
            new_bounds = gpd.GeoSeries(Polygon([(0, 50), (10, 50), (10, 55), (0, 55)]))
        case "NO":  # Norway
            new_bounds = gpd.GeoSeries(Polygon([(0, 55), (35, 55), (35, 73), (0, 73)]))
        case "NZ":  # New Zealand
            new_bounds = gpd.GeoSeries(
                Polygon([(165, -50), (180, -50), (180, -30), (165, -30)])
            )
        case "PT":  # Portugal
            new_bounds = gpd.GeoSeries(
                Polygon([(-10, 35), (0, 35), (0, 45), (-10, 45)])
            )

    if new_bounds is not None:
        # Convert the GeoSeries to a GeoDataFrame and set the coordinate reference system to EPSG 4326.
        new_bounds = gpd.GeoDataFrame.from_features(new_bounds, crs=4326)

        # Remove any region outside the new bounds.
        region_shape = region_shape.overlay(new_bounds, how="intersection")

    return region_shape


def get_geopandas_region(
    code: str, make_plot: bool = True, remove_remote_islands: bool = True
) -> gpd.GeoDataFrame:
    """
    Get region shape from cartopy, convert it to a geoDataFrame, and plot it.

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

    if "_" not in code:
        # The code is the ISO Alpha-2 code of the country.
        iso_alpha_2_code = code

        # Load the shapefile containing the world countries.
        region_shapes = shpreader.natural_earth(
            resolution="50m", category="cultural", name="admin_0_countries"
        )

        # Define a reader for the shapefile.
        reader = shpreader.Reader(region_shapes)

        try:
            try:
                # Read the shape of the region of interest by searching for the ISO Alpha-2 code.
                region_shape = [
                    ii
                    for ii in list(reader.records())
                    if ii.attributes["ISO_A2"] == iso_alpha_2_code
                ][0]
            except IndexError:
                # Read the shape of the region of interest by searching for the name of the country.
                country_name = pycountry.countries.get(alpha_2=iso_alpha_2_code).name
                region_shape = [
                    ii
                    for ii in list(reader.records())
                    if ii.attributes["NAME"] == country_name
                ][0]

            # Convert the shape to a GeoDataFrame.
            region_shape = pd.Series({"geometry": region_shape.geometry})
            region_shape = gpd.GeoSeries(region_shape)
            region_shape = gpd.GeoDataFrame.from_features(region_shape, crs=4326)

            # Remove small remote islands from the shape of some countries.
            if remove_remote_islands:
                region_shape = remove_islands(region_shape, iso_alpha_2_code)

            # Add the ISO Alpha-2 code as the name of the region.
            region_shape["ISO_A2_code"] = iso_alpha_2_code
            region_shape = region_shape.set_index("ISO_A2_code")

            if make_plot:
                figure_utilities.simple_plot(
                    region_shape, f"region_shape_{iso_alpha_2_code}"
                )

        except (AttributeError, IndexError):
            logging.critical(f"Region shape for {iso_alpha_2_code} not found.")
            region_shape = None

    else:
        # Split the country code into the ISO Alpha-2 code and the region code.
        iso_alpha_2_code, region_code = code.split("_")

        if iso_alpha_2_code == "US":
            # Load the shapefile of the US regions.
            region_shapes = gpd.read_file("data/us_balancing_authorities/regions.shp")
            region_shape = region_shapes[region_shapes["EIAcode"] == region_code]
            region_shape = gpd.GeoDataFrame.from_features(region_shape["geometry"])

            if region_shape.empty:
                logging.critical(
                    f"Region shape for {iso_alpha_2_code}_{region_code} not found."
                )
                region_shape = None
            else:
                # Add the full region code.
                region_shape["region_code"] = iso_alpha_2_code + "_" + region_code
                region_shape = region_shape.set_index("region_code")

                if make_plot:
                    figure_utilities.simple_plot(
                        region_shape, f"region_shape_{iso_alpha_2_code}_{region_code}"
                    )

    return region_shape


def get_region_bounds(region_shape: gpd.GeoDataFrame) -> list[float]:
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


def harmonize_coords(ds: xr.DataArray | xr.Dataset) -> xr.DataArray | xr.Dataset:
    """
    Rename coordinates to "x" and "y" and reset longitudes from the range [0, 360] to [-180, 180].

    Parameters
    ----------
    ds : xarray.Dataset or xarray.DataArray
        Dataset or DataArray to be renamed

    Returns
    -------
    ds : xarray.Dataset or xarray.DataArray
        Dataset or DataArray with renamed coordinates and reset longitudes
    """

    # Rename longitude and latitude coordinates to x and y coordinates.
    if "longitude" in ds.coords and "latitude" in ds.coords:
        ds = ds.rename({"longitude": "x", "latitude": "y"})
    elif "lon" in ds.coords and "lat" in ds.coords:
        ds = ds.rename({"lon": "x", "lat": "y"})

    # Reset longitudes from the range [0, 360] to [-180, 180].
    if ds["x"].min() == 0:
        ds = ds.assign_coords(x=(ds["x"] + 179.99999) % 360 - 179.99999).sortby("x")

    # Sort the dataset by the y coordinate.
    ds = ds.sortby("y")

    return ds


def get_fraction_of_grid_cells_in_shape(
    region_shape: gpd.GeoDataFrame, resolution: float = 0.25, make_plot: bool = True
) -> xr.DataArray:
    """
    Calculate the fraction of each grid cell that is in the given shape.

    Parameters
    ----------
    region_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the region of interest
    resolution : float
        The resolution of the grid cells in degrees
    make_plot : bool
        Whether to make a plot of the fraction of each grid cell that is in the given region

    Returns
    -------
    fraction_of_grid_cells_in_shape : xarray.DataArray
        Fraction of each grid cell that is in the given shape
    """

    # Calculate the lateral bounds for the cutout based on the lateral bounds of the region of interest.
    cutout_bounds = get_region_bounds(region_shape)

    # Create a temporary cutout to have the grid cell of the region of interest.
    cutout = atlite.Cutout(
        "temporary_cutout",
        module="era5",
        bounds=cutout_bounds,
        dx=resolution,
        dy=resolution,
        time=slice("2013-01-01", "2013-01-02"),
    )

    # Calculate the fraction of each grid cell that is in the given shape.
    fraction_of_grid_cells_in_shape_np = atlite.gis.compute_indicatormatrix(
        cutout.grid, region_shape, orig_crs=4326, dest_crs=4326
    ).toarray()

    # Fix NaN and Inf values to 0.0 to avoid numerical issues.
    fraction_of_grid_cells_in_shape_np = np.nan_to_num(
        (
            fraction_of_grid_cells_in_shape_np.T
            / fraction_of_grid_cells_in_shape_np.sum(axis=1)
        ).T,
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )

    # Convert the numpy array to a xarray DataArray.
    fraction_of_grid_cells_in_shape = xr.DataArray(
        fraction_of_grid_cells_in_shape_np.reshape(
            len(cutout.data["y"]), len(cutout.data["x"])
        ),
        coords={"y": cutout.data["y"], "x": cutout.data["x"]},
    )

    # Clip the values between 0 and 1.
    fraction_of_grid_cells_in_shape = (
        fraction_of_grid_cells_in_shape / fraction_of_grid_cells_in_shape.max()
    )

    if make_plot:
        figure_utilities.simple_plot(
            fraction_of_grid_cells_in_shape,
            f"fraction_of_grid_cells_in_shape_{region_shape.index[0]}",
        )

    return fraction_of_grid_cells_in_shape


def load_xarray(
    file_path: str, engine: str = "netcdf4", dataarray_or_dataset: str = "dataarray"
) -> xr.DataArray | xr.Dataset:
    """
    Load an xarray dataset from a file.

    Parameters
    ----------
    file_path : str
        The path to the file to load
    engine : str
        The engine to use to load the xarray dataset
    dataarray_or_dataset : str
        Whether to load a dataarray or a dataset

    Returns
    -------
    xarray_data : xarray.DataArray or xarray.Dataset
        The loaded dataset
    """

    # Load the xarray.
    if dataarray_or_dataset == "dataarray":
        xarray_data = xr.open_dataarray(file_path, engine=engine)
    elif dataarray_or_dataset == "dataset":
        xarray_data = xr.open_dataset(file_path, engine=engine)

    # Harmonize the coordinates of the xarray dataset.
    xarray_data = harmonize_coords(xarray_data)

    return xarray_data
