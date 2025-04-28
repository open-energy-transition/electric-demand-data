import atlite
import geopandas
import numpy
import util.figures
import util.shapes
import xarray


def harmonize_coords(
    ds: xarray.DataArray | xarray.Dataset,
) -> xarray.DataArray | xarray.Dataset:
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
    entity_shape: geopandas.GeoDataFrame,
    resolution: float = 0.25,
    make_plot: bool = True,
) -> xarray.DataArray:
    """
    Calculate the fraction of each grid cell that is in the given shape.

    Parameters
    ----------
    entity_shape : geopandas.GeoDataFrame
        GeoDataFrame containing the country or subdivision of interest
    resolution : float
        The resolution of the grid cells in degrees
    make_plot : bool
        Whether to make a plot of the fraction of each grid cell that is in the given country or subdivision

    Returns
    -------
    fraction_of_grid_cells_in_shape : xarray.DataArray
        Fraction of each grid cell that is in the given shape
    """

    # Calculate the lateral bounds for the cutout based on the lateral bounds of the country or subdivision of interest.
    cutout_bounds = util.shapes.get_entity_bounds(entity_shape)

    # Create a temporary cutout to have the grid cell of the country or subdivision of interest.
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
        cutout.grid, entity_shape, orig_crs=4326, dest_crs=4326
    ).toarray()

    # Fix NaN and Inf values to 0.0 to avoid numerical issues.
    fraction_of_grid_cells_in_shape_np = numpy.nan_to_num(
        (
            fraction_of_grid_cells_in_shape_np.T
            / fraction_of_grid_cells_in_shape_np.sum(axis=1)
        ).T,
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )

    # Convert the numpy array to a xarray DataArray.
    fraction_of_grid_cells_in_shape = xarray.DataArray(
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
        util.figures.simple_plot(
            fraction_of_grid_cells_in_shape,
            f"fraction_of_grid_cells_in_shape_{entity_shape.index[0]}",
        )

    return fraction_of_grid_cells_in_shape


def load_xarray(
    file_path: str, engine: str = "netcdf4", dataarray_or_dataset: str = "dataarray"
) -> xarray.DataArray | xarray.Dataset:
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
        xarray_data = xarray.open_dataarray(file_path, engine=engine)
    elif dataarray_or_dataset == "dataset":
        xarray_data = xarray.open_dataset(file_path, engine=engine)

    # Harmonize the coordinates of the xarray dataset.
    xarray_data = harmonize_coords(xarray_data)

    return xarray_data
