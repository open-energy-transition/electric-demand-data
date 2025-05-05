import atlite.gis
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


def load_xarray(
    file_paths: str | list[str],
    engine: str = "netcdf4",
    dataarray_or_dataset: str = "dataarray",
) -> xarray.DataArray | xarray.Dataset:
    """
    Load an xarray dataset from a file.

    Parameters
    ----------
    file_paths : str or list of str
        The path to the file or list of files to load
    engine : str
        The engine to use to load the xarray dataset
    dataarray_or_dataset : str
        Whether to load a dataarray or a dataset

    Returns
    -------
    xarray_data : xarray.DataArray or xarray.Dataset
        The loaded dataset
    """

    # Check if the file_paths is a string or a list of strings.
    if isinstance(file_paths, str):
        file_paths = [file_paths]

    # Initialize an empty list to store the xarray data.
    xarray_data = []

    # Loop through the file paths and load the xarray data.
    for file_path in file_paths:
        # Load the xarray data.
        if dataarray_or_dataset == "dataarray":
            xarray_to_append = xarray.open_dataarray(file_path, engine=engine)
        elif dataarray_or_dataset == "dataset":
            xarray_to_append = xarray.open_dataset(file_path, engine=engine)
        else:
            raise ValueError(
                "dataarray_or_dataset must be either 'dataarray' or 'dataset'."
            )
        # Harmonize the coordinates of the xarray data.
        xarray_to_append = harmonize_coords(xarray_to_append)

        # Append the xarray data to the list.
        xarray_data.append(xarray_to_append)

    if len(xarray_data) > 1:
        # Concatenate the xarray data along the time dimension.
        xarray_data = xarray.concat(xarray_data, dim="time")
    else:
        # If there is only one xarray data, return it.
        xarray_data = xarray_data[0]

    return xarray_data


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


def coarsen(
    original_xarray: xarray.DataArray,
    bounds: list[float],
    target_resolution: float = 0.25,
) -> xarray.DataArray:
    """
    Coarsen the xarray data to the target resolution.

    Parameters
    ----------
    original_xarray : xarray.DataArray
        The xarray data to coarsen
    bounds : list of float
        The lateral bounds (West, South, East, North) used to clip the data
    target_resolution : float
        The target resolution of the coarsened data

    Returns
    -------
    coarsened_xarray : xarray.DataArray
        The coarsened xarray data
    """

    # Get the original resolution of the xarray data.
    original_x_resolution = abs(original_xarray.x[1] - original_xarray.x[0]).item()
    original_y_resolution = abs(original_xarray.y[1] - original_xarray.y[0]).item()
    original_resolution = max(original_x_resolution, original_y_resolution)

    # Check if the target resolution is greater than the original resolution.
    assert target_resolution > original_resolution, (
        "Target resolution must be greater than the original resolution."
    )

    # Check if the target resolution provides an integer number of bins.
    assert 360 % target_resolution == 0, (
        "Target resolution must result in an integer number when dividing 360."
    )

    # Define the new coarser resolution.
    x_list = numpy.linspace(-180, 180, int(360 / target_resolution) + 1)
    y_list = numpy.linspace(-90, 90, int(180 / target_resolution) + 1)

    # Define the bins where to aggregate the original data.
    # The next(...) function in this case calculates the first value that satisfies the specified condition.
    # The resulting bins are the first and last values of the x_list and y_list that are within the bounds.
    x_bins = numpy.arange(
        x_list[next(x for x, val in enumerate(x_list) if val >= bounds[0])] - 0.25 / 2,
        x_list[next(x for x, val in enumerate(x_list) if val >= bounds[2]) + 1]
        + 0.25 / 2,
        0.25,
    )
    y_bins = numpy.arange(
        y_list[next(x for x, val in enumerate(y_list) if val >= bounds[1])] - 0.25 / 2,
        y_list[next(x for x, val in enumerate(y_list) if val >= bounds[3]) + 1]
        + 0.25 / 2,
        0.25,
    )

    # Aggregate the original data to the new coarser resolution, first in the x direction and then in the y direction.
    coarsened_xarray = original_xarray.groupby_bins("x", x_bins).sum()
    coarsened_xarray = coarsened_xarray.groupby_bins("y", y_bins).sum()

    # For each coordinate, substitute the bin range with the middle of the bin.
    coarsened_xarray["x_bins"] = numpy.arange(
        x_list[next(x for x, val in enumerate(x_list) if val >= bounds[0])],
        x_list[next(x for x, val in enumerate(x_list) if val >= bounds[2]) + 1],
        0.25,
    )
    coarsened_xarray["y_bins"] = numpy.arange(
        y_list[next(x for x, val in enumerate(y_list) if val >= bounds[1])],
        y_list[next(x for x, val in enumerate(y_list) if val >= bounds[3]) + 1],
        0.25,
    )

    # Rename the bins to "x" and "y".
    coarsened_xarray = coarsened_xarray.rename({"x_bins": "x", "y_bins": "y"})

    return coarsened_xarray
