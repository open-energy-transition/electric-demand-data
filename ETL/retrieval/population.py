import geopandas
import numpy
import util.figures
import util.shapes
import xarray


def get_url(year: int) -> str:
    """
    Get the URL of the population density data from SEDAC.

    Parameters
    ----------
    year : int
        The year of the population density data
    """

    # Check if the year is supported.
    assert year in [2000, 2005, 2010, 2015, 2020], (
        "Year not supported. Supported years are 2000, 2005, 2010, 2015, and 2020."
    )

    # Return the URL of the population density data.
    return f"https://data.ghg.center/sedac-popdensity-yeargrid5yr-v4.11/gpw_v4_population_density_rev11_{year}_30_sec_{year}.tif"


def _coarsen_population_density(
    population_density: xarray.DataArray, entity_bounds: list[float]
) -> xarray.DataArray:
    """
    Coarsen the population density data to the same resolution as the weather data. The population density resolution is 30 arc-seconds, while the resource data resolution is 900 arc-seconds (0.25 degrees).

    Parameters
    ----------
    population_density : xarray.DataArray
        The population density data
    entity_bounds : list of float
        The lateral bounds of the entity of interest (West, South, East, North)

    Returns
    -------
    population_density : xarray.DataArray
        The population density data coarsened to the same resolution as the weather data
    """

    # Define the new coarser resolution.
    x_list = numpy.linspace(-180, 180, int(360 / 0.25) + 1)
    y_list = numpy.linspace(-90, 90, int(180 / 0.25) + 1)

    # Define the bins where to aggregate the population density data of the finer resolution.
    # The next(...) function in this case calculates the first value that satisfies the specified condition.
    # The resulting bins are the first and last values of the x_list and y_list that are within the bounds of the entity of interest.
    x_bins = numpy.arange(
        x_list[next(x for x, val in enumerate(x_list) if val >= entity_bounds[0])]
        - 0.25 / 2,
        x_list[next(x for x, val in enumerate(x_list) if val >= entity_bounds[2]) + 1]
        + 0.25 / 2,
        0.25,
    )
    y_bins = numpy.arange(
        y_list[next(x for x, val in enumerate(y_list) if val >= entity_bounds[1])]
        - 0.25 / 2,
        y_list[next(x for x, val in enumerate(y_list) if val >= entity_bounds[3]) + 1]
        + 0.25 / 2,
        0.25,
    )

    # Aggregate the population density data to the new coarser resolution, first in the x direction and then in the y direction.
    population_density = population_density.groupby_bins("x", x_bins).sum()
    population_density = population_density.groupby_bins("y", y_bins).sum()

    # For each coordinate, substitute the bin range with the middle of the bin.
    population_density["x_bins"] = numpy.arange(
        x_list[next(x for x, val in enumerate(x_list) if val >= entity_bounds[0])],
        x_list[next(x for x, val in enumerate(x_list) if val >= entity_bounds[2]) + 1],
        0.25,
    )
    population_density["y_bins"] = numpy.arange(
        y_list[next(x for x, val in enumerate(y_list) if val >= entity_bounds[1])],
        y_list[next(x for x, val in enumerate(y_list) if val >= entity_bounds[3]) + 1],
        0.25,
    )

    # Rename the bins to "x" and "y".
    population_density = population_density.rename({"x_bins": "x", "y_bins": "y"})

    return population_density


def extract_population_density_of_entity(
    population_density: xarray.DataArray,
    entity_shape: geopandas.GeoDataFrame,
    file_path: str,
    make_plot: bool = False,
) -> None:
    """
    Extract the population density of a country or subdivision of interest, coarsen it to the same resolution as the weather data, and save it to a file.

    Parameters
    ----------
    population_density : xarray.DataArray
        The population density data.
    entity_shape : geopandas.GeoDataFrame
        The shape of the entity of interest
    file_path : str
        The path to store the population density data
    make_plot : bool
        Whether to make a plot of the population density data
    """

    # Get the lateral bounds of the country or subdivision of interest.
    entity_bounds = util.shapes.get_entity_bounds(
        entity_shape
    )  # West, South, East, North

    # Select the population density data in the bounding box of the country or subdivision of interest.
    population_density = population_density.sel(
        x=slice(entity_bounds[0], entity_bounds[2]),
        y=slice(entity_bounds[1], entity_bounds[3]),
    )

    # Coarsen the population density data to the same resolution as the weather data.
    population_density = _coarsen_population_density(population_density, entity_bounds)

    # Clean the dataset.
    population_density = population_density.squeeze("band")
    population_density = population_density.drop_vars(["band", "spatial_ref"])
    population_density = population_density.drop_attrs()
    population_density = population_density.rename("population_density")

    # Save the population density data.
    population_density.to_netcdf(file_path)

    if make_plot:
        util.figures.simple_plot(
            population_density, f"population_density_{entity_shape.index[0]}"
        )
