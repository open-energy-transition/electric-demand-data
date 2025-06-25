# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This file contains unit tests for the geospatial module in the ETL
    utility package.
"""

import numpy
import pytest
import utils.geospatial
import xarray


def test_harmonize_coords():
    """
    Test the harmonization of geospatial coordinates.

    This function tests the harmonize_coords function to ensure it
    correctly transforms coordinate names and values in an xarray
    Dataset..
    """
    # Test the renaming of longitude and latitude to x and y.
    ds = xarray.Dataset(coords={"longitude": [0, 20], "latitude": [50, 60]})
    result = utils.geospatial.harmonize_coords(ds)
    assert "x" in result.coords and "y" in result.coords

    # Test the renaming of lon and lat to x and y.
    ds = xarray.Dataset(coords={"lon": [0, 20], "lat": [50, 60]})
    result = utils.geospatial.harmonize_coords(ds)
    assert "x" in result.coords and "y" in result.coords

    # Test the remapping of longitudes from [0, 360] to [-180, 180].
    ds = xarray.Dataset(coords={"x": [0, 90, 270], "y": [50, 60]})
    result = utils.geospatial.harmonize_coords(ds)
    assert numpy.all(result["x"].values >= -180)
    assert numpy.all(result["x"].values <= 180)

    # Test the dropping of duplicate coordinates.
    ds = xarray.Dataset(coords={"x": [0, 0, 10], "y": [50, 60]})
    n_duplicates = len(ds["x"].values) - len(numpy.unique(ds["x"].values))
    result = utils.geospatial.harmonize_coords(ds)
    assert len(result["x"].values) == len(ds["x"].values) - n_duplicates

    # Test the handling of coordinates with values outside the valid
    # range.
    ds = xarray.Dataset(coords={"x": [-200, -170], "y": [50, 60]})
    with pytest.raises(ValueError):
        utils.geospatial.harmonize_coords(ds)
    ds = xarray.Dataset(coords={"x": [-10, 200], "y": [50, 60]})
    with pytest.raises(ValueError):
        utils.geospatial.harmonize_coords(ds)

    # Test the sorting of coordinates.
    ds = xarray.Dataset(coords={"x": [60, 50], "y": [60, 50]})
    result = utils.geospatial.harmonize_coords(ds)
    sorted_x = numpy.sort(ds["x"].values)
    assert numpy.allclose(result["x"].values, sorted_x)
    sorted_y = numpy.sort(ds["y"].values)
    assert numpy.allclose(result["y"].values, sorted_y)


def test_clean_raster():
    """
    Test the cleaning of raster data.

    This function tests the clean_raster utility function to ensure it
    correctly removes unnecessary dimensions and coordinates from an
    xarray DataArray.
    """
    # Create a mock DataArray with 'band' dimension and extra variables.
    data = xarray.DataArray(
        numpy.random.rand(1, 5, 5),
        dims=["band", "y", "x"],
        coords={"band": [1], "spatial_ref": 0},
        name="original_var",
        attrs={"description": "test raster"},
    )

    # Clean the raster data using the utility function.
    cleaned = utils.geospatial.clean_raster(data, "cleaned_var")

    # Check the properties of the cleaned DataArray.
    assert isinstance(cleaned, xarray.DataArray)
    assert cleaned.name == "cleaned_var"
    assert "band" not in cleaned.dims
    assert "band" not in cleaned.coords
    assert "spatial_ref" not in cleaned.coords
    assert cleaned.attrs == {}
    assert cleaned.shape == (5, 5)
