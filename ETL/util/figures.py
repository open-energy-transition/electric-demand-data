# -*- coding: utf-8 -*-
"""
License: AGPL-3.0.

Description:

    This module contains functions to plot data and save the figures.
"""

import os

import geopandas
import matplotlib.pyplot
import util.directories
import xarray


def simple_plot(
    data_to_plot: geopandas.GeoDataFrame | xarray.DataArray, figure_name: str
) -> None:
    """
    Plot the data and save the figure.

    Parameters
    ----------
    data_to_plot : geopandas.GeoDataFrame or xarray.DataArray
        The data to plot.
    figure_name : str
        The name of the figure.
    """
    # Create a directory to store the figures.
    figure_directory = util.directories.read_folders_structure()["figures_folder"]
    os.makedirs(figure_directory, exist_ok=True)

    # Plot the data.
    fig, ax = matplotlib.pyplot.subplots()
    data_to_plot.plot(ax=ax)
    fig.savefig(
        os.path.join(figure_directory, figure_name + ".png"),
        dpi=300,
        bbox_inches="tight",
    )
    matplotlib.pyplot.close(fig)
