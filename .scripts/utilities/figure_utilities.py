import os

import matplotlib.pyplot as plt


def plot(data_to_plot, figure_name):
    """
    Plot the data and save the figure.

    Parameters
    ----------
    data_to_plot : GeoDataFrame or xarray.DataArray
        The data to plot
    figure_name : str
        The name of the figure
    """

    # Create a directory to store the figures.
    figure_directory = "figures"
    if not os.path.exists(figure_directory):
        os.makedirs(figure_directory)

    # Plot the data.
    fig, ax = plt.subplots()
    data_to_plot.plot(ax=ax)
    fig.savefig(f"{figure_directory}/{figure_name}.png")
    plt.close(fig)
