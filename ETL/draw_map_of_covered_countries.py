import os

import cartopy.crs as ccrs
import cartopy.feature
import matplotlib.pyplot as plt
import util.general
import util.shapes

# Create a directory to store the figures.
figure_directory = util.general.read_folders_structure()["figures_folder"]
os.makedirs(figure_directory, exist_ok=True)

# Read the codes of all countries and regions.
region_codes = util.general.read_all_codes()

# Define the map projection and the coordinate reference system of the data to plot.
map_projection = ccrs.Robinson(central_longitude=0)
data_crs = ccrs.PlateCarree()

# Initialize the figure.
fig, ax = plt.subplots(figsize=(8, 10), subplot_kw={"projection": map_projection})

# Plot the land.
ax.add_feature(cartopy.feature.LAND, facecolor="lightgray")

# Loop over the countries.
for region_code in region_codes:
    # Get the shape of the region.
    region_shape = util.shapes.get_region_shape(region_code, make_plot=False)

    # Plot the region.
    region_shape.plot(
        ax=ax,
        transform=data_crs,
        facecolor="red",
        edgecolor="black",
        linewidth=0.5,
        aspect=None,
        autolim=False,
    )

# Save the figure.
fig.savefig(figure_directory + "/available_countries.png", dpi=600, bbox_inches="tight")
