import geopandas as gpd

# Define the path to the shapefile of the US balancing authorities.
us_balancing_authorities_path = "data/us_balancing_authorities/"

# Read the shapefile of the balancing authorities. Source: https://atlas.eia.gov/datasets/eia::balancing-authorities/about
balancing_authorities = gpd.read_file(
    us_balancing_authorities_path + "Balancing_Authorities.shp"
)

# Change the projection of the shapefile to EPSG 4326.
balancing_authorities = balancing_authorities.to_crs(epsg=4326)

# Merge the balancing authorities belonging to the same region.
regions = balancing_authorities.dissolve(by="EIAregion")

# Define the codes of the regions.
region_codes = {
    "California": "CAL",
    "Carolinas": "CAR",
    "Central": "CENT",
    "Florida": "FLA",
    "Mid-Atlantic": "MIDA",
    "Midwest": "MIDW",
    "New England": "NE",
    "New York": "NY",
    "Northwest": "NW",
    "Southeast": "SE",
    "Southwest": "SW",
    "Tennessee": "TEN",
    "Texas": "TEX",
}

# Add the codes to the regions shapefile.
for region_name in regions.index:
    regions.loc[region_name, "EIAcode"] = region_codes[region_name]

# Select the columns of interest.
regions = regions[["EIAcode", "geometry"]]

# Save the regions shapefile.
regions.to_file(us_balancing_authorities_path + "regions.shp")
