import requests
import os
import time
import pandas as pd
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor

# Base URL structure (assuming the format is consistent)
base_url = "https://aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_{}_{}.csv"

# List of regions to download data for
regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]

# Create the aggregate folder for saving merged files
aggregate_folder = "aggregate"
os.makedirs(aggregate_folder, exist_ok=True)


def download_and_process(region):
    """Download and process files for a specific region."""

    # Create the region folder
    region_folder = os.path.join(region)
    os.makedirs(region_folder, exist_ok=True)

    # Initialize a list to collect all dataframes for merging
    dfs = []

    # Loop through the years from 2018 to 2024
    for year in range(2018, 2025):
        for month in range(1, 13):
            # Format the month-year string
            month_str = f"{year}{month:02d}"

            # Construct the download URL
            url = base_url.format(month_str, region)

            # Set the headers to mimic a browser
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
            }

            # Attempt to download the file
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                # Extract filename
                filename = os.path.basename(urlsplit(url).path)

                # Define save path
                file_path = os.path.join(region_folder, filename)

                # Save the file
                with open(file_path, "wb") as file:
                    file.write(response.content)
                print(f"File downloaded: {file_path}")

                # Read the downloaded CSV into a dataframe
                df = pd.read_csv(file_path)
                dfs.append(df)
            else:
                print(
                    f"Failed to download file for {month_str} ({region}). Status code: {response.status_code}"
                )

            # Sleep to avoid rate-limiting issues
            time.sleep(5)

    # After all files for the region are downloaded, merge them into one file
    if dfs:
        # Concatenate all dataframes for this region
        aggregated_df = pd.concat(dfs, ignore_index=True)

        # Save the aggregated dataframe as a CSV file
        aggregated_file_path = os.path.join(
            aggregate_folder, f"aggregated_{region}.csv"
        )
        aggregated_df.to_csv(aggregated_file_path, index=False)
        print(f"Aggregated file saved for {region}: {aggregated_file_path}")


def aggregate_all_region_files():
    """Aggregate all region files into one final sorted file with regions as columns."""

    print(
        "Now merging all regional data into a single dataset with regions as columns..."
    )

    # Get all the aggregated files
    all_aggregated_files = [
        os.path.join(aggregate_folder, f)
        for f in os.listdir(aggregate_folder)
        if f.startswith("aggregated_")
    ]

    # Dictionary to store dataframes with SETTLEMENTDATE as the index
    region_data = {}

    for file in all_aggregated_files:
        # Extract region name from the filename
        region = os.path.basename(file).split("_")[1].split(".")[0]

        # Read the aggregated CSV
        df = pd.read_csv(file)

        # Ensure 'SETTLEMENTDATE' is in datetime format
        df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"])

        # Extract only SETTLEMENTDATE and the Demand column (assuming the demand column name)
        if "TOTALDEMAND" in df.columns:
            df = df[["SETTLEMENTDATE", "TOTALDEMAND"]].rename(
                columns={"TOTALDEMAND": f"{region}_DEMAND"}
            )
        else:
            print(f"Warning: TOTALDEMAND column not found in {file}")

        # Store dataframe in dictionary
        region_data[region] = df

    # Merge all dataframes on SETTLEMENTDATE
    final_aggregated_df = None
    for region, df in region_data.items():
        if final_aggregated_df is None:
            final_aggregated_df = df
        else:
            final_aggregated_df = final_aggregated_df.merge(
                df, on="SETTLEMENTDATE", how="outer"
            )

    # Add a TOTAL_DEMAND column summing all region demands
    demand_columns = [
        col for col in final_aggregated_df.columns if col.endswith("_DEMAND")
    ]
    final_aggregated_df["TOTAL_DEMAND"] = final_aggregated_df[demand_columns].sum(
        axis=1
    )

    # Sort by 'SETTLEMENTDATE'
    final_aggregated_df = final_aggregated_df.sort_values(
        by="SETTLEMENTDATE", ascending=True
    )

    # Save the final aggregated dataframe as a CSV file
    final_aggregated_file_path = os.path.join(aggregate_folder, "final_aggregated.csv")
    final_aggregated_df.to_csv(final_aggregated_file_path, index=False)
    print(
        f"Final aggregated file saved with all regions as columns: {final_aggregated_file_path}"
    )


# Using ThreadPoolExecutor to download files concurrently for each region
with ThreadPoolExecutor() as executor:
    for region in regions:
        executor.submit(download_and_process, region)

# After all regions are downloaded and aggregated separately, merge them into one final file
aggregate_all_region_files()
