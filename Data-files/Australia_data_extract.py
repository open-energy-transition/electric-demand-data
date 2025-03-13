import os
import time
import requests
import pandas as pd
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor

# Base URL structure (ensure it follows expected formatting)
BASE_URL = (
    "https://aemo.com.au/aemo/data/nem/priceanddemand/"
    "PRICE_AND_DEMAND_{}_{}.csv"
)

# List of regions to download data for
REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]

# Create directories for aggregated files
AGGREGATE_FOLDER = "aggregate"
os.makedirs(AGGREGATE_FOLDER, exist_ok=True)


def download_and_process(region: str) -> None:
    """Download and process files for a given region."""
    region_folder = os.path.join(region)
    os.makedirs(region_folder, exist_ok=True)
    
    dfs = []

    for year in range(2018, 2024):
        for month in range(1, 13):
            month_str = f"{year}{month:02d}"
            url = BASE_URL.format(month_str, region)

            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
                )
            }

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                filename = os.path.basename(urlsplit(url).path)
                file_path = os.path.join(region_folder, filename)

                with open(file_path, "wb") as file:
                    file.write(response.content)

                print(f"File downloaded: {file_path}")

                try:
                    df = pd.read_csv(file_path)
                    dfs.append(df)
                except pd.errors.EmptyDataError:
                    print(f"Warning: Empty CSV file {file_path}, skipping.")
            else:
                print(
                    f"Failed to download file for {month_str} ({region}). "
                    f"Status code: {response.status_code}"
                )

            time.sleep(5)  # Sleep to avoid rate limits

    if dfs:
        aggregated_df = pd.concat(dfs, ignore_index=True)
        aggregated_file_path = os.path.join(AGGREGATE_FOLDER, f"aggregated_{region}.csv")
        aggregated_df.to_csv(aggregated_file_path, index=False)
        print(f"Aggregated file saved for {region}: {aggregated_file_path}")


def aggregate_all_region_files() -> None:
    """Aggregate all regional data into one dataset with regions as columns."""
    print("Merging all regional data into a single dataset...")

    aggregated_files = [
        os.path.join(AGGREGATE_FOLDER, f)
        for f in os.listdir(AGGREGATE_FOLDER)
        if f.startswith("aggregated_")
    ]

    region_data = {}

    for file in aggregated_files:
        region = os.path.basename(file).split("_")[1].split(".")[0]

        try:
            df = pd.read_csv(file)
            df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"])

            if "TOTALDEMAND" in df.columns:
                df = df[["SETTLEMENTDATE", "TOTALDEMAND"]].rename(
                    columns={"TOTALDEMAND": f"{region}_DEMAND"}
                )
                region_data[region] = df
            else:
                print(f"Warning: TOTALDEMAND column not found in {file}")

        except pd.errors.EmptyDataError:
            print(f"Warning: Skipping empty file {file}.")

    # Merge all regions into a single dataframe
    if region_data:
        final_aggregated_df = pd.concat(region_data.values(), axis=1)
        final_aggregated_df = final_aggregated_df.loc[:, ~final_aggregated_df.columns.duplicated()]  # Drop duplicates

        # Compute total demand
        demand_columns = [col for col in final_aggregated_df.columns if col.endswith("_DEMAND")]
        final_aggregated_df["TOTAL_DEMAND"] = final_aggregated_df[demand_columns].sum(axis=1)

        # Sort and save the final file
        final_aggregated_df.sort_values(by="SETTLEMENTDATE", ascending=True, inplace=True)
        final_file = os.path.join(AGGREGATE_FOLDER, "final_aggregated.csv")
        final_aggregated_df.to_csv(final_file, index=False)
        print(f"Final aggregated file saved: {final_file}")


# Execute downloads in parallel using ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    executor.map(download_and_process, REGIONS)

# Merge region files after downloading
aggregate_all_region_files()
