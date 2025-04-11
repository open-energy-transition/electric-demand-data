# Extract Transform Load (ETL)

This part of the project focusses on getting the data from various sources into a usable format.
The process is as follows.

## 1. Fetch the data

Regardless of source, the data must be obtained through download or API call.

## 2. Transform into tabular format

In this step the data has to be transformed into tabular format.
Note, there is no further modification of the raw data.
Only logic that processes the input into a table (parquet compatible) should exist here.

## 3. Data cleaning

Process the tabular data to account for:

- Convert timesteps to UTC
- Demand is represented in MW
- Missing (NaN) values
- Missing timesteps
- Synchronize begin and end timesteps


## 4. Save the processed data

In this step you can either save it locally as a parquet file, or to cloud storage (work in progress)
