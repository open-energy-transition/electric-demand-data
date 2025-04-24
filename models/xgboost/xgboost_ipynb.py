# %%
import os

# %%
import matplotlib.pyplot as plt
import numpy
import pandas
import pycountry
import yaml
from dotenv import dotenv_values
from sklearn.model_selection import LeaveOneGroupOut, cross_validate
from tqdm import tqdm
from xgboost import XGBRegressor

figure_directory = "./figures"
os.makedirs(figure_directory, exist_ok=True)

# %%
os.listdir("./data/toktarova_et_al_2019/")

# %%
with open("./data/gegis__all_countries.yaml", "r") as file:
    data = yaml.safe_load(file)

# %%
items = data["items"]
gegis_countries = [[item["country_name"], item["country_code"]] for item in items]

# %%
gegis_country_codes = numpy.array(gegis_countries).T[1]

# %%
gegis_countries

# %% [markdown]
# ## Features
#
# """
#
# We take time series of hourly electricity demand for 44 countries from Toktarova et al. [15] and fit a gradient boosting regression model [32] to demand time series for each country normalized to their annual mean
#
# Estimates of annual country-level annual electricity generation in 2050 were produced by extrapolating annual demand in 2016 [33] using regional demand growth in the SSP2-26 scenario
#
# Therefore we chose to train our model on ten independent var­
# iables:
#
# (i + ii) annual per-capita electricity demand and purchase-power adjusted GDP (for prediction, we extrapolated this to 2050 using the SSP2 scenario in a similar way to demand as above),
#
# (iii) average hourly temperature profiles over the year in the 3 most densely populated areas of each country [35,36],
#
# (iv) the mean annual temperature level,
#
# (v) the 1st temperature percentile across the year (to represent how low the temperature dips go),
#
# (vi) the 99th percentile (to represent how high temperature spikes go),
#
# (vii) hour of the day,
#
# (viii) a weekday/weekend indicator,
#
# (ix) mean monthly temperature levels, and
#
# (x) a temperature-based ranking of months of the year
# (where the first month is the coldest month, and the month ranked last is the warmest across the year).
# The temperature ranking of months was chosen in order to reflect that different countries have summer in different calendar months.
#
# """
#
#

# %% [markdown]
# ### (i) annual per-capita electricity demand

# %%
ElectricityPerCapita = pandas.read_csv(
    "./data/toktarova_et_al_2019/ElectricityperCapita.csv", index_col=0, header=1
)

# %%
ElectricityPerCapita.columns = [
    int(float(col_name)) if col_name.split(".")[0].isdigit() else col_name
    for col_name in ElectricityPerCapita.columns
]

# %%
ElectricityPerCapita.head()

# %%


def search_pycountry(country_name: str) -> str | None:
    try:
        # Try to find the country
        country = pycountry.countries.search_fuzzy(country_name)[0]
        return country.alpha_2
    except LookupError:
        return None


def get_country_codes(country_names):
    country_codes = []
    for country_name in country_names:
        found_country_code = search_pycountry(country_name)
        if not (found_country_code):
            print("Not Found:", country_name)

        country_codes.append(found_country_code)
    return country_codes


# %%
ElectricityPerCapita.insert(
    1, "country_code", get_country_codes(ElectricityPerCapita["Countries"])
)

# %%
# Print gegis countries not found by search
for code in gegis_country_codes:
    if code not in ElectricityPerCapita["country_code"].values:
        print(code)

# %%
# Adjust the missing countries
index_cc_BA = ElectricityPerCapita[
    ElectricityPerCapita["Countries"] == "Bosnia-Herzegovina"
].index
ElectricityPerCapita.loc[index_cc_BA, "country_code"] = "BA"

index_cc_RS = ElectricityPerCapita[
    ElectricityPerCapita["Countries"] == "Serbia (former Yugoslavia)"
].index
ElectricityPerCapita.loc[index_cc_RS, "country_code"] = "RS"

index_cc_KR = ElectricityPerCapita[
    ElectricityPerCapita["Countries"] == "Korea. Republic of"
].index
ElectricityPerCapita.loc[index_cc_KR, "country_code"] = "KR"

index_cc_TR = ElectricityPerCapita[ElectricityPerCapita["Countries"] == "Turkey"].index
ElectricityPerCapita.loc[index_cc_TR, "country_code"] = "TR"

# %%
list_ids_gegis = []
for i in range(len(ElectricityPerCapita)):
    current_row = ElectricityPerCapita.iloc[i]

    if current_row["country_code"] in gegis_country_codes:
        list_ids_gegis += [i]

# %%
ElectricityPerCapita = ElectricityPerCapita.iloc[list_ids_gegis]

# %%
# Drop manually found incorrect countries (usually territories)
ElectricityPerCapita = ElectricityPerCapita.drop([207, 211, 220])

# %%
ElectricityPerCapita[["country_code", 2015]]

# %%
toktarova_indices = ElectricityPerCapita.index

# %%
numpy.savetxt("./train/toktarova_indices.txt", toktarova_indices.to_numpy())

# %%
ElectricityPerCapita_2015 = ElectricityPerCapita[["country_code", 2015]].copy()
ElectricityPerCapita_2015.rename(
    columns={2015: "ElectricityPerCapita_2015"}, inplace=True
)

# %%
ElectricityPerCapita_2015.head()

# %% [markdown]
# ## (ii) purchase-power adjusted GDP

# %%
GDP_PPP = pandas.read_csv(
    "./data/toktarova_et_al_2019/GDPperCapita.csv", index_col=0, header=1
)

# %%
GDP_PPP.columns = [
    int(float(col_name)) if col_name.split(".")[0].isdigit() else col_name
    for col_name in GDP_PPP.columns
]

# %%
GDP_PPP[2015].count()

# %%
GDP_PPP = GDP_PPP.loc[toktarova_indices]

# %%
GDP_PPP.insert(1, "country_code", ElectricityPerCapita["country_code"])

# %%
GDP_PPP.head()

# %%
GDP_2015 = GDP_PPP[["country_code", 2015]].copy()
GDP_2015.rename(columns={2015: "GDP_PPP_2015"}, inplace=True)

# %%
GDP_2015.head()

# %% [markdown]
# ### (iii) average hourly temperature profiles over the year in the 3 most densely populated areas of each country [35,36]

# %%

config = dotenv_values(".env")

# %%
# ETL/temperature
storage_bucket = config["GCSBUCKET"]

# %%
df_temperature_top_3 = pandas.DataFrame()
for country_code in tqdm(gegis_country_codes, desc="Processing countries"):  # +
    current_df = pandas.read_csv(
        f"{storage_bucket}/temperature/temperature_time_series_top_3_{country_code}_2015.csv"
    )
    current_df.reset_index(inplace=True)
    current_df["country_code"] = country_code
    df_temperature_top_3 = pandas.concat(
        [df_temperature_top_3, current_df], ignore_index=True
    )

# %%
df_temperature_top_3.rename(
    columns={"Temperature (K)": "Temperature top_3 (K)"}, inplace=True
)

# %%
df_temperature_top_3.head()

# %%
df_temperature_top_3.to_parquet("./data/temperature_top_3.parquet")

# %%
df_temperature_top_3 = pandas.read_parquet("./data/temperature_top_3.parquet")

# %% [markdown]
# ## ETL/temperature
#
# (iv) the mean annual temperature level,
#
# (v) the 1st temperature percentile across the year (to represent how low the temperature dips go), (5th percentile in our case)
#
# (vi) the 99th percentile (to represent how high temperature spikes go), (95th percentile in our case)
#
# (vii) hour of the day,
#
# (viii) a weekday/weekend indicator,
#
# (ix) mean monthly temperature levels, and
#
# (x) a temperature-based ranking of months of the year

# %%
df_temperature_top_1 = pandas.DataFrame()
for country_code in tqdm(gegis_country_codes, desc="Processing countries"):  # +
    current_df = pandas.read_csv(
        f"{storage_bucket}/temperature/temperature_time_series_top_1_{country_code}_2015.csv"
    )
    current_df.reset_index(inplace=True)
    current_df["country_code"] = country_code
    df_temperature_top_1 = pandas.concat(
        [df_temperature_top_1, current_df], ignore_index=True
    )

# %%
df_temperature_top_1["weekend_indicator"] = (
    df_temperature_top_1["Local day of the week"] >= 5
).astype(int)

# %%
df_temperature_top_1.head()

# %%
df_temperature_top_1.to_parquet("./data/temperature_top_1.parquet")

# %%
df_temperature_top_1 = pandas.read_parquet("./data/temperature_top_1.parquet")

# %% [markdown]
# ## Target hourly electricity load data

# %%
Hourly_demand = pandas.read_csv(
    "./data/toktarova_et_al_2019/Real load hourly data.csv", index_col=0, header=0
)

# %%
Hourly_demand = Hourly_demand.T

# %%
Hourly_demand.index = Hourly_demand.index.astype(int)

# %%
selected_hourly_demand = Hourly_demand.loc[toktarova_indices]

# %%
selected_hourly_demand.insert(1, "country_code", ElectricityPerCapita["country_code"])

# %%
temp = selected_hourly_demand.loc[9]

# %%
selected_hourly_demand.head()

# %%
selected_hourly_demand.drop(
    columns=[
        "Electricity consumption",
        "Countryname",
        "annual electricity consumption in TWh",
        "average",
        "R",
        "Psyn",
        "Preal",
    ],
    inplace=True,
)

# %%
selected_hourly_demand.columns

# %%
selected_hourly_demand.columns = [
    int(col_name.split(" ")[0].split("_")[-1]) - 1
    if col_name.split(" ")[0].split("_")[-1].isdigit()
    else col_name
    for col_name in selected_hourly_demand.columns
]

# %%
selected_hourly_demand.head()

# %%
all_hourly_demand = pandas.DataFrame()

for i in tqdm(range(len(selected_hourly_demand)), desc="Processing countries"):
    current_country_hourly_demand = pandas.DataFrame(selected_hourly_demand.iloc[i, 1:])
    current_country_hourly_demand.columns = ["hourly_demand"]

    current_country_hourly_demand["country_code"] = selected_hourly_demand.iloc[i, 0]
    current_country_hourly_demand.reset_index(inplace=True)

    all_hourly_demand = pandas.concat(
        [all_hourly_demand, current_country_hourly_demand], ignore_index=True
    )

# %%
all_hourly_demand["country_code"].unique()

# %% [markdown]
# ## Combine into a single dataframe for training

# %%
combined_dataset = pandas.merge(
    all_hourly_demand, df_temperature_top_1, on=["country_code", "index"]
)

# %%
combined_dataset = pandas.merge(
    combined_dataset,
    df_temperature_top_3[["index", "country_code", "Temperature top_3 (K)"]],
    on=["country_code", "index"],
)

# %%
combined_dataset = pandas.merge(combined_dataset, GDP_2015, on=["country_code"])
combined_dataset = pandas.merge(
    combined_dataset, ElectricityPerCapita_2015, on=["country_code"]
)

# %%
combined_dataset.columns

# %%
combined_dataset.drop(
    columns=[
        "index",
        "Time (UTC)",
        "Local day of the week",
        "Local year",
        "Temperature (K)",
        "Annual average temperature (K)",
    ],
    inplace=True,
)
# To get the same as GEGIS: https://github.com/niclasmattsson/GlobalEnergyGIS?tab=readme-ov-file#synthetic-demand-list-of-training-variables

# %%
combined_dataset = combined_dataset.rename(
    columns={
        "GDP_PPP_2015": "GDP_PPP",
        "ElectricityPerCapita_2015": "ElectricityPerCapita",
    }
)

# %%
combined_dataset.head()

# %%
combined_dataset.to_parquet("./train/train_dataset.parquet")

# %%
combined_dataset = pandas.read_parquet("./train/train_dataset.parquet")

# %% [markdown]
# ### Model

# %%


# %%
features = combined_dataset.drop(columns=["hourly_demand", "country_code"])
target = combined_dataset["hourly_demand"]
groups = combined_dataset["country_code"]

# %%
features.head()

# %%
# Initialize the XGBoost regressor
xgb_model = XGBRegressor(
    n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42
)

# %%
# Train the model
xgb_model.fit(features, target)

# %%
xgb_model.save_model("./train/xgboost_model.bin")

# %% [markdown]
# ### Visualizations

# %%
# Get feature importance
importance = xgb_model.feature_importances_
feature_names = features.columns

# Sort features by importance
indices = numpy.argsort(importance)

# Plot feature importance
plt.figure(figsize=(10, 6))
plt.title("Feature Importance")
plt.barh(range(len(importance)), importance[indices], color="skyblue")
plt.yticks(range(len(importance)), [feature_names[i] for i in indices])
plt.xlabel("Importance")
plt.tight_layout()

plt.savefig(
    os.path.join(figure_directory, "feature_importance.png"),
    dpi=300,
    bbox_inches="tight",
)
plt.show()

# %% [markdown]
# ## Cross-validation
#
# Validate the approach by performing k-fold cross-validation.

# %%


# %%
combined_dataset["country_code"].unique()

# %%
# Perform cross-validation
cv_results = cross_validate(
    xgb_model,
    features,
    target,
    groups=groups,
    cv=LeaveOneGroupOut(),
    scoring=["neg_mean_absolute_error"],
    return_train_score=True,
    return_indices=True,
    n_jobs=-1,
)

# %%
cv_results.keys()

# %%
cv_results

# %%
# Assuming 'cv_results' is your original dictionary and 'indices' is the key you want to exclude
cv_results_filtered = {k: v for k, v in cv_results.items() if k != "indices"}

# Create DataFrame from the filtered dictionary
df_cv_results = pandas.DataFrame(cv_results_filtered)

df_cv_results["test_mae"] = -df_cv_results["test_neg_mean_absolute_error"]
df_cv_results["train_mae"] = -df_cv_results["train_neg_mean_absolute_error"]

# %%
list_test_group_id = []
for test_indices in cv_results["indices"]["test"]:
    list_test_group_id.append(groups[test_indices[0]])

df_cv_results["group_id"] = list_test_group_id

# %%
df_cv_results.head()

# %% [markdown]
# ### Visualizations

# %%
# Plot the cross-validation test set error metrics

df_sorted = df_cv_results.sort_values("test_mae")

# Create the bar plot
fig, ax = plt.subplots(figsize=(15, 8))
bars = ax.bar(range(len(df_sorted)), df_sorted["test_mae"], color="skyblue")
x = numpy.arange(len(df_sorted))

# Customize the plot
ax.set_xlabel("Countries")
ax.set_ylabel("Mean Absolute Error")
ax.set_title("Cross-validation: Mean Absolute Error by Country")
ax.set_xticks(x)
ax.set_xticklabels(df_sorted["group_id"])


plt.tight_layout()

plt.savefig(
    os.path.join(figure_directory, "cv_mae_per_country.png"),
    dpi=300,
    bbox_inches="tight",
)
plt.show()

# %%
# Plot comparison between train and test error metrics

df_sorted = df_cv_results.sort_values("test_mae")

# Create the bar plot
fig, ax = plt.subplots(figsize=(15, 8))

width = 0.35
x = numpy.arange(len(df_sorted))

# Create the bars
bars1 = ax.bar(
    x - width / 2, df_sorted["test_mae"], width, label="Test MAE", color="skyblue"
)
bars2 = ax.bar(
    x + width / 2, df_sorted["train_mae"], width, label="Train MAE", color="lightgreen"
)

# Customize the plot
ax.set_xlabel("Countries")
ax.set_ylabel("Mean Absolute Error")
ax.set_title("Cross-validation: Comparison of MAE Train vs Test")
ax.set_xticks(x)
ax.set_xticklabels(df_sorted["group_id"])


plt.legend()
plt.tight_layout()

plt.savefig(
    os.path.join(figure_directory, "cv_mae_comparison.png"),
    dpi=300,
    bbox_inches="tight",
)
plt.show()

# %% [markdown]
# ## Inference

# %%
# Use the weather year of 2015, same as the training dataset
weather_dataset = pandas.read_parquet("./train/train_dataset.parquet")

# Drop unnecessary columns
weather_dataset.drop(
    columns=[
        "hourly_demand",
        "GDP_PPP_2015",
        "ElectricityPerCapita_2015",
    ],
    inplace=True,
)

# %%
weather_dataset.shape

# %%
weather_dataset.columns

# %%
weather_dataset.head()

# %%
numpy.loadtxt("./train/toktarova_indices.txt")


# %%
def get_year_ElectricityPerCapita(
    year: int, df_EPC: pandas.DataFrame
) -> pandas.DataFrame:
    df_result = df_EPC[["country_code", year]].copy()
    df_result.rename(columns={year: "ElectricityPerCapita"}, inplace=True)
    return df_result


# %%
def get_year_GDP_PPP(year: int, df_GDP_PPP: pandas.DataFrame) -> pandas.DataFrame:
    df_result = df_GDP_PPP[["country_code", year]].copy()
    df_result.rename(columns={year: "GDP_PPP"}, inplace=True)
    return df_result


# %%
future_prediction_years = numpy.arange(2020, 2100 + 1, 5)

df_all_prediction_data = pandas.DataFrame()
for year in future_prediction_years:
    df_GDP_PPP_current_year = get_year_GDP_PPP(year, GDP_PPP)

    # Merge GDP with combined weather + electricity per capita data
    df_combine_weather_GDP = pandas.merge(
        weather_dataset, df_GDP_PPP_current_year, on=["country_code"]
    ).copy()

    df_EPC_current_year = get_year_ElectricityPerCapita(year, ElectricityPerCapita)
    df_EPC_current_year["year"] = year

    # Merge electricity per capita with weather data
    df_combine_weather_GDP_EDP = pandas.merge(
        df_combine_weather_GDP, df_EPC_current_year, on=["country_code"]
    ).copy()

    df_all_prediction_data = pandas.concat(
        [df_all_prediction_data, df_combine_weather_GDP_EDP], ignore_index=True
    )

# %%
df_all_prediction_data.shape

# %%
df_all_prediction_data.head()

# %%
trained_xgb_model = XGBRegressor()
trained_xgb_model.load_model("./train/xgboost_model.bin")

# %%
df_all_prediction_data["prediction"] = trained_xgb_model.predict(
    df_all_prediction_data.loc[
        :, ~df_all_prediction_data.columns.isin(["country_code", "year"])
    ]
)

# %%
df_all_prediction_data

# %%
os.makedirs("./predictions", exist_ok=True)

# %%
# df_all_prediction_data.to_parquet("./predictions/predictions_2020_2100.parquet")

# %%
list_countries = df_all_prediction_data["country_code"].unique()

# %%
for country in tqdm(list_countries):
    df_country_predictions = df_all_prediction_data[
        df_all_prediction_data["country_code"] == country
    ].copy()

    # Add a new column 'hour_of_year' that increments within each year
    df_country_predictions["hour_of_year"] = df_country_predictions.groupby(
        "year"
    ).cumcount()

    df_country_selection = df_country_predictions[
        ["country_code", "year", "hour_of_year", "prediction"]
    ].copy()

    df_country_selection.to_parquet(
        f"./predictions/{country}_{future_prediction_years.min()}_to_{future_prediction_years.max()}.parquet",
        index=False,
    )

# %%
