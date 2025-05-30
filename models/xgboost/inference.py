# This file includes code to perform inference using a pre-trained model.

import argparse
import datetime
import os

import pandas
from xgboost import XGBRegressor


def load_input_data(data_path: str) -> pandas.DataFrame:
    """
    Load input data from local file for inference.
    """
    # Determine file format and load accordingly
    if data_path.endswith(".parquet"):
        return pandas.read_parquet(data_path)
    elif data_path.endswith(".csv"):
        return pandas.read_csv(data_path, parse_dates=True, index_col=0)
    else:
        raise ValueError(f"Unsupported file format: {data_path}")


def load_model(model_path: str) -> XGBRegressor:
    """
    Load the pre-trained XGBoost model.
    """
    try:
        model = XGBRegressor()
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model file not found at {model_path}")
        model.load_model(model_path)
        return model
    except Exception as e:
        raise RuntimeError(f"Failed to load model from {model_path}: {str(e)}")


def read_command_line_arguments() -> argparse.Namespace:
    """
    Create a parser for the command line arguments, returns parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Run inference using a pre-trained XGBoost model"
    )

    parser.add_argument(
        "-ml",
        "--model_location",
        type=str,
        default="inference/xgboost_model.bin",
        help='Location of the XGBoost model file (default: "inference/xgboost_model.bin")',
        required=False,
    )
    parser.add_argument(
        "--input", type=str, required=True, help="Path to the input data"
    )

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    # Read command line arguments
    args = read_command_line_arguments()

    # Load input data
    input_data = load_input_data(args.input)

    # Load the pre-trained model
    model = load_model(args.model_location)

    # Run inference
    predictions = model.predict(input_data)

    # Save predictions
    output_path = (
        "./" + datetime.datetime.today().strftime("%Y_%m_%d") + "_prediction.parquet"
    )
    predictions.to_parquet(output_path)
