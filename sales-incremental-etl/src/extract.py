# 2nd File

# =========================
# IMPORTS
# =========================

import pandas as pd
# pandas is a third-party library used for data manipulation and analysis
# pd.read_csv() will be used to read CSV files into DataFrames

from pathlib import Path
# pathlib is a STANDARD PYTHON LIBRARY
# Path is used for safe and OS-independent file path handling

from utils import  get_project_root, load_config, setup_logging
from typing import Dict

# load_config -> our custom function from utils.py
# setup_logging -> our custom logging setup function

# =========================
# EXTRACT FUNCTION
# =========================

def extract_data(config: dict, logger) -> pd.DataFrame:
    """
    The below green part is known as DocString of a function, which provides the clarity for the purpose & usage of it.
    "pd.DataFrame" after a sign "->" is known as  return annotation or a return type hint of a function. (Shared below).

    About return type hint: -> a return type hint (e.g., -> pd.DataFrame AND -> Path) is a piece of metadata in a
        function's signature that specifies what type of value the function is expected to return.

        Though, Python is a dynamically typed language, so return hints are not enforced at runtime. If you declare a
        function returns an int, but it returns a str, the code will still run without errors unless you use a separate
        tool.

        Professional workflows often use tools like Mypy or Pyright to scan code before it runs and flag any functions
        that return the wrong type based on these hints.

        1. Simple Type: -> int indicates the function returns an integer.
        2. No Return: -> None is used for functions that perform an action (like printing) but don't return a value.
        3. Multiple/Optional Types: You can use the "pipe" operator (introduced in Python 3.10) for
            flexibility: -> int | None means it might return an integer or nothing at all.
        4. Complex Types: -> list[str] specifies a list of strings.

    pd.dataframe is a return type of this function which is a part of pandas (third party library for python DA)
    Extract raw sales data from CSV file.

    :param config: Path to config file.
    :param logger: logging in-case of errors or success
    :return: pandas.DataFrame: Raw sales data
    """
    logger.info("Starting data extraction step")

    project_root = get_project_root()

    # Build full path to raw data file
    # config["raw_data_path"] is a RELATIVE path from config.yaml

    raw_data_path = project_root / config["raw_data_path"]

    logger.info(f"Raw data path resolved to: {raw_data_path}")

    # ==================================================
    # Checking FILE EXISTENCE on raw_data_path
    # ==================================================

    # Path.exists() is a method from pathlib.Path
    if not raw_data_path.exists():
        logger.error(f"Raw data file does not exist: {raw_data_path}")
        raise FileNotFoundError(f" Raw data does not exists at: {raw_data_path}")

    # =========================
    # READ CSV FILE
    # =========================

    try:
        # pd.read_csv() is from pandas library
        # It reads CSV file and returns a DataFrame
        # Added seperator since we saw an error where csv was reading in different format or was saved in different
        # format

        df = pd.read_csv(raw_data_path, sep=None, engine="python", encoding="utf-8-sig")

        # =========================
        # HEADER NORMALIZATION
        # =========================

        # Case: Entire row is packed into first column (Excel corruption)
        if len(df.columns) <= 2 and "," in df.columns[0]:
            logger.warning(
                "Detected malformed CSV structure. Normalizing columns and data."
            )

            # Extract correct column names
            corrected_columns = df.columns[0].split(",")

            # Split the single column into multiple columns
            df = df[df.columns[0]].str.split(",", expand=True)

            # Assign corrected column names
            df.columns = corrected_columns

            logger.info(f"Corrected columns after normalization: {df.columns.tolist()}")

        # Schema validation (production mindset)
        expected_columns = {
            "order_id", "order_date", "customer_id",
            "product", "quantity", "price"
        }

        actual_columns = set(df.columns)

        # Cross Validating expected columns with columns that got fetched from file
        if not (expected_columns.issubset(actual_columns)):
            # if not (expected_columns.issubset(df.corrected_columns)):
            logger.error(f"CSV Schema mismatched error")
            logger.error(f"Expected columns: {expected_columns}")
            logger.error(f"Actual columns: {actual_columns}")

            raise ValueError(f"Expected columns {expected_columns}, but got {actual_columns}")

        # =========================
        # SUCCESS LOGGING
        # =========================

        logger.info(f"Extracted columns: {list(df.columns)}")
        logger.info(
            f"Successfully extracted data | Rows: {df.shape[0]} | Columns: {df.shape[1]}"
        )

        return df

    except pd.errors.EmptyDataError:
        # pandas raises EmptyDataError if file exists but is empty
        logger.error("Raw data file is empty")
        raise

    except Exception as e:
        # Catch any unexpected error
        logger.error(f"Unexpected error during data extraction: {e}")
        raise

#
#
# config = load_config()
# logger = setup_logging(
#     config["log_file_path"],
#     config["logging"]["level"]
# )
# df_raw = extract_data(config, logger)
