# 3rd File
"""
Raw Issues Identified and resolving in this module
Issue	                            Fix
1. Missing quantity	                Replace with 1
2. Duplicate order_id	            Remove duplicates
3. order_date as string	            Convert to datetime
4. quantity, price as strings	    Convert to numeric
5. No revenue column	            Create revenue = quantity × price

"""
# =========================
# IMPORTS
# =========================

import pandas as pd
# pandas is a third-party library used for data manipulation

from utils import load_config, setup_logging
# load_config() -> reads YAML config
# setup_logging() -> sets up logging

# =========================
# TRANSFORM FUNCTION
# =========================


def transform_data(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    The below green part is known as DocString of a function, which provides the clarity for the purpose & usage of it.
    "pd.DataFrame" after a sign "->" is known as return annotation or a return type hint of a function. (Shared below)
    And "pd.DataFrame" after "df:" is known as "parameter type hint" or what should be datatype/expected data for
        that parameter
    "function return type hint" and "Annotation/parameter return type hint" are almost serve the same purpose.

    Now, you should understand the difference between the function annotation and function docstrings.

    df: This is the name of the parameter (variable) that the function expects to receive when it's called.
    : (colon): This separates the parameter name from its type hint.
    pd.DataFrame: This specifies the expected type of the object that should be passed as the df argument.
        It indicates that the function is designed to work with a pandas DataFrame object.

    About return type hint: -> a return type hint (e.g., -> pd.DataFrame) is a piece of metadata in a function's
        signature that specifies what type of value the function is expected to return.

        Python is a dynamically typed language, so return hints are not enforced at runtime. If you declare a
        function returns an int, but it returns a str, the code will still run without errors unless you use a separate
        tool.

        Professional workflows often use tools like Mypy or Pyright to scan code before it runs and flag any functions
        that return the wrong type based on these hints.

        1. Simple Type: -> int indicates the function returns an integer.
        2. No Return: -> None is used for functions that perform an action (like printing) but don't return a value.
        3. Multiple/Optional Types: You can use the "pipe" operator (introduced in Python 3.10) for
            flexibility: -> int | None means it might return an integer or nothing at all.
        4. Complex Types: -> list[str] specifies a list of strings.

    In your specific example, -> pd.DataFrame tells anyone reading the code (and their IDE) that they should receive a
        pandas DataFrame object after calling that function.

    ------------------------- DocString below --------------------
    Clean and transform raw sales data.

    :param df:
    :param logger:

    Args:
        df (pandas.DataFrame): Raw extracted data

    Returns:
        pandas.DataFrame: Transformed data
    """

    logger.info("Starting data transformation step")

    # =========================
    # DATA VALIDATION
    # =========================

    # Check if DataFrame is empty
    # df.empty is a pandas attribute
    if df.empty:
        logger.error("Input Dataframe is empty")
        raise ValueError("Cannot transform empty dataframe.")

    # =========================
    # HANDLE MISSING VALUES
    # =========================

    # Fill missing quantity values with 1
    # pandas.Series.fillna() fills missing values
    df["quantity"] = df["quantity"].fillna(1)

    logger.info("Missing value in 'quantity' filled with 1")

    # =========================
    # DATA TYPE CONVERSIONS
    # =========================

    # Convert quantity column to integer
    # pandas.to_numeric() converts values to numeric type
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    # Handle missing or invalid quantity AFTER conversion, since it can create conversion issues
    df["quantity"] =  df["quantity"].fillna(1)

    # Now safe to convert to int
    df["quantity"] = df["quantity"].astype(int)

    # Convert price column to float and fill na after conversion since it can create conversion issues
    df["price"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price"] = df["price"].fillna(0.0)

    # Convert order_date column to datetime
    # pandas.to_datetime() converts string to datetime
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["order_date"] = df["order_date"].fillna(9999-99-99)

    logger.info("Data type conversion completed.")

    # =========================
    # REMOVE DUPLICATES
    # =========================

    # drop_duplicates() is a pandas DataFrame method
    # Keeping the first occurrence of duplicate order_id
    initial_row_count = df.shape[0]                               # getting row count using shape method of pandas

    # df = df.drop_duplicates(subset=["order_id"], keep="first")
    df = df.drop_duplicates(subset="order_id", keep="first").copy()
    removed_rows = initial_row_count - df.shape[0]

    logger.info(f" Removed {removed_rows} duplicate rows based on order_id 4.")

    # =========================
    # BUSINESS LOGIC
    # =========================

    # Create revenue column
    # Vectorized operation in pandas (fast & efficient)
    df["revenue"] = df["quantity"] * df["price"]

    logger.info("Revenue column created.")

    # =========================
    # FINAL VALIDATION
    # =========================

    # Ensure no negative revenue
    if (df["revenue"] < 0).any():
        logger.warning("Negative revenue values found.")

    logger.info("Data transformation completed successfully.")

    return df
