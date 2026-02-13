# 4th File
"""
Data Quality checks
"""


def check_nulls(df, required_columns, logger):
    """

    :param df:
    :param required_columns:
    :param logger:
    :return:
    """
    for col in required_columns:
        if df[col].isnull().any():
            raise ValueError(f"Null values found in required column: {col}")
    logger.info("Null check passed")


def check_ranges(df, logger):
    """

    :param df:
    :param logger:
    :return:
    """
    if (df["quantity"] <= 0).any():
        raise ValueError("Quantity must be > 0")

    if (df["price"] <= 0).any():
        raise ValueError("Price must be > 0")

    logger.info("Range checks passed")


def check_duplicates(df, logger):
    """

    :param df:
    :param logger:
    :return:
    """
    dup_count = df.duplicated(subset=["order_id"]).sum()
    if dup_count > 0:
        raise ValueError(f"Duplicate order_id found: {dup_count}")
    logger.info("Duplicate check passed")



EXPECTED_SCHEMA = {
    "order_id": "int64",
    "order_date": "datetime64[ns]",
    "customer_id": "int64",
    "product": "object",
    "quantity": "int64",
    "price": "float64",
    "revenue": "float64",
}
