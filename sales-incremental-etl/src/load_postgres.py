# 6th file
# =========================
# IMPORTS
# =========================


import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from utils import load_config
from urllib.parse import quote_plus
from incremental import get_last_watermark


def load_to_postgres(df: pd.DataFrame, logger):
    """
    The below green part is known as DocString of a function, which provides the clarity for the purpose & usage of it.
    If there is anything written after a sign "->" is known as return annotation or a return type hint of a function.
        (Shared below).

    Currently, in our case there is no function return type.

    About return type hint: -> a return type hint (e.g., -> pd.DataFrame AND -> dict) is a piece of metadata in a
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

    df: This is the name of the parameter (variable) that the function expects to receive when it's called.
    : (colon): This separates the parameter name from its type hint.
    pd.DataFrame: This specifies the expected type of the object that should be passed as the df argument.
        It indicates that the function is designed to work with a pandas DataFrame object.

    Incase no value is passed when calling function it will return an error.

    This function, Load transformed data into PostgreSQL.

    Args:
        df (pd.DataFrame): Transformed sales data
        logger (logging.Logger): Application logger

    """
    logger.info("Starting PostgreSQL load step")

    # =========================
    # LOAD CONFIGURATIONS
    # =========================

    config = load_config()
    pg_config = config["postgres"]

    host = pg_config["host"]
    port = pg_config["port"]
    database = pg_config["database"]
    user = pg_config["user"]
    password = pg_config["password"]                    # 👈 PASSWORD READ HERE
    schema = pg_config["schema"]
    table = pg_config["table"]

    # ----------------------------------------------------------------------
    # URL-ENCODE PASSWORD IF IT INVOLVES SPECIAL CHARACTERS (CRITICAL FIX)
    # ----------------------------------------------------------------------

    encoded_password = quote_plus(password)

    logger.info(
        f"Postgres Config Loaded | host={pg_config['host']} | port={pg_config['port']} | "
        f"db={pg_config['database']} | user={pg_config['user']}"
    )

    # =========================
    # BUILD CONNECTION STRING
    # =========================

    try:
        connection_url = (
            f"postgresql+psycopg2://{user}:{encoded_password}"
            f"@{host}:{port}/{database}"
        )

        engine = create_engine(connection_url)

        # Test connection explicitly
        with engine.connect() as conn:
            logger.info("PostgreSQL connection established successfully")

    except Exception:
        logger.exception("Failed to establish PostgreSQL connection")
        raise

    # =========================
    # LOAD DATA based on last-watermark is not null
    # =========================

    try:
        if df.empty:
            logger.error("Empty DataFrame received for PostgreSQL load")
            raise ValueError("Cannot load empty DataFrame")

        # -------------------------
        # INCREMENTAL LOAD LOGIC
        # -------------------------

        last_watermark = get_last_watermark(
            engine=engine,
            schema=schema,
            table=table,
            logger=logger
        )

        # -------------------------
        # TYPE NORMALIZATION (CRITICAL)
        # Because we were seeing an error earlier when comparing the data from pandas "df" dataset order date
        # with sql max of order date
        # -------------------------

        if last_watermark is not None:
            last_watermark = pd.to_datetime(last_watermark)
            logger.info(
                f"Watermark/Incremental value normalized to pandas Timestamp: {last_watermark}"
            )

        if last_watermark:
            original_count = len(df)
            df = df[df["order_date"] > last_watermark]                  # VERY IMPORTANT POINT BELOW
            """ Above line means:
            # “Give me only those rows whose order_date is greater than the last order_date that already exists in the
            # database.” And it will compare all order dates in the incoming dataframe and filter the data
            # according to dates only
            Why We NEVER Compute MAX from Incoming Data
                If you do: 
                    df["order_date"].max()
                🚨 That is WRONG for incremental loads because:
                    1. CSV might contain old data and new data as well.
                    2. File might be reprocessed
                    3. Late-arriving data exists
                    4. You lose idempotency
                📌 Production rule:    
                Incremental watermark must come from the TARGET, not the SOURCE.
            """
            logger.info(
                f"Incremental filter applied | "
                f"Before: {original_count}, After: {len(df)}"
            )

        if df.empty:
            logger.info("No new records to load. Skipping write.")
            return

        # TILL HERE, If you do not want to run incremental comment the above code between incremental to this.

        # Implementing staging table -
        staging_table = f"{table}_staging"

        df.to_sql(
            name=staging_table,                     # replaced from "table" to "staging_table"
            con=engine,
            schema=schema,
            if_exists="replace",      # Changed to "replace" from "append" & # later change this for incremental load
            index=False,
            method="multi",
            chunksize=500
        )

        logger.info("Data loaded to staging table")

        merge_sql = f"""
            INSERT INTO {schema}.{table} (
                order_id, order_date, customer_id, product,
                quantity, price, revenue, created_date, created_by
            )
            SELECT  order_id, order_date, customer_id, product,
            quantity, price, revenue, current_timestamp as created_date, 'system' as created_by
            FROM {schema}.{staging_table} ON CONFLICT ON CONSTRAINT sales_orders_pkey
            DO UPDATE SET 
                order_date = EXCLUDED.order_date,
                customer_id = EXCLUDED.customer_id,
                product = EXCLUDED.product,
                quantity = EXCLUDED.quantity,
                price = EXCLUDED.price,
                revenue = EXCLUDED.revenue,
                modified_date = current_timestamp,
                modified_by = 'system';
            """

        with engine.begin() as conn:
            conn.execute(text(merge_sql))               # This text function is a part of sqlalchemy

        logger.info(
            f"Loaded {len(df)} records into "
            f"{schema}.{table}"
        )

        # drop_sql = f"DROP TABLE IF EXISTS {schema}.{staging_table};"
        #
        # with engine.begin() as conn:
        #     conn.execute(text(drop_sql))

    except SQLAlchemyError:
        logger.exception("PostgreSQL load failed")
        raise
