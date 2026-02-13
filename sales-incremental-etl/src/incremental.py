# 5th File

from sqlalchemy import text
from utils import load_config


def get_last_watermark(engine, schema, table, logger):
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

    Incase no value is passed when calling function it will return an error.

    This function, get last_load_date or max of order date for incremental data load into PostgreSQL.

    :param engine:
    :param schema:
    :param table:
    :param logger:
    :return:
    """

    try:
        query = text(f"SELECT MAX(order_date) FROM {schema}.{table}")

        with engine.connect() as conn:
            result = conn.execute(query).scalar()

        logger.info(f"Last watermark/incremental value fetched: {result}")
        return result

    except Exception:
        logger.exception("Failed to fetch watermark value from postgre orders table.")
        raise
