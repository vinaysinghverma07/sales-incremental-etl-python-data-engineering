# 1st File

import logging, yaml, os
from pathlib import Path

"""
If your python version doesn't have the above mentioned libraries, then install it first 
it in your existing version so that it can create virtual environment for the same as well for running the package
"""


def get_project_root() -> Path:
    """
    The below green part is known as DocString of a function, which provides the clarity for the purpose & usage of it.
    "Path" after a sign "->" is known as  return annotation or a return type hint of a function. (Shared below).

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

    Path is a return type of this function.

    Get project root directory.
    Assumes this file is located at: project_root/src/utils.py

    :return: Path
    """

    return Path(__file__).resolve().parent.parent


def load_config(config_path: str = "config/config.yaml") -> dict:
    """
    The below green part is known as DocString of a function, which provides the clarity for the purpose & usage of it.
    "dict" after a sign "->" is known as  return annotation or a return type hint of a function. (Shared below).

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

    config_path: This is the name of the parameter (variable) that the function expects to receive when it's called.
    : (colon): This separates the parameter name from its type hint.
    str: This specifies the expected type of the object that should be passed as the df argument.

    Incase no value is passed when calling function it will return the default value from YAML config file.

    :param config_path:
        config_path (str): Path to config file default value.

    :return:
        dict: Configuration dictionary.
    """

    project_root = get_project_root()
    full_config_path = project_root / config_path

    try:
        with open(full_config_path, "r") as file:
            config = yaml.safe_load(file)
            return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at {full_config_path}")


def setup_logging(log_file_path: str, level: str = "INFO") -> logging.Logger:
    """
    The below green part is known as DocString of a function, which provides the clarity for the purpose & usage of it.
    "logging.Logger" after a sign "->" is known as return annotation or return type hint of a function. (Shared below).

    About return type hint: -> a return type hint (e.g., -> pd.DataFrame AND -> logging.Logger:) is a piece of metadata
        in a function's signature that specifies what type of value the function is expected to return.

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

    In this function "log_file_path" is argument with a datatype string and "level" is also a string datatype
    parameter default value is already given incase no value is being passed, and return type of "logger" which is
    a class of library logging.

    Setup logging configuration.
    Setup logging configuration ONCE for the application/run.

    :param log_file_path:  Path to log file with a string datatype.
    :param level: Logging level with default value already given.
    :return:
    """
    project_root = get_project_root()
    full_log_path = project_root / log_file_path

    # Full_log_path value will be given at the time of calling the function.

    # Get root logger
    logger = logging.getLogger()

    # Setting value of logger variable that we defined above
    logger.setLevel(getattr(logging, level))

    if logger.handlers:
        return

    # Create log formatter Or we can setting-up formatter for logging
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )

    # File handlers (write logs to the file)
    file_handler = logging.FileHandler(full_log_path)
    file_handler.setFormatter(formatter)

    # prints logs to terminal/console or Python Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Attach handlers to root logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
