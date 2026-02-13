# 7th File calling all modules in 1 place

from utils import load_config, setup_logging
from extract import extract_data
from transform import transform_data
from checkpoints import check_nulls, check_ranges, check_duplicates
from load_postgres import load_to_postgres
# Incremental logic is a part of load_postgres module, therefore we haven't imported the same in main module


def main():
    config = load_config()
    logger = setup_logging(
        config["log_file_path"],
        config["logging"]["level"]
    )

    try:
        df_raw = extract_data(config, logger)
        df_clean = transform_data(df_raw, logger)
        df_data_quality_null_checks = check_nulls(df_clean, df_clean.columns, logger)
        df_data_quality_range_checks = check_ranges(df_clean, logger)
        df_data_quality_duplicate_checks = check_duplicates(df_clean, logger)
        load_to_postgres(df_clean, logger)
        # data_engineering_learning_projects_salesdb

        logger.info("Project 2 – PostgreSQL ETL completed successfully")

    except Exception:
        logger.exception("Project 2 ETL failed")
        raise


if __name__ == "__main__":
    main()
