from snowflake_conn import get_snowflake_connection
from query_marketshare import set_database, verify_schema, update_tables
from log_handler import setup_logging
import logging

def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
    try:
        logger.info("Getting Snowflake connection...")
        with get_snowflake_connection() as sf:
            logger.info("Snowflake connection was a success!")

            set_database(sf)
            logger.info("Database set.")

            verify_schema(sf)
            logger.info("Schema verified.")

            update_tables(sf)
            logger.info("Tables populated.")

            logger.info("Closed!")
            return
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        print("An error occurred. Check logs for details.")

if __name__ == "__main__":
    main()

