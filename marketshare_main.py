from snowflake_conn import get_snowflake_connection
from query_marketshare import QueryHandler
from checkpoint_handler import CheckpointHandler, CheckpointError
from log_handler import setup_logging
import logging

def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
    try:
        logger.info("Getting Snowflake connection...")
        with get_snowflake_connection() as sf:
            logger.info("Snowflake connection was a success!")

            checkpoint_handler = CheckpointHandler(sf)
            qh = QueryHandler(sf, checkpoint_handler)

            qh.set_database()
            logger.info("Database set.")

            qh.verify_schema()
            logger.info("Schema verified.")

            # qh.run_migrations()
            # logger.info("Migrations applied.")

            qh.update_tables()
            logger.info("Tables populated.")

            logger.info("Closed!")
            return
    except CheckpointError as e:
        logger.error(f"A checkpoint error occurred: {e}", exc_info=True)
        print("A checkpoint error occurred. Check logs for details.")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        print("An error occurred. Check logs for details.")

if __name__ == "__main__":
    main()
