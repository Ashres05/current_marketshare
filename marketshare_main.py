from snowflake_conn import get_snowflake_connection
from query_marketshare import set_database, verify_schema, update_tables

def main() -> None:
    print("Getting Snowflake connection...")
    with get_snowflake_connection() as sf:
        print("Success!")

        set_database(sf)
        print("Database set.")

        verify_schema(sf)
        print("Schema verified.")

        update_tables(sf)
        print("Tables populated.")

        print("Closed!")
        return

if __name__ == "__main__":
    main()
