from query_marketshare import get_snowflake_connection

def main():
    with get_snowflake_connection() as sf:
        pass

if __name__ == "__main__":
    main()
