from snowflake_conn import Snowflake
from pathlib import Path
from dataclasses import dataclass
import logging

@dataclass
class TableConfig:
    create_sql: str
    update_sql: str

# Dictionary of table name configurations, maps to .sql queries in folder
TABLE_CONFIGS = {
    'marketshare_map_label_hierarchy': TableConfig(
        create_sql='create_map_label_hierarchy.sql', 
        update_sql='update_map_label_hierarchy.sql'
    ),
    'marketshare_map_icpns': TableConfig(
        create_sql='create_map_icpns.sql', 
        update_sql='update_map_icpns.sql'
    ),
    'marketshare_map_isrcs': TableConfig(
        create_sql='create_map_isrcs.sql', 
        update_sql='update_map_isrcs.sql'
    ),
    'marketshare_weekly': TableConfig(
        create_sql='create_marketshare_weekly.sql', 
        update_sql='update_marketshare_weekly.sql'
    ),
    'marketshare_ytd': TableConfig(
        create_sql='create_marketshare_ytd.sql', 
        update_sql='update_marketshare_ytd.sql'
    ),
    'marketshare_forecasts': TableConfig(
        create_sql='create_marketshare_forecasts.sql', 
        update_sql='update_marketshare_forecasts.sql'
    )
}

# Set up logger for module
LOGGER = logging.getLogger(__name__)

# Main Marketshare functions
def set_database(_sf: Snowflake) -> None:
    """Takes in Snowflake connection sets database."""
    _sf.query(load_sql('set_database.sql'))

def verify_schema(_sf: Snowflake) -> None:
    """
    Takes in Snowflake connection and verifies if tables are created, if not they are created.
    Done by looping through TABLE_CONFIGS.
    """
    for table_name, config in TABLE_CONFIGS.items():
        clean_name = table_name.replace('_', ' ')
        if not verify_table(_sf, table_name):
            _sf.query(load_sql(config.create_sql))
            LOGGER.info(f"Created {clean_name} table.")

def update_tables(_sf: Snowflake) -> None:
    """Takes in Snowflake connection and updates tables. Done by looping through TABLE_CONFIGS."""
    for table_name, config in TABLE_CONFIGS.items():
        clean_name = table_name.replace('_', ' ')
        
        if verify_table(_sf, table_name):
            _sf.query(load_sql(config.update_sql))
            LOGGER.info(f"Updated {clean_name} table.")
        else:
            LOGGER.warning(f"Table {clean_name} does not exist... failed to populate.")

# Helper Marketshare functions

def load_sql(file_name: str) -> str:
    """Takes file name and goes into the queries folder to return the file as text."""
    current_dir = Path(__file__).parent
    path = current_dir / 'queries' / file_name
    return path.read_text(encoding='utf-8')


def verify_table(_sf: Snowflake, file_name: str) -> bool:
    """Takes file name and returns whether it exists in CURRENT_DEV.DATA schema"""
    sql = load_sql('verify_current_table.sql')
    sql = sql.replace('{placeholder}', file_name.upper())
    df = _sf.query(sql)
    return bool(df.iloc[0,0])
