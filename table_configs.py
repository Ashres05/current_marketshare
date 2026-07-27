from dataclasses import dataclass

@dataclass
class TableConfig:
    create_sql: str
    update_sql: str
    use_checkpoint: bool = False # If True, will use data/checkpoint.json to track the last updated date for the table

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
    ),
    'marketshare_forecasts_weekly': TableConfig(
        create_sql='create_marketshare_forecasts_weekly.sql', 
        update_sql='update_marketshare_forecasts_weekly.sql'
    ),
    'marketshare_weekly_albums': TableConfig(
        create_sql='create_marketshare_weekly_albums.sql', 
        update_sql='update_marketshare_weekly_albums.sql',
        use_checkpoint=True
    ),
    'marketshare_weekly_artists': TableConfig(
        create_sql='create_marketshare_weekly_artists.sql',
        update_sql='update_marketshare_weekly_artists.sql'
    )
}