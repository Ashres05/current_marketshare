from snowflake_conn import Snowflake
from pathlib import Path
import logging
from table_configs import TABLE_CONFIGS, TableConfig
from checkpoint_handler import CheckpointHandler
from datetime import datetime
# Set up logger for module
LOGGER = logging.getLogger(__name__)

class QueryHandler:
    def __init__(self,
        sf: Snowflake,
        checkpoint_handler: CheckpointHandler,
        table_configs: dict[str, TableConfig] = TABLE_CONFIGS):
        LOGGER.info("Initializing QueryHandler...")
        self._sf = sf
        self._checkpoint_handler = checkpoint_handler
        self._table_configs = table_configs
        LOGGER.info("QueryHandler initialized successfully.")

    # Main Marketshare functions

    def set_database(self) -> None:
        """Takes in Snowflake connection sets database."""
        LOGGER.info("Setting database...")
        self._sf.query(self.load_sql('set_database.sql'))
        LOGGER.info("Database set successfully.")

    def verify_schema(self) -> None:
        """
        Takes in Snowflake connection and verifies if tables are created, if not they are created.
        Done by looping through TABLE_CONFIGS.
        """
        LOGGER.info("Verifying schema...")
        for table_name, config in self._table_configs.items():
            clean_name = table_name.replace('_', ' ')
            if not self.verify_table(table_name):
                self._sf.query(self.load_sql(config.create_sql))
                LOGGER.info(f"Created {clean_name} table.")
        LOGGER.info("Schema verified successfully.")

    def run_migrations(self) -> None:
        """
        Runs all SQL files found in queries/migrations/ in alphabetical order.
        Each file must contain idempotent statements (e.g. ALTER TABLE ... ADD COLUMN IF NOT EXISTS).
        """
        LOGGER.info("Running migrations...")
        migrations_dir = Path(__file__).parent / 'queries' / 'migrations'
        migration_files = sorted(migrations_dir.glob('*.sql'))
        for migration_file in migration_files:
            LOGGER.info(f"Applying migration: {migration_file.name}...")
            sql = migration_file.read_text(encoding='utf-8')
            for statement in sql.split(';'):
                statement = statement.strip()
                if statement:
                    self._sf.query(statement)
            LOGGER.info(f"Migration {migration_file.name} applied successfully.")
        LOGGER.info("Migrations complete.")

    BACKFILL_DATE = datetime(2023, 12, 29) # 2024 Luminate year start date

    def update_tables(self) -> None:
        """Takes in Snowflake connection and updates tables. Done by looping through TABLE_CONFIGS."""
        LOGGER.info("Updating tables...")
        for table_name, config in self._table_configs.items():
            clean_name = table_name.replace('_', ' ')
            if self.verify_table(table_name):
                if config.use_checkpoint:
                    date = self._checkpoint_handler.get_checkpoint_date(table_name)
                    if date is None:
                        LOGGER.warning(f"Checkpoint date for {clean_name} is None... backfilling from {self.BACKFILL_DATE.date()}.")
                        self._run_update_sql(self.load_sql(config.update_sql, checkpoint_date=self.BACKFILL_DATE))
                        LOGGER.info(f"Updated {clean_name} table.")
                    else:
                        self._run_update_sql(self.load_sql(config.update_sql, checkpoint_date=date))
                        LOGGER.info(f"Updated {clean_name} table.")
                else:
                    self._run_update_sql(self.load_sql(config.update_sql))
                    LOGGER.info(f"Updated {clean_name} table.")
            else:
                LOGGER.warning(f"Table {clean_name} does not exist... failed to populate.")
        self._checkpoint_handler.update_checkpoint()
        LOGGER.info("Tables updated successfully.")

    def _run_update_sql(self, sql: str) -> None:
        """
        Execute one or more statements from an update SQL file.
        Split on ';' like migrations so files can pair DELETE + MERGE
        (Snowflake has no WHEN NOT MATCHED BY SOURCE).
        Line comments are stripped before splitting so ';' inside -- comments
        does not truncate statements.
        """
        cleaned_lines = []
        for line in sql.splitlines():
            in_single = False
            out = []
            i = 0
            while i < len(line):
                ch = line[i]
                # Start of -- comment outside a string: drop rest of line
                if not in_single and ch == '-' and i + 1 < len(line) and line[i + 1] == '-':
                    break
                if ch == "'" and not in_single:
                    in_single = True
                    out.append(ch)
                elif ch == "'" and in_single:
                    # Handle escaped '' inside Snowflake strings
                    if i + 1 < len(line) and line[i + 1] == "'":
                        out.append("''")
                        i += 2
                        continue
                    in_single = False
                    out.append(ch)
                else:
                    out.append(ch)
                i += 1
            cleaned_lines.append(''.join(out))
        cleaned_sql = '\n'.join(cleaned_lines)

        for statement in cleaned_sql.split(';'):
            statement = statement.strip()
            if statement:
                self._sf.query(statement)
    
    # Helper Marketshare functions

    def load_sql(self, file_name: str, *, checkpoint_date: datetime = None) -> str:
        """
        Takes in file name and goes into the queries folder to return the file as text.
        If checkpoint_date is provided, it will be used to filter the data.
        """
        LOGGER.info(f"Loading SQL file {file_name}...")
        current_dir = Path(__file__).parent
        path = current_dir / 'queries' / file_name
        sql = path.read_text(encoding='utf-8')
        if checkpoint_date is not None:
            sql = sql.replace('{checkpoint_date}', checkpoint_date.strftime('%Y-%m-%d'))
        LOGGER.info(f"SQL file {file_name} loaded successfully.")
        return sql


    def verify_table(self, file_name: str) -> bool:
        """Takes file name and returns whether it exists in CURRENT_DEV.DATA schema"""
        LOGGER.info(f"Verifying table {file_name}...")
        sql = self.load_sql('verify_current_table.sql')
        sql = sql.replace('{placeholder}', file_name.upper())
        df = self._sf.query(sql)
        LOGGER.info(f"Table {file_name} verified successfully.")
        return bool(df.iloc[0,0])
