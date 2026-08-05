# EC2 runs Python 3.9; defers annotation evaluation so PEP 604 unions parse there.
from __future__ import annotations

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from snowflake_conn import Snowflake
from table_configs import TableConfig, TABLE_CONFIGS
import logging

# Set up logger for module
LOGGER = logging.getLogger(__name__)

# Data directory and checkpoint file
DATA_DIR = Path(__file__).parent / 'data'
CHECKPOINT_FILE = DATA_DIR / 'checkpoint.json'

class CheckpointHandler:
    def __init__(self,
        _sf: Snowflake,
        checkpoint_file: str = CHECKPOINT_FILE,
        table_configs: dict[str, TableConfig] = TABLE_CONFIGS):
        LOGGER.info("Initializing CheckpointHandler...")
        self._checkpoint_file = Path(checkpoint_file)
        self._sf = _sf
        self._table_configs = table_configs
        LOGGER.info("CheckpointHandler initialized successfully.")

    def create_checkpoint(self) -> None:
        """Create a checkpoint file and fill it if missing or empty."""
        LOGGER.info("Creating checkpoint file...")
        if not self._checkpoint_file.exists():
            try:
                DATA_DIR.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                raise CheckpointError(f"Failed to create data directory: {e}")
            try:
                self._checkpoint_file.write_text('{}', encoding='utf-8')
            except Exception as e:
                raise CheckpointError(f"Failed to create checkpoint file: {e}")
            self._run_checkpoint_fill()
            LOGGER.info("Checkpoint file created successfully.")
        elif self._checkpoint_file.stat().st_size == 0:
            self._checkpoint_file.write_text('{}', encoding='utf-8')
            self._run_checkpoint_fill()
            LOGGER.info("Empty checkpoint file re-seeded successfully.")

    def get_update_dates(self) -> dict[str, str]:
        """Get the update dates for each table as ISO date strings."""
        LOGGER.info("Getting update dates...")
        if self._needs_checkpoint_create():
            self.create_checkpoint()
        with open(self._checkpoint_file, 'r') as f:
            update_dates: dict[str, str] = json.load(f)
        LOGGER.info("Update dates retrieved successfully.")
        return update_dates

    def update_checkpoint(self) -> None:
        """Update the checkpoint file with the latest date for each table."""
        LOGGER.info("Updating checkpoint file...")
        if self._needs_checkpoint_create():
            self.create_checkpoint()
        else:
            self._run_checkpoint_fill()
        LOGGER.info("Checkpoint file updated successfully.")

    def get_checkpoint_date(self, table_name: str) -> datetime | None:
        """Get the checkpoint date for a table. Returns None if not set or table is empty."""
        LOGGER.info(f"Getting checkpoint date for table {table_name}...")
        try:
            if self._needs_checkpoint_create():
                self.update_checkpoint()
            with open(self._checkpoint_file, 'r') as f:
                update_dates: dict[str, str] = json.load(f)
            value = update_dates.get(table_name)
            if value is None:
                LOGGER.info(f"No checkpoint date found for {table_name}.")
                return None
            LOGGER.info(f"Checkpoint date retrieved for {table_name}.")
            return datetime.strptime(value, '%Y-%m-%d')
        except Exception as e:
            raise CheckpointError(f"Failed to get checkpoint date for {table_name}: {e}")

    def _needs_checkpoint_create(self) -> bool:
        """Return True if the checkpoint file is missing or empty."""
        if not self._checkpoint_file.exists() or self._checkpoint_file.stat().st_size == 0:
            return True
        return False

    def _run_checkpoint_fill(self) -> None:
        """Query MAX(week_ending_date) for each checkpoint table and persist to file."""
        try:
            LOGGER.info("Running checkpoint fill...")
            with open(self._checkpoint_file, 'r') as f:
                update_dates: dict[str, str] = json.load(f)

            for table_name, table_config in self._table_configs.items():
                if table_config.use_checkpoint:
                    sql = f"SELECT MAX(week_ending_date) FROM CURRENT_DEV.DATA.{table_name.upper()}"
                    df = self._sf.query(sql)
                    val = df.iloc[0, 0]
                    if val is None or pd.isna(val):
                        update_dates[table_name] = None
                    else:
                        update_dates[table_name] = pd.to_datetime(val).strftime('%Y-%m-%d')

            with open(self._checkpoint_file, 'w') as f:
                json.dump(update_dates, f, indent=4)
            LOGGER.info("Checkpoint fill completed successfully.")
        except Exception as e:
            raise CheckpointError(f"Failed to run checkpoint fill: {e}")


class CheckpointError(Exception):
    """Exception raised for errors in the checkpoint file."""
    pass
