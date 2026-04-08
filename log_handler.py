from pathlib import Path
import os
import logging
from logging.handlers import RotatingFileHandler

# Constants for logs directory
LOGS_DIR_NAME = 'logs'
MARKETSHARE_LOG_FILE = 'marketshare_current.log'

def setup_logging():
    """Configures logging to write to both a file and the console."""
    target_dir = Path(__file__).parent.parent / LOGS_DIR_NAME
    
    # Use environment variable if exists, otherwise default to 'logs' in the current directory
    if target_dir.exists() and os.access(target_dir, os.W_OK):
        log_dir = target_dir
    else:
        log_dir = Path(__file__).parent / LOGS_DIR_NAME
        log_dir.mkdir(parents=True, exist_ok=True)
        
    log_file = log_dir / MARKETSHARE_LOG_FILE
    
    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=5 * 1024 * 1024, # 5 MB
        backupCount=1,             
        encoding='utf-8'
    )
    
    console_handler = logging.StreamHandler()
    
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Configure the global logging rules
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler
        ]
    )

    # Snowflake connector can be chatty at WARNING for transient network hiccups.
    # Keep these visible as ERROR+ so real failures still surface.
    logging.getLogger("snowflake.connector.vendored.urllib3").setLevel(logging.ERROR)
    logging.getLogger("snowflake.connector.vendored.urllib3.connectionpool").setLevel(logging.ERROR)
