from pathlib import Path
import os
import logging
from logging.handlers import RotatingFileHandler

# Constants for logs directory
LOGS_DIR_NAME = 'logs'
MARKETSHARE_LOG_FILE = 'marketshare_main.log'

def setup_logging():
    """Configures logging to write to both a file and the console."""
    log_dir = os.getenv(LOGS_DIR_NAME)
    
    # Use environment variable if set, otherwise default to 'logs' in the current directory
    if log_dir:
        log_dir = Path(log_dir)
    else:
        log_dir = Path(__file__).parent / LOGS_DIR_NAME
    
    log_dir.mkdir(parents=True,exist_ok=True)
    
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
