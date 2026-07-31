from pathlib import Path
import os
import logging
from logging.handlers import RotatingFileHandler

# Constants for logs directory
LOGS_DIR_NAME = 'logs'
MARKETSHARE_LOG_FILE = 'marketshare_current.log'

def setup_logging():
    """Configures logging to write to both a file and the console."""
    project_dir = Path(__file__).parent
    configured_dir = os.getenv("MARKETSHARE_LOG_DIR")
    ec2_logs_dir = project_dir.parent / "scripts" / LOGS_DIR_NAME

    if configured_dir:
        log_dir = Path(configured_dir).expanduser()
    elif ec2_logs_dir.exists() and os.access(ec2_logs_dir, os.W_OK):
        log_dir = ec2_logs_dir
    else:
        log_dir = project_dir / LOGS_DIR_NAME

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
