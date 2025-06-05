"""Load CSV query for valid_emp"""

# Dependencies
import pandas as pd
from base_data_project.data_manager.managers.base import BaseDataManager
from base_data_project.log_config import get_logger

# Local stuff
from src.config import PROJECT_NAME, CONFIG

logger = get_logger(PROJECT_NAME)

def valid_emp():
    """Load function for valid emp query"""

    try:
        file_path_fallback = ''
        file_path = CONFIG.get('dummy_data_filepaths', {}).get('valid_emp', file_path_fallback)
        df = pd.read_csv(file_path)

    except FileNotFoundError :
        logger.error(f"File not found error: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading valid_emp: {e}")
        raise

    return df