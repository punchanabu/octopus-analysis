import json
from typing import Dict, List, Any
from src.utils.logger import setup_logger
from pathlib import Path

logger = setup_logger(__name__)

class ScopusLoader:
    def __init__(self, data_path: str = "data/scopus_data.json"):
        self.data_path = Path(data_path)

    def load_data(self) -> List[Dict[str, Any]]:
        try:
            with open(self.data_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            logger.info(f"Successfully loaded {len(data)} records from {self.data_path}")
            return data
        except FileNotFoundError:
            logger.error(f"Data file not found at {self.data_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading data: {str(e)}")
            raise