import json
from typing import Dict, List, Any
from pathlib import Path
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class ScopusLoader:
    def __init__(self, base_path: str = "data/scopus"):
        # Initialize with the base path to the scopus data directory
        self.base_path = Path(base_path)

    def _read_json_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Reads a single JSON file and returns its contents.
        Handles files without .json extension.
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON in {file_path}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise

    def load_data(self, start_year: int = None, end_year: int = None) -> List[Dict[str, Any]]:
        """
        Loads Scopus data from all years or specified year range.
        Parameters:
            start_year: Optional starting year to load data from
            end_year: Optional end year to load data until
        Returns:
            List of dictionaries containing Scopus data
        """
        all_data = []
        
        try:
            # Get all year directories
            year_dirs = sorted([d for d in self.base_path.iterdir() if d.is_dir()])
            
            # Filter years if range is specified
            if start_year or end_year:
                year_dirs = [
                    d for d in year_dirs 
                    if (not start_year or int(d.name) >= start_year) and
                       (not end_year or int(d.name) <= end_year)
                ]

            # Process each year directory
            for year_dir in year_dirs:
                logger.info(f"Processing year directory: {year_dir.name}")
                
                # Get all files in the year directory (excluding hidden files)
                json_files = [f for f in year_dir.iterdir() if f.is_file() and not f.name.startswith('.')]
                
                # Process each file
                for file_path in json_files:
                    try:
                        data = self._read_json_file(file_path)
                        # Add year and filename metadata to the data
                        data['_metadata'] = {
                            'year': year_dir.name,
                            'source_file': file_path.name
                        }
                        all_data.append(data)
                        logger.info(f"Successfully loaded data from {file_path}")
                    except Exception as e:
                        logger.error(f"Error processing {file_path}: {str(e)}")
                        continue  # Continue with next file even if one fails

            logger.info(f"Successfully loaded {len(all_data)} total records")
            return all_data

        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise

    def get_years_available(self) -> List[str]:
        """
        Returns a list of available years in the data directory
        """
        try:
            return sorted([d.name for d in self.base_path.iterdir() if d.is_dir()])
        except Exception as e:
            logger.error(f"Error getting available years: {str(e)}")
            raise