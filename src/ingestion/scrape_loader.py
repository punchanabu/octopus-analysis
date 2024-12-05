import orjson as json
from typing import Dict, List, Generator, Iterator
from pathlib import Path
from src.utils.logger import setup_logger
import os

logger = setup_logger(__name__)

class ScrapeDataLoader:
    def __init__(self, base_path: str = None, chunk_size: int = 1000):
        """
        Initialize the scrape data loader with configurable chunk size.
        
        Args:
            base_path: Path to the scrape data directory
            chunk_size: Number of records to yield in each batch
        """
        self.base_path = Path(base_path or os.getenv("SCRAPE_BASE_PATH", "./scrape/scopus"))
        self.chunk_size = chunk_size

    def stream_files(self) -> Generator[Path, None, None]:
        """
        Yield file paths from the scrape data directory one at a time.
        """
        for year_dir in sorted(self.base_path.iterdir()):
            if year_dir.is_dir():
                logger.info(f"Processing year directory: {year_dir.name}")
                for file_path in year_dir.iterdir():
                    if file_path.is_file() and file_path.suffix == ".json":
                        yield file_path

    def stream_records(self, file_path: Path) -> Iterator[Dict]:
        """
        Stream individual records from a JSON file.
        
        Args:
            file_path: Path to the JSON file containing the scraped data
        """
        if file_path.stat().st_size == 0:  # Skip empty files
            logger.warning(f"Skipping empty file: {file_path}")
            return
        try:
            with open(file_path, "r", encoding="utf-8", buffering=1024 * 1024) as file:
                try:
                    records = json.loads(file.read())
                    if isinstance(records, list):
                        for record in records:
                            yield record
                    else:
                        logger.warning(f"Expected list of records in {file_path}, got: {type(records)}")
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON from: {file_path}: {str(e)}")
        except Exception as e:
            logger.error(f"Error reading file: {file_path}: {str(e)}")
            raise

    def process_data(self) -> Generator[List[Dict], None, None]:
        """
        Process the scrape data and yield in chunks of size `chunk_size`.
        """
        current_chunk = []

        for file_path in self.stream_files():
            try:
                for record in self.stream_records(file_path):
                    current_chunk.append(record)
                    if len(current_chunk) >= self.chunk_size:
                        logger.info(f"Yielding chunk of size: {len(current_chunk)}")
                        yield current_chunk
                        current_chunk = []
            except Exception as e:
                logger.error(f"Error processing file: {file_path}: {str(e)}")
                continue

        if current_chunk:
            logger.info(f"Yielding last chunk of size: {len(current_chunk)}")
            yield current_chunk
