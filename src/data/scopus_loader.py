import json
from typing import Dict, List, Any, Generator, Iterator
from pathlib import Path
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class StreamingScopusLoader:
    def __init__(self, base_path: str = "data/scopus", chunk_size: int = 1000):
        """
        Initialize the streaming loader with configurable chunk size.
        
        Args:
            base_path: Path to the scopus data directory
            chunk_size: Number of records to yield in each batch
        """
        self.base_path = Path(base_path)
        self.chunk_size = chunk_size
        
    def stream_files(self) -> Generator[Path, None, None]:
        """
        Yield files path one at a time instead of loading all paths into memory.
        """
        for year_dir in sorted(self.base_path.iterdir()):
            if year_dir.is_dir():
                logger.info(f"Processing year directory: {year_dir.name}")
                for file_path in year_dir.iterdir():
                    if file_path.is_file() and not file_path.name.startswith("."): 
                        yield file_path
                        
    def stream_records(self, file_path: Path) -> Iterator[Dict]:
        """
        Streams records from a single file using json streaming parser.
        This will avoid loading the actual entire file in the memory ?
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                # Trying to read opening bracket '{'
                char = file.read(1)
                if not char == '{':
                    raise ValueError(f"Expected '{{' at start of file, got {char}")
                
                buffer = ""
                bracket_count = 1
                
                # Stream the character by character
                while char:
                    char = file.read(1)
                    if not char: # Check for EOF
                        break
                    buffer += char
                    if char == "{":
                        bracket_count += 1
                    elif char == "}":
                        bracket_count -= 1
                        
                    if bracket_count == 0:
                        try:
                            yield json.loads(buffer)
                        except json.JSONDecodeError as e:
                            logger.error(f"Error decoding JSON from: {file_path}: {str(e)}")
                            
        except Exception as e:
            logger.error(f"Error reading file: {file_path}: {str(e)}")
            raise
        
    def process_data(self) -> Generator[List[Dict], None, None]:
        """
        Process the data and yield in chunks of size `chunk_size`.
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