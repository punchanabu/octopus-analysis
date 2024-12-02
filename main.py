from src.spark.streaming import ScopusProcessor
import os
from dotenv import load_dotenv

# Load .env file 
load_dotenv()

if __name__ == "__main__":
    # Check if .env are successfully loaded
    db_host = os.getenv("DB_HOST")
    if not db_host:
        raise EnvironmentError("Environment variables not loaded properly!")
    processor = ScopusProcessor()
    processor.run()