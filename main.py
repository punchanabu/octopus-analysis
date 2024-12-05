from src.spark.streaming import ScopusProcessor
from src.spark.scrape_streaming import ScrapeStreamingProcessor
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

def main():
    # Validate environment variables
    db_host = os.getenv("DB_HOST")
    if not db_host:
        raise EnvironmentError("Environment variables not loaded properly!")

    # Process original Scopus data
    # print("Starting Scopus data processing...")
    # scopus_processor = ScopusProcessor()
    # scopus_processor.run()
    # print("Scopus data processing completed.")

    # Process scraped Scopus data
    print("Starting scraped Scopus data processing...")
    scrape_processor = ScrapeStreamingProcessor()
    scrape_processor.run()
    print("Scraped Scopus data processing completed.")

if __name__ == "__main__":
    main()
