from src.spark.streaming import ScopusProcessor

if __name__ == "__main__":
    processor = ScopusProcessor()
    processor.run()