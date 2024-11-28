from src.spark.streaming import SparkStreamingJob
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def run_spark_job(**context):
    try:
        spark_job = SparkStreamingJob()
        spark_job.start_streaming()
        return "Spark streaming jobs successfully started"
    except Exception as e:
        logger.error(f"Error while running spark job: {e}")
        raise