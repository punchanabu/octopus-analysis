from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.utils.logger import setup_logger
from src.spark.transformations import transform_scopus_data

logger = setup_logger(__name__)

class SparkStreamingJob:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("ScopusAnalysis") \
            .getOrCreate()
        
        self.schema = StructType([
            StructField("abstracts-retrieval-response", StructType([
                StructField("coredata", StructType([
                    StructField("dc:title", StringType()),
                    StructField("dc:description", StringType()),
                    StructField("prism:doi", StringType()),
                    StructField("citedby-count", IntegerType())
                ])),
                StructField("authors", StructType([
                    StructField("author", StringType())
                ]))
            ]))
        ])

    def start_streaming(self):
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "scopus_data") \
                .load()

            parsed_df = df.select(
                from_json(col("value").cast("string"), self.schema).alias("data")
            )

            transformed_df = transform_scopus_data(parsed_df)

            query = transformed_df \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .start()

            query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in Spark streaming: {str(e)}")
            raise