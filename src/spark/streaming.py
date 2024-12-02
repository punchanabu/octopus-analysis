from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from src.ingestion.scopus_loader import StreamingScopusLoader
from pyspark.sql.functions import col, explode_outer, to_date, concat_ws
from src.utils.logger import setup_logger
from cassandra.cluster import Cluster
import json
import os
logger = setup_logger(__name__)

class ScopusProcessor:
    def __init__(self):
        
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "9042")
        db_cluster = os.getenv("DB_CLUSTER", "localhost")
        
        self.spark = SparkSession.builder \
            .appName("ScopusAnalysis") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
            .config("spark.cassandra.connection.host", db_host) \
            .config("spark.cassandra.connection.port", db_port) \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
            
        cluster = Cluster([db_cluster])
        session = cluster.connect()

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS scopus_data
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS scopus_data.records (
                doi text PRIMARY KEY,
                title text,
                abstract text,
                document_type text,
                source_type text,
                publication_date text,
                source_title text,
                publisher text,
                author_keywords text,
                subject_code text,
                subject_name text,
                subject_abbrev text,
                author_given_name text,
                author_surname text,
                author_url text,
                author_affiliation text,
                author_details text,
                funding_details text,
                ref_count int,
                open_access boolean,
                affiliation text,
                language text,
                cited_by int,
                status_state text,
                delivered_date text,
                subject_area text
            )
        """)

        session.shutdown()

        self.loader = StreamingScopusLoader(chunk_size=1000)
        self.loader.rename_files_to_json()


    def process_chunk(self, chunk):
        try:
            # Convert each record to JSON string
            json_strings = [json.dumps(record) for record in chunk]
            # Create an RDD from the JSON strings
            rdd = self.spark.sparkContext.parallelize(json_strings)
            # Read JSON data into DataFrame with inferred schema
            df = self.spark.read.json(rdd)
    
            # Transform data
            transformed_df = df.select(
                col("abstracts-retrieval-response.coredata.prism:doi").alias("doi"),
                col("abstracts-retrieval-response.coredata.dc:title").alias("title"),
                col("abstracts-retrieval-response.coredata.dc:description").alias("abstract"),
                col("abstracts-retrieval-response.coredata.subtypeDescription").alias("document_type"),
                col("abstracts-retrieval-response.coredata.prism:aggregationType").alias("source_type"),
                to_date(col("abstracts-retrieval-response.coredata.prism:coverDate")).alias("publication_date"),
                col("abstracts-retrieval-response.coredata.prism:publicationName").alias("source_title"),
                col("abstracts-retrieval-response.coredata.dc:publisher").alias("publisher"),
                col("abstracts-retrieval-response.authkeywords.author-keyword").alias("author_keywords"),
                explode_outer(col("abstracts-retrieval-response.subject-areas.subject-area")).alias("subject_area"),
                col("abstracts-retrieval-response.coredata.citedby-count").cast(IntegerType()).alias("cited_by"),
                col("abstracts-retrieval-response.coredata.openaccess").cast(IntegerType()).alias("open_access"),
                col("abstracts-retrieval-response.language.@xml:lang").alias("language"),
                col("abstracts-retrieval-response.item.ait:process-info.ait:status.@state").alias("status_state"),
                col("abstracts-retrieval-response.item.bibrecord.tail.bibliography.@refcount").cast(IntegerType()).alias("ref_count"),
                concat_ws("-", 
                    col("abstracts-retrieval-response.item.ait:process-info.ait:date-delivered.@year"),
                    col("abstracts-retrieval-response.item.ait:process-info.ait:date-delivered.@month"),
                    col("abstracts-retrieval-response.item.ait:process-info.ait:date-delivered.@day")
                ).alias("delivered_date"),
                col("abstracts-retrieval-response.affiliation").alias("affiliation"),
                explode_outer(col("abstracts-retrieval-response.authors.author")).alias("author_details"),
                col("abstracts-retrieval-response.item.xocs:meta.xocs:funding-list").alias("funding_details"),
            ).select(
                "*",
                col("subject_area.@code").alias("subject_code"),
                col("subject_area.@abbrev").alias("subject_abbrev"),
                col("subject_area.$").alias("subject_name"),
                col("author_details.preferred-name.ce:given-name").alias("author_given_name"),
                col("author_details.preferred-name.ce:surname").alias("author_surname"),
                col("author_details.author-url").alias("author_url"),
                col("author_details.affiliation").alias("author_affiliation")
            )
    
            transformed_df = transformed_df.filter(col("doi").isNotNull())
    
            # Write to Cassandra
            transformed_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="records", keyspace="scopus_data") \
                .mode("append") \
                .save()

            transformed_df.unpersist()
            
            logger.info("Successfully wrote chunk to Cassandra")
    
        except Exception as e:
            logger.error(f"Error processing chunk: {str(e)}")# Log first record in the chunk for debugging
            raise
        
        
    def run(self):
        try:
            logger.info("Starting data processing pipeline...")

            for chunk in self.loader.process_data():
                self.process_chunk(chunk)

            logger.info("Pipeline completed successfully")

        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}")
            raise

        finally:
            self.spark.stop()


