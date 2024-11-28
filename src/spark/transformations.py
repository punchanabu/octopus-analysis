from pyspark.sql.functions import col, explode, arrays_zip
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def transform_scopus_data(df):
    try:
        return df.select(
            col("data.abstracts-retrieval-response.coredata.dc:title").alias("title"),
            col("data.abstracts-retrieval-response.coredata.dc:description").alias("abstract"),
            col("data.abstracts-retrieval-response.coredata.prism:doi").alias("doi"),
            col("data.abstracts-retrieval-response.coredata.citedby-count").alias("citations"),
            explode(
                arrays_zip(
                    "data.abstracts-retrieval-response.authors.author.ce:indexed-name",
                    "data.abstracts-retrieval-response.authors.author.ce:given-name",
                    "data.abstracts-retrieval-response.authors.author.ce:surname"
                )
            ).alias("author_info")
        ).select(
            "title",
            "abstract",
            "doi",
            "citations",
            col("author_info.0").alias("indexed_name"),
            col("author_info.1").alias("given_name"),
            col("author_info.2").alias("surname")
        )

    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise