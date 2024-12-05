from pyspark.sql.functions import col, explode_outer, to_date, concat_ws
from pyspark.sql.types import IntegerType
from src.spark.transform.base_tranform import BaseTransformer

class ScrapedDataTransformer(BaseTransformer):
    def apply_all_transforms(self, df):
        """Apply transformations specific to the scraped Scopus data."""
        schema_map = {
            "dc:identifier": "identifier",
            "dc:title": "title",
            "dc:creator": "creator",
            "prism:publicationName": "source_title",
            "prism:eIssn": "eissn",
            "prism:volume": "volume",
            "prism:pageRange": "page_range",
            "prism:coverDate": "publication_date",
            "prism:coverDisplayDate": "display_date",
            "prism:doi": "doi",
            "citedby-count": "cited_by",
            "affiliation": "affiliation_details",
            "prism:aggregationType": "aggregation_type",
            "subtype": "subtype",
            "subtypeDescription": "subtype_description",
            "openaccess": "open_access",
            "author": "authors",
            "source-id": "source_id",
        }

        extra_transforms = [
            lambda df: df.withColumn(
                "publication_date", to_date(col("publication_date"))
            ),
            lambda df: df.withColumn("affiliation_name", col("affiliation_details.affilname")),
            lambda df: df.withColumn("affiliation_city", col("affiliation_details.affiliation-city")),
            lambda df: df.withColumn("affiliation_country", col("affiliation_details.affiliation-country")),
            lambda df: df.withColumn("author_given_name", col("authors.ce:given-name")),
            lambda df: df.withColumn("author_surname", col("authors.ce:surname")),
        ]

        df = self.apply_common_transforms(df, schema_map, extra_transforms)
        return self.apply_common_filters(df)