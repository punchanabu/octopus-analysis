from pyspark.sql.functions import col, explode_outer, to_date, concat_ws
from pyspark.sql.types import IntegerType
from src.spark.transform.base_tranform import BaseTransformer

class ScopusTransformer(BaseTransformer):
    def apply_all_transforms(self, df):
        """Apply transformations specific to Scopus data."""
        schema_map = {
            "abstracts-retrieval-response.coredata.prism:doi": "doi",
            "abstracts-retrieval-response.coredata.dc:title": "title",
            "abstracts-retrieval-response.coredata.dc:description": "abstract",
            "abstracts-retrieval-response.coredata.subtypeDescription": "document_type",
            "abstracts-retrieval-response.coredata.prism:aggregationType": "source_type",
            "abstracts-retrieval-response.coredata.prism:coverDate": "publication_date",
            "abstracts-retrieval-response.coredata.prism:publicationName": "source_title",
            "abstracts-retrieval-response.coredata.dc:publisher": "publisher",
            "abstracts-retrieval-response.authkeywords.author-keyword": "author_keywords",
            "abstracts-retrieval-response.coredata.citedby-count": "cited_by",
            "abstracts-retrieval-response.coredata.openaccess": "open_access",
            "abstracts-retrieval-response.language.@xml:lang": "language",
            "abstracts-retrieval-response.item.ait:process-info.ait:status.@state": "status_state",
            "abstracts-retrieval-response.item.bibrecord.tail.bibliography.@refcount": "ref_count",
            "abstracts-retrieval-response.affiliation": "affiliation",
            "abstracts-retrieval-response.authors.author": "author_details",
        }

        extra_transforms = [
            lambda df: df.withColumn(
                "publication_date", to_date(col("publication_date"))
            ),
            lambda df: df.withColumn(
                "delivered_date",
                concat_ws(
                    "-",
                    col(
                        "abstracts-retrieval-response.item.ait:process-info.ait:date-delivered.@year"
                    ),
                    col(
                        "abstracts-retrieval-response.item.ait:process-info.ait:date-delivered.@month"
                    ),
                    col(
                        "abstracts-retrieval-response.item.ait:process-info.ait:date-delivered.@day"
                    ),
                ),
            ),
            lambda df: df.withColumn("subject_code", col("subject_area.@code")),
            lambda df: df.withColumn("subject_abbrev", col("subject_area.@abbrev")),
            lambda df: df.withColumn("subject_name", col("subject_area.$")),
            lambda df: df.withColumn(
                "author_given_name", col("author_details.preferred-name.ce:given-name")
            ),
            lambda df: df.withColumn(
                "author_surname", col("author_details.preferred-name.ce:surname")
            ),
        ]

        df = self.apply_common_transforms(df, schema_map, extra_transforms)
        
        conference_and_proceed = (
            (col("document_type") == "Conference Paper")
            & (col("source_type") == "Conference Proceeding")
        )
        not_conference_and_not_proceed = (
            (col("document_type") != "Conference Paper")
            & (col("source_type") != "Conference Proceeding")
        )
        
        
        df = df.filter(df, conference_and_proceed | not_conference_and_not_proceed)
        return self.apply_common_filters(df)