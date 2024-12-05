from pyspark.sql.functions import col, explode_outer, to_date, concat_ws
from pyspark.sql.types import IntegerType

class BaseTransformer:
    def apply_common_transforms(self, df, schema_map, extra_transforms=None):
        """
        Apply common transformations based on the schema map and extra transforms.
        Args:
            df: Input DataFrame
            schema_map: A dictionary mapping source columns to alias names
            extra_transforms: Additional transformations specific to the subclass
        Returns:
            Transformed DataFrame
        """
        for src, alias in schema_map.items():
            df = df.withColumn(alias, col(src))
        
        if extra_transforms:
            for transform in extra_transforms:
                df = transform(df)

        return df

    def apply_common_filters(self, df):
        return df.filter(col("doi").isNotNull())