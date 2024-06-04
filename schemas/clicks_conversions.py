from pyspark.sql.types import StructType, StructField, StringType

# Define the schema for clicks and conversions
clicks_conversions_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("conversion_type", StringType(), True)
])
