from pyspark.sql.types import StructType, StructField, StringType
from helper.utils import read_avro_schema
from helper.config_reader import load_config

config = load_config()
# Define the schema for ad impressions
ad_impressions_schema = StructType([
    StructField("ad_creative_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("website", StringType(), True)
])
