from pyspark.sql import SparkSession, DataFrame
import logging
import os

class SparkProcessor:
    def __init__(self, app_name="AdvertiseXDataProcessing"):
        """
        Initialize Spark session for processing.

        :param app_name: Name of the Spark application
        """
        logging.info("Initializing Spark session")
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "16g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.hive.convertMetastoreParquet", "false") \
            .config("spark.driver.extraJavaOptions", f"-Djava.library.path={os.getenv('SPARK_HOME')}/jars") \
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hive") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", os.path.join(os.getcwd(), 'warehouse')) \
            .config("spark.sql.defaultCatalog", "local") \
            .getOrCreate()
        logging.info("Spark session initialized")

    def read_stream_from_kafka(self, topic):
        """
        Read data stream from a Kafka topic.

        :param topic: Kafka topic to read from
        :return: DataFrame containing the Kafka data stream
        """
        try:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .load()

            logging.info(f"Successfully read from Kafka topic {topic}")
            logging.info(f"Schema: {df.printSchema()}")
            logging.info(f"Data sample: {df}")

            return df
        except Exception as e:
            logging.error(f"Failed to read from Kafka topic {topic}: {e}")
            return None
