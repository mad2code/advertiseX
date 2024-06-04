import logging
import json
import csv
import os
import prometheus_client as prom
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.avro.functions import from_avro
from kafka.kafka_client import KafkaClient
from spark.spark_processor import SparkProcessor
from spark.data_quality import filter_valid_records, filter_invalid_records, deduplicate_records
from helper.config_reader import load_config
from schemas.ad_impressions import ad_impressions_schema
from schemas.clicks_conversions import clicks_conversions_schema
from schemas.bid_requests import bid_requests_schema
from schemas.attribution import create_attribution_schema

class AirflowTasks:
    def __init__(self):
        """
        Initialize Airflow tasks with loaded configuration.
        """
        self.config = load_config()
        logging.info("Configuration loaded successfully.")

    def kafka_setup(self):
        """
        Set up Kafka topics and send example messages.
        """
        logging.info("Setting up Kafka topics and sending example messages.")
        kafka_client = KafkaClient(bootstrap_servers=self.config['kafka']['bootstrap_servers'])
        kafka_client.create_topics(self.config['kafka']['topics'])
        logging.info("Kafka topics created successfully.")
        self._send_ad_impressions(kafka_client)
        self._send_clicks_conversions(kafka_client)
        self._send_bid_requests(kafka_client)

    def _send_ad_impressions(self, kafka_client):
        logging.info("Sending ad impressions to Kafka.")
        with open('input/ad_impressions.json') as f:
            ad_impressions = json.load(f)
            for record in ad_impressions:
                kafka_client.send_json_message('ad_impressions', record)
        logging.info("Ad impressions sent successfully.")

    def _send_clicks_conversions(self, kafka_client):
        logging.info("Sending clicks conversions to Kafka.")
        with open('input/clicks_conversions.csv') as f:
            reader = csv.DictReader(f)
            for row in reader:
                kafka_client.send_json_message('clicks_conversions', row)
        logging.info("Clicks conversions sent successfully.")

    def _send_bid_requests(self, kafka_client):
        logging.info("Sending bid requests to Kafka.")
        with open('input/bid_requests.json') as f:
            bid_requests = json.load(f)
            for record in bid_requests:
                kafka_client.send_avro_message('bid_requests', record, self.config['schema_path']['bid_requests'])
        logging.info("Bid requests sent successfully.")

    def spark_processing(self):
        """
        Process data streams from Kafka using Spark.
        """
        logging.info("Starting Spark processing of data streams from Kafka.")
        self.spark_processor = SparkProcessor()
        ad_impressions_df, clicks_conversions_df, bid_requests_df = self._read_streams()
        if ad_impressions_df and clicks_conversions_df and bid_requests_df:
            self._process_and_write_data(ad_impressions_df, clicks_conversions_df, bid_requests_df)
        else:
            logging.error("Failed to initialize data streams.")

    def _read_streams(self):
        logging.info("Reading data streams from Kafka.")
        ad_impressions_df = self.spark_processor.read_stream_from_kafka("ad_impressions")
        clicks_conversions_df = self.spark_processor.read_stream_from_kafka("clicks_conversions")
        bid_requests_df = self.spark_processor.read_stream_from_kafka("bid_requests")
        logging.info("Data streams read successfully.")
        return ad_impressions_df, clicks_conversions_df, bid_requests_df

    def _process_and_write_data(self, ad_impressions_df, clicks_conversions_df, bid_requests_df):
        try:
            logging.info("Processing and writing data streams.")
            valid_ad_impressions_df, invalid_ad_impressions_df = self._process_ad_impressions(ad_impressions_df)
            valid_clicks_conversions_df, invalid_clicks_conversions_df = self._process_clicks_conversions(clicks_conversions_df)
            valid_bid_requests_df, invalid_bid_requests_df = self._process_bid_requests(bid_requests_df)

            attribution_df = create_attribution_schema(valid_ad_impressions_df, valid_clicks_conversions_df, valid_bid_requests_df)

            self._write_to_iceberg(valid_ad_impressions_df, "ad_impressions")
            self._write_to_iceberg(valid_clicks_conversions_df, "clicks_conversions")
            self._write_to_iceberg(valid_bid_requests_df, "bid_requests")
            self._write_to_iceberg(attribution_df, "attribution")

            self._write_to_iceberg(invalid_ad_impressions_df, "dead_letter_ad_impressions")
            self._write_to_iceberg(invalid_clicks_conversions_df, "dead_letter_clicks_conversions")
            self._write_to_iceberg(invalid_bid_requests_df, "dead_letter_bid_requests")

            logging.info("Data streams processed and written successfully.")
        except Exception as e:
            logging.error(f"Failed to process data stream: {e}")

    def _process_ad_impressions(self, ad_impressions_df):
        logging.info("Processing ad impressions data.")
        ad_impressions_df = ad_impressions_df.selectExpr("CAST(value AS STRING)") \
                                             .select(from_json(col("value"), ad_impressions_schema).alias("data")) \
                                             .select("data.*")
        valid_ad_impressions_df = filter_valid_records(ad_impressions_df, col("ad_creative_id").isNotNull() & col("user_id").isNotNull())
        invalid_ad_impressions_df = filter_invalid_records(ad_impressions_df, col("ad_creative_id").isNull() | col("user_id").isNull())
        valid_ad_impressions_df = deduplicate_records(valid_ad_impressions_df, ["user_id", "timestamp"])
        logging.info("Ad impressions data processed successfully.")
        return valid_ad_impressions_df, invalid_ad_impressions_df

    def _process_clicks_conversions(self, clicks_conversions_df):
        logging.info("Processing clicks conversions data.")
        clicks_conversions_df = clicks_conversions_df.selectExpr("CAST(value AS STRING)") \
                                                     .select(from_json(col("value"), clicks_conversions_schema).alias("data")) \
                                                     .select("data.*")
        valid_clicks_conversions_df = filter_valid_records(clicks_conversions_df, col("user_id").isNotNull())
        invalid_clicks_conversions_df = filter_invalid_records(clicks_conversions_df, col("user_id").isNull())
        valid_clicks_conversions_df = deduplicate_records(valid_clicks_conversions_df, ["user_id", "timestamp"])
        logging.info("Clicks conversions data processed successfully.")
        return valid_clicks_conversions_df, invalid_clicks_conversions_df

    def _process_bid_requests(self, bid_requests_df):
        logging.info("Processing bid requests data.")
        bid_requests_df = bid_requests_df.selectExpr("CAST(value AS BINARY)") \
                                         .select(from_avro(col("value"), bid_requests_schema).alias("data")) \
                                         .select("data.*")
        valid_bid_requests_df = filter_valid_records(bid_requests_df, col("user_id").isNotNull())
        invalid_bid_requests_df = filter_invalid_records(bid_requests_df, col("user_id").isNull())
        valid_bid_requests_df = deduplicate_records(valid_bid_requests_df, ["user_id", "timestamp"])
        logging.info("Bid requests data processed successfully.")
        return valid_bid_requests_df, invalid_bid_requests_df

    def _write_to_iceberg(self, df, table_name):
        try:
            logging.info(f"Writing data to Iceberg table {table_name}.")
            df.writeStream \
                .format("iceberg") \
                .outputMode("append") \
                .option("checkpointLocation", f"{self.config['iceberg']['checkpoint_path']}/{table_name}") \
                .option("path", f"hdfs://nn:8020/warehouse/{table_name}") \
                .start

            logging.info(f"Data written to Iceberg table {table_name} successfully.")
        except Exception as e:
            logging.error(f"Failed to write data to Iceberg table {table_name}: {e}")

    def read_and_log_iceberg_data(self):
        """
        Read and log data from Iceberg tables.
        """
        logging.info("Reading and logging data from Iceberg tables.")
        self.spark_processor = SparkProcessor()
        self._read_and_log_table('ad_impressions')
        self._read_and_log_table('clicks_conversions')
        self._read_and_log_table('bid_requests')
        self._read_and_log_table('attribution')
        logging.info("Data from Iceberg tables read and logged successfully.")

    def _read_and_log_table(self, table_name):
        try:
            df = self.spark_processor.spark.read.format("iceberg") \
                    .load(f"{self.config['iceberg']['table_path']}/{table_name}") 
            logging.info(f"{table_name.replace('_', ' ').title()} Data:")
            df.show(truncate=False)
        except Exception as e:
            logging.error(f"Failed to read Iceberg data for {table_name}: {e}")

    def monitor_data_quality(self):
        """
        Monitor data quality using Prometheus metrics.
        """
        logging.info("Monitoring data quality using Prometheus metrics.")
        try:
            error_count = 0
            # Example error checking logic
            if error_count > 0:
                prom.Counter('data_quality_errors', 'Count of data quality errors').inc(error_count)
                logging.error(f"Data quality errors detected: {error_count}")
                raise ValueError("Data quality errors detected")
        except Exception as e:
            logging.error(f"Failed to monitor data quality: {e}")
        logging.info("Data quality monitoring completed successfully.")
