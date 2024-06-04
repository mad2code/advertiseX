# kafka/kafka_client.py

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json
import logging

class KafkaClient:
    def __init__(self, bootstrap_servers):
        """
        Initialize Kafka client with producer and admin capabilities.

        :param bootstrap_servers: Kafka bootstrap servers
        """
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    def create_topics(self, topics):
        """
        Create Kafka topics.

        :param topics: List of topics to create
        """
        topic_list = [NewTopic(topic['name'], num_partitions=topic['num_partitions'], replication_factor=topic['replication_factor']) for topic in topics]
        fs = self.admin_client.create_topics(topic_list)
        # Wait for each operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logging.info(f"Topic {topic} created successfully.")
            except KafkaError as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logging.warning(f"Topic {topic} already exists.")
                else:
                    logging.error(f"Failed to create topic {topic}: {e}")
            except Exception as e:
                if isinstance(e.args[0], KafkaError) and e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logging.warning(f"Topic {topic} already exists.")
                else:
                    logging.error(f"Unexpected error occurred while creating topic {topic}: {e}")

    def send_json_message(self, topic, message):
        """
        Send a JSON message to a Kafka topic.

        :param topic: Kafka topic to send the message to
        :param message: Message data in JSON format
        """
        try:
            self.producer.produce(topic, json.dumps(message).encode('utf-8'))
            self.producer.flush()
            logging.info("Message sent successfully.")
        except KafkaError as e:
            logging.error(f"Failed to send message to Kafka: {e}")

    def send_avro_message(self, topic, message, schema_file):
        """
        Send an Avro message to a Kafka topic.

        :param topic: Kafka topic to send the message to
        :param message: Message data in Avro format
        :param schema_file: Path to the Avro schema file
        """
        from avro.io import DatumWriter, BinaryEncoder
        import avro.schema
        import io

        schema = avro.schema.Parse(open(schema_file).read())
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write(message, encoder)
        raw_bytes = bytes_writer.getvalue()

        try:
            self.producer.produce(topic, raw_bytes)
            self.producer.flush()
            logging.info("Message sent successfully.")
        except KafkaError as e:
            logging.error(f"Failed to send message to Kafka: {e}")
