import sys
import os

# Print Python version and path
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")

# Ensure the current directory is in the sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Print sys.path for debugging
print("sys.path:")
for path in sys.path:
    print(path)

from kafka.kafka_client import KafkaClient
from helper.config_reader import load_config

# Load configuration
config = load_config()

# Kafka setup and send example messages
kafka_client = KafkaClient(bootstrap_servers=config['kafka']['bootstrap_servers'])
kafka_client.create_topics(config['kafka']['topics'])

# Example JSON data for ad impressions
ad_impression = {"ad_creative_id": "123", "user_id": "abc", "timestamp": "2024-05-31T12:00:00Z", "website": "example.com"}
kafka_client.send_json_message('ad_impressions', ad_impression)

# Example CSV data for clicks and conversions
click_conversion = "2024-05-31T12:00:00Z,abc,456,signup"
kafka_client.send_json_message('clicks_conversions', click_conversion)

# Example Avro data for bid requests
bid_request = {"user_id": "abc", "auction_id": "456", "targeting_criteria": {"age": "25"}}
kafka_client.send_avro_message('bid_requests', bid_request, config['schema_path'])
