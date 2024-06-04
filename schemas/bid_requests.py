from helper.utils import read_avro_schema
from helper.config_reader import load_config

# Load the configuration to get the schema path for the Avro schema
config = load_config()

# Load and parse the Avro schema for bid requests
bid_requests_schema = read_avro_schema(config['schema_path']['bid_requests'])
