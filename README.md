# AdvertiseX Data Pipeline

## Overview

This project implements a data engineering pipeline for AdvertiseX, a digital advertising technology company. The pipeline handles ingestion, processing, and storage of ad impressions, clicks, conversions, and bid requests data. It also includes error handling and monitoring.

## Folder Structure

```
advertisex-data-pipeline/
│
├── advertisex/
│   └── ...
├── airflow/
│   ├──dags/
│       └── advertisex_data_pipeline.py
│   └── tasks/
│       └── data_workflow.py
├── config/
│   └── constants.yaml
├── helper/
│   ├── config_reader.py
│   └── utils.py
├── input/
│   ├── ad_impressions.json
│   ├── clicks_conversions.csv
│   └── bid_requests.json
├── output/
│   ├── ad_impressions
│   ├── bid_requests
│   ├── clicks_conversions
│   └── attribution
├── output/
│   ├── ad_impressions.py
│   ├── attribution.py
│   ├── bid_request.avsc
│   ├── bid_requests.py
│   └── clicks_conversions.py
├── kafka/
│   └── kafka_client.py
├── spark/
│   ├── spark-3.4.3-bin-hadoop3
│   ├── spark_processor.py
│   └── data_quality.py
├── tests/
│   ├── test_kafka_client.py
│   ├── test_spark_processor.py
│   ├── test_data_processing.py
│   ├── test_data_quality.py
│   └── test_data_writing.py
├── main.py
├── README.md
├── requirements.txt
└── docker-compose.yml

```

## Components

### KafkaClient

- Handles Kafka topic creation and message sending for JSON, CSV, and Avro formats.

### SparkProcessor

- Manages data ingestion from Kafka, processing of JSON, CSV, and Avro data, filtering, deduplication, and writing processed data to Hudi.

### AirflowTasks

- Defines Airflow tasks for monitoring data quality and orchestrating the pipeline.

## Prerequisites and Setup

1. Create a Virtual Environment
    ```bash
    cd /path/to/advertisex-data-pipeline
    python -m venv advertisex
    source advertisex/bin/activate
    pip install -r requirements.txt
    ```

2. Start Docker Services
    ```bash
    docker-compose up -d
    ```

3. Run the Main Application
    ```bash
    python main.py
    ```

3. Ensure Docker is installed and start Kafka and Zookeeper using Docker Compose:
    ```bash
    docker-compose up -d
    ```

4. Verify Kafka Topics
    ```bash
    # List all topics
    docker ps
    docker exec -it <kafka-container-id> kafka-topics --bootstrap-server localhost:9092 --list

    # Verify ad_impressions topic
    docker exec -it <kafka-container-id> kafka-console-consumer --bootstrap-server localhost:9092 --topic ad_impressions --from-beginning

    # Verify clicks_conversions topic
    docker exec -it <kafka-container-id> kafka-console-consumer --bootstrap-server localhost:9092 --topic clicks_conversions --from-beginning

    # Verify bid_requests topic
    docker exec -it <kafka-container-id> kafka-console-consumer --bootstrap-server localhost:9092 --topic bid_requests --from-beginning
    ```

5. Setup Airflow
    ```bash
    # Set the AIRFLOW_HOME environment variable
    export AIRFLOW_HOME=~/path/to/advertisex-data-pipeline/airflow
    echo "export AIRFLOW_HOME=~/path/to/advertisex-data-pipeline/airflow" >> ~/.zshrc # or ~/.bashrc

    # Initialize Airflow DB
    airflow db init

    # Create Airflow User
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com

    # Start Airflow Scheduler and Webserver
    airflow scheduler
    airflow webserver --port 8080  
    ```

6. Install Java and Spark
    ```bash
    # Install OpenJDK 11
    brew install openjdk@11
    sudo ln -sfn /usr/local/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk

    # Set JAVA_HOME environment variable
    export JAVA_HOME=/usr/local/opt/openjdk@11
    export PATH=$JAVA_HOME/bin:$PATH
    source ~/.zshrc  # or source ~/.bashrc
    java -version

    # Download and Install Spark
    wget https://downloads.apache.org/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz
    tar xvf spark-3.4.3-bin-hadoop3.tgz
    mv spark-3.4.3-bin-hadoop3 /Users/SGL/Scrape/misc/advertisex-data-pipeline/spark

    # Set SPARK_HOME environment variable
    nano /path/to/advertisex/bin/activate
    export SPARK_HOME=/path/to/advertisex/spark
    export PATH=$SPARK_HOME/bin:$PATH
    ```

7. Download required JAR files
    
    Download the following JAR files and place them in the appropriate directory:

    - iceberg-spark-runtime-3.4_2.12-1.5.2.jar
    - spark-streaming-kafka-0-10_2.12-3.4.3.jar
    - spark-avro_2.12-3.4.3.jar
    - commons-pool2-2.12.0.jar
    - kafka-clients-3.7.0.jar
    - spark-sql-kafka-0-10_2.12-3.4.3.jar

    These JAR files are required for the Spark and Iceberg integration.



## Running the Pipeline

1. Set up and run Airflow for the data pipeline and monitoring:
    ```sh
    # List and Trigger Airflow DAGs
    airflow dags list
    airflow dags trigger advertisex_data_pipeline
    ```

## Data Flow

### 1. Data Ingestion
Data is ingested from three Kafka topics: `ad_impressions`, `clicks_conversions`, and `bid_requests`.

### 2. Data Processing
The data is processed using Spark:

- **Ad Impressions:** The raw ad impressions data is filtered for valid records and deduplicated based on `user_id` and `timestamp`.
- **Clicks Conversions:** The raw clicks conversions data is filtered for valid records and deduplicated based on `user_id` and `timestamp`.
- **Bid Requests:** The raw bid requests data is filtered for valid records and deduplicated based on `user_id` and `timestamp`.

### 3. Data Attribution
An attribution schema is created to join ad impressions, clicks conversions, and bid requests data based on `user_id` and timestamp conditions.

### 4. Data Storage
The processed data and the attribution data are written to Iceberg tables:

- `ad_impressions`
- `clicks_conversions`
- `bid_requests`
- `attribution`

Invalid records are written to corresponding dead-letter queues in Iceberg tables.

### 5. Data Quality Monitoring
Data quality is monitored using Prometheus metrics.

### 6. Reading Data
Data can be read from the Iceberg tables for further analysis and reporting.


## Assumptions
- Kafka is running locally on `localhost:9092`
- Spark and Iceberg are properly configured in your environment.
- `bid_request.avsc` contains the Avro schema for bid requests.
- The dead-letter topics (`dead_letter_ad_impressions` and `dead_letter_clicks_conversions`) are used for storing invalid records for manual review.

## Notes
- The pipeline is designed to handle high data volumes and includes error handling to ensure robustness.
- The modular structure makes it easy to extend and maintain.
- Environment-specific configurations and secrets are managed securely.