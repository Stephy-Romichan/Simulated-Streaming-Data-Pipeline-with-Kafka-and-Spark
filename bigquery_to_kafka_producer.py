from datetime import datetime
from google.cloud import bigquery
from confluent_kafka import Producer
import json
import os

# Ensure Google credentials are set inside the container
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not credentials_path:
    raise Exception("Google credentials not set in environment variables")

# Initialize BigQuery client
client = bigquery.Client()

# BigQuery SQL query
query = """
SELECT * FROM `tidy-etching-447916-t0.nyc_taxi_data.nyc_taxi_data`
"""
query_job = client.query(query)

# Kafka producer configuration
KAFKA_BROKER = 'kafka:9092'
topic = 'nyc-taxi'

producer = Producer({'bootstrap.servers': KAFKA_BROKER})


# Function to convert row data to JSON-serializable format
def serialize_row(row):
    row_dict = dict(row)
    for key, value in row_dict.items():
        if isinstance(value, datetime):  # Convert datetime to string
            row_dict[key] = value.isoformat()
    return json.dumps(row_dict)

# Publish query results to Kafka
for row in query_job:
    message = serialize_row(row)
    producer.produce(topic, key=str(row[0]), value=message)

producer.flush()
print("Data pushed to Kafka successfully!")


