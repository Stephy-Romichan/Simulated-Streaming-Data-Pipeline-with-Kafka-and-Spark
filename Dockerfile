FROM bitnami/spark:latest

# Install required Python packages for BigQuery, Kafka, and Spark
RUN pip install google-cloud-bigquery pandas pyarrow confluent-kafka pyspark

# Set environment variable for Google Cloud authentication
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/bigquery-service.json"

# Copy service account key into container
COPY bigquery-service.json /app/bigquery-service.json

# Copy the producer and consumer Python scripts into the container
COPY ./wait_for_kafka.py /app/wait_for_kafka.py
COPY ./bigquery_to_kafka_producer.py /app/bigquery_to_kafka_producer.py
COPY ./bigquery_kafka_consumer.py /app/bigquery_kafka_consumer.py

# Set working directory
WORKDIR /app

