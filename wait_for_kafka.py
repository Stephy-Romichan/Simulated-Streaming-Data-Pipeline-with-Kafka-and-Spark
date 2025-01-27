# wait_for_kafka.py
import socket
import time

# Loop to wait until Kafka is ready
while True:
    try:
        with socket.create_connection(("kafka", 9092), timeout=2):
            break
    except (socket.error, OSError):
        print("Waiting for Kafka to be ready...")
        time.sleep(2)
