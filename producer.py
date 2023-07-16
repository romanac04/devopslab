import os
import time
import random
import json

import requests
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "latest_events")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.3.1")

REQUEST_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=KAFKA_API_VERSION,
)

response = requests.get(REQUEST_URL, stream=True)

for line in response.iter_lines():
    if line:
        producer.send(
            KAFKA_TOPIC,
            line
        )

producer.flush()