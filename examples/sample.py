"""
An example message for kafka
"""
import json

from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

data = {"a": 1, "b": 2}

producer.produce("temp_topic", json.dumps(data).encode('utf-8'))
producer.flush()
