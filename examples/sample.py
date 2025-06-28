from confluent_kafka import Producer
import json

producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

data = {"a": 1, "b": 2}

producer.produce("temp_topic", json.dumps(data).encode('utf-8'))
producer.flush()