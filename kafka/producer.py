from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topics = "real_time_data"

while True:
    message = {
        "user_id": random.randint(1, 1000),
        "timestamp": int(time.time()),
        "event_type": random.choice(["click", "purchase", "like"])
    }
    producer.send(topics, message)
    print(f"Produced: {message}")
    time.sleep(1)
