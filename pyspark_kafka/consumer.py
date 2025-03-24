import json
import time

from kafka import KafkaConsumer

KAFKA_TOPIC_NAME = "testtopic"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_CONSUMER_GROUP_NAME = "test_consumer_group"

if __name__ == "__main__":
    consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_GROUP_NAME,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for consumer_data in consumer:
        print("Decoded value => ", consumer_data.value)
        time.sleep(2)
