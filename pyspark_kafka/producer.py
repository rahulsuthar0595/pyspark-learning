import json
import time

import pandas as pd
from kafka import KafkaProducer

KAFKA_TOPIC_NAME = "testtopic"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    df = pd.read_csv('/home/mind/Source/pyspark-python/data/salaries.csv')
    df.head()

    while True:
        data = df.sample(1).to_dict(orient='records')
        if not data:
            break

        print("data => ", data[0])
        producer.send(KAFKA_TOPIC_NAME, value=data[0])
        time.sleep(2)

    # clean up
    producer.flush()
