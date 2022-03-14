from kafka import KafkaProducer
from dataset import streaming_dataset
import json
import time


def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'],
                         value_serializer=json_serializer)


if __name__ == "__main__":
    while True:
        streaming = streaming_dataset()
        print(streaming)
        producer.send('stream', streaming)
        time.sleep(4)
