import json
from kafka import KafkaProducer
import config as conf


# Producer class:
class MyKafka:

    # Create Kafka Producer Properties:
    def __init__(self, kafka_broker):
        self.producer = KafkaProducer(
            key_serializer=lambda key: json.dumps(key).encode('utf-8'),  # convert key = symbol into str
            value_serializer=lambda value: json.dumps(value).encode('utf-8'),  # convert value = json_data into str
            bootstrap_servers=conf.kafka_broker)

    # New method which sends data to our topic:
    def send_data(self, key, partition_num, json_data):
        with open(f"{key}.txt", "w") as text_file_1:
            text_file_1.write(str(json_data))

        self.producer.send(conf.topic, key=key, value=json_data, partition=partition_num)
        self.producer.flush()
        # record_metadata = future.get(timeout=10)
        # print(conf.symbol, "TOPIC :== ", record_metadata.topic)
        # print(conf.symbol, "PARTITION :== ", record_metadata.partition)
        # print(conf.symbol, "OFFSET:==", record_metadata.offset)



