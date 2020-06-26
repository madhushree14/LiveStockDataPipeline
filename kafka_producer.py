from kafka import KafkaProducer
import json
import config as conf

class MyKafka:

    def __init__(self, kafka_brokers):
        self.producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode('utf-8'), # telling it to use json parser as our input data is in json format
            bootstrap_servers= conf.kafka_broker
        )

    def send_data(self, json_data):  # new method which sends data to our topic

        self.producer.send(conf.topic_1, json_data)
