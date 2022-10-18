from kafka import KafkaProducer
from json import dumps

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                      value_serializer=lambda x: dumps(x).encode('utf-8'))

    def produce(self, topic, partition, data):
        for d in data: self.producer.send(topic, value=d, partition=partition)

    def produce_once(self, topic, partition, data):
        self.producer.send(topic, value=data, partition=partition)