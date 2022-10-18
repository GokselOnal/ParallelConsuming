from kafka.structs import TopicPartition
from kafka import KafkaConsumer
from producer import Producer
from database import Database
import json


class Consumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers= 'localhost:9092',
            auto_offset_reset='earliest'
        )

    def consume(self, topic, partition, processor=None, dt=None):
        database = Database()

        if topic == "raw_topic":
            database.create_table_raw_data()
            tp = TopicPartition(topic, partition)
            self.consumer.assign([tp])

            if processor:
                producer = Producer()
                for msg in self.consumer:
                    processed_data = processor.process(json.loads(msg.value.decode("utf-8")))
                    print(processed_data)
                    producer.produce_once(topic="processed_topic", partition=0, data=processed_data)
            else:
                last = self.consumer.end_offsets([tp])[tp]
                for msg in self.consumer:
                    print((msg.value.decode("utf-8")))
                    year  = json.loads(msg.value.decode("utf-8"))["year"]
                    month = json.loads(msg.value.decode("utf-8"))["month"]
                    day   = json.loads(msg.value.decode("utf-8"))["day"]
                    event = json.loads(msg.value.decode("utf-8"))["event"]
                    database.insert_into_raw_data(year, month, day, event)

        elif topic == "processed_topic":
            self.consumer.assign([TopicPartition(topic, partition)])

            if dt == "con1":
                database.create_table_processed_data("processed_data1")
                for msg in self.consumer:
                    print(msg.value.decode("utf-8"))
                    date = json.loads(msg.value.decode("utf-8"))["date"]
                    event = json.loads(msg.value.decode("utf-8"))["event"]
                    database.insert_into_processed_data("processed_data1", date, event)
            elif dt == "con2":
                database.create_table_processed_data("processed_data2")
                for msg in self.consumer:
                    print(msg.value.decode("utf-8"))
                    date = json.loads(msg.value.decode("utf-8"))["date"]
                    event = json.loads(msg.value.decode("utf-8"))["event"]
                    database.insert_into_processed_data("processed_data2", date, event)

