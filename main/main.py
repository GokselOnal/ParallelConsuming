from create_topic import Topic
from producer import Producer
from consumer import Consumer
from api import APIdata

api = APIdata()
topic = Topic()
producer = Producer()
consumer = Consumer()

if __name__ == "__main__":
    topic.create(topics=["raw_topic", "processed_topic"], partition=3, replication=1)
    raw_data = api.get_data()
    producer.produce(topic="raw_topic", partition=1, data=raw_data)
    producer.produce(topic="raw_topic", partition=0, data=raw_data)

