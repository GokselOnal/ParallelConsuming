from data_processor import Processor
from consumer import Consumer

consumer = Consumer()
processor = Processor()

if __name__ == "__main__":
    consumer.consume(topic="raw_topic", partition=0, processor=processor)