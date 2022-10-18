from consumer import Consumer

consumer = Consumer()

if __name__ == "__main__":
    consumer.consume(topic="raw_topic", partition=1)
