from consumer import Consumer

consumer = Consumer()

if __name__ == "__main__":
    consumer.consume(topic="processed_topic", partition=0, dt="con2")