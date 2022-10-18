from kafka import KafkaAdminClient
from kafka.admin import NewTopic


class Topic:
    def __init__(self):
        self.admin_client = KafkaAdminClient(client_id="kafka_")
        self.topic_list = list()

    def create(self, topics, partition, replication):
        for topic in topics:
            new_topic = NewTopic(topic, num_partitions=partition, replication_factor=replication)
            self.topic_list.append(new_topic)
        self.admin_client.create_topics(new_topics= self.topic_list)

    def delete(self, topic):
        self.admin_client.delete_topics([topic])
