"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


# defining broker and schema URLs to start the broker
broker_url = "PLAINTEXT://localhost:9092"
schema_url = "http://localhost:8081"
class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    
    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # Adding broker URL and schema registry URL to the broker properties
        self.broker_properties = {
            "bootstrap.servers": broker_url,
            "schema.registry.url": schema_url
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configuring a producer using the broker and schema properties
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema = key_schema,
            default_value_schema = value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        
        # creating an admin client 
        admin_client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})

        # defining the new topic with name, partitions, and replicas
        new_topic = NewTopic(
            topic=self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas
        )

        # obtaining a list of topics
        all_topics = admin_client.list_topics(self.topic_name)
        
        # checking if topic already exists
        if self.topic_name not in all_topics.topics:
            admin_client.create_topics([new_topic])

            # creating topic if not exists
            logger.info(f"Created topics: {self.topic_name}")
        else:
            logger.info(f"Topic '{self.topic_name}' already exists")

        admin_client.close()

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
