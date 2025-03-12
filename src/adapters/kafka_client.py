import asyncio
import json
from aiokafka import AIOKafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from concurrent.futures import ThreadPoolExecutor
from src.globals.config import KAFKA_BROKER_URL
from src.globals.singleton import SingletonMeta  # Import SingletonMeta
from src.utils.logger import get_logger

class KafkaClient(metaclass=SingletonMeta):  # Apply Singleton
    def __init__(self):
        if not hasattr(self, "initialized"):  # Ensure one-time initialization
            self.producer = None
            self.admin = None
            self.executor = None
            self.loop = None
            self.initialized = False
            self.logger = get_logger(__name__)

    async def initialize(self):
        """
        Initializes Kafka producer and admin client.
        """
        if not self.initialized:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BROKER_URL,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            await self.producer.start()

            # Admin client runs in a separate thread
            self.loop = asyncio.get_running_loop()
            self.executor = ThreadPoolExecutor()
            self.admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER_URL)

            self.initialized = True
            self.logger.info("✅ KafkaClient initialized successfully.")

    async def get_producer(self):
        """
        Returns the Kafka producer instance.
        """
        return self.producer

    async def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        """
        Creates a Kafka topic asynchronously.
        """
        def create():
            try:
                topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
                self.admin.create_topics([topic])
                self.logger.info(f"✅ Topic '{topic_name}' created successfully.")
            except Exception as e:
                self.logger.error(f"⚠️ Error creating topic '{topic_name}': {e}")

        await self.loop.run_in_executor(self.executor, create)

    async def delete_topic(self, topic_name):
        """
        Deletes a Kafka topic asynchronously.
        """
        def delete():
            try:
                self.admin.delete_topics([topic_name])
                self.logger.info(f"❌ Topic '{topic_name}' deleted successfully.")
            except Exception as e:
                self.logger.error(f"⚠️ Error deleting topic '{topic_name}': {e}")

        await self.loop.run_in_executor(self.executor, delete)

    async def stop(self):
        """
        Gracefully stops Kafka producer and cleans up resources.
        """
        if self.producer:
            await self.producer.stop()
        if self.admin:
            self.admin.close()
        if self.executor:
            self.executor.shutdown()

        self.logger.info("🛑 KafkaClient stopped successfully.")
