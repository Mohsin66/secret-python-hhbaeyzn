import asyncio
from adapters.kafka_client import KafkaClient
from src.globals.singleton import SingletonMeta
from src.utils.logger import get_logger  # Import logger

class Producer(metaclass=SingletonMeta):    # Apply Singleton pattern

    def __init__(self):
        if not hasattr(self, "initialized"):  # Prevent reinitialization
            self.kafka_client = KafkaClient()
            self.producer = None
            self.initialized = False
            self.logger = get_logger(__name__)

    async def initialize(self):
        if not self.initialized:  # Initialize only once
            await self.kafka_client.initialize()
            self.producer = await self.kafka_client.get_producer()
            self.initialized = True  # Mark as initialized

    async def send_message(self, topic, key, value):
        try:
            await self.producer.send_and_wait(
                topic,
                key=key.encode() if key else None,
                value=value
            )
            self.logger.info(f"✅ Produced message: {value} to topic: {topic}")
        except Exception as e:
            self.logger.info(f"⚠️ Error producing message: {e}")
