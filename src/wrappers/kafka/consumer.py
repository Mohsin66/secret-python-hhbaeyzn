import asyncio
import json
from aiokafka import AIOKafkaConsumer
from src.globals.config import KAFKA_BROKER_URL
from src.globals.singleton import SingletonMeta  # Import SingletonMeta
from src.utils.logger import get_logger  # Import logger

class ConsumerService(metaclass=SingletonMeta):  # Apply Singleton
    def __init__(self):
        if not hasattr(self, "initialized"):  # Ensure one-time initialization
            self.consumers = []
            self.initialized = False
            self.logger = get_logger(__name__)

    async def consume(self, topic, group_id, message_handler):
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKER_URL,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        await consumer.start()
        self.consumers.append(consumer)
        self.logger.info(f"🚀 Consumer for topic [{topic}] started...")

        try:
            async for message in consumer:
                self.logger.info(f"✅ Message received from {topic}: {message.value}")
                await message_handler(message)
                await consumer.commit()
        except Exception as e:
            self.logger.error(f"⚠️ Error in consumer {topic}: {e}")
        finally:
            await consumer.stop()

    async def register_consumers(self, topic_handlers):
        if not self.initialized:  # Ensures consumers start only once
            tasks = [
                self.consume(topic, f"{topic}-group", handler)
                for topic, handler in topic_handlers.items()
            ]
            await asyncio.gather(*tasks)
            self.initialized = True

    async def stop_all(self):
        for consumer in self.consumers:
            await consumer.stop()
        self.logger.info("🛑 All consumers stopped successfully.")
