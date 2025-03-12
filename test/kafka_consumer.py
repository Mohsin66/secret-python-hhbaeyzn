import asyncio
import json
from aiokafka import AIOKafkaConsumer

KAFKA_BROKER_URL = "localhost:9092"  # Change if needed
TOPIC_NAME = "filtered-gateway-notification"

async def consume():
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER_URL,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    await consumer.start()
    try:
        print("👂 Waiting for messages...")
        async for message in consumer:
            print(f"✅ Received: {message.value}")
    finally:
        await consumer.stop()
        print("✅ Consumer stopped.")

if __name__ == "__main__":
    asyncio.run(consume())
