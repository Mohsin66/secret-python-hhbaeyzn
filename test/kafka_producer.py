import asyncio
import json
from aiokafka import AIOKafkaProducer

KAFKA_BROKER_URL = "localhost:9092"  # Change if needed
TOPIC_NAME = "fleet_data"

async def send_message():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    await producer.start()
    try:
        message = {"msg": "Hello from Python Producer!"}
        print(f"🚀 Sending: {message}")
        await producer.send_and_wait(TOPIC_NAME, value=message)
    finally:
        await producer.stop()
        print("✅ Producer stopped.")

if __name__ == "__main__":
    asyncio.run(send_message())
